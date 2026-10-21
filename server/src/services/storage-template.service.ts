import { Injectable } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import handlebar from 'handlebars';
import { DateTime } from 'luxon';
import path from 'node:path';
import sanitize from 'sanitize-filename';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { JobOf, StorageAsset } from 'src/types.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { ConfigTemplateStorageOptionDto } from 'src/dtos/config.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetPathType,
  AssetType,
  DatabaseLock,
  JobName,
  JobStatus,
  QueueName,
  StorageFolder,
} from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { getAssetFile } from 'src/utils/asset.util.js';
import { getFilenameExtension, getLivePhotoMotionFilename } from 'src/utils/file.js';

const storageTokens = {
  secondOptions: ['s', 'ss', 'SSS'],
  minuteOptions: ['m', 'mm'],
  dayOptions: ['d', 'dd'],
  weekOptions: ['W', 'WW'],
  hourOptions: ['h', 'hh', 'H', 'HH'],
  yearOptions: ['y', 'yy'],
  monthOptions: ['M', 'MM', 'MMM', 'MMMM'],
};

const storagePresets = [
  '{{y}}/{{y}}-{{MM}}-{{dd}}/{{filename}}',
  '{{y}}/{{MM}}-{{dd}}/{{filename}}',
  '{{y}}/{{MMMM}}-{{dd}}/{{filename}}',
  '{{y}}/{{MM}}/{{filename}}',
  '{{y}}/{{#if album}}{{album}}{{else}}Other/{{MM}}{{/if}}/{{filename}}',
  '{{#if album}}{{album-startDate-y}}/{{album}}{{else}}{{y}}/Other/{{MM}}{{/if}}/{{filename}}',
  '{{y}}/{{MMM}}/{{filename}}',
  '{{y}}/{{MMMM}}/{{filename}}',
  '{{y}}/{{MM}}/{{dd}}/{{filename}}',
  '{{y}}/{{MMMM}}/{{dd}}/{{filename}}',
  '{{y}}/{{y}}-{{MM}}/{{y}}-{{MM}}-{{dd}}/{{filename}}',
  '{{y}}-{{MM}}-{{dd}}/{{filename}}',
  '{{y}}-{{MMM}}-{{dd}}/{{filename}}',
  '{{y}}-{{MMMM}}-{{dd}}/{{filename}}',
  '{{y}}/{{y}}-{{MM}}/{{filename}}',
  '{{y}}/{{y}}-{{WW}}/{{filename}}',
  '{{y}}/{{y}}-{{MM}}-{{dd}}/{{assetId}}',
  '{{y}}/{{y}}-{{MM}}/{{assetId}}',
  '{{y}}/{{y}}-{{WW}}/{{assetId}}',
  '{{album}}/{{filename}}',
  '{{make}}/{{model}}/{{lensModel}}/{{filename}}',
];

export interface MoveAssetMetadata {
  storageLabel: string | null;
  filename: string;
}

interface RenderMetadata {
  asset: StorageAsset;
  filename: string;
  extension: string;
  albumName: string | null;
  albumStartDate: Date | null;
  albumEndDate: Date | null;
  make: string | null;
  model: string | null;
  lensModel: string | null;
}

/** Run a Promise-based collaborator as an Effect; the error stays the original thrown value. */
const attempt = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => cause });

/** The original thrown value behind a wrapped repository error (`DatabaseError`, `LibraryError`, ...). */
@Injectable()
export class StorageTemplateService extends BaseService {
  private _template: {
    compiled: HandlebarsTemplateDelegate<any>;
    raw: string;
    needsAlbum: boolean;
    needsAlbumMetadata: boolean;
  } | null = null;

  private get template() {
    if (!this._template) {
      throw new Error('Template not initialized');
    }
    return this._template;
  }

  @OnEvent({ name: 'ConfigInit' })
  onConfigInit({ newConfig }: ArgOf<'ConfigInit'>) {
    const template = newConfig.storageTemplate.template;
    if (this._template && template === this.template.raw) {
      return;
    }

    this.logger.debug(`Compiling new storage template: ${template}`);
    this._template = this.compile(template);
  }

  @OnEvent({ name: 'ConfigUpdate', server: true })
  onConfigUpdate({ newConfig }: ArgOf<'ConfigUpdate'>) {
    this.onConfigInit({ newConfig });
  }

  @OnEvent({ name: 'ConfigValidate' })
  onConfigValidate({ newConfig }: ArgOf<'ConfigValidate'>) {
    try {
      const { compiled } = this.compile(newConfig.storageTemplate.template);
      this.render(compiled, {
        asset: {
          fileCreatedAt: new Date(),
          originalPath: '/upload/test/IMG_123.jpg',
          type: AssetType.Image,
          id: 'd587e44b-f8c0-4832-9ba3-43268bbf5d4e',
        } as StorageAsset,
        filename: 'IMG_123',
        extension: 'jpg',
        albumName: 'album',
        albumStartDate: new Date(),
        albumEndDate: new Date(),
        make: 'FUJIFILM',
        model: 'X-T50',
        lensModel: 'XF27mm F2.8 R WR',
      });
    } catch (error) {
      this.logger.warn(`Storage template validation failed: ${JSON.stringify(error)}`);
      throw new Error('Invalid storage template', { cause: error });
    }
  }

  getStorageTemplateOptions(): ConfigTemplateStorageOptionDto {
    return { ...storageTokens, presetOptions: storagePresets };
  }

  @OnEvent({ name: 'AssetMetadataExtracted' })
  async onAssetMetadataExtracted({ source, assetId }: ArgOf<'AssetMetadataExtracted'>) {
    return runPromise(
      this.fx.job.queue({ name: JobName.StorageTemplateMigrationSingle, data: { source, id: assetId } }),
    );
  }

  @OnJob({ name: JobName.StorageTemplateMigrationSingle, queue: QueueName.StorageTemplateMigration })
  async handleMigrationSingle({ id }: JobOf<JobName.StorageTemplateMigrationSingle>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: true });
        const isStorageTemplateEnabled = config.storageTemplate.enabled;
        if (!isStorageTemplateEnabled) {
          return JobStatus.Skipped;
        }

        const asset = yield* this.fx.assetJob.getForStorageTemplateJob(id);
        if (!asset) {
          return JobStatus.Failed;
        }

        const user = yield* this.fx.user.get(asset.ownerId, {});
        const storageLabel = user?.storageLabel || null;
        const filename = asset.originalFileName || asset.id;
        yield* this.moveAssetFx(asset, { storageLabel, filename });

        // move motion part of live photo
        if (asset.livePhotoVideoId) {
          const livePhotoVideo = yield* this.fx.assetJob.getForStorageTemplateJob(asset.livePhotoVideoId, {
            includeHidden: true,
          });
          if (!livePhotoVideo) {
            return JobStatus.Failed;
          }
          const motionFilename = getLivePhotoMotionFilename(filename, livePhotoVideo.originalPath);
          yield* this.moveAssetFx(livePhotoVideo, { storageLabel, filename: motionFilename }, asset);
        }
        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.StorageTemplateMigration, queue: QueueName.StorageTemplateMigration })
  async handleMigration(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log('Starting storage template migration');
        const { storageTemplate } = yield* this.getConfigFx({ withCache: true });
        const { enabled } = storageTemplate;
        if (!enabled) {
          this.logger.log('Storage template migration disabled, skipping');
          return JobStatus.Skipped;
        }

        yield* this.fx.move.cleanMoveHistory();

        const assets = yield* this.fx.assetJob.streamForStorageTemplateJob();
        const users = yield* this.fx.user.getList();

        const iterator = assets[Symbol.asyncIterator]();
        // mirrors `for await`: the iterator is closed when the loop exits early or fails
        yield* Effect.ensuring(
          Effect.gen({ self: this }, function* () {
            while (true) {
              const next = yield* attempt(() => iterator.next());
              if (next.done) {
                break;
              }

              const asset = next.value;
              const user = users.find((user) => user.id === asset.ownerId);
              const storageLabel = user?.storageLabel || null;
              const filename = asset.originalFileName || asset.id;
              yield* this.moveAssetFx(asset, { storageLabel, filename });

              if (!asset.livePhotoVideoId) {
                continue;
              }

              // move motion part of live photo
              const livePhotoVideo = yield* this.fx.assetJob.getForStorageTemplateJob(asset.livePhotoVideoId, {
                includeHidden: true,
              });
              if (!livePhotoVideo) {
                continue;
              }

              const motionFilename = getLivePhotoMotionFilename(filename, livePhotoVideo.originalPath);
              yield* this.moveAssetFx(livePhotoVideo, { storageLabel, filename: motionFilename }, asset);
            }
          }),
          Effect.promise(async () => {
            try {
              await iterator.return?.();
            } catch {
              // closing a finished or failed stream must not mask the original outcome
            }
          }),
        );

        this.logger.debug('Cleaning up empty directories...');
        const libraryFolder = StorageCore.getBaseFolder(StorageFolder.Library);
        yield* this.fx.storage.removeEmptyDirs(libraryFolder);

        this.logger.log('Finished storage template migration');

        return JobStatus.Success;
      }),
    );
  }

  @OnEvent({ name: 'AssetDelete' })
  async handleMoveHistoryCleanup({ assetId }: ArgOf<'AssetDelete'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.debug(`Cleaning up move history for asset ${assetId}`);
        yield* this.fx.move.cleanMoveHistorySingle(assetId);
      }),
    );
  }

  async moveAsset(asset: StorageAsset, metadata: MoveAssetMetadata, stillPhoto?: StorageAsset) {
    return runPromise(this.moveAssetFx(asset, metadata, stillPhoto));
  }

  private moveAssetFx(asset: StorageAsset, metadata: MoveAssetMetadata, stillPhoto?: StorageAsset) {
    return Effect.gen({ self: this }, function* () {
      if (asset.isExternal || StorageCore.isAndroidMotionPath(asset.originalPath)) {
        // External assets are not affected by storage template
        // TODO: shouldn't this only apply to external assets?
        return;
      }

      return yield* this.fx.database.withLock(DatabaseLock.StorageTemplateMigration, () =>
        runPromise(
          Effect.gen({ self: this }, function* () {
            const { id, originalPath, checksum, fileSizeInByte } = asset;
            const oldPath = originalPath;
            const newPath = yield* this.getTemplatePath(asset, metadata, stillPhoto);

            if (!fileSizeInByte) {
              this.logger.error(`Asset ${id} missing exif info, skipping storage template migration`);
              return;
            }

            yield* Effect.gen({ self: this }, function* () {
              yield* attempt(() =>
                this.storageCore.moveFile({
                  entityId: id,
                  pathType: AssetPathType.Original,
                  oldPath,
                  newPath,
                  assetInfo: { sizeInBytes: fileSizeInByte, checksum },
                }),
              );

              const sidecarPath = getAssetFile(asset.files, AssetFileType.Sidecar, { isEdited: false })?.path;
              if (sidecarPath) {
                yield* attempt(() =>
                  this.storageCore.moveFile({
                    entityId: id,
                    pathType: AssetFileType.Sidecar,
                    oldPath: sidecarPath,
                    newPath: `${newPath}.xmp`,
                  }),
                );
              }
            }).pipe(
              Effect.catch((error: any) => {
                this.logger.error(`Problem applying storage template`, error?.stack, { id, oldPath, newPath });
                return Effect.void;
              }),
            );
          }),
        ),
      );
    });
  }

  private getTemplatePath(asset: StorageAsset, metadata: MoveAssetMetadata, stillPhoto?: StorageAsset) {
    const { storageLabel, filename } = metadata;

    return Effect.gen({ self: this }, function* () {
      const filenameWithoutExtension = path.basename(filename, getFilenameExtension(filename));

      const source = asset.originalPath;
      let extension = getFilenameExtension(source).split('.').pop() as string;
      const sanitized = sanitize(path.basename(filenameWithoutExtension, `.${extension}`));
      extension = extension?.toLowerCase();
      const rootPath = StorageCore.getLibraryFolder({ id: asset.ownerId, storageLabel });

      switch (extension) {
        case 'jpeg':
        case 'jpe': {
          extension = 'jpg';
          break;
        }
        case 'tif': {
          extension = 'tiff';
          break;
        }
        case '3gpp': {
          extension = '3gp';
          break;
        }
        case 'mpeg':
        case 'mpe': {
          extension = 'mpg';
          break;
        }
        case 'm2ts':
        case 'm2t': {
          extension = 'mts';
          break;
        }
      }

      let albumName = null;
      let albumStartDate = null;
      let albumEndDate = null;
      const assetForMetadata = stillPhoto || asset;

      if (this.template.needsAlbum) {
        // For motion videos, use the still photo's album information since motion videos
        // don't have album metadata attached directly
        const albums = yield* this.fx.album.getByAssetId(assetForMetadata.ownerId, assetForMetadata.id);
        const album = albums?.[0];
        if (album) {
          albumName = album.albumName || null;

          if (this.template.needsAlbumMetadata) {
            const [metadata] = yield* this.fx.album.getMetadataForIds([album.id]);
            albumStartDate = metadata?.startDate || null;
            albumEndDate = metadata?.endDate || null;
          }
        }
      }

      // For motion videos that are part of live photos, use the still photo's date
      // to ensure both parts end up in the same folder
      const storagePath = this.render(this.template.compiled, {
        asset: assetForMetadata,
        filename: sanitized,
        extension,
        albumName,
        albumStartDate,
        albumEndDate,
        make: assetForMetadata.make,
        model: assetForMetadata.model,
        lensModel: assetForMetadata.lensModel,
      });
      const fullPath = path.normalize(path.join(rootPath, storagePath));
      let destination = `${fullPath}.${extension}`;

      const relativePath = path.relative(rootPath, fullPath);
      if (
        !relativePath ||
        relativePath === '..' ||
        relativePath.startsWith(`..${path.sep}`) ||
        path.isAbsolute(relativePath)
      ) {
        this.logger.warn(`Skipped attempt to access an invalid path: ${fullPath}. Path should be inside ${rootPath}`);
        return source;
      }

      if (source === destination) {
        return source;
      }

      /**
       * In case of migrating duplicate filename to a new path, we need to check if it is already migrated
       * Due to the mechanism of appending +1, +2, +3, etc to the filename
       *
       * Example:
       * Source = upload/abc/def/FullSizeRender+7.heic
       * Expected Destination = upload/abc/def/FullSizeRender.heic
       *
       * The file is already at the correct location, but since there are other FullSizeRender.heic files in the
       * destination, it was renamed to FullSizeRender+7.heic.
       *
       * The lines below will be used to check if the differences between the source and destination is only the
       * +7 suffix, and if so, it will be considered as already migrated.
       */
      if (source.startsWith(fullPath) && source.endsWith(`.${extension}`)) {
        const diff = source.replace(fullPath, '').replace(`.${extension}`, '');
        const hasDuplicationAnnotation = /^\+\d+$/.test(diff);
        if (hasDuplicationAnnotation) {
          return source;
        }
      }

      let duplicateCount = 0;

      while (true) {
        const isExists = yield* this.fx.storage.checkFileExists(destination);
        if (!isExists) {
          break;
        }

        duplicateCount++;
        destination = `${fullPath}+${duplicateCount}.${extension}`;
      }

      return destination;
    }).pipe(
      // sync throws (template, render) are defects, so recover from the whole cause like the original try/catch
      Effect.catchCause((cause) => {
        const error: any = unwrap(Cause.squash(cause));
        this.logger.error(`Unable to get template path for ${filename}: ${error}`);
        return Effect.succeed(asset.originalPath);
      }),
    );
  }

  private compile(template: string) {
    return {
      raw: template,
      compiled: handlebar.compile(template, { knownHelpers: undefined, strict: true }),
      needsAlbum: template.includes('album'),
      needsAlbumMetadata: template.includes('album-startDate') || template.includes('album-endDate'),
    };
  }

  private render(template: HandlebarsTemplateDelegate<any>, options: RenderMetadata) {
    const { filename, extension, asset, albumName, albumStartDate, albumEndDate, make, model, lensModel } = options;
    const substitutions: Record<string, string> = {
      filename,
      ext: extension,
      filetype: asset.type === AssetType.Image ? 'IMG' : 'VID',
      filetypefull: asset.type === AssetType.Image ? 'IMAGE' : 'VIDEO',
      assetId: asset.id,
      assetIdShort: asset.id.slice(-12),
      //just throw into the root if it doesn't belong to an album
      album: (albumName && sanitize(albumName.replaceAll(/\.+/g, ''))) || '',
      make: sanitize(make ?? ''),
      model: sanitize(model ?? ''),
      lensModel: sanitize(lensModel ?? ''),
    };

    const dt = DateTime.fromJSDate(asset.fileCreatedAt);

    for (const token of Object.values(storageTokens).flat()) {
      substitutions[token] = dt.toFormat(token);
      if (!albumName) {
        continue;
      }

      // Album date tokens are rendered in the server time zone to match storage template datetime behavior.
      substitutions['album-startDate-' + token] = albumStartDate
        ? DateTime.fromJSDate(albumStartDate).toFormat(token)
        : '';
      substitutions['album-endDate-' + token] = albumEndDate ? DateTime.fromJSDate(albumEndDate).toFormat(token) : '';
    }

    return template(substitutions).replaceAll(/\/{2,}/gm, '/');
  }
}
