import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { isUndefined, omitBy } from 'lodash-es';
import { DateTime, Duration } from 'luxon';
import type { AssetFile } from 'src/database.js';
import type { AuthDto } from 'src/dtos/auth.dto.js';
import type { JobItem, JobOf } from 'src/types.js';
import { OnJob } from 'src/decorators.js';
import { AssetResponseDto, SanitizedAssetResponseDto, mapAsset } from 'src/dtos/asset-response.dto.js';
import {
  AssetBulkDeleteDto,
  AssetBulkUpdateDto,
  AssetCopyDto,
  AssetJobName,
  AssetJobsDto,
  AssetMetadataBulkDeleteDto,
  AssetMetadataBulkResponseDto,
  AssetMetadataBulkUpsertDto,
  AssetMetadataResponseDto,
  AssetMetadataUpsertDto,
  AssetStatsDto,
  UpdateAssetDto,
  mapStats,
} from 'src/dtos/asset.dto.js';
import {
  AssetEditAction,
  type AssetEditActionItem,
  AssetEditsCreateDto,
  AssetEditsResponseDto,
} from 'src/dtos/editing.dto.js';
import { runPromise } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetStatus,
  AssetType,
  AssetVisibility,
  JobName,
  JobStatus,
  Permission,
  QueueName,
} from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { requireElevatedPermission } from 'src/utils/access.js';
import {
  getAssetFiles,
  getDimensions,
  isPanorama,
  onAfterUnlinkFx,
  onBeforeLinkFx,
  onBeforeUnlinkFx,
} from 'src/utils/asset.util.js';
import { updateLockedColumns } from 'src/utils/database.js';
import { extractTimeZone } from 'src/utils/date.js';
import { batched } from 'src/utils/misc.js';

/** Run a Promise-based collaborator (`this.get`, iterator plumbing) as an Effect; the error stays the original thrown value. */
const attempt = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => cause });

@Injectable()
export class AssetService extends BaseService {
  async getStatistics(auth: AuthDto, dto: AssetStatsDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (dto.visibility === AssetVisibility.Locked) {
          requireElevatedPermission(auth);
        }

        const stats = yield* this.fx.asset.getStatistics(auth.user.id, dto);
        return mapStats(stats);
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<AssetResponseDto | SanitizedAssetResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetRead, ids: [id] });

        const asset = yield* this.fx.asset.getById(id, {
          exifInfo: true,
          owner: true,
          stack: { assets: true },
          edits: true,
          tags: true,
        });

        if (!asset) {
          return yield* Effect.fail(new BadRequestException('Asset not found'));
        }

        return mapAsset(asset, { withStack: true, auth });
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: UpdateAssetDto): Promise<AssetResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpdate, ids: [id] });

        const { description, dateTimeOriginal, latitude, longitude, rating, ...rest } = dto;
        const repos = { asset: this.fx.asset, event: this.fx.event };

        let previousMotion: { id: string } | null = null;
        if (rest.livePhotoVideoId) {
          const { livePhotoVideoId } = rest;
          yield* onBeforeLinkFx(repos, { userId: auth.user.id, livePhotoVideoId });
        } else if (rest.livePhotoVideoId === null) {
          const asset = yield* this.findOrFail(id);
          if (asset.livePhotoVideoId) {
            const { livePhotoVideoId } = asset;
            previousMotion = yield* onBeforeUnlinkFx(repos, { livePhotoVideoId });
          }
        }

        const wroteMetadata = yield* this.updateExif({
          id,
          description,
          dateTimeOriginal,
          latitude,
          longitude,
          rating,
        });

        const asset = yield* this.fx.asset.update({ id, ...rest });

        if (previousMotion && asset) {
          const { id: livePhotoVideoId } = previousMotion;
          yield* onAfterUnlinkFx(repos, {
            userId: auth.user.id,
            livePhotoVideoId,
            visibility: asset.visibility,
          });
        }

        if (!asset) {
          return yield* Effect.fail(new BadRequestException('Asset not found'));
        }

        if (!wroteMetadata) {
          yield* this.fx.websocket.clientSend('on_asset_update', auth.user.id, mapAsset(asset, { auth }));
        }

        return yield* attempt(() => this.get(auth, id) as Promise<AssetResponseDto>);
      }),
    );
  }

  async updateAll(auth: AuthDto, dto: AssetBulkUpdateDto): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const {
          ids,
          isFavorite,
          visibility,
          dateTimeOriginal,
          latitude,
          longitude,
          rating,
          description,
          dateTimeRelative,
          timeZone,
        } = dto;
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpdate, ids });

        const assetDto = omitBy({ isFavorite, visibility }, isUndefined);
        const exifDto = omitBy(
          {
            latitude,
            longitude,
            rating,
            description,
            dateTimeOriginal,
          },
          isUndefined,
        );

        let shouldWriteSidecar = false;
        if (Object.keys(exifDto).length > 0) {
          yield* this.fx.asset.updateAllExif(ids, exifDto);
          shouldWriteSidecar = true;
        }

        const extractedTimeZone = extractTimeZone(dateTimeOriginal);

        if (
          (dateTimeRelative !== undefined && dateTimeRelative !== 0) ||
          timeZone !== undefined ||
          extractedTimeZone?.type === 'fixed'
        ) {
          yield* this.fx.asset.updateDateTimeOriginal(ids, dateTimeRelative, timeZone ?? extractedTimeZone?.name);
          shouldWriteSidecar = true;
        }

        if (Object.keys(assetDto).length > 0) {
          yield* this.fx.asset.updateAll(ids, assetDto);
        }

        if (visibility === AssetVisibility.Locked) {
          yield* this.fx.album.removeAssetsFromAll(ids);
        }

        if (shouldWriteSidecar) {
          yield* this.fx.job.queueAll(ids.map((id) => ({ name: JobName.SidecarWrite, data: { id } })));
        } else {
          const assets = yield* this.fx.asset.getByIds(ids);
          for (const asset of assets) {
            yield* this.fx.websocket.clientSend('on_asset_update', auth.user.id, mapAsset(asset, { auth }));
          }
        }
      }),
    );
  }

  async copy(
    auth: AuthDto,
    {
      sourceId,
      targetId,
      albums = true,
      sidecar = true,
      sharedLinks = true,
      stack = true,
      favorite = true,
    }: AssetCopyDto,
  ) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetCopy, ids: [sourceId, targetId] });
        const sourceAsset = yield* this.fx.asset.getForCopy(sourceId);
        const targetAsset = yield* this.fx.asset.getForCopy(targetId);

        if (!sourceAsset || !targetAsset) {
          return yield* Effect.fail(new BadRequestException('Both assets must exist'));
        }

        if (sourceId === targetId) {
          return yield* Effect.fail(new BadRequestException('Source and target id must be distinct'));
        }

        if (albums) {
          yield* this.fx.album.copyAlbums({ sourceAssetId: sourceId, targetAssetId: targetId });
        }

        if (sharedLinks) {
          yield* this.fx.sharedLinkAsset.copySharedLinks({ sourceAssetId: sourceId, targetAssetId: targetId });
        }

        if (stack) {
          yield* this.copyStack({ sourceAsset, targetAsset });
        }

        if (favorite) {
          yield* this.fx.asset.update({ id: targetId, isFavorite: sourceAsset.isFavorite });
        }

        if (sidecar) {
          yield* this.copySidecar({ sourceAsset, targetAsset });
        }
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private copyStack = Effect.fn('AssetService.copyStack')(function* (
    this: AssetService,
    {
      sourceAsset,
      targetAsset,
    }: {
      sourceAsset: { id: string; stackId: string | null };
      targetAsset: { id: string; stackId: string | null };
    },
  ) {
    if (!sourceAsset.stackId) {
      return;
    }

    if (targetAsset.stackId) {
      yield* this.fx.stack.merge({ sourceId: sourceAsset.stackId, targetId: targetAsset.stackId });
      yield* this.fx.stack.delete(sourceAsset.stackId);
    } else {
      yield* this.fx.asset.update({ id: targetAsset.id, stackId: sourceAsset.stackId });
    }
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private copySidecar = Effect.fn('AssetService.copySidecar')(function* (
    this: AssetService,
    {
      sourceAsset,
      targetAsset,
    }: {
      sourceAsset: { files: AssetFile[] };
      targetAsset: { id: string; files: AssetFile[]; originalPath: string };
    },
  ) {
    const { sidecarFile: sourceFile } = getAssetFiles(sourceAsset.files);
    if (!sourceFile?.path) {
      return;
    }

    const { sidecarFile: targetFile } = getAssetFiles(targetAsset.files ?? []);
    if (targetFile?.path) {
      yield* this.fx.storage.unlink(targetFile.path);
    }

    yield* this.fx.storage.copyFile(sourceFile.path, `${targetAsset.originalPath}.xmp`);
    yield* this.fx.asset.upsertFile({
      assetId: targetAsset.id,
      path: `${targetAsset.originalPath}.xmp`,
      type: AssetFileType.Sidecar,
    });
    yield* this.fx.job.queue({ name: JobName.AssetExtractMetadata, data: { id: targetAsset.id } });
  });

  @OnJob({ name: JobName.AssetDeleteCheck, queue: QueueName.BackgroundTask })
  async handleAssetDeletionCheck(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        const trashedDays = config.trash.enabled ? config.trash.days : 0;
        const trashedBefore = DateTime.now()
          .minus(Duration.fromObject({ days: trashedDays }))
          .toJSDate();

        let count = 0;
        const batches = batched(yield* this.fx.assetJob.streamForDeletedJob(trashedBefore));
        // mirrors `for await`: the iterator is closed when the loop exits early or fails
        yield* Effect.ensuring(
          Effect.gen({ self: this }, function* () {
            while (true) {
              const next = yield* attempt(() => batches.next());
              if (next.done) {
                break;
              }

              const assets = next.value;
              yield* this.fx.job.queueAll(
                assets.map(({ id }) => ({ name: JobName.AssetDelete, data: { id, deleteOnDisk: true } })),
              );
              count += assets.length;
            }
          }),
          Effect.promise(() => batches.return(undefined).catch(() => {})),
        );
        if (count > 0) {
          this.logger.log(`Automatically queued ${count} expired trash asset(s) for deletion`);
        }

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.AssetDelete, queue: QueueName.BackgroundTask })
  async handleAssetDeletion(job: JobOf<JobName.AssetDelete>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { id, deleteOnDisk: isDeleteOnDiskRequest, deleteOffline = false } = job;

        const asset = yield* this.fx.assetJob.getForAssetDeletion(id);

        if (!asset) {
          return JobStatus.Failed;
        }

        // an offline asset is trashed because its file is gone or excluded from the library, so its file is only
        // deleted when the user asked for that (an excluded file is still on disk)
        const deleteOnDisk = isDeleteOnDiskRequest && (!asset.isOffline || deleteOffline);

        if (asset.stack) {
          // asset.stack.assets only includes timeline visible assets and excludes the primary asset
          const remainingStackAssetIds = asset.stack.assets.map((a) => a.id).filter((assetId) => assetId !== id);

          // the primary survives unless it is the asset being deleted
          let remainingCount = remainingStackAssetIds.length;
          if (asset.stack.primaryAssetId !== id) {
            remainingCount++;
          }

          if (remainingCount < 2) {
            // 0 or 1 asset would remain: dissolve the stack so it does not linger as a single-asset stack
            yield* this.fx.stack.delete(asset.stack.id);
          } else if (asset.stack.primaryAssetId === id) {
            // the primary is being deleted but others remain: promote a new primary
            yield* this.fx.stack.update(asset.stack.id, {
              id: asset.stack.id,
              primaryAssetId: remainingStackAssetIds[0],
            });
          }
        }

        yield* this.fx.asset.remove(asset);
        if (!asset.libraryId) {
          yield* this.fx.user.updateUsage(asset.ownerId, -(asset.exifInfo?.fileSizeInByte || 0));
        }

        yield* this.fx.event.emit('AssetDelete', { assetId: id, userId: asset.ownerId });

        // delete the motion if it is not used by another asset
        if (asset.livePhotoVideoId) {
          const count = yield* this.fx.asset.getLivePhotoCount(asset.livePhotoVideoId);
          if (count === 0) {
            yield* this.fx.job.queue({
              name: JobName.AssetDelete,
              data: { id: asset.livePhotoVideoId, deleteOnDisk, deleteOffline: deleteOffline || undefined },
            });
          }
        }

        const assetFiles = getAssetFiles(asset.files ?? []);
        const files = [
          assetFiles.thumbnailFile?.path,
          assetFiles.previewFile?.path,
          assetFiles.fullsizeFile?.path,
          assetFiles.editedFullsizeFile?.path,
          assetFiles.editedPreviewFile?.path,
          assetFiles.editedThumbnailFile?.path,
          assetFiles.encodedVideoFile?.path,
        ];

        if (deleteOnDisk) {
          files.push(assetFiles.sidecarFile?.path, asset.originalPath);
        }

        yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: files.filter(Boolean) } });

        return JobStatus.Success;
      }),
    );
  }

  async deleteAll(auth: AuthDto, dto: AssetBulkDeleteDto): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { ids, force } = dto;

        yield* this.requireAccessFx({ auth, permission: Permission.AssetDelete, ids });
        yield* this.fx.asset.updateAll(ids, {
          deletedAt: new Date(),
          status: force ? AssetStatus.Deleted : AssetStatus.Trashed,
        });
        yield* this.fx.event.emit(force ? 'AssetDeleteAll' : 'AssetTrashAll', {
          assetIds: ids,
          userId: auth.user.id,
        });
      }),
    );
  }

  async getMetadata(auth: AuthDto, id: string): Promise<AssetMetadataResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetRead, ids: [id] });
        return yield* this.fx.asset.getMetadata(id);
      }),
    );
  }

  async upsertBulkMetadata(auth: AuthDto, dto: AssetMetadataBulkUpsertDto): Promise<AssetMetadataBulkResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({
          auth,
          permission: Permission.AssetUpdate,
          ids: dto.items.map((item) => item.assetId),
        });

        const uniqueKeys = new Set<string>();
        for (const item of dto.items) {
          const key = `(${item.assetId}, ${item.key})`;
          if (uniqueKeys.has(key)) {
            return yield* Effect.fail(new BadRequestException(`Duplicate items are not allowed: "${key}"`));
          }

          uniqueKeys.add(key);
        }

        return yield* this.fx.asset.upsertBulkMetadata(dto.items);
      }),
    );
  }

  async upsertMetadata(auth: AuthDto, id: string, dto: AssetMetadataUpsertDto): Promise<AssetMetadataResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpdate, ids: [id] });

        const uniqueKeys = new Set<string>();
        for (const { key } of dto.items) {
          if (uniqueKeys.has(key)) {
            return yield* Effect.fail(new BadRequestException(`Duplicate items are not allowed: "${key}"`));
          }

          uniqueKeys.add(key);
        }

        return yield* this.fx.asset.upsertMetadata(id, dto.items);
      }),
    );
  }

  async getMetadataByKey(auth: AuthDto, id: string, key: string): Promise<AssetMetadataResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetRead, ids: [id] });

        const item = yield* this.fx.asset.getMetadataByKey(id, key);
        if (!item) {
          return yield* Effect.fail(
            new BadRequestException(`Metadata with key "${key}" not found for asset with id "${id}"`),
          );
        }
        return item;
      }),
    );
  }

  async deleteMetadataByKey(auth: AuthDto, id: string, key: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpdate, ids: [id] });
        return yield* this.fx.asset.deleteMetadataByKey(id, key);
      }),
    );
  }

  async deleteBulkMetadata(auth: AuthDto, dto: AssetMetadataBulkDeleteDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({
          auth,
          permission: Permission.AssetUpdate,
          ids: dto.items.map((item) => item.assetId),
        });
        yield* this.fx.asset.deleteBulkMetadata(dto.items);
      }),
    );
  }

  async run(auth: AuthDto, dto: AssetJobsDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpdate, ids: dto.assetIds });

        const jobs: JobItem[] = [];

        for (const id of dto.assetIds) {
          switch (dto.name) {
            case AssetJobName.REFRESH_METADATA: {
              jobs.push({ name: JobName.AssetExtractMetadata, data: { id } });
              break;
            }

            case AssetJobName.REGENERATE_THUMBNAIL: {
              jobs.push({ name: JobName.AssetGenerateThumbnails, data: { id } });
              break;
            }

            case AssetJobName.TRANSCODE_VIDEO: {
              jobs.push({ name: JobName.AssetEncodeVideo, data: { id } });
              break;
            }
          }
        }

        yield* this.fx.job.queueAll(jobs);
      }),
    );
  }

  private findOrFail = Effect.fn('AssetService.findOrFail')(function* (this: AssetService, id: string) {
    const value = yield* this.fx.asset.getById(id);
    if (!value) {
      return yield* Effect.fail(new BadRequestException('Asset not found'));
    }

    return value;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private updateExif = Effect.fn('AssetService.updateExif')(function* (
    this: AssetService,
    dto: {
      id: string;
      description?: string;
      dateTimeOriginal?: string;
      latitude?: number;
      longitude?: number;
      rating?: number | null;
    },
  ) {
    const { id, description, dateTimeOriginal, latitude, longitude, rating } = dto;
    const writes = omitBy(
      {
        description,
        dateTimeOriginal,
        timeZone: extractTimeZone(dateTimeOriginal)?.name,
        latitude,
        longitude,
        rating,
      },
      isUndefined,
    );

    if (Object.keys(writes).length === 0) {
      return false;
    }

    yield* this.fx.asset.upsertExif({
      exif: updateLockedColumns({
        assetId: id,
        ...writes,
      }),
      lockedPropertiesBehavior: 'append',
    });
    yield* this.fx.job.queue({ name: JobName.SidecarWrite, data: { id } });
    return true;
  });

  async getAssetEdits(auth: AuthDto, id: string): Promise<AssetEditsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetRead, ids: [id] });
        const edits = yield* this.fx.assetEdit.getAll(id);

        return {
          assetId: id,
          edits,
        };
      }),
    );
  }

  async editAsset(auth: AuthDto, id: string, dto: AssetEditsCreateDto): Promise<AssetEditsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetEditCreate, ids: [id] });

        const asset = yield* this.fx.asset.getForEdit(id);
        if (!asset) {
          return yield* Effect.fail(new BadRequestException('Asset not found'));
        }

        if (asset.type !== AssetType.Image) {
          return yield* Effect.fail(new BadRequestException('Only images can be edited'));
        }

        if (asset.livePhotoVideoId) {
          return yield* Effect.fail(new BadRequestException('Editing live photos is not supported'));
        }

        if (isPanorama(asset)) {
          return yield* Effect.fail(new BadRequestException('Editing panorama images is not supported'));
        }

        if (asset.originalPath?.toLowerCase().endsWith('.gif')) {
          return yield* Effect.fail(new BadRequestException('Editing GIF images is not supported'));
        }

        if (asset.originalPath?.toLowerCase().endsWith('.svg')) {
          return yield* Effect.fail(new BadRequestException('Editing SVG images is not supported'));
        }

        // check that crop parameters will not go out of bounds
        const { width: assetWidth, height: assetHeight } = getDimensions(asset);

        if (!assetWidth || !assetHeight) {
          return yield* Effect.fail(new BadRequestException('Asset dimensions are not available for editing'));
        }

        const edits = dto.edits as AssetEditActionItem[];
        const crop = edits.find((e) => e.action === AssetEditAction.Crop);
        if (crop) {
          if (edits[0].action !== AssetEditAction.Crop) {
            return yield* Effect.fail(new BadRequestException('Crop action must be the first edit action'));
          }

          // check that crop parameters will not go out of bounds
          const { width: assetWidth, height: assetHeight } = getDimensions(asset);

          if (!assetWidth || !assetHeight) {
            return yield* Effect.fail(new BadRequestException('Asset dimensions are not available for editing'));
          }

          const { x, y, width, height } = crop.parameters;
          if (x + width > assetWidth || y + height > assetHeight) {
            return yield* Effect.fail(new BadRequestException('Crop parameters are out of bounds'));
          }
        }

        const newEdits = yield* this.fx.assetEdit.replaceAll(id, edits);
        yield* this.fx.job.queue({ name: JobName.AssetEditThumbnailGeneration, data: { id } });

        // Return the asset and its applied edits
        return {
          assetId: id,
          edits: newEdits,
        };
      }),
    );
  }

  async removeAssetEdits(auth: AuthDto, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetEditDelete, ids: [id] });

        const asset = yield* this.fx.asset.getById(id);
        if (!asset) {
          return yield* Effect.fail(new BadRequestException('Asset not found'));
        }

        yield* this.fx.assetEdit.replaceAll(id, []);
        yield* this.fx.job.queue({ name: JobName.AssetEditThumbnailGeneration, data: { id } });
      }),
    );
  }
}
