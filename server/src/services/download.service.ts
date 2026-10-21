import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { parse } from 'node:path';
import sanitize from 'sanitize-filename';
import { StorageCore } from 'src/cores/storage.core.js';
import { AuthDto, isEditedOnly } from 'src/dtos/auth.dto.js';
import {
  DownloadArchiveDto,
  DownloadArchiveInfo,
  DownloadInfoDto,
  DownloadResponseDto,
} from 'src/dtos/download.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission } from 'src/enum.js';
import { ImmichReadStream } from 'src/repositories/storage.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { HumanReadableSize } from 'src/utils/bytes.js';
import { getPreferences } from 'src/utils/preferences.js';

@Injectable()
export class DownloadService extends BaseService {
  async getDownloadInfo(auth: AuthDto, dto: DownloadInfoDto): Promise<DownloadResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        let assets;

        if (dto.assetIds) {
          const assetIds = dto.assetIds;
          yield* this.requireAccessFx({ auth, permission: Permission.AssetDownload, ids: assetIds });
          assets = yield* this.fx.download.downloadAssetIds(assetIds);
        } else if (dto.albumId) {
          const albumId = dto.albumId;
          yield* this.requireAccessFx({ auth, permission: Permission.AlbumDownload, ids: [albumId] });
          assets = yield* this.fx.download.downloadAlbumId(albumId);
        } else if (dto.userId) {
          const userId = dto.userId;
          yield* this.requireAccessFx({ auth, permission: Permission.TimelineDownload, ids: [userId] });
          assets = yield* this.fx.download.downloadUserId(userId);
        } else {
          return yield* Effect.fail(new BadRequestException('assetIds, albumId, or userId is required'));
        }

        const targetSize = dto.archiveSize || HumanReadableSize.GiB * 4;
        const metadata = yield* this.fx.user.getMetadata(auth.user.id);
        const preferences = getPreferences(metadata);
        const motionIds = new Set<string>();
        const archives: DownloadArchiveInfo[] = [];
        let archive: DownloadArchiveInfo = { size: 0, assetIds: [] };

        const addToArchive = ({ id, size }: { id: string; size: number | null }) => {
          archive.assetIds.push(id);
          archive.size += Number(size || 0);

          if (archive.size <= targetSize) {
            return;
          }

          archives.push(archive);
          archive = { size: 0, assetIds: [] };
        };

        // the streams are lazy async iterables, they are consumed here without buffering
        const source = assets;
        yield* Effect.tryPromise({
          try: async () => {
            for await (const asset of source) {
              // motion part of live photos
              if (asset.livePhotoVideoId) {
                motionIds.add(asset.livePhotoVideoId);
              }

              addToArchive(asset);
            }
          },
          catch: (cause) => cause,
        });

        if (motionIds.size > 0) {
          const motionAssets = yield* this.fx.download.downloadMotionAssetIds([...motionIds]);
          yield* Effect.tryPromise({
            try: async () => {
              for await (const motionAsset of motionAssets) {
                if (
                  StorageCore.isAndroidMotionPath(motionAsset.originalPath) &&
                  !preferences.download.includeEmbeddedVideos
                ) {
                  continue;
                }

                addToArchive(motionAsset);
              }
            },
            catch: (cause) => cause,
          });
        }

        if (archive.assetIds.length > 0) {
          archives.push(archive);
        }

        let totalSize = 0;
        for (const archive of archives) {
          totalSize += archive.size;
        }

        return { totalSize, archives };
      }),
    );
  }

  async downloadArchive(auth: AuthDto, dto: DownloadArchiveDto): Promise<ImmichReadStream> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetDownload, ids: dto.assetIds });

        const edited = isEditedOnly(auth, dto.edited);
        const zip = yield* this.fx.storage.createZipStream();
        const assets = yield* this.fx.asset.getForOriginals(dto.assetIds, edited);
        const assetMap = new Map(assets.map((asset) => [asset.id, asset]));
        const paths: Record<string, number> = {};

        for (const assetId of dto.assetIds) {
          const asset = assetMap.get(assetId);
          if (!asset) {
            continue;
          }

          const { originalPath, editedPath, originalFileName } = asset;

          let filename = sanitize(originalFileName) || 'unnamed';
          const count = paths[filename] || 0;
          paths[filename] = count + 1;
          if (count !== 0) {
            const parsedFilename = parse(filename);
            filename = `${parsedFilename.name}+${count}${parsedFilename.ext}`;
          }

          let realpath = edited && editedPath ? editedPath : originalPath;

          realpath = yield* this.fx.storage.realpath(realpath).pipe(
            Effect.catch(() => {
              this.logger.warn('Unable to resolve realpath', { originalPath });
              return Effect.succeed(realpath);
            }),
          );

          zip.addFile(realpath, filename);
        }

        void zip.finalize();

        return {
          stream: zip.stream,
          disposition: dto.archiveName && `attachment; filename*=UTF-8''${encodeURIComponent(dto.archiveName)}.zip`,
        };
      }),
    );
  }
}
