import { BadRequestException, Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import sanitize from 'sanitize-filename';
import type { UploadFile, UploadRequest } from 'src/types.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { Asset, AuthSharedLink } from 'src/database.js';
import {
  AssetBulkUploadCheckResponseDto,
  AssetMediaResponseDto,
  AssetMediaStatus,
  AssetRejectReason,
  AssetUploadAction,
} from 'src/dtos/asset-media-response.dto.js';
import {
  AssetBulkUploadCheckDto,
  AssetMediaCreateDto,
  AssetMediaOptionsDto,
  AssetMediaSize,
  UploadFieldName,
} from 'src/dtos/asset-media.dto.js';
import { AssetDownloadOriginalDto } from 'src/dtos/asset.dto.js';
import { AuthDto, isEditedOnly, isMetadataHidden } from 'src/dtos/auth.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetVisibility,
  CacheControl,
  ChecksumAlgorithm,
  JobName,
  Permission,
  StorageFolder,
} from 'src/enum.js';
import { AuthRequest } from 'src/middleware/auth.guard.js';
import { BaseService } from 'src/services/base.service.js';
import { requireUploadAccess } from 'src/utils/access.js';
import { asUploadRequest, onBeforeLinkFx } from 'src/utils/asset.util.js';
import { isAssetChecksumConstraint } from 'src/utils/database.js';
import { ImmichFileResponse, getFileNameWithoutExtension, getFilenameExtension } from 'src/utils/file.js';
import { mimeTypes } from 'src/utils/mime-types.js';
import { fromChecksum } from 'src/utils/request.js';

/** The value originally thrown, for errors that wrap it in `cause` (see `src/effect/errors.ts`). */
export interface AssetMediaRedirectResponse {
  targetSize: AssetMediaSize | 'original';
}

@Injectable()
export class AssetMediaService extends BaseService {
  async getUploadAssetIdByChecksum(auth: AuthDto, checksum?: string): Promise<AssetMediaResponseDto | undefined> {
    if (!checksum) {
      return;
    }

    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpload, ids: [auth.user.id] });

        const assetId = yield* this.fx.asset.getUploadAssetIdByChecksum(auth.user.id, fromChecksum(checksum));
        if (!assetId) {
          return;
        }

        return { id: assetId, status: AssetMediaStatus.DUPLICATE };
      }),
    );
  }

  canUploadFile({ auth, fieldName, file, body }: UploadRequest): true {
    requireUploadAccess(auth);

    const filename = body.filename || file.originalName;

    switch (fieldName) {
      case UploadFieldName.ASSET_DATA: {
        if (mimeTypes.isAsset(filename)) {
          return true;
        }
        break;
      }

      case UploadFieldName.SIDECAR_DATA: {
        if (mimeTypes.isSidecar(filename)) {
          return true;
        }
        break;
      }

      case UploadFieldName.PROFILE_DATA: {
        if (mimeTypes.isProfile(filename)) {
          return true;
        }
        break;
      }
    }

    this.logger.error(`Unsupported file type ${filename}`);
    throw new BadRequestException(`Unsupported file type ${filename}`);
  }

  getUploadFilename({ auth, fieldName, file, body }: UploadRequest): string {
    requireUploadAccess(auth);

    const extension = getFilenameExtension(body.filename || file.originalName);
    const lookup = {
      [UploadFieldName.ASSET_DATA]: extension,
      [UploadFieldName.SIDECAR_DATA]: '.xmp',
      [UploadFieldName.PROFILE_DATA]: extension,
    };

    return sanitize(`${file.uuid}${lookup[fieldName]}`);
  }

  getUploadFolder({ auth, fieldName, file }: UploadRequest): string {
    auth = requireUploadAccess(auth);

    let folder = StorageCore.getNestedFolder(StorageFolder.Upload, auth.user.id, file.uuid);
    if (fieldName === UploadFieldName.PROFILE_DATA) {
      folder = StorageCore.getFolderLocation(StorageFolder.Profile, auth.user.id);
    }

    this.storageRepository.mkdirSync(folder);

    return folder;
  }

  async onUploadError(request: AuthRequest, file: Express.Multer.File) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const uploadFilename = this.getUploadFilename(asUploadRequest(request, file));
        const uploadFolder = this.getUploadFolder(asUploadRequest(request, file));
        const uploadPath = `${uploadFolder}/${uploadFilename}`;

        yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: [uploadPath] } });
      }),
    );
  }

  async uploadAsset(
    auth: AuthDto,
    dto: AssetMediaCreateDto,
    file: UploadFile,
    sidecarFile?: UploadFile,
  ): Promise<AssetMediaResponseDto> {
    let asset: Asset | undefined;
    return runPromise(
      Effect.gen({ self: this }, function* () {
        return yield* Effect.gen({ self: this }, function* () {
          yield* this.requireAccessFx({
            auth,
            permission: Permission.AssetUpload,
            // do not need an id here, but the interface requires it
            ids: [auth.user.id],
          });

          yield* Effect.try({ try: () => this.requireQuota(auth, file.size), catch: (cause) => cause });

          const livePhotoVideoId = dto.livePhotoVideoId;
          if (livePhotoVideoId) {
            yield* onBeforeLinkFx(
              { asset: this.fx.asset, event: this.fx.event },
              { userId: auth.user.id, livePhotoVideoId },
            );
          }

          asset = yield* this.fx.asset.create({
            ownerId: auth.user.id,
            libraryId: null,

            checksum: file.checksum,
            checksumAlgorithm: ChecksumAlgorithm.sha1File,
            originalPath: file.originalPath,

            fileCreatedAt: dto.fileCreatedAt,
            fileModifiedAt: dto.fileModifiedAt,
            localDateTime: dto.fileCreatedAt,

            type: mimeTypes.assetType(file.originalPath),
            isFavorite: dto.isFavorite,
            duration: dto.duration || null,
            visibility: dto.visibility ?? AssetVisibility.Timeline,
            livePhotoVideoId: dto.livePhotoVideoId,
            originalFileName: dto.filename || file.originalName,
          });

          if (dto.metadata?.length) {
            yield* this.fx.asset.upsertMetadata(asset.id, dto.metadata);
          }

          if (sidecarFile) {
            yield* this.fx.asset.upsertFile({
              assetId: asset.id,
              path: sidecarFile.originalPath,
              type: AssetFileType.Sidecar,
            });
            yield* this.fx.storage.utimes(sidecarFile.originalPath, new Date(), new Date(dto.fileModifiedAt));
          }
          yield* this.fx.storage.utimes(file.originalPath, new Date(), new Date(dto.fileModifiedAt));
          yield* this.fx.asset.upsertExif({
            exif: { assetId: asset.id, fileSizeInByte: file.size },
            lockedPropertiesBehavior: 'override',
          });

          yield* this.fx.job.queue({ name: JobName.AssetExtractMetadata, data: { id: asset.id, source: 'upload' } });

          if (auth.sharedLink) {
            yield* this.addToSharedLink(auth.sharedLink, asset.id);
          }

          yield* this.fx.event.emit('AssetCreate', { asset, file });

          return { id: asset.id, status: AssetMediaStatus.CREATED } as AssetMediaResponseDto;
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen({ self: this }, function* () {
              const error: any = unwrap(Cause.squash(cause));

              // clean up files
              yield* this.fx.job.queue({
                name: JobName.FileDelete,
                data: { files: [file.originalPath, sidecarFile?.originalPath] },
              });

              // handle duplicates with a success response
              if (isAssetChecksumConstraint(error)) {
                const duplicateId = yield* this.fx.asset.getUploadAssetIdByChecksum(auth.user.id, file.checksum);
                if (!duplicateId) {
                  this.logger.error(`Error locating duplicate for checksum constraint`);
                  return yield* Effect.fail(new InternalServerErrorException());
                }

                if (auth.sharedLink) {
                  yield* this.addToSharedLink(auth.sharedLink, duplicateId);
                }

                this.logger.debug(`Duplicate asset upload rejected: existing asset ${duplicateId}`);
                return { status: AssetMediaStatus.DUPLICATE, id: duplicateId } as AssetMediaResponseDto;
              }

              // clean up the asset row if one was created
              if (asset) {
                yield* this.fx.asset.remove({ id: asset.id });
              }

              this.logger.error(`Error uploading file ${error}`, error?.stack);
              return yield* Effect.fail(error);
            }),
          ),
        );
      }),
    );
  }

  async downloadOriginal(auth: AuthDto, id: string, dto: AssetDownloadOriginalDto): Promise<ImmichFileResponse> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetDownload, ids: [id] });

        const { originalPath, originalFileName, editedPath } = yield* this.fx.asset.getForOriginal(
          id,
          isEditedOnly(auth, dto.edited),
        );

        const path = editedPath ?? originalPath!;

        return new ImmichFileResponse({
          path,
          fileName: getFileNameWithoutExtension(originalFileName) + getFilenameExtension(path),
          contentType: mimeTypes.lookup(path),
          cacheControl: CacheControl.PrivateWithCache,
        });
      }),
    );
  }

  async viewThumbnail(
    auth: AuthDto,
    id: string,
    dto: AssetMediaOptionsDto,
  ): Promise<ImmichFileResponse | AssetMediaRedirectResponse> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetView, ids: [id] });

        if (dto.size === AssetMediaSize.Original) {
          return yield* Effect.fail(new BadRequestException('May not request original file'));
        }

        const edited = isEditedOnly(auth, dto.edited);
        const size = (dto.size ?? AssetMediaSize.THUMBNAIL) as unknown as AssetFileType;
        const { originalPath, originalFileName, path } = yield* this.fx.asset.getForThumbnail(id, size, edited);

        if (size === AssetFileType.FullSize && mimeTypes.isWebSupportedImage(originalPath) && !edited) {
          // use original file for web supported images
          return { targetSize: 'original' } as AssetMediaRedirectResponse;
        }

        if (dto.size === AssetMediaSize.FULLSIZE && !path) {
          // downgrade to preview if fullsize is not available.
          // e.g. disabled or not yet (re)generated
          return { targetSize: AssetMediaSize.PREVIEW } as AssetMediaRedirectResponse;
        }

        if (!path) {
          return yield* Effect.fail(new NotFoundException('Asset media not found'));
        }

        const fileNameBase = isMetadataHidden(auth) ? id : getFileNameWithoutExtension(originalFileName);
        const fileName = `${fileNameBase}_${size}${getFilenameExtension(path)}`;

        return new ImmichFileResponse({
          fileName,
          path,
          contentType: mimeTypes.lookup(path),
          cacheControl: CacheControl.PrivateWithCache,
        });
      }),
    );
  }

  async playbackVideo(auth: AuthDto, id: string): Promise<ImmichFileResponse> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetView, ids: [id] });

        const asset = yield* this.fx.asset.getForVideo(id);

        if (!asset) {
          return yield* Effect.fail(new NotFoundException('Asset not found or asset is not a video'));
        }

        const filepath = asset.encodedVideoPath || asset.originalPath;

        return new ImmichFileResponse({
          path: filepath,
          contentType: mimeTypes.lookup(filepath),
          cacheControl: CacheControl.PrivateWithCache,
        });
      }),
    );
  }

  async bulkUploadCheck(auth: AuthDto, dto: AssetBulkUploadCheckDto): Promise<AssetBulkUploadCheckResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const checksums: Buffer[] = dto.assets.map((asset) => fromChecksum(asset.checksum));
        const results = yield* this.fx.asset.getByChecksums(auth.user.id, checksums);
        const checksumMap: Record<string, { id: string; isTrashed: boolean }> = {};

        for (const { id, deletedAt, checksum } of results) {
          checksumMap[checksum.toString('hex')] = { id, isTrashed: !!deletedAt };
        }

        return {
          results: dto.assets.map(({ id, checksum }) => {
            const duplicate = checksumMap[fromChecksum(checksum).toString('hex')];
            if (duplicate) {
              return {
                id,
                action: AssetUploadAction.REJECT,
                reason: AssetRejectReason.DUPLICATE,
                assetId: duplicate.id,
                isTrashed: duplicate.isTrashed,
              };
            }

            return {
              id,
              action: AssetUploadAction.ACCEPT,
            };
          }),
        };
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private addToSharedLink = Effect.fn('AssetMediaService.addToSharedLink')(function* (
    this: AssetMediaService,
    sharedLink: AuthSharedLink,
    assetId: string,
  ) {
    if (!sharedLink.albumId) {
      yield* this.fx.sharedLink.addAssets(sharedLink.id, [assetId]);
      return;
    }

    const album = yield* this.fx.album.getById(sharedLink.albumId);
    if (!album) {
      return;
    }

    yield* this.fx.album.addAssetIds(album.id, [assetId]);
    const userIds = album.albumUsers.map(({ user }) => user.id);
    yield* this.fx.event.emit('AlbumUpdate', {
      id: album.id,
      userIds,
      recipientIds: userIds,
    });
  });

  private requireQuota(auth: AuthDto, size: number) {
    if (auth.user.quotaSizeInBytes !== null && auth.user.quotaSizeInBytes < auth.user.quotaUsageInBytes + size) {
      throw new BadRequestException('Quota has been exceeded!');
    }
  }
}
