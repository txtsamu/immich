import { Effect } from 'effect';
import { dirname, join, resolve } from 'node:path';
import type { VideoInterfaces } from 'src/types.js';
import { StorageAsset } from 'src/database.js';
import { effectOf } from 'src/effect/bridge.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetPathType,
  ImageFormat,
  PathType,
  RawExtractedFormat,
  StorageFolder,
  UserPathType,
} from 'src/enum.js';
import { AssetRepo, AssetRepository } from 'src/repositories/asset.repository.js';
import { ConfigRepo, ConfigRepository } from 'src/repositories/config.repository.js';
import { CryptoRepo, CryptoRepository } from 'src/repositories/crypto.repository.js';
import { LoggingRepo, LoggingRepository } from 'src/repositories/logging.repository.js';
import { MoveRepo, MoveRepository } from 'src/repositories/move.repository.js';
import { StorageRepo, StorageRepository } from 'src/repositories/storage.repository.js';
import { SystemMetadataRepo, SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { getAssetFile } from 'src/utils/asset.util.js';
import { getConfigFx } from 'src/utils/config.js';

export interface MoveRequest {
  entityId: string;
  pathType: PathType;
  oldPath: string | null;
  newPath: string;
  assetInfo?: {
    sizeInBytes: number;
    checksum: Buffer;
  };
}

export type ThumbnailPathEntity = { id: string; ownerId: string };

export type HlsSessionFolder = { ownerId: string; sessionId: string };

export type HlsVariantFolder = { ownerId: string; sessionId: string; variantIndex: number };

export type ImagePathOptions = { fileType: AssetFileType; format: ImageFormat | RawExtractedFormat; isEdited: boolean };

let instance: StorageCore | null;

let mediaLocation: string | undefined;

export class StorageCore {
  private constructor(
    private assetRepository: AssetRepository,
    private configRepository: ConfigRepository,
    private cryptoRepository: CryptoRepository,
    private moveRepository: MoveRepository,
    private storageRepository: StorageRepository,
    private systemMetadataRepository: SystemMetadataRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(StorageCore.name);
  }

  static create(
    assetRepository: AssetRepository,
    configRepository: ConfigRepository,
    cryptoRepository: CryptoRepository,
    moveRepository: MoveRepository,
    storageRepository: StorageRepository,
    systemMetadataRepository: SystemMetadataRepository,
    logger: LoggingRepository,
  ) {
    if (!instance) {
      instance = new StorageCore(
        assetRepository,
        configRepository,
        cryptoRepository,
        moveRepository,
        storageRepository,
        systemMetadataRepository,
        logger,
      );
    }

    return instance;
  }

  static reset() {
    instance = null;
  }

  static getMediaLocation(): string {
    if (mediaLocation === undefined) {
      throw new Error('Media location is not set.');
    }

    return mediaLocation;
  }

  static setMediaLocation(location: string) {
    mediaLocation = location;
  }

  static getFolderLocation(folder: StorageFolder, userId: string) {
    return join(StorageCore.getBaseFolder(folder), userId);
  }

  static getLibraryFolder(user: { storageLabel: string | null; id: string }) {
    return join(StorageCore.getBaseFolder(StorageFolder.Library), user.storageLabel || user.id);
  }

  static getBaseFolder(folder: StorageFolder) {
    return join(StorageCore.getMediaLocation(), folder);
  }

  static getImagePath(asset: ThumbnailPathEntity, { fileType, format, isEdited }: ImagePathOptions) {
    return StorageCore.getNestedPath(
      StorageFolder.Thumbnails,
      asset.ownerId,
      `${asset.id}_${fileType}${isEdited ? '_edited' : ''}.${format}`,
    );
  }

  static getEncodedVideoPath(asset: ThumbnailPathEntity) {
    return StorageCore.getNestedPath(StorageFolder.EncodedVideo, asset.ownerId, `${asset.id}.mp4`);
  }

  static getHlsSessionFolder({ ownerId, sessionId }: HlsSessionFolder) {
    return StorageCore.getNestedPath(StorageFolder.EncodedVideo, ownerId, sessionId);
  }

  static getHlsVariantFolder({ ownerId, sessionId, variantIndex }: HlsVariantFolder) {
    return join(StorageCore.getHlsSessionFolder({ ownerId, sessionId }), variantIndex.toString());
  }

  static getAndroidMotionPath(asset: ThumbnailPathEntity, uuid: string) {
    return StorageCore.getNestedPath(StorageFolder.EncodedVideo, asset.ownerId, `${uuid}-MP.mp4`);
  }

  static isAndroidMotionPath(originalPath: string) {
    return originalPath.startsWith(StorageCore.getBaseFolder(StorageFolder.EncodedVideo));
  }

  static isImmichPath(path: string) {
    const resolvedPath = resolve(path);
    const resolvedAppMediaLocation = StorageCore.getMediaLocation();
    const normalizedPath = resolvedPath.endsWith('/') ? resolvedPath : resolvedPath + '/';
    const normalizedAppMediaLocation = resolvedAppMediaLocation.endsWith('/')
      ? resolvedAppMediaLocation
      : resolvedAppMediaLocation + '/';
    return normalizedPath.startsWith(normalizedAppMediaLocation);
  }

  private get fx() {
    return {
      asset: effectOf(AssetRepo, this.assetRepository),
      config: effectOf(ConfigRepo, this.configRepository),
      crypto: effectOf(CryptoRepo, this.cryptoRepository),
      move: effectOf(MoveRepo, this.moveRepository),
      storage: effectOf(StorageRepo, this.storageRepository),
      systemMetadata: effectOf(SystemMetadataRepo, this.systemMetadataRepository),
      logger: effectOf(LoggingRepo, this.logger),
    };
  }

  moveAssetImageFx(asset: StorageAsset, fileType: AssetFileType, format: ImageFormat) {
    const { id: entityId, files } = asset;
    const oldFile = getAssetFile(files, fileType, { isEdited: false });
    return this.moveFileFx({
      entityId,
      pathType: fileType,
      oldPath: oldFile?.path || null,
      newPath: StorageCore.getImagePath(asset, { fileType, format, isEdited: false }),
    });
  }

  async moveAssetImage(asset: StorageAsset, fileType: AssetFileType, format: ImageFormat) {
    return runPromise(this.moveAssetImageFx(asset, fileType, format));
  }

  moveAssetVideoFx(asset: StorageAsset) {
    const encodedVideoFile = getAssetFile(asset.files, AssetFileType.EncodedVideo, { isEdited: false });
    return this.moveFileFx({
      entityId: asset.id,
      pathType: AssetPathType.EncodedVideo,
      oldPath: encodedVideoFile?.path || null,
      newPath: StorageCore.getEncodedVideoPath(asset),
    });
  }

  async moveAssetVideo(asset: StorageAsset) {
    return runPromise(this.moveAssetVideoFx(asset));
  }

  moveFileFx(request: MoveRequest) {
    return Effect.gen({ self: this }, function* () {
      const { entityId, pathType, oldPath, newPath, assetInfo } = request;
      if (!oldPath || oldPath === newPath) {
        return;
      }

      yield* this.ensureFoldersFx(newPath);

      let move = yield* this.fx.move.getByEntity(entityId, pathType);
      if (move) {
        this.logger.log(`Attempting to finish incomplete move: ${move.oldPath} => ${move.newPath}`);
        const isOldPathExists = yield* this.fx.storage.checkFileExists(move.oldPath);
        const isNewPathExists = yield* this.fx.storage.checkFileExists(move.newPath);
        const newPathCheck = isNewPathExists ? move.newPath : null;
        const actualPath = isOldPathExists ? move.oldPath : newPathCheck;
        if (!actualPath) {
          this.logger.warn('Unable to complete move. File does not exist at either location.');
          return;
        }

        const isFileAtNewLocation = actualPath === move.newPath;
        this.logger.log(`Found file at ${isFileAtNewLocation ? 'new' : 'old'} location`);

        if (
          isFileAtNewLocation &&
          !(yield* this.verifyNewPathContentsMatchesExpectedFx(move.oldPath, move.newPath, assetInfo))
        ) {
          this.logger.fatal(
            `Skipping move as file verification failed, old file is missing and new file is different to what was expected`,
          );
          return;
        }

        move = yield* this.fx.move.update(move.id, { id: move.id, oldPath: actualPath, newPath });
      } else {
        move = yield* this.fx.move.create({ entityId, pathType, oldPath, newPath });
      }

      if (pathType === AssetPathType.Original && !assetInfo) {
        this.logger.warn(`Unable to complete move. Missing asset info for ${entityId}`);
        return;
      }

      if (move.oldPath !== newPath) {
        this.logger.debug(`Attempting to rename file: ${move.oldPath} => ${newPath}`);
        const renameFailure = yield* this.fx.storage.rename(move.oldPath, newPath).pipe(
          Effect.as(undefined),
          Effect.catch((error) => Effect.succeed({ error: unwrap(error) as any })),
        );
        if (renameFailure) {
          const { error } = renameFailure;
          if (error.code !== 'EXDEV') {
            this.logger.warn(
              `Unable to complete move. Error renaming file with code ${error.code} and message: ${error.message}`,
            );
            return;
          }
          this.logger.debug(`Unable to rename file. Falling back to copy, verify and delete`);
          yield* this.fx.storage.copyFile(move.oldPath, newPath);

          if (!(yield* this.verifyNewPathContentsMatchesExpectedFx(move.oldPath, newPath, assetInfo))) {
            this.logger.warn(`Skipping move due to file size mismatch`);
            yield* this.fx.storage.unlink(newPath);
            return;
          }

          const { atime, mtime } = yield* this.fx.storage.stat(move.oldPath);
          yield* this.fx.storage.utimes(newPath, atime, mtime);

          yield* this.fx.storage.unlink(move.oldPath).pipe(
            Effect.catch((error_) => {
              const error = unwrap(error_) as any;
              this.logger.warn(
                `Unable to delete old file, it will now no longer be tracked by Immich: ${error.message}`,
              );
              return Effect.void;
            }),
          );
        }
      }

      yield* this.savePathFx(pathType, entityId, newPath);
      yield* this.fx.move.delete(move.id);
    });
  }

  async moveFile(request: MoveRequest) {
    return runPromise(this.moveFileFx(request));
  }

  private verifyNewPathContentsMatchesExpectedFx(
    oldPath: string,
    newPath: string,
    assetInfo?: { sizeInBytes: number; checksum: Buffer },
  ) {
    return Effect.gen({ self: this }, function* () {
      const oldStat = yield* this.fx.storage.stat(oldPath);
      const newStat = yield* this.fx.storage.stat(newPath);
      const oldPathSize = assetInfo ? assetInfo.sizeInBytes : oldStat.size;
      const newPathSize = newStat.size;
      this.logger.debug(`File size check: ${newPathSize} === ${oldPathSize}`);
      if (newPathSize !== oldPathSize) {
        this.logger.warn(`Unable to complete move. File size mismatch: ${newPathSize} !== ${oldPathSize}`);
        return false;
      }
      const { config: configRepo, systemMetadata: metadataRepo, logger } = this.fx;
      const config = yield* getConfigFx({ configRepo, metadataRepo, logger }, { withCache: true });
      if (assetInfo && config.storageTemplate.hashVerificationEnabled) {
        const { checksum } = assetInfo;
        const newChecksum = yield* this.fx.crypto.hashFile(newPath);
        if (!newChecksum.equals(checksum)) {
          this.logger.warn(
            `Unable to complete move. File checksum mismatch: ${newChecksum.toString('base64')} !== ${checksum.toString(
              'base64',
            )}`,
          );
          return false;
        }
        this.logger.debug(`File checksum check: ${newChecksum.toString('base64')} === ${checksum.toString('base64')}`);
      }
      return true;
    });
  }

  ensureFoldersFx(input: string) {
    return this.fx.storage.mkdirSync(dirname(input));
  }

  ensureFolders(input: string) {
    this.storageRepository.mkdirSync(dirname(input));
  }

  removeEmptyDirsFx(folder: StorageFolder) {
    return this.fx.storage.removeEmptyDirs(StorageCore.getBaseFolder(folder));
  }

  removeEmptyDirs(folder: StorageFolder) {
    return runPromise(this.removeEmptyDirsFx(folder));
  }

  getVideoInterfacesFx(): Effect.Effect<VideoInterfaces, unknown> {
    return Effect.all({ dri: this.getDevicesFx(), mali: this.hasMaliOpenCLFx() }, { concurrency: 'unbounded' });
  }

  async getVideoInterfaces(): Promise<VideoInterfaces> {
    return runPromise(this.getVideoInterfacesFx());
  }

  private savePathFx(pathType: PathType, id: string, newPath: string) {
    switch (pathType) {
      case AssetPathType.Original: {
        return this.fx.asset.update({ id, originalPath: newPath });
      }

      case AssetFileType.FullSize:
      case AssetFileType.EncodedVideo:
      case AssetFileType.Thumbnail:
      case AssetFileType.Preview:
      case AssetFileType.Sidecar:
      case AssetPathType.EncodedVideo: {
        return this.fx.asset.upsertFile({ assetId: id, type: pathType as AssetFileType, path: newPath });
      }

      case UserPathType.Profile: {
        this.logger.warn('Unexpected path type:', pathType);
        return Effect.void;
      }
    }
  }

  static getNestedFolder(folder: StorageFolder, ownerId: string, filename: string): string {
    return join(StorageCore.getFolderLocation(folder, ownerId), filename.slice(0, 2), filename.slice(2, 4));
  }

  static getNestedPath(folder: StorageFolder, ownerId: string, filename: string): string {
    return join(StorageCore.getNestedFolder(folder, ownerId, filename), filename);
  }

  private getDevicesFx() {
    return this.fx.storage.readdir('/dev/dri').pipe(
      Effect.catchCause(() => {
        this.logger.debug('No devices found in /dev/dri.');
        return Effect.succeed([] as string[]);
      }),
    );
  }

  private hasMaliOpenCLFx() {
    return Effect.all([this.fx.storage.stat('/etc/OpenCL/vendors/mali.icd'), this.fx.storage.stat('/dev/mali0')], {
      concurrency: 'unbounded',
    }).pipe(
      Effect.flatMap(([maliIcdStat, maliDeviceStat]) =>
        Effect.try({
          try: () => maliIcdStat.isFile() && maliDeviceStat.isCharacterDevice(),
          catch: (error) => error,
        }),
      ),
      Effect.catchCause(() => {
        this.logger.debug('OpenCL not available for transcoding, so RKMPP acceleration will use CPU tonemapping');
        return Effect.succeed(false);
      }),
    );
  }
}
