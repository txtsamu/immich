import { BadRequestException } from '@nestjs/common';
import { Context, Effect } from 'effect';
import type { IBulkAsset, ImmichFile, UploadFile, UploadRequest } from 'src/types.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { AssetFile } from 'src/database.js';
import { BulkIdErrorReason, BulkIdResponseDto } from 'src/dtos/asset-ids.response.dto.js';
import { UploadFieldName } from 'src/dtos/asset-media.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { effectOf } from 'src/effect/bridge.js';
import { LiftedError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { AssetFileType, AssetType, AssetVisibility, Permission } from 'src/enum.js';
import { AuthRequest } from 'src/middleware/auth.guard.js';
import { AccessRepo, AccessRepository } from 'src/repositories/access.repository.js';
import { AssetRepo, AssetRepository } from 'src/repositories/asset.repository.js';
import { EventRepo, EventRepository } from 'src/repositories/event.repository.js';
import { PartnerRepo, PartnerRepository } from 'src/repositories/partner.repository.js';
import { AccessFx, checkAccessFx } from 'src/utils/access.js';

/** Effect version of `IBulkAsset` (see `src/types.ts`). */
export interface IBulkAssetFx<E = never> {
  getAssetIds: (id: string, assetIds: string[]) => Effect.Effect<Set<string>, E>;
  addAssetIds: (id: string, assetIds: string[]) => Effect.Effect<void, E>;
  removeAssetIds: (id: string, assetIds: string[]) => Effect.Effect<void, E>;
}

/** Lifts a Promise-based `IBulkAsset` (e.g. a repository adapter) to its Effect shape. */
const liftBulk = (bulk: IBulkAsset): IBulkAssetFx<LiftedError> => ({
  getAssetIds: (id, assetIds) =>
    Effect.tryPromise({
      try: async () => await bulk.getAssetIds(id, assetIds),
      catch: (cause) => new LiftedError({ cause }),
    }),
  addAssetIds: (id, assetIds) =>
    Effect.tryPromise({
      try: async () => await bulk.addAssetIds(id, assetIds),
      catch: (cause) => new LiftedError({ cause }),
    }),
  removeAssetIds: (id, assetIds) =>
    Effect.tryPromise({
      try: async () => await bulk.removeAssetIds(id, assetIds),
      catch: (cause) => new LiftedError({ cause }),
    }),
});

export const getAssetFile = (files: AssetFile[], type: AssetFileType, { isEdited }: { isEdited: boolean }) => {
  return files.find((file) => file.type === type && file.isEdited === isEdited);
};

export const getAssetFiles = (files: AssetFile[]) => ({
  fullsizeFile: getAssetFile(files, AssetFileType.FullSize, { isEdited: false }),
  previewFile: getAssetFile(files, AssetFileType.Preview, { isEdited: false }),
  thumbnailFile: getAssetFile(files, AssetFileType.Thumbnail, { isEdited: false }),
  sidecarFile: getAssetFile(files, AssetFileType.Sidecar, { isEdited: false }),

  editedFullsizeFile: getAssetFile(files, AssetFileType.FullSize, { isEdited: true }),
  editedPreviewFile: getAssetFile(files, AssetFileType.Preview, { isEdited: true }),
  editedThumbnailFile: getAssetFile(files, AssetFileType.Thumbnail, { isEdited: true }),

  encodedVideoFile: getAssetFile(files, AssetFileType.EncodedVideo, { isEdited: false }),
});

export const addAssetsFx = <E>(
  auth: AuthDto,
  repositories: { access: AccessFx; bulk: IBulkAssetFx<E> },
  dto: { parentId: string; assetIds: string[]; permission: Permission },
) =>
  Effect.gen(function* () {
    const { access, bulk } = repositories;
    const existingAssetIds = yield* bulk.getAssetIds(dto.parentId, dto.assetIds);
    const notPresentAssetIds = dto.assetIds.filter((id) => !existingAssetIds.has(id));
    const allowedAssetIds = yield* checkAccessFx(access, {
      auth,
      permission: dto.permission,
      ids: notPresentAssetIds,
    });

    const results: BulkIdResponseDto[] = [];
    for (const assetId of dto.assetIds) {
      const hasAsset = existingAssetIds.has(assetId);
      if (hasAsset) {
        results.push({ id: assetId, success: false, error: BulkIdErrorReason.DUPLICATE });
        continue;
      }

      const hasAccess = allowedAssetIds.has(assetId);
      if (!hasAccess) {
        results.push({ id: assetId, success: false, error: BulkIdErrorReason.NO_PERMISSION });
        continue;
      }

      existingAssetIds.add(assetId);
      results.push({ id: assetId, success: true });
    }

    const newAssetIds = results.filter(({ success }) => success).map(({ id }) => id);
    if (newAssetIds.length > 0) {
      yield* bulk.addAssetIds(dto.parentId, newAssetIds);
    }

    return results;
  });

export const addAssets = (
  auth: AuthDto,
  repositories: { access: AccessRepository; bulk: IBulkAsset },
  dto: { parentId: string; assetIds: string[]; permission: Permission },
): Promise<BulkIdResponseDto[]> =>
  runPromise(
    addAssetsFx(auth, { access: effectOf(AccessRepo, repositories.access), bulk: liftBulk(repositories.bulk) }, dto),
  );

export const removeAssetsFx = <E>(
  auth: AuthDto,
  repositories: { access: AccessFx; bulk: IBulkAssetFx<E> },
  dto: { parentId: string; assetIds: string[]; canAlwaysRemove: Permission },
) =>
  Effect.gen(function* () {
    const { access, bulk } = repositories;

    // check if the user can always remove from the parent album, memory, etc.
    const canAlwaysRemove = yield* checkAccessFx(access, {
      auth,
      permission: dto.canAlwaysRemove,
      ids: [dto.parentId],
    });
    const existingAssetIds = yield* bulk.getAssetIds(dto.parentId, dto.assetIds);
    const allowedAssetIds = canAlwaysRemove.has(dto.parentId)
      ? existingAssetIds
      : yield* checkAccessFx(access, { auth, permission: Permission.AssetShare, ids: existingAssetIds });

    const results: BulkIdResponseDto[] = [];
    for (const assetId of dto.assetIds) {
      const hasAsset = existingAssetIds.has(assetId);
      if (!hasAsset) {
        results.push({ id: assetId, success: false, error: BulkIdErrorReason.NOT_FOUND });
        continue;
      }

      const hasAccess = allowedAssetIds.has(assetId);
      if (!hasAccess) {
        results.push({ id: assetId, success: false, error: BulkIdErrorReason.NO_PERMISSION });
        continue;
      }

      existingAssetIds.delete(assetId);
      results.push({ id: assetId, success: true });
    }

    const removedIds = results.filter(({ success }) => success).map(({ id }) => id);
    if (removedIds.length > 0) {
      yield* bulk.removeAssetIds(dto.parentId, removedIds);
    }

    return results;
  });

export const removeAssets = (
  auth: AuthDto,
  repositories: { access: AccessRepository; bulk: IBulkAsset },
  dto: { parentId: string; assetIds: string[]; canAlwaysRemove: Permission },
): Promise<BulkIdResponseDto[]> =>
  runPromise(
    removeAssetsFx(auth, { access: effectOf(AccessRepo, repositories.access), bulk: liftBulk(repositories.bulk) }, dto),
  );

export type PartnerIdOptions = {
  userId: string;
  repository: PartnerRepository;
  /** only include partners with `inTimeline: true` */
  timelineEnabled?: boolean;
};
export type PartnerIdOptionsFx = {
  userId: string;
  repository: Context.Service.Shape<typeof PartnerRepo>;
  /** only include partners with `inTimeline: true` */
  timelineEnabled?: boolean;
};
export const getMyPartnerIdsFx = ({ userId, repository, timelineEnabled }: PartnerIdOptionsFx) =>
  Effect.gen(function* () {
    const partnerIds = new Set<string>();
    const partners = yield* repository.getAll(userId);
    for (const partner of partners) {
      // ignore deleted users
      if (!partner.sharedBy || !partner.sharedWith) {
        continue;
      }

      // wrong direction
      if (partner.sharedWithId !== userId) {
        continue;
      }

      if (timelineEnabled && !partner.inTimeline) {
        continue;
      }

      partnerIds.add(partner.sharedById);
    }

    return [...partnerIds];
  });

export const getMyPartnerIds = ({ userId, repository, timelineEnabled }: PartnerIdOptions): Promise<string[]> =>
  runPromise(getMyPartnerIdsFx({ userId, repository: effectOf(PartnerRepo, repository), timelineEnabled }));

export type AssetHookRepositories = { asset: AssetRepository; event: EventRepository };

/** The Effect shapes of `AssetHookRepositories`. */
export type AssetHookFx = {
  asset: Context.Service.Shape<typeof AssetRepo>;
  event: Context.Service.Shape<typeof EventRepo>;
};

const toHookFx = ({ asset, event }: AssetHookRepositories): AssetHookFx => ({
  asset: effectOf(AssetRepo, asset),
  event: effectOf(EventRepo, event),
});

export const onBeforeLinkFx = (
  { asset: assetRepository, event: eventRepository }: AssetHookFx,
  { userId, livePhotoVideoId }: { userId: string; livePhotoVideoId: string },
) =>
  Effect.gen(function* () {
    const motionAsset = yield* assetRepository.getById(livePhotoVideoId);
    if (!motionAsset) {
      return yield* Effect.fail(new BadRequestException('Live photo video not found'));
    }
    if (motionAsset.type !== AssetType.Video) {
      return yield* Effect.fail(new BadRequestException('Live photo video must be a video'));
    }
    if (motionAsset.ownerId !== userId) {
      return yield* Effect.fail(new BadRequestException('Live photo video does not belong to the user'));
    }

    if (!motionAsset || motionAsset.visibility !== AssetVisibility.Timeline) {
      return;
    }

    yield* assetRepository.update({ id: livePhotoVideoId, visibility: AssetVisibility.Hidden });
    yield* eventRepository.emit('AssetHide', { assetId: motionAsset.id, userId });
  });

export const onBeforeLink = (
  repositories: AssetHookRepositories,
  options: { userId: string; livePhotoVideoId: string },
): Promise<void> => runPromise(onBeforeLinkFx(toHookFx(repositories), options));

export const onBeforeUnlinkFx = (
  { asset: assetRepository }: Pick<AssetHookFx, 'asset'>,
  { livePhotoVideoId }: { livePhotoVideoId: string },
) =>
  Effect.gen(function* () {
    const motion = yield* assetRepository.getById(livePhotoVideoId);
    if (!motion) {
      return null;
    }

    if (StorageCore.isAndroidMotionPath(motion.originalPath)) {
      return yield* Effect.fail(new BadRequestException('Cannot unlink Android motion photos'));
    }

    return motion;
  });

export const onBeforeUnlink = (repositories: AssetHookRepositories, options: { livePhotoVideoId: string }) =>
  runPromise(onBeforeUnlinkFx({ asset: effectOf(AssetRepo, repositories.asset) }, options));

export const onAfterUnlinkFx = (
  { asset: assetRepository, event: eventRepository }: AssetHookFx,
  { userId, livePhotoVideoId, visibility }: { userId: string; livePhotoVideoId: string; visibility: AssetVisibility },
) =>
  Effect.gen(function* () {
    yield* assetRepository.update({ id: livePhotoVideoId, visibility });
    yield* eventRepository.emit('AssetShow', { assetId: livePhotoVideoId, userId });
  });

export const onAfterUnlink = (
  repositories: AssetHookRepositories,
  options: { userId: string; livePhotoVideoId: string; visibility: AssetVisibility },
): Promise<void> => runPromise(onAfterUnlinkFx(toHookFx(repositories), options));

export function mapToUploadFile(file: ImmichFile): UploadFile {
  return {
    uuid: file.uuid,
    checksum: file.checksum,
    originalPath: file.path,
    originalName: Buffer.from(file.originalname, 'latin1').toString('utf8'),
    size: file.size,
  };
}

export const asUploadRequest = (request: AuthRequest, file: Express.Multer.File): UploadRequest => {
  return {
    auth: request.user || null,
    body: request.body,
    fieldName: file.fieldname as UploadFieldName,
    file: mapToUploadFile(file as ImmichFile),
  };
};

const isFlipped = (orientation?: string | null) => {
  const value = Number(orientation);
  return value && [5, 6, 7, 8, -90, 90].includes(value);
};

export const getDimensions = ({
  exifImageHeight: height,
  exifImageWidth: width,
  orientation,
}: {
  exifImageHeight: number | null;
  exifImageWidth: number | null;
  orientation: string | null;
}) => {
  if (!width || !height) {
    return { width: 0, height: 0 };
  }

  if (isFlipped(orientation)) {
    return { width: height, height: width };
  }

  return { width, height };
};

export const isPanorama = (asset: { projectionType: string | null; originalFileName: string }) => {
  return asset.projectionType === 'EQUIRECTANGULAR' || asset.originalFileName.toLowerCase().endsWith('.insp');
};
