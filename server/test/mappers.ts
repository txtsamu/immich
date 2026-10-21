import { Selectable, ShallowDehydrateObject } from 'kysely';
import type { AudioStreamInfo, VideoFormat, VideoStreamInfo } from 'src/types.js';
import { MapAsset } from 'src/dtos/asset-response.dto.js';
import { AssetEditActionItem } from 'src/dtos/editing.dto.js';
import { ActivityTable } from 'src/schema/tables/activity.table.js';
import { PartnerTable } from 'src/schema/tables/partner.table.js';
import { AlbumFactory } from 'test/factories/album.factory.js';
import { AssetFactory } from 'test/factories/asset.factory.js';
import { MemoryFactory } from 'test/factories/memory.factory.js';
import { SharedLinkFactory } from 'test/factories/shared-link.factory.js';
import { StackFactory } from 'test/factories/stack.factory.js';
import { UserFactory } from 'test/factories/user.factory.js';

export const getForStorageTemplate = (asset: ReturnType<AssetFactory['build']>) => {
  return {
    id: asset.id,
    ownerId: asset.ownerId,
    livePhotoVideoId: asset.livePhotoVideoId,
    type: asset.type,
    isExternal: asset.isExternal,
    checksum: asset.checksum,
    timeZone: asset.exifInfo.timeZone,
    visibility: asset.visibility,
    fileCreatedAt: asset.fileCreatedAt,
    originalPath: asset.originalPath,
    originalFileName: asset.originalFileName,
    fileSizeInByte: asset.exifInfo.fileSizeInByte,
    files: asset.files,
    make: asset.exifInfo.make,
    model: asset.exifInfo.model,
    lensModel: asset.exifInfo.lensModel,
    isEdited: asset.isEdited,
  };
};

export const getDehydrated = <T extends Record<string, unknown>>(entity: T) => {
  const copiedEntity = structuredClone(entity);
  for (const [key, value] of Object.entries(copiedEntity)) {
    if (value instanceof Date) {
      Object.assign(copiedEntity, { [key]: value.toISOString() });
    }
  }

  return copiedEntity as ShallowDehydrateObject<T>;
};

export const getForAlbum = (album: ReturnType<AlbumFactory['build']>) => ({
  ...album,
  assets: album.assets.map((asset) =>
    getDehydrated({ ...getForAsset(asset), exifInfo: getDehydrated(asset.exifInfo) }),
  ),
  albumUsers: album.albumUsers.map((albumUser) => ({
    ...albumUser,
    createdAt: albumUser.createdAt.toISOString(),
    user: getDehydrated(albumUser.user),
  })),
  sharedLinks: album.sharedLinks.map((sharedLink) => getDehydrated(sharedLink)),
});

export const getForActivity = (activity: Selectable<ActivityTable> & { user: ReturnType<UserFactory['build']> }) => ({
  ...activity,
  user: getDehydrated(activity.user),
});

export const getForAsset = (asset: ReturnType<AssetFactory['build']>) => {
  return {
    ...asset,
    owner: getDehydrated(asset.owner),
    stack: asset.stack
      ? { ...getDehydrated(asset.stack), assets: asset.stack.assets.map((asset) => getDehydrated(asset)) }
      : null,
    files: asset.files.map((file) => getDehydrated(file)),
    exifInfo: asset.exifInfo ? getDehydrated(asset.exifInfo) : null,
    edits: asset.edits.map(({ action, parameters }) => ({ action, parameters })) as AssetEditActionItem[],
  };
};

export const getForPartner = (
  partner: Selectable<PartnerTable> & Record<'sharedWith' | 'sharedBy', ReturnType<UserFactory['build']>>,
) => ({
  ...partner,
  sharedBy: getDehydrated(partner.sharedBy),
  sharedWith: getDehydrated(partner.sharedWith),
});

export const getForMemory = (memory: ReturnType<MemoryFactory['build']>) => ({
  ...memory,
  assets: memory.assets.map((asset) => getDehydrated(asset)),
});

export const getForMetadataExtraction = (asset: ReturnType<AssetFactory['build']>) => ({
  id: asset.id,
  checksum: asset.checksum,
  checksumAlgorithm: asset.checksumAlgorithm,
  fileCreatedAt: asset.fileCreatedAt,
  fileModifiedAt: asset.fileModifiedAt,
  isExternal: asset.isExternal,
  visibility: asset.visibility,
  libraryId: asset.libraryId,
  livePhotoVideoId: asset.livePhotoVideoId,
  localDateTime: asset.localDateTime,
  originalFileName: asset.originalFileName,
  originalPath: asset.originalPath,
  ownerId: asset.ownerId,
  type: asset.type,
  isEdited: asset.isEdited,
  width: asset.width,
  height: asset.height,
  files: asset.files.map((file) => getDehydrated(file)),
});

export const getForGenerateThumbnail = (asset: ReturnType<AssetFactory['build']>) => ({
  id: asset.id,
  visibility: asset.visibility,
  originalFileName: asset.originalFileName,
  originalPath: asset.originalPath,
  ownerId: asset.ownerId,
  thumbhash: asset.thumbhash,
  type: asset.type,
  files: asset.files.map((file) => getDehydrated(file)),
  exifInfo: getDehydrated(asset.exifInfo),
  edits: asset.edits.map(({ action, parameters }) => ({ action, parameters })) as AssetEditActionItem[],
  videoStream: null as (VideoStreamInfo & { timeBase: number }) | null,
  audioStream: null as AudioStreamInfo | null,
  format: null as VideoFormat | null,
});

export const getForSidecarWrite = (asset: ReturnType<AssetFactory['build']>) => ({
  id: asset.id,
  originalPath: asset.originalPath,
  files: asset.files.map((file) => getDehydrated(file)),
  exifInfo: getDehydrated(asset.exifInfo),
});

export const getForAssetDeletion = (asset: ReturnType<AssetFactory['build']>) => ({
  id: asset.id,
  visibility: asset.visibility,
  libraryId: asset.libraryId,
  ownerId: asset.ownerId,
  livePhotoVideoId: asset.livePhotoVideoId,
  originalPath: asset.originalPath,
  isOffline: asset.isOffline,
  exifInfo: asset.exifInfo ? getDehydrated(asset.exifInfo) : null,
  files: asset.files.map((file) => getDehydrated(file)),
  stack: asset.stack
    ? {
        ...getDehydrated(asset.stack),
        assets: asset.stack.assets.filter(({ id }) => id !== asset.stack?.primaryAssetId).map(({ id }) => ({ id })),
      }
    : null,
});

export const getForStack = (stack: ReturnType<StackFactory['build']>) => ({
  ...stack,
  assets: stack.assets.map((asset) => ({
    ...getDehydrated(asset),
    exifInfo: getDehydrated(asset.exifInfo),
  })),
});

export const getForDuplicate = (asset: ReturnType<AssetFactory['build']>) =>
  ({
    ...getDehydrated(asset),
    exifInfo: getDehydrated(asset.exifInfo),
  }) as unknown as MapAsset;

export const getForSharedLink = (sharedLink: ReturnType<SharedLinkFactory['build']>) => ({
  ...sharedLink,
  assets: sharedLink.assets.map((asset) => ({
    ...getDehydrated({ ...getForAsset(asset) }),
    exifInfo: getDehydrated(asset.exifInfo),
  })),
  album: sharedLink.album
    ? {
        ...getDehydrated(sharedLink.album),
        assets: sharedLink.album.assets.map((asset) => getDehydrated(asset)),
      }
    : null,
});
