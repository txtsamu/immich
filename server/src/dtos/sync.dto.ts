import { createZodDto } from 'nestjs-zod';
import z from 'zod';
import { ExtraModel } from 'src/decorators.js';
import { AssetEditActionSchema } from 'src/dtos/editing.dto.js';
import {
  AlbumUserRole,
  AlbumUserRoleSchema,
  AssetOrderSchema,
  AssetTypeSchema,
  AssetVisibilitySchema,
  MemoryTypeSchema,
  SyncEntityType,
  SyncEntityTypeSchema,
  SyncRequestTypeSchema,
  UserAvatarColorSchema,
  UserMetadataKeySchema,
} from 'src/enum.js';
import { isoDatetimeToDate } from 'src/validation.js';

const SyncUserV1Schema = z
  .object({
    id: z.uuidv4().describe('User ID'),
    name: z.string().describe('User name'),
    email: z.string().describe('User email'),
    avatarColor: UserAvatarColorSchema.nullish(),
    deletedAt: isoDatetimeToDate.nullable().describe('User deleted at'),
    hasProfileImage: z.boolean().describe('User has profile image'),
    profileChangedAt: isoDatetimeToDate.describe('User profile changed at'),
  })
  .meta({ id: 'SyncUserV1' });

const SyncAuthUserV1Schema = SyncUserV1Schema.merge(
  z.object({
    isAdmin: z.boolean().describe('User is admin'),
    pinCode: z.string().nullable().describe('User pin code'),
    oauthId: z.string().describe('User OAuth ID'),
    storageLabel: z.string().nullable().describe('User storage label'),
    quotaSizeInBytes: z.int().nullable().describe('Quota size in bytes'),
    quotaUsageInBytes: z.int().describe('Quota usage in bytes'),
  }),
).meta({ id: 'SyncAuthUserV1' });

const SyncAuthUserV2Schema = SyncAuthUserV1Schema.extend({
  oauthId: z.string().nullable().describe('User OAuth ID'),
}).meta({ id: 'SyncAuthUserV2' });

const SyncUserDeleteV1Schema = z.object({ userId: z.uuidv4().describe('User ID') }).meta({ id: 'SyncUserDeleteV1' });

const SyncPartnerV1Schema = z
  .object({
    sharedById: z.uuidv4().describe('Shared by ID'),
    sharedWithId: z.uuidv4().describe('Shared with ID'),
    inTimeline: z.boolean().describe('In timeline'),
  })
  .meta({ id: 'SyncPartnerV1' });

const SyncPartnerDeleteV1Schema = z
  .object({
    sharedById: z.uuidv4().describe('Shared by ID'),
    sharedWithId: z.uuidv4().describe('Shared with ID'),
  })
  .meta({ id: 'SyncPartnerDeleteV1' });

const SyncAssetV1Schema = z
  .object({
    id: z.uuidv4().describe('Asset ID'),
    ownerId: z.uuidv4().describe('Owner ID'),
    originalFileName: z.string().describe('Original file name'),
    thumbhash: z.string().nullable().describe('Thumbhash'),
    checksum: z.string().describe('Checksum'),
    fileCreatedAt: isoDatetimeToDate.nullable().describe('File created at'),
    fileModifiedAt: isoDatetimeToDate.nullable().describe('File modified at'),
    createdAt: isoDatetimeToDate.nullable().describe('Uploaded to Immich at'),
    localDateTime: isoDatetimeToDate.nullable().describe('Local date time'),
    duration: z.string().nullable().describe('Duration'),
    type: AssetTypeSchema,
    deletedAt: isoDatetimeToDate.nullable().describe('Deleted at'),
    isFavorite: z.boolean().describe('Is favorite'),
    visibility: AssetVisibilitySchema,
    livePhotoVideoId: z.string().nullable().describe('Live photo video ID'),
    stackId: z.string().nullable().describe('Stack ID'),
    libraryId: z.string().nullable().describe('Library ID'),
    width: z.int().nullable().describe('Asset width'),
    height: z.int().nullable().describe('Asset height'),
    isEdited: z.boolean().describe('Is edited'),
  })
  .meta({ id: 'SyncAssetV1' });

const SyncAssetV2Schema = z
  .object({
    id: z.uuidv4().describe('Asset ID'),
    ownerId: z.uuidv4().describe('Owner ID'),
    originalFileName: z.string().describe('Original file name'),
    thumbhash: z.string().nullable().describe('Thumbhash'),
    checksum: z.string().describe('Checksum'),
    fileCreatedAt: isoDatetimeToDate.nullable().describe('File created at'),
    fileModifiedAt: isoDatetimeToDate.nullable().describe('File modified at'),
    createdAt: isoDatetimeToDate.nullable().describe('Uploaded to Immich at'),
    localDateTime: isoDatetimeToDate.nullable().describe('Local date time'),
    duration: z.int32().min(0).nullable().describe('Duration'),
    type: AssetTypeSchema,
    deletedAt: isoDatetimeToDate.nullable().describe('Deleted at'),
    isFavorite: z.boolean().describe('Is favorite'),
    visibility: AssetVisibilitySchema,
    livePhotoVideoId: z.string().nullable().describe('Live photo video ID'),
    stackId: z.string().nullable().describe('Stack ID'),
    libraryId: z.string().nullable().describe('Library ID'),
    width: z.int().nullable().describe('Asset width'),
    height: z.int().nullable().describe('Asset height'),
    isEdited: z.boolean().describe('Is edited'),
  })
  .meta({ id: 'SyncAssetV2' });

@ExtraModel()
class SyncUserV1 extends createZodDto(SyncUserV1Schema) {}
@ExtraModel()
class SyncAuthUserV1 extends createZodDto(SyncAuthUserV1Schema) {}
@ExtraModel()
class SyncAuthUserV2 extends createZodDto(SyncAuthUserV2Schema) {}
@ExtraModel()
class SyncUserDeleteV1 extends createZodDto(SyncUserDeleteV1Schema) {}
@ExtraModel()
class SyncPartnerV1 extends createZodDto(SyncPartnerV1Schema) {}
@ExtraModel()
class SyncPartnerDeleteV1 extends createZodDto(SyncPartnerDeleteV1Schema) {}
@ExtraModel()
export class SyncAssetV1 extends createZodDto(SyncAssetV1Schema) {}
@ExtraModel()
export class SyncAssetV2 extends createZodDto(SyncAssetV2Schema) {}

const SyncAssetDeleteV1Schema = z
  .object({ assetId: z.uuidv4().describe('Asset ID') })
  .meta({ id: 'SyncAssetDeleteV1' });

const SyncAssetExifV1Schema = z
  .object({
    assetId: z.uuidv4().describe('Asset ID'),
    description: z.string().nullable().describe('Description'),
    exifImageWidth: z.int().nullable().describe('Exif image width'),
    exifImageHeight: z.int().nullable().describe('Exif image height'),
    fileSizeInByte: z.int().nullable().describe('File size in byte'),
    orientation: z.string().nullable().describe('Orientation'),
    dateTimeOriginal: isoDatetimeToDate.nullable().describe('Date time original'),
    modifyDate: isoDatetimeToDate.nullable().describe('Modify date'),
    timeZone: z.string().nullable().describe('Time zone'),
    latitude: z.number().meta({ format: 'double' }).nullable().describe('Latitude'),
    longitude: z.number().meta({ format: 'double' }).nullable().describe('Longitude'),
    projectionType: z.string().nullable().describe('Projection type'),
    city: z.string().nullable().describe('City'),
    state: z.string().nullable().describe('State'),
    country: z.string().nullable().describe('Country'),
    make: z.string().nullable().describe('Make'),
    model: z.string().nullable().describe('Model'),
    lensModel: z.string().nullable().describe('Lens model'),
    fNumber: z.number().meta({ format: 'double' }).nullable().describe('F number'),
    focalLength: z.number().meta({ format: 'double' }).nullable().describe('Focal length'),
    iso: z.int().nullable().describe('ISO'),
    exposureTime: z.string().nullable().describe('Exposure time'),
    profileDescription: z.string().nullable().describe('Profile description'),
    rating: z.int().nullable().describe('Rating'),
    fps: z.number().meta({ format: 'double' }).nullable().describe('FPS'),
  })
  .meta({ id: 'SyncAssetExifV1' });

const SyncAssetMetadataV1Schema = z
  .object({
    assetId: z.uuidv4().describe('Asset ID'),
    key: z.string().describe('Key'),
    value: z.record(z.string(), z.unknown()).describe('Value'),
  })
  .meta({ id: 'SyncAssetMetadataV1' });

const SyncAssetMetadataDeleteV1Schema = z
  .object({
    assetId: z.uuidv4().describe('Asset ID'),
    key: z.string().describe('Key'),
  })
  .meta({ id: 'SyncAssetMetadataDeleteV1' });

const SyncAssetEditV1Schema = z
  .object({
    id: z.uuidv4().describe('Edit ID'),
    assetId: z.uuidv4().describe('Asset ID'),
    action: AssetEditActionSchema,
    parameters: z.record(z.string(), z.unknown()).describe('Edit parameters'),
    sequence: z.int().describe('Edit sequence'),
  })
  .meta({ id: 'SyncAssetEditV1' });

const SyncAssetEditDeleteV1Schema = z
  .object({ editId: z.uuidv4().describe('Edit ID') })
  .meta({ id: 'SyncAssetEditDeleteV1' });

@ExtraModel()
class SyncAssetDeleteV1 extends createZodDto(SyncAssetDeleteV1Schema) {}
@ExtraModel()
export class SyncAssetExifV1 extends createZodDto(SyncAssetExifV1Schema) {}
@ExtraModel()
class SyncAssetMetadataV1 extends createZodDto(SyncAssetMetadataV1Schema) {}
@ExtraModel()
class SyncAssetMetadataDeleteV1 extends createZodDto(SyncAssetMetadataDeleteV1Schema) {}
@ExtraModel()
export class SyncAssetEditV1 extends createZodDto(SyncAssetEditV1Schema) {}
@ExtraModel()
class SyncAssetEditDeleteV1 extends createZodDto(SyncAssetEditDeleteV1Schema) {}

const SyncAlbumDeleteV1Schema = z
  .object({ albumId: z.uuidv4().describe('Album ID') })
  .meta({ id: 'SyncAlbumDeleteV1' });

const SyncAlbumUserDeleteV1Schema = z
  .object({
    albumId: z.uuidv4().describe('Album ID'),
    userId: z.uuidv4().describe('User ID'),
  })
  .meta({ id: 'SyncAlbumUserDeleteV1' });

const SyncAlbumUserV1Schema = z
  .object({
    albumId: z.uuidv4().describe('Album ID'),
    userId: z.uuidv4().describe('User ID'),
    role: AlbumUserRoleSchema,
  })
  .meta({ id: 'SyncAlbumUserV1' });

const SyncAlbumV1Schema = z
  .object({
    id: z.uuidv4().describe('Album ID'),
    ownerId: z.uuidv4().describe('Owner ID'),
    name: z.string().describe('Album name'),
    description: z.string().describe('Album description'),
    createdAt: isoDatetimeToDate.describe('Created at'),
    updatedAt: isoDatetimeToDate.describe('Updated at'),
    thumbnailAssetId: z.string().nullable().describe('Thumbnail asset ID'),
    isActivityEnabled: z.boolean().describe('Is activity enabled'),
    order: AssetOrderSchema,
  })
  .meta({ id: 'SyncAlbumV1' });

const SyncAlbumV2Schema = z
  .object({
    id: z.uuidv4().describe('Album ID'),
    name: z.string().describe('Album name'),
    description: z.string().describe('Album description'),
    createdAt: isoDatetimeToDate.describe('Created at'),
    updatedAt: isoDatetimeToDate.describe('Updated at'),
    thumbnailAssetId: z.string().nullable().describe('Thumbnail asset ID'),
    isActivityEnabled: z.boolean().describe('Is activity enabled'),
    order: AssetOrderSchema,
  })
  .meta({ id: 'SyncAlbumV2' });

const SyncAlbumToAssetV1Schema = z
  .object({
    albumId: z.uuidv4().describe('Album ID'),
    assetId: z.uuidv4().describe('Asset ID'),
  })
  .meta({ id: 'SyncAlbumToAssetV1' });

const SyncAlbumToAssetDeleteV1Schema = z
  .object({
    albumId: z.uuidv4().describe('Album ID'),
    assetId: z.uuidv4().describe('Asset ID'),
  })
  .meta({ id: 'SyncAlbumToAssetDeleteV1' });

@ExtraModel()
class SyncAlbumDeleteV1 extends createZodDto(SyncAlbumDeleteV1Schema) {}
@ExtraModel()
class SyncAlbumUserDeleteV1 extends createZodDto(SyncAlbumUserDeleteV1Schema) {}
@ExtraModel()
class SyncAlbumUserV1 extends createZodDto(SyncAlbumUserV1Schema) {}
@ExtraModel()
class SyncAlbumV1 extends createZodDto(SyncAlbumV1Schema) {}
@ExtraModel()
class SyncAlbumV2 extends createZodDto(SyncAlbumV2Schema) {}
@ExtraModel()
class SyncAlbumToAssetV1 extends createZodDto(SyncAlbumToAssetV1Schema) {}
@ExtraModel()
class SyncAlbumToAssetDeleteV1 extends createZodDto(SyncAlbumToAssetDeleteV1Schema) {}

export function syncAlbumV2ToV1(
  albumV2: SyncAlbumV2,
  albumUsers: { userId: string; role: AlbumUserRole }[],
): SyncAlbumV1 {
  const owner = albumUsers.find(({ role }) => role === AlbumUserRole.Owner)!;

  return { ...albumV2, ownerId: owner.userId };
}

const SyncMemoryV1Schema = z
  .object({
    id: z.uuidv4().describe('Memory ID'),
    createdAt: isoDatetimeToDate.describe('Created at'),
    updatedAt: isoDatetimeToDate.describe('Updated at'),
    deletedAt: isoDatetimeToDate.nullable().describe('Deleted at'),
    ownerId: z.uuidv4().describe('Owner ID'),
    type: MemoryTypeSchema.extract(['OnThisDay']).meta({ id: 'MemoryTypeV1' }),
    data: z.record(z.string(), z.unknown()).describe('Data'),
    isSaved: z.boolean().describe('Is saved'),
    memoryAt: isoDatetimeToDate.describe('Memory at'),
    seenAt: isoDatetimeToDate.nullable().describe('Seen at'),
    showAt: isoDatetimeToDate.nullable().describe('Show at'),
    hideAt: isoDatetimeToDate.nullable().describe('Hide at'),
  })
  .meta({ id: 'SyncMemoryV1' });

const SyncMemoryV2Schema = SyncMemoryV1Schema.extend({
  type: MemoryTypeSchema,
}).meta({ id: 'SyncMemoryV2' });

const SyncMemoryDeleteV1Schema = z
  .object({ memoryId: z.uuidv4().describe('Memory ID') })
  .meta({ id: 'SyncMemoryDeleteV1' });

const SyncMemoryAssetV1Schema = z
  .object({
    memoryId: z.uuidv4().describe('Memory ID'),
    assetId: z.uuidv4().describe('Asset ID'),
  })
  .meta({ id: 'SyncMemoryAssetV1' });

const SyncMemoryAssetDeleteV1Schema = z
  .object({
    memoryId: z.uuidv4().describe('Memory ID'),
    assetId: z.uuidv4().describe('Asset ID'),
  })
  .meta({ id: 'SyncMemoryAssetDeleteV1' });

const SyncStackV1Schema = z
  .object({
    id: z.uuidv4().describe('Stack ID'),
    createdAt: isoDatetimeToDate.describe('Created at'),
    updatedAt: isoDatetimeToDate.describe('Updated at'),
    primaryAssetId: z.uuidv4().describe('Primary asset ID'),
    ownerId: z.uuidv4().describe('Owner ID'),
  })
  .meta({ id: 'SyncStackV1' });

const SyncStackDeleteV1Schema = z
  .object({ stackId: z.uuidv4().describe('Stack ID') })
  .meta({ id: 'SyncStackDeleteV1' });

const SyncUserMetadataV1Schema = z
  .object({
    userId: z.uuidv4().describe('User ID'),
    key: UserMetadataKeySchema,
    value: z.record(z.string(), z.unknown()).describe('User metadata value'),
  })
  .meta({ id: 'SyncUserMetadataV1' });

const SyncUserMetadataDeleteV1Schema = z
  .object({
    userId: z.uuidv4().describe('User ID'),
    key: UserMetadataKeySchema,
  })
  .meta({ id: 'SyncUserMetadataDeleteV1' });

const SyncAckV1Schema = z.object({}).meta({ id: 'SyncAckV1' });
const SyncResetV1Schema = z.object({}).meta({ id: 'SyncResetV1' });
const SyncCompleteV1Schema = z.object({}).meta({ id: 'SyncCompleteV1' });

@ExtraModel()
class SyncMemoryV1 extends createZodDto(SyncMemoryV1Schema) {}
@ExtraModel()
class SyncMemoryV2 extends createZodDto(SyncMemoryV2Schema) {}
@ExtraModel()
class SyncMemoryDeleteV1 extends createZodDto(SyncMemoryDeleteV1Schema) {}
@ExtraModel()
class SyncMemoryAssetV1 extends createZodDto(SyncMemoryAssetV1Schema) {}
@ExtraModel()
class SyncMemoryAssetDeleteV1 extends createZodDto(SyncMemoryAssetDeleteV1Schema) {}

@ExtraModel()
class SyncStackV1 extends createZodDto(SyncStackV1Schema) {}
@ExtraModel()
class SyncStackDeleteV1 extends createZodDto(SyncStackDeleteV1Schema) {}
@ExtraModel()
class SyncUserMetadataV1 extends createZodDto(SyncUserMetadataV1Schema) {}
@ExtraModel()
class SyncUserMetadataDeleteV1 extends createZodDto(SyncUserMetadataDeleteV1Schema) {}
@ExtraModel()
class SyncAckV1 extends createZodDto(SyncAckV1Schema) {}
@ExtraModel()
class SyncResetV1 extends createZodDto(SyncResetV1Schema) {}
@ExtraModel()
class SyncCompleteV1 extends createZodDto(SyncCompleteV1Schema) {}

export type SyncItem = {
  [SyncEntityType.AuthUserV1]: SyncAuthUserV1;
  [SyncEntityType.AuthUserV2]: SyncAuthUserV2;
  [SyncEntityType.UserV1]: SyncUserV1;
  [SyncEntityType.UserDeleteV1]: SyncUserDeleteV1;
  [SyncEntityType.PartnerV1]: SyncPartnerV1;
  [SyncEntityType.PartnerDeleteV1]: SyncPartnerDeleteV1;
  [SyncEntityType.AssetV2]: SyncAssetV2;
  [SyncEntityType.AssetDeleteV1]: SyncAssetDeleteV1;
  [SyncEntityType.AssetMetadataV1]: SyncAssetMetadataV1;
  [SyncEntityType.AssetMetadataDeleteV1]: SyncAssetMetadataDeleteV1;
  [SyncEntityType.AssetExifV1]: SyncAssetExifV1;
  [SyncEntityType.AssetEditV1]: SyncAssetEditV1;
  [SyncEntityType.AssetEditDeleteV1]: SyncAssetEditDeleteV1;
  [SyncEntityType.PartnerAssetV2]: SyncAssetV2;
  [SyncEntityType.PartnerAssetBackfillV2]: SyncAssetV2;
  [SyncEntityType.PartnerAssetDeleteV1]: SyncAssetDeleteV1;
  [SyncEntityType.PartnerAssetExifV1]: SyncAssetExifV1;
  [SyncEntityType.PartnerAssetExifBackfillV1]: SyncAssetExifV1;
  [SyncEntityType.AlbumV1]: SyncAlbumV1;
  [SyncEntityType.AlbumV2]: SyncAlbumV2;
  [SyncEntityType.AlbumDeleteV1]: SyncAlbumDeleteV1;
  [SyncEntityType.AlbumUserV1]: SyncAlbumUserV1;
  [SyncEntityType.AlbumUserBackfillV1]: SyncAlbumUserV1;
  [SyncEntityType.AlbumUserDeleteV1]: SyncAlbumUserDeleteV1;
  [SyncEntityType.AlbumAssetCreateV2]: SyncAssetV2;
  [SyncEntityType.AlbumAssetUpdateV2]: SyncAssetV2;
  [SyncEntityType.AlbumAssetBackfillV2]: SyncAssetV2;
  [SyncEntityType.AlbumAssetExifCreateV1]: SyncAssetExifV1;
  [SyncEntityType.AlbumAssetExifUpdateV1]: SyncAssetExifV1;
  [SyncEntityType.AlbumAssetExifBackfillV1]: SyncAssetExifV1;
  [SyncEntityType.AlbumToAssetV1]: SyncAlbumToAssetV1;
  [SyncEntityType.AlbumToAssetBackfillV1]: SyncAlbumToAssetV1;
  [SyncEntityType.AlbumToAssetDeleteV1]: SyncAlbumToAssetDeleteV1;
  [SyncEntityType.MemoryV1]: SyncMemoryV1;
  [SyncEntityType.MemoryV2]: SyncMemoryV2;
  [SyncEntityType.MemoryDeleteV1]: SyncMemoryDeleteV1;
  [SyncEntityType.MemoryToAssetV1]: SyncMemoryAssetV1;
  [SyncEntityType.MemoryToAssetV2]: SyncMemoryAssetV1;
  [SyncEntityType.MemoryToAssetDeleteV1]: SyncMemoryAssetDeleteV1;
  [SyncEntityType.StackV1]: SyncStackV1;
  [SyncEntityType.StackDeleteV1]: SyncStackDeleteV1;
  [SyncEntityType.PartnerStackBackfillV1]: SyncStackV1;
  [SyncEntityType.PartnerStackDeleteV1]: SyncStackDeleteV1;
  [SyncEntityType.PartnerStackV1]: SyncStackV1;
  [SyncEntityType.UserMetadataV1]: SyncUserMetadataV1;
  [SyncEntityType.UserMetadataDeleteV1]: SyncUserMetadataDeleteV1;
  [SyncEntityType.SyncAckV1]: SyncAckV1;
  [SyncEntityType.SyncCompleteV1]: SyncCompleteV1;
  [SyncEntityType.SyncResetV1]: SyncResetV1;
};

const SyncStreamSchema = z
  .object({
    types: z.array(SyncRequestTypeSchema).describe('Sync request types'),
    reset: z.boolean().optional().describe('Reset sync state'),
  })
  .meta({ id: 'SyncStreamDto' });

const SyncAckSchema = z
  .object({
    type: SyncEntityTypeSchema,
    ack: z.string().describe('Acknowledgment ID'),
  })
  .meta({ id: 'SyncAckDto' });

const SyncAckSetSchema = z
  .object({
    acks: z.array(z.string()).max(1000).describe('Acknowledgment IDs (max 1000)'),
  })
  .meta({ id: 'SyncAckSetDto' });

const SyncAckDeleteSchema = z
  .object({
    types: z.array(SyncEntityTypeSchema).optional().describe('Sync entity types to delete acks for'),
  })
  .meta({ id: 'SyncAckDeleteDto' });

export class SyncStreamDto extends createZodDto(SyncStreamSchema) {}
export class SyncAckDto extends createZodDto(SyncAckSchema) {}
export class SyncAckSetDto extends createZodDto(SyncAckSetSchema) {}
export class SyncAckDeleteDto extends createZodDto(SyncAckDeleteSchema) {}
