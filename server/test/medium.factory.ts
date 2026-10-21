import { Insertable, Kysely } from 'kysely';
import { DateTime } from 'luxon';
import { createHash, randomBytes } from 'node:crypto';
import { Stats } from 'node:fs';
import { resolve } from 'node:path';
import { Writable } from 'node:stream';
import { Mocked } from 'vitest';
import type { ClassConstructor, ClassConstructorsToInstances, UploadFile } from 'src/types.js';
import { AuthDto, LoginResponseDto } from 'src/dtos/auth.dto.js';
import { SystemConfig } from 'src/dtos/config.dto.js';
import { AssetEditActionItem, AssetEditsCreateDto } from 'src/dtos/editing.dto.js';
import {
  AlbumUserRole,
  AssetType,
  AssetVisibility,
  ChecksumAlgorithm,
  MemoryType,
  SyncEntityType,
  SyncRequestType,
} from 'src/enum.js';
import { AccessRepository } from 'src/repositories/access.repository.js';
import { ActivityRepository } from 'src/repositories/activity.repository.js';
import { AlbumUserRepository } from 'src/repositories/album-user.repository.js';
import { AlbumRepository } from 'src/repositories/album.repository.js';
import { ApiKeyRepository } from 'src/repositories/api-key.repository.js';
import { AssetEditRepository } from 'src/repositories/asset-edit.repository.js';
import { AssetFileRepository } from 'src/repositories/asset-file.repository.js';
import { AssetJobRepository } from 'src/repositories/asset-job.repository.js';
import { AssetRepository } from 'src/repositories/asset.repository.js';
import { AuthChallengeRepository } from 'src/repositories/auth-challenge.repository.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { CronRepository } from 'src/repositories/cron.repository.js';
import { CryptoRepository } from 'src/repositories/crypto.repository.js';
import { DatabaseRepository } from 'src/repositories/database.repository.js';
import { EmailRepository } from 'src/repositories/email.repository.js';
import { EventRepository } from 'src/repositories/event.repository.js';
import { IntegrityRepository } from 'src/repositories/integrity.repository.js';
import { JobRepository } from 'src/repositories/job.repository.js';
import { LibraryRepository } from 'src/repositories/library.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { MapRepository } from 'src/repositories/map.repository.js';
import { MediaRepository } from 'src/repositories/media.repository.js';
import { MemoryRepository } from 'src/repositories/memory.repository.js';
import { MetadataRepository } from 'src/repositories/metadata.repository.js';
import { NotificationRepository } from 'src/repositories/notification.repository.js';
import { PartnerRepository } from 'src/repositories/partner.repository.js';
import { PasskeyRepository } from 'src/repositories/passkey.repository.js';
import { PluginRepository } from 'src/repositories/plugin.repository.js';
import { SearchRepository } from 'src/repositories/search.repository.js';
import { SessionRepository } from 'src/repositories/session.repository.js';
import { SharedLinkAssetRepository } from 'src/repositories/shared-link-asset.repository.js';
import { SharedLinkRepository } from 'src/repositories/shared-link.repository.js';
import { StackRepository } from 'src/repositories/stack.repository.js';
import { StorageRepository } from 'src/repositories/storage.repository.js';
import { SyncCheckpointRepository } from 'src/repositories/sync-checkpoint.repository.js';
import { SyncRepository } from 'src/repositories/sync.repository.js';
import { SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { TagRepository } from 'src/repositories/tag.repository.js';
import { TelemetryRepository } from 'src/repositories/telemetry.repository.js';
import { UserRepository } from 'src/repositories/user.repository.js';
import { VersionHistoryRepository } from 'src/repositories/version-history.repository.js';
import { WebAuthnRepository } from 'src/repositories/webauthn.repository.js';
import { WebsocketRepository } from 'src/repositories/websocket.repository.js';
import { WorkflowRepository } from 'src/repositories/workflow.repository.js';
import { DB } from 'src/schema/index.js';
import { AlbumTable } from 'src/schema/tables/album.table.js';
import { AssetExifTable } from 'src/schema/tables/asset-exif.table.js';
import { AssetFileTable } from 'src/schema/tables/asset-file.table.js';
import { AssetJobStatusTable } from 'src/schema/tables/asset-job-status.table.js';
import { AssetMetadataTable } from 'src/schema/tables/asset-metadata.table.js';
import { AssetTable } from 'src/schema/tables/asset.table.js';
import { MemoryTable } from 'src/schema/tables/memory.table.js';
import { PasskeyTable } from 'src/schema/tables/passkey.table.js';
import { SessionTable } from 'src/schema/tables/session.table.js';
import { StackTable } from 'src/schema/tables/stack.table.js';
import { TagAssetTable } from 'src/schema/tables/tag-asset.table.js';
import { TagTable } from 'src/schema/tables/tag.table.js';
import { UserTable } from 'src/schema/tables/user.table.js';
import { BASE_SERVICE_DEPENDENCIES, BaseService } from 'src/services/base.service.js';
import { MetadataService } from 'src/services/metadata.service.js';
import { SyncService } from 'src/services/sync.service.js';
import { getConfig, updateConfig } from 'src/utils/config.js';
import { mockEnvData } from 'test/repositories/config.repository.mock.js';
import { newTelemetryRepositoryMock } from 'test/repositories/telemetry.repository.mock.js';
import { factory, newDate, newUuid } from 'test/small.factory.js';
import { automock, wait } from 'test/utils.js';

export const testAssetsDir = resolve(import.meta.dirname, '../../e2e/test-assets');

type MediumTestOptions = {
  mock: Array<(typeof BASE_SERVICE_DEPENDENCIES)[number]>;
  real: Array<(typeof BASE_SERVICE_DEPENDENCIES)[number]>;
  database: Kysely<DB>;
};

type BaseServiceDeps = typeof BASE_SERVICE_DEPENDENCIES;

export const newMediumService = <S extends ClassConstructor<typeof BaseService>>(
  Service: S,
  options: MediumTestOptions,
) => {
  const ctx = new MediumTestContext(Service, options);
  return { sut: ctx.sut, ctx };
};

export class MediumTestContext<S extends ClassConstructor<typeof BaseService> = ClassConstructor<typeof BaseService>> {
  private repoCache: Record<string, any> = {};
  private sutDeps: ClassConstructorsToInstances<BaseServiceDeps>;

  sut: InstanceType<S>;
  database: Kysely<DB>;

  constructor(
    Service: S,
    private options: MediumTestOptions,
  ) {
    this.sutDeps = this.makeDeps(options);
    this.sut = new Service(...this.sutDeps) as InstanceType<S>;
    this.database = options.database;
  }

  private makeDeps(options: MediumTestOptions) {
    const deps = BASE_SERVICE_DEPENDENCIES;

    for (const dep of options.mock) {
      if (!deps.includes(dep)) {
        throw new Error(`Mocked repository ${dep.name} is not a valid dependency`);
      }
    }

    for (const dep of options.real) {
      if (!deps.includes(dep)) {
        throw new Error(`Real repository ${dep.name} is not a valid dependency`);
      }
    }
    return deps.map((dep) => {
      if (options.real.includes(dep)) {
        return this.get(dep);
      }

      if (options.mock.includes(dep)) {
        return newMockRepository(dep);
      }
    }) as unknown as ClassConstructorsToInstances<BaseServiceDeps>;
  }

  get<T extends BaseServiceDeps[number]>(key: T): InstanceType<T> {
    if (!Object.hasOwn(this.repoCache, key.name)) {
      const real = newRealRepository(key, this.options.database);
      this.repoCache[key.name] = real;
    }

    return this.repoCache[key.name];
  }

  getMock<T extends BaseServiceDeps[number], R = Mocked<InstanceType<T>>>(key: T): R {
    const index = BASE_SERVICE_DEPENDENCIES.indexOf(key);
    if (index === -1 || !this.options.mock.includes(key)) {
      throw new Error(`getMock called with a key that is not a mock: ${key.name}`);
    }

    return this.sutDeps[index] as R;
  }

  async newUser(dto: Partial<Insertable<UserTable>> = {}) {
    const user = mediumFactory.userInsert(dto);
    const result = await this.get(UserRepository).create(user);
    return { user, result };
  }

  async newPasskey(dto: Partial<Insertable<PasskeyTable>> & { userId: string }) {
    const passkey = mediumFactory.passkeyInsert(dto);
    const result = await this.get(PasskeyRepository).create(passkey);
    return { passkey, result };
  }

  async newPartner(dto: { sharedById: string; sharedWithId: string; inTimeline?: boolean }) {
    const partner = { inTimeline: true, ...dto };
    const result = await this.get(PartnerRepository).create(partner);
    return { partner, result };
  }

  async newStack(dto: Omit<Insertable<StackTable>, 'primaryAssetId'>, assetIds: string[]) {
    const date = factory.date();
    const stack = {
      id: factory.uuid(),
      createdAt: date,
      updatedAt: date,
      ...dto,
    };

    const result = await this.get(StackRepository).create(stack, assetIds);
    return { stack: { ...stack, primaryAssetId: assetIds[0] }, result };
  }

  async newAsset(dto: Partial<Insertable<AssetTable>> = {}) {
    const asset = mediumFactory.assetInsert(dto);
    const result = await this.get(AssetRepository).create(asset);
    return { asset, result };
  }

  async newMetadata(dto: Insertable<AssetMetadataTable>) {
    const { assetId, ...item } = dto;
    const result = await this.get(AssetRepository).upsertMetadata(assetId, [item]);
    return { metadata: dto, result };
  }

  async newAssetFile(dto: Insertable<AssetFileTable>) {
    const result = await this.get(AssetRepository).upsertFile(dto);
    return { result };
  }

  async newMemory(dto: Partial<Insertable<MemoryTable>> = {}) {
    const memory = mediumFactory.memoryInsert(dto);
    const result = await this.get(MemoryRepository).create(memory, new Set<string>());
    return { memory, result };
  }

  async newMemoryAsset(dto: { memoryId: string; assetId: string }) {
    const result = await this.get(MemoryRepository).addAssetIds(dto.memoryId, [dto.assetId]);
    return { memoryAsset: dto, result };
  }

  async newExif(dto: Insertable<AssetExifTable>) {
    const result = await this.get(AssetRepository).upsertExif({ exif: dto, lockedPropertiesBehavior: 'override' });
    return { result };
  }

  async newAlbum({ ownerId, ...dto }: Insertable<AlbumTable> & { ownerId: string }, assetIds?: string[]) {
    const album = mediumFactory.albumInsert(dto);
    const result = await this.get(AlbumRepository).create(
      album,
      assetIds ?? [],
      [{ userId: ownerId, role: AlbumUserRole.Owner }],
      ownerId,
    );
    return { album, result };
  }

  async newAlbumAsset(albumAsset: { albumId: string; assetId: string }) {
    const result = await this.get(AlbumRepository).addAssetIds(albumAsset.albumId, [albumAsset.assetId]);
    return { albumAsset, result };
  }

  async newAlbumUser(dto: { albumId: string; userId: string; role?: AlbumUserRole }) {
    const { albumId, userId, role = AlbumUserRole.Editor } = dto;
    const result = await this.get(AlbumUserRepository).create({ albumId, userId, role });
    return { albumUser: { albumId, userId, role }, result };
  }

  /** An album owned by one user, containing one asset, shared with a second user */
  async newSharedAlbum(dto: { role?: AlbumUserRole } = {}) {
    const { user: owner } = await this.newUser();
    const { user: sharedWith } = await this.newUser();
    const { asset } = await this.newAsset({ ownerId: owner.id });
    const { album } = await this.newAlbum({ ownerId: owner.id }, [asset.id]);
    await this.newAlbumUser({ albumId: album.id, userId: sharedWith.id, role: dto.role ?? AlbumUserRole.Editor });

    return { album, asset, owner, sharedWith };
  }

  async softDeleteAsset(assetId: string) {
    await this.database.updateTable('asset').set({ deletedAt: new Date() }).where('id', '=', assetId).execute();
  }

  async softDeleteAlbum(albumId: string) {
    await this.database.updateTable('album').set({ deletedAt: new Date() }).where('id', '=', albumId).execute();
  }

  async newJobStatus(dto: Partial<Insertable<AssetJobStatusTable>> & { assetId: string }) {
    const jobStatus = mediumFactory.assetJobStatusInsert({ assetId: dto.assetId });
    const result = await this.get(AssetRepository).upsertJobStatus(jobStatus);
    return { jobStatus, result };
  }

  async newSession(dto: Partial<Insertable<SessionTable>> & { userId: string }) {
    const session = mediumFactory.sessionInsert(dto);
    const result = await this.get(SessionRepository).create(session);
    return { session, result };
  }

  async newSyncAuthUser() {
    const { user } = await this.newUser();
    const { session } = await this.newSession({ userId: user.id });
    const auth = factory.auth({
      session,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
      },
    });

    return {
      auth,
      session,
      user,
    };
  }

  async newTag(dto: Insertable<TagTable>) {
    const tag = mediumFactory.tagInsert(dto);
    const result = await this.get(TagRepository).create(tag);
    return { tag, result };
  }

  async newTagAsset(tagBulkAssets: { tagIds: string[]; assetIds: string[] }) {
    const tagsAssets: Insertable<TagAssetTable>[] = [];
    for (const tagId of tagBulkAssets.tagIds) {
      for (const assetId of tagBulkAssets.assetIds) {
        tagsAssets.push({ tagId, assetId });
      }
    }

    const result = await this.get(TagRepository).upsertAssetIds(tagsAssets);
    return { tagsAssets, result };
  }

  async newEdits(assetId: string, dto: AssetEditsCreateDto) {
    const edits = await this.get(AssetEditRepository).replaceAll(assetId, dto.edits as AssetEditActionItem[]);
    return { edits };
  }

  async getConfig({ withCache = true }: { withCache?: boolean } = {}) {
    return getConfig(
      {
        configRepo: this.get(ConfigRepository),
        metadataRepo: this.get(SystemMetadataRepository),
        logger: this.get(LoggingRepository),
      },
      { withCache },
    );
  }

  async updateConfig(config: SystemConfig) {
    return updateConfig(
      {
        configRepo: this.get(ConfigRepository),
        metadataRepo: this.get(SystemMetadataRepository),
        logger: this.get(LoggingRepository),
      },
      config,
    );
  }
}

export class SyncTestContext extends MediumTestContext<typeof SyncService> {
  constructor(database: Kysely<DB>) {
    super(SyncService, {
      database,
      real: [SyncRepository, SyncCheckpointRepository, SessionRepository],
      mock: [LoggingRepository],
    });
  }

  async syncStream(auth: AuthDto, types: SyncRequestType[], shouldReset?: boolean) {
    const stream = mediumFactory.syncStream();
    // Wait for 2ms to ensure all updates are available and account for setTimeout inaccuracy
    await wait(2);
    await this.sut.stream(auth, stream, { types, reset: shouldReset });

    return stream.getResponse();
  }

  async assertSyncIsComplete(auth: AuthDto, types: SyncRequestType[]) {
    await expect(this.syncStream(auth, types)).resolves.toEqual([
      expect.objectContaining({ type: SyncEntityType.SyncCompleteV1 }),
    ]);
  }

  async syncAckAll(auth: AuthDto, response: Array<{ type: string; ack: string }>) {
    const acks: Record<string, string> = {};
    const syncAcks: string[] = [];
    for (const { type, ack } of response) {
      if (type === SyncEntityType.SyncAckV1) {
        syncAcks.push(ack);
        continue;
      }
      acks[type] = ack;
    }

    await this.sut.setAcks(auth, { acks: [...Object.values(acks), ...syncAcks] });
  }
}

const mockDate = new Date('2024-06-01T12:00:00.000Z');
const mockStats = {
  mtime: mockDate,
  atime: mockDate,
  ctime: mockDate,
  birthtime: mockDate,
  atimeMs: 0,
  mtimeMs: 0,
  ctimeMs: 0,
  birthtimeMs: 0,
};

export class ExifTestContext extends MediumTestContext<typeof MetadataService> {
  constructor(database: Kysely<DB>) {
    super(MetadataService, {
      database,
      real: [
        AssetRepository,
        AssetJobRepository,
        MediaRepository,
        MetadataRepository,
        SystemMetadataRepository,
        TagRepository,
      ],
      mock: [ConfigRepository, EventRepository, LoggingRepository, MapRepository, StorageRepository],
    });

    this.getMock(ConfigRepository).getEnv.mockReturnValue(mockEnvData({}));
    this.getMock(EventRepository).emit.mockResolvedValue();
    this.getMock(MapRepository).reverseGeocode.mockResolvedValue({ country: null, state: null, city: null });
    this.getMock(StorageRepository).stat.mockResolvedValue(mockStats as Stats);
  }

  getMockStats() {
    return mockStats;
  }

  getGps(assetId: string) {
    return this.database
      .selectFrom('asset_exif')
      .select(['latitude', 'longitude'])
      .where('assetId', '=', assetId)
      .executeTakeFirstOrThrow();
  }

  getTags(assetId: string) {
    return this.database
      .selectFrom('tag')
      .innerJoin('tag_asset', 'tag.id', 'tag_asset.tagId')
      .where('tag_asset.assetId', '=', assetId)
      .selectAll()
      .execute();
  }

  getDates(assetId: string) {
    return this.database
      .selectFrom('asset')
      .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
      .where('id', '=', assetId)
      .select(['asset.fileCreatedAt', 'asset.localDateTime', 'asset_exif.dateTimeOriginal', 'asset_exif.timeZone'])
      .executeTakeFirstOrThrow();
  }
}

const newRealRepository = <T extends BaseServiceDeps[number]>(key: T, db: Kysely<DB>): InstanceType<T> => {
  switch (key) {
    case AccessRepository:
    case AlbumRepository:
    case AlbumUserRepository:
    case ActivityRepository:
    case ApiKeyRepository:
    case AssetRepository:
    case AssetEditRepository:
    case AssetFileRepository:
    case AssetJobRepository:
    case IntegrityRepository:
    case MemoryRepository:
    case LibraryRepository:
    case NotificationRepository:
    case AuthChallengeRepository:
    case PartnerRepository:
    case PasskeyRepository:
    case SearchRepository:
    case SessionRepository:
    case SharedLinkRepository:
    case SharedLinkAssetRepository:
    case StackRepository:
    case SyncRepository:
    case SyncCheckpointRepository:
    case SystemMetadataRepository:
    case UserRepository:
    case VersionHistoryRepository:
    case WorkflowRepository: {
      return new key(db) as InstanceType<T>;
    }

    case ConfigRepository:
    case CryptoRepository:
    case WebAuthnRepository: {
      return new key() as InstanceType<T>;
    }

    case DatabaseRepository: {
      return new key(db, LoggingRepository.create(), new ConfigRepository()) as InstanceType<T>;
    }

    case EmailRepository: {
      return new key(LoggingRepository.create()) as InstanceType<T>;
    }

    case MediaRepository:
    case MetadataRepository: {
      return new key(LoggingRepository.create()) as InstanceType<T>;
    }

    case PluginRepository: {
      return new key(db, LoggingRepository.create()) as InstanceType<T>;
    }

    case StorageRepository: {
      return new key(LoggingRepository.create()) as InstanceType<T>;
    }

    case TagRepository: {
      return new key(db, LoggingRepository.create()) as InstanceType<T>;
    }

    case LoggingRepository: {
      return new key(undefined, undefined) as InstanceType<T>;
    }

    default: {
      throw new Error(`Unable to create repository instance for key: ${key?.name || key}`);
    }
  }
};

const newMockRepository = <T>(key: ClassConstructor<T>) => {
  switch (key) {
    case ActivityRepository:
    case AlbumRepository:
    case AssetRepository:
    case AssetJobRepository:
    case AuthChallengeRepository:
    case ConfigRepository:
    case CryptoRepository:
    case LibraryRepository:
    case MemoryRepository:
    case IntegrityRepository:
    case NotificationRepository:
    case PartnerRepository:
    case PasskeyRepository:
    case SessionRepository:
    case SyncRepository:
    case SyncCheckpointRepository:
    case SystemMetadataRepository:
    case UserRepository:
    case VersionHistoryRepository:
    case TagRepository:
    case WorkflowRepository: {
      return automock(key);
    }

    case MapRepository: {
      return automock(MapRepository, { args: [undefined, undefined, { setContext: () => {} }] });
    }

    case TelemetryRepository: {
      return newTelemetryRepositoryMock();
    }

    case DatabaseRepository: {
      return automock(DatabaseRepository, {
        args: [undefined, { setContext: () => {} }, { getEnv: () => ({ database: { vectorExtension: '' } }) }],
      });
    }

    case CronRepository: {
      return automock(CronRepository, { args: [undefined, { setContext: () => {} }], strict: false });
    }

    case EmailRepository: {
      return automock(EmailRepository, { args: [{ setContext: () => {} }] });
    }

    case EventRepository: {
      return automock(EventRepository, { args: [undefined, undefined, { setContext: () => {} }] });
    }

    case JobRepository: {
      return automock(JobRepository, {
        args: [
          undefined,
          undefined,
          undefined,
          {
            setContext: () => {},
          },
        ],
      });
    }

    case LoggingRepository as unknown as ClassConstructor<T>: {
      const configMock = { getEnv: () => ({ noColor: false }) };
      return automock(LoggingRepository, { args: [undefined, configMock], strict: false });
    }

    case StorageRepository: {
      return automock(StorageRepository, { args: [{ setContext: () => {} }] });
    }

    case WebsocketRepository: {
      return automock(WebsocketRepository, { args: [undefined, { setContext: () => {} }] });
    }

    default: {
      throw new Error(`Invalid repository key: ${key}`);
    }
  }
};

const assetInsert = (asset: Partial<Insertable<AssetTable>> = {}) => {
  const id = asset.id || newUuid();
  const now = newDate();
  const defaults: Insertable<AssetTable> = {
    originalFileName: '',
    checksum: randomBytes(32),
    checksumAlgorithm: ChecksumAlgorithm.sha1File,
    type: AssetType.Image,
    originalPath: '/path/to/something.jpg',
    ownerId: 'not-a-valid-uuid',
    isFavorite: false,
    fileCreatedAt: now,
    fileModifiedAt: now,
    localDateTime: now,
    visibility: AssetVisibility.Timeline,
    isEdited: false,
  };

  return {
    ...defaults,
    ...asset,
    id,
  };
};

const albumInsert = (album: Partial<Insertable<AlbumTable>>) => {
  const id = album.id || newUuid();
  const defaults: Insertable<AlbumTable> = {
    albumName: 'Album',
  };

  return {
    ...defaults,
    ...album,
    id,
  };
};

const assetJobStatusInsert = (
  job: Partial<Insertable<AssetJobStatusTable>> & { assetId: string },
): Insertable<AssetJobStatusTable> => {
  const date = DateTime.now().minus({ days: 15 }).toISO();
  const defaults: Omit<Insertable<AssetJobStatusTable>, 'assetId'> = {
    metadataExtractedAt: date,
  };

  return {
    ...defaults,
    ...job,
  };
};

const sha256 = (value: string) => createHash('sha256').update(value).digest();

const sessionInsert = ({
  id = newUuid(),
  userId,
  ...session
}: Partial<Insertable<SessionTable>> & { userId: string }) => {
  const defaults: Insertable<SessionTable> = {
    id,
    userId,
    isPendingSyncReset: false,
    token: sha256(id),
  };

  return {
    ...defaults,
    ...session,
    id,
  };
};

const passkeyInsert = ({
  id = newUuid(),
  userId,
  ...passkey
}: Partial<Insertable<PasskeyTable>> & { userId: string }) => {
  const defaults: Insertable<PasskeyTable> = {
    id,
    userId,
    name: 'Passkey',
    credentialId: `credential-${id}`,
    publicKey: Buffer.from('public-key'),
    counter: 0,
    transports: ['internal'],
    backedUp: false,
    deviceType: 'singleDevice',
  };

  return {
    ...defaults,
    ...passkey,
    id,
  };
};

const userInsert = (user: Partial<Insertable<UserTable>> = {}) => {
  const id = user.id || newUuid();

  const defaults = {
    email: `${id}@immich.cloud`,
    name: `User ${id}`,
    deletedAt: null,
    isAdmin: false,
    profileImagePath: '',
    profileChangedAt: newDate(),
    shouldChangePassword: true,
    storageLabel: null,
    pinCode: null,
    oauthId: null,
    avatarColor: null,
    quotaSizeInBytes: null,
    quotaUsageInBytes: 0,
  };

  return { ...defaults, ...user, id };
};

const memoryInsert = (memory: Partial<Insertable<MemoryTable>> = {}) => {
  const id = memory.id || newUuid();
  const date = newDate();

  const defaults: Insertable<MemoryTable> = {
    id,
    createdAt: date,
    updatedAt: date,
    deletedAt: null,
    type: MemoryType.OnThisDay,
    data: { year: 2025 },
    showAt: null,
    hideAt: null,
    seenAt: null,
    isSaved: false,
    memoryAt: date,
    ownerId: memory.ownerId || newUuid(),
  };

  return { ...defaults, ...memory, id };
};

const tagInsert = (tag: Partial<Insertable<TagTable>>) => {
  const id = tag.id || newUuid();

  const defaults: Insertable<TagTable> = {
    id,
    userId: '',
    value: '',
    createdAt: newDate(),
    updatedAt: newDate(),
    color: '',
    parentId: null,
    updateId: newUuid(),
  };

  return { ...defaults, ...tag, id };
};

class CustomWritable extends Writable {
  private data = '';

  // eslint-disable-next-line unicorn/prefer-private-class-fields
  _write(chunk: any, encoding: string, callback: () => void) {
    this.data += chunk.toString();
    callback();
  }

  getResponse() {
    const result = this.data;
    return result
      .split('\n')
      .filter((x) => x.length > 0)
      .map((x) => JSON.parse(x));
  }
}

const syncStream = () => {
  return new CustomWritable();
};

const loginDetails = () => {
  return { isSecure: false, clientIp: '', deviceType: '', deviceOS: '', appVersion: null };
};

const loginResponse = (): LoginResponseDto => {
  const user = userInsert();
  return {
    accessToken: 'access-token',
    userId: user.id,
    userEmail: user.email,
    name: user.name,
    profileImagePath: user.profileImagePath,
    isAdmin: user.isAdmin,
    shouldChangePassword: user.shouldChangePassword,
    isOnboarded: false,
  };
};

const uploadFile = (file: Partial<UploadFile> = {}) => {
  return {
    uuid: newUuid(),
    checksum: randomBytes(32),
    originalPath: '/path/to/file.jpg',
    originalName: 'file.jpg',
    size: 123_456,
    ...file,
  };
};

export const mediumFactory = {
  assetInsert,
  assetJobStatusInsert,
  albumInsert,
  passkeyInsert,
  sessionInsert,
  syncStream,
  userInsert,
  memoryInsert,
  loginDetails,
  loginResponse,
  tagInsert,
  uploadFile,
};
