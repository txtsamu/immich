import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Kysely, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import type { SyncAck } from 'src/types.js';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { MemoryType } from 'src/enum.js';
import { DB } from 'src/schema/index.js';

export type SyncBackfillOptions = {
  nowId: string;
  afterUpdateId?: string;
  beforeUpdateId: string;
};

const dummyBackfillOptions = {
  nowId: DummyValue.UUID,
  beforeUpdateId: DummyValue.UUID,
  afterUpdateId: DummyValue.UUID,
};

export type SyncCreatedAfterOptions = {
  nowId: string;
  userId: string;
  afterCreateId?: string;
};

const dummyCreateAfterOptions = {
  nowId: DummyValue.UUID,
  userId: DummyValue.UUID,
  afterCreateId: DummyValue.UUID,
};

export type SyncQueryOptions = {
  nowId: string;
  userId: string;
  ack?: SyncAck;
};

const dummyQueryOptions = {
  nowId: DummyValue.UUID,
  userId: DummyValue.UUID,
  ack: {
    updateId: DummyValue.UUID,
  },
};

const makeSyncService = (db: Kysely<DB>) => {
  // `db.dynamic` is read lazily: unit tests construct the repository with a stub Kysely
  const dyn = () => db.dynamic;

  const backfillQuery = <T extends keyof DB>(t: T, { nowId, beforeUpdateId, afterUpdateId }: SyncBackfillOptions) => {
    const { table, ref } = dyn();
    const updateIdRef = ref(`${t}.updateId`);

    return db
      .selectFrom(table(t).as(t))
      .where(updateIdRef, '<', nowId)
      .where(updateIdRef, '<=', beforeUpdateId)
      .$if(!!afterUpdateId, (qb) => qb.where(updateIdRef, '>', afterUpdateId!))
      .orderBy(updateIdRef, 'asc');
  };

  const auditQuery = <T extends keyof DB>(t: T, { nowId, ack }: SyncQueryOptions) => {
    const { table, ref } = dyn();
    const idRef = ref(`${t}.id`);

    return db
      .selectFrom(table(t).as(t))
      .where(idRef, '<', nowId)
      .$if(!!ack, (qb) => qb.where(idRef, '>', ack!.updateId))
      .orderBy(idRef, 'asc');
  };

  const upsertQuery = <T extends keyof DB>(t: T, { nowId, ack }: SyncQueryOptions) => {
    const { table, ref } = dyn();
    const updateIdRef = ref(`${t}.updateId`);

    return db
      .selectFrom(table(t).as(t))
      .where(updateIdRef, '<', nowId)
      .$if(!!ack, (qb) => qb.where(updateIdRef, '>', ack!.updateId))
      .orderBy(updateIdRef, 'asc');
  };

  // The stream-returning methods below hand back the lazy Kysely stream untouched (via Effect.sync); nothing is buffered.
  return {
    auditCleanup: Effect.fn('Sync.auditCleanup')(function* <T extends keyof DB>(t: T, days: number) {
      const { table, ref } = dyn();
      return yield* query(db, () =>
        db
          .deleteFrom(table(t).as(t))
          .where(ref(`${t}.deletedAt`), '<', sql.raw(`now() - interval '${days} days'`))
          .execute(),
      );
    }),

    album: {
      getCreatedAfter: Effect.fn('Sync.album.getCreatedAfter')(function* (options: SyncCreatedAfterOptions) {
        const { nowId, userId, afterCreateId } = options;
        return yield* query(db, () =>
          db
            .selectFrom('album_user')
            .select(['albumId as id', 'createId'])
            .where('userId', '=', userId)
            .$if(!!afterCreateId, (qb) => qb.where('createId', '>=', afterCreateId!))
            .where('createId', '<', nowId)
            .orderBy('createId', 'asc')
            .execute(),
        );
      }),

      getDeletes: Effect.fn('Sync.album.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('album_audit', options).select(['id', 'albumId']).where('userId', '=', options.userId).stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.album.getUpserts')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('album', options)
            .distinctOn(['album.id', 'album.updateId'])
            .leftJoin('album_user as album_users', 'album.id', 'album_users.albumId')
            .where('album_users.userId', '=', userId)
            .select([
              'album.id',
              'album.albumName as name',
              'album.description',
              'album.createdAt',
              'album.updatedAt',
              'album.albumThumbnailAssetId as thumbnailAssetId',
              'album.isActivityEnabled',
              'album.order',
              'album.updateId',
            ])
            .stream(),
        );
      }),

      getAlbumUsers: Effect.fn('Sync.album.getAlbumUsers')(function* (albumId: string) {
        return yield* query(db, () =>
          db.selectFrom('album_user').select(['userId', 'role']).where('albumId', '=', albumId).execute(),
        );
      }),
    },

    albumAsset: {
      getBackfill: Effect.fn('Sync.albumAsset.getBackfill')(function* (
        options: SyncBackfillOptions,
        albumId: string,
        userId: string,
      ) {
        return yield* Effect.sync(() =>
          backfillQuery('album_asset', options)
            .innerJoin('asset', 'asset.id', 'album_asset.assetId')
            .select(columns.syncAlbumAsset)
            .select((eb) =>
              eb
                .case()
                .when('asset.ownerId', '=', userId)
                .then(eb.ref('asset.isFavorite'))
                .else(eb.val(false))
                .end()
                .as('isFavorite'),
            )
            .select('album_asset.updateId')
            .where('album_asset.albumId', '=', albumId)
            .stream(),
        );
      }),

      getUpdates: Effect.fn('Sync.albumAsset.getUpdates')(function* (
        options: SyncQueryOptions,
        albumToAssetAck: SyncAck,
      ) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('asset', options)
            .innerJoin('album_asset', 'album_asset.assetId', 'asset.id')
            .select(columns.syncAlbumAsset)
            .select((eb) =>
              eb
                .case()
                .when('asset.ownerId', '=', userId)
                .then(eb.ref('asset.isFavorite'))
                .else(eb.val(false))
                .end()
                .as('isFavorite'),
            )
            .select('asset.updateId')
            .where('album_asset.updateId', '<=', albumToAssetAck.updateId) // Ensure we only send updates for assets that the client already knows about
            .innerJoin('album_user', 'album_user.albumId', 'album_asset.albumId')
            .where('album_user.userId', '=', userId)
            .stream(),
        );
      }),

      getCreates: Effect.fn('Sync.albumAsset.getCreates')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('album_asset', options)
            .select('album_asset.updateId')
            .innerJoin('asset', 'asset.id', 'album_asset.assetId')
            .select(columns.syncAlbumAsset)
            .select((eb) =>
              eb
                .case()
                .when('asset.ownerId', '=', userId)
                .then(eb.ref('asset.isFavorite'))
                .else(eb.val(false))
                .end()
                .as('isFavorite'),
            )
            .innerJoin('album_user', 'album_user.albumId', 'album_asset.albumId')
            .where('album_user.userId', '=', userId)
            .stream(),
        );
      }),
    },

    albumAssetExif: {
      getBackfill: Effect.fn('Sync.albumAssetExif.getBackfill')(function* (
        options: SyncBackfillOptions,
        albumId: string,
      ) {
        return yield* Effect.sync(() =>
          backfillQuery('album_asset', options)
            .innerJoin('asset_exif', 'asset_exif.assetId', 'album_asset.assetId')
            .select(columns.syncAssetExif)
            .select('album_asset.updateId')
            .where('album_asset.albumId', '=', albumId)
            .stream(),
        );
      }),

      getUpdates: Effect.fn('Sync.albumAssetExif.getUpdates')(function* (
        options: SyncQueryOptions,
        albumToAssetAck: SyncAck,
      ) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('asset_exif', options)
            .innerJoin('album_asset', 'album_asset.assetId', 'asset_exif.assetId')
            .select(columns.syncAssetExif)
            .select('asset_exif.updateId')
            .where('album_asset.updateId', '<=', albumToAssetAck.updateId) // Ensure we only send exif updates for assets that the client already knows about
            .innerJoin('album_user', 'album_user.albumId', 'album_asset.albumId')
            .where('album_user.userId', '=', userId)
            .stream(),
        );
      }),

      getCreates: Effect.fn('Sync.albumAssetExif.getCreates')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('album_asset', options)
            .select('album_asset.updateId')
            .innerJoin('asset_exif', 'asset_exif.assetId', 'album_asset.assetId')
            .select(columns.syncAssetExif)
            .innerJoin('album', 'album.id', 'album_asset.albumId')
            .leftJoin('album_user', 'album_user.albumId', 'album_asset.albumId')
            .where('album_user.userId', '=', userId)
            .stream(),
        );
      }),
    },

    albumToAsset: {
      getBackfill: Effect.fn('Sync.albumToAsset.getBackfill')(function* (
        options: SyncBackfillOptions,
        albumId: string,
      ) {
        return yield* Effect.sync(() =>
          backfillQuery('album_asset', options)
            .select(['album_asset.assetId as assetId', 'album_asset.albumId as albumId', 'album_asset.updateId'])
            .where('album_asset.albumId', '=', albumId)
            .stream(),
        );
      }),

      getDeletes: Effect.fn('Sync.albumToAsset.getDeletes')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          auditQuery('album_asset_audit', options)
            .select(['id', 'assetId', 'albumId'])
            .where((eb) =>
              eb(
                'albumId',
                'in',
                eb
                  .selectFrom('album_user')
                  .select(['album_user.albumId as id'])
                  .where('album_user.userId', '=', userId),
              ),
            )
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.albumToAsset.getUpserts')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('album_asset', options)
            .select(['album_asset.assetId as assetId', 'album_asset.albumId as albumId', 'album_asset.updateId'])
            .innerJoin('album_user', 'album_user.albumId', 'album_asset.albumId')
            .where('album_user.userId', '=', userId)
            .stream(),
        );
      }),
    },

    albumUser: {
      getBackfill: Effect.fn('Sync.albumUser.getBackfill')(function* (options: SyncBackfillOptions, albumId: string) {
        return yield* Effect.sync(() =>
          backfillQuery('album_user', options)
            .select(columns.syncAlbumUser)
            .select('album_user.updateId')
            .where('albumId', '=', albumId)
            .stream(),
        );
      }),

      getDeletes: Effect.fn('Sync.albumUser.getDeletes')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          auditQuery('album_user_audit', options)
            .select(['id', 'userId', 'albumId'])
            .where((eb) =>
              eb(
                'albumId',
                'in',
                eb
                  .selectFrom('album_user')
                  .select(['album_user.albumId as id'])
                  .where('album_user.userId', '=', userId),
              ),
            )
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.albumUser.getUpserts')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('album_user', options)
            .select(columns.syncAlbumUser)
            .select('album_user.updateId')
            .where((eb) =>
              eb(
                'album_user.albumId',
                'in',
                eb
                  .selectFrom('album_user as albumUsers')
                  .select(['albumUsers.albumId as id'])
                  .where('albumUsers.userId', '=', userId),
              ),
            )
            .stream(),
        );
      }),
    },

    asset: {
      getDeletes: Effect.fn('Sync.asset.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('asset_audit', options).select(['id', 'assetId']).where('ownerId', '=', options.userId).stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.asset.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('asset', options)
            .select(columns.syncAsset)
            .select('asset.updateId')
            .where('ownerId', '=', options.userId)
            .stream(),
        );
      }),
    },

    authUser: {
      getUpserts: Effect.fn('Sync.authUser.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('user', options)
            .select(columns.syncUser)
            .select(['isAdmin', 'pinCode', 'oauthId', 'storageLabel', 'quotaSizeInBytes', 'quotaUsageInBytes'])
            .where('id', '=', options.userId)
            .stream(),
        );
      }),
    },

    assetExif: {
      getUpserts: Effect.fn('Sync.assetExif.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('asset_exif', options)
            .select(columns.syncAssetExif)
            .select('asset_exif.updateId')
            .where('assetId', 'in', (eb) => eb.selectFrom('asset').select('id').where('ownerId', '=', options.userId))
            .stream(),
        );
      }),
    },

    assetEdit: {
      getDeletes: Effect.fn('Sync.assetEdit.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('asset_edit_audit', options)
            .select(['asset_edit_audit.id', 'editId'])
            .innerJoin('asset', 'asset.id', 'asset_edit_audit.assetId')
            .where('asset.ownerId', '=', options.userId)
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.assetEdit.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('asset_edit', options)
            .select([...columns.syncAssetEdit, 'asset_edit.updateId'])
            .innerJoin('asset', 'asset.id', 'asset_edit.assetId')
            .where('asset.ownerId', '=', options.userId)
            .stream(),
        );
      }),
    },

    memory: {
      getDeletes: Effect.fn('Sync.memory.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('memory_audit', options).select(['id', 'memoryId']).where('userId', '=', options.userId).stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.memory.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('memory', options)
            .select([
              'id',
              'createdAt',
              'updatedAt',
              'deletedAt',
              'ownerId',
              'type',
              'data',
              'isSaved',
              'memoryAt',
              'seenAt',
              'showAt',
              'hideAt',
            ])
            .select('updateId')
            .where('ownerId', '=', options.userId)
            .stream(),
        );
      }),
    },

    memoryToAsset: {
      getDeletes: Effect.fn('Sync.memoryToAsset.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('memory_asset_audit', options)
            .select(['id', 'memoryId', 'assetId'])
            .where('memoryId', 'in', (eb) => eb.selectFrom('memory').select('id').where('ownerId', '=', options.userId))
            .stream(),
        );
      }),

      getUpsertsV1: Effect.fn('Sync.memoryToAsset.getUpsertsV1')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('memory_asset', options)
            .select(['memoriesId as memoryId', 'assetId as assetId'])
            .select('updateId')
            .where('memoriesId', 'in', (eb) =>
              eb
                .selectFrom('memory')
                .select('id')
                .where('ownerId', '=', options.userId)
                .where('type', '=', sql.lit(MemoryType.OnThisDay)),
            )
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.memoryToAsset.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('memory_asset', options)
            .select(['memoriesId as memoryId', 'assetId as assetId'])
            .select('updateId')
            .where('memoriesId', 'in', (eb) =>
              eb.selectFrom('memory').select('id').where('ownerId', '=', options.userId),
            )
            .stream(),
        );
      }),
    },

    partner: {
      getCreatedAfter: Effect.fn('Sync.partner.getCreatedAfter')(function* (options: SyncCreatedAfterOptions) {
        const { nowId, userId, afterCreateId } = options;
        return yield* query(db, () =>
          db
            .selectFrom('partner')
            .select(['sharedById', 'createId'])
            .where('sharedWithId', '=', userId)
            .$if(!!afterCreateId, (qb) => qb.where('createId', '>=', afterCreateId!))
            .where('createId', '<', nowId)
            .orderBy('partner.createId', 'asc')
            .execute(),
        );
      }),

      getDeletes: Effect.fn('Sync.partner.getDeletes')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          auditQuery('partner_audit', options)
            .select(['id', 'sharedById', 'sharedWithId'])
            .where((eb) => eb.or([eb('sharedById', '=', userId), eb('sharedWithId', '=', userId)]))
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.partner.getUpserts')(function* (options: SyncQueryOptions) {
        const userId = options.userId;
        return yield* Effect.sync(() =>
          upsertQuery('partner', options)
            .select(['sharedById', 'sharedWithId', 'inTimeline', 'updateId'])
            .where((eb) => eb.or([eb('sharedById', '=', userId), eb('sharedWithId', '=', userId)]))
            .stream(),
        );
      }),
    },

    partnerAsset: {
      getBackfill: Effect.fn('Sync.partnerAsset.getBackfill')(function* (
        options: SyncBackfillOptions,
        partnerId: string,
      ) {
        return yield* Effect.sync(() =>
          backfillQuery('asset', options)
            .select(columns.syncPartnerAsset)
            .select(sql.val(false).as('isFavorite'))
            .select('asset.updateId')
            .where('ownerId', '=', partnerId)
            .stream(),
        );
      }),

      getDeletes: Effect.fn('Sync.partnerAsset.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('asset_audit', options)
            .select(['id', 'assetId'])
            .where('ownerId', 'in', (eb) =>
              eb.selectFrom('partner').select(['sharedById']).where('sharedWithId', '=', options.userId),
            )
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.partnerAsset.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('asset', options)
            .select(columns.syncPartnerAsset)
            .select(sql.val(false).as('isFavorite'))
            .select('asset.updateId')
            .where('ownerId', 'in', (eb) =>
              eb.selectFrom('partner').select(['sharedById']).where('sharedWithId', '=', options.userId),
            )
            .stream(),
        );
      }),
    },

    partnerAssetExif: {
      getBackfill: Effect.fn('Sync.partnerAssetExif.getBackfill')(function* (
        options: SyncBackfillOptions,
        partnerId: string,
      ) {
        return yield* Effect.sync(() =>
          backfillQuery('asset_exif', options)
            .select(columns.syncAssetExif)
            .select('asset_exif.updateId')
            .innerJoin('asset', 'asset.id', 'asset_exif.assetId')
            .where('asset.ownerId', '=', partnerId)
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.partnerAssetExif.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('asset_exif', options)
            .select(columns.syncAssetExif)
            .select('asset_exif.updateId')
            .where('assetId', 'in', (eb) =>
              eb
                .selectFrom('asset')
                .select('id')
                .where('ownerId', 'in', (eb) =>
                  eb.selectFrom('partner').select(['sharedById']).where('sharedWithId', '=', options.userId),
                ),
            )
            .stream(),
        );
      }),
    },

    stack: {
      getDeletes: Effect.fn('Sync.stack.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('stack_audit', options).select(['id', 'stackId']).where('userId', '=', options.userId).stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.stack.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('stack', options)
            .select(columns.syncStack)
            .select('updateId')
            .where('ownerId', '=', options.userId)
            .stream(),
        );
      }),
    },

    partnerStack: {
      getDeletes: Effect.fn('Sync.partnerStack.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('stack_audit', options)
            .select(['id', 'stackId'])
            .where('userId', 'in', (eb) =>
              eb.selectFrom('partner').select(['sharedById']).where('sharedWithId', '=', options.userId),
            )
            .stream(),
        );
      }),

      getBackfill: Effect.fn('Sync.partnerStack.getBackfill')(function* (
        options: SyncBackfillOptions,
        partnerId: string,
      ) {
        return yield* Effect.sync(() =>
          backfillQuery('stack', options)
            .select(columns.syncStack)
            .select('updateId')
            .where('ownerId', '=', partnerId)
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.partnerStack.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('stack', options)
            .select(columns.syncStack)
            .select('updateId')
            .where('ownerId', 'in', (eb) =>
              eb.selectFrom('partner').select(['sharedById']).where('sharedWithId', '=', options.userId),
            )
            .stream(),
        );
      }),
    },

    user: {
      getDeletes: Effect.fn('Sync.user.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() => auditQuery('user_audit', options).select(['id', 'userId']).stream());
      }),

      getUpserts: Effect.fn('Sync.user.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() => upsertQuery('user', options).select(columns.syncUser).stream());
      }),
    },

    userMetadata: {
      getDeletes: Effect.fn('Sync.userMetadata.getDeletes')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          auditQuery('user_metadata_audit', options)
            .select(['id', 'userId', 'key'])
            .where('userId', '=', options.userId)
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.userMetadata.getUpserts')(function* (options: SyncQueryOptions) {
        return yield* Effect.sync(() =>
          upsertQuery('user_metadata', options)
            .select(['userId', 'key', 'value', 'updateId'])
            .where('userId', '=', options.userId)
            .stream(),
        );
      }),
    },

    assetMetadata: {
      getDeletes: Effect.fn('Sync.assetMetadata.getDeletes')(function* (options: SyncQueryOptions, userId: string) {
        return yield* Effect.sync(() =>
          auditQuery('asset_metadata_audit', options)
            .select(['asset_metadata_audit.id', 'assetId', 'key'])
            .leftJoin('asset', 'asset.id', 'asset_metadata_audit.assetId')
            .where('asset.ownerId', '=', userId)
            .stream(),
        );
      }),

      getUpserts: Effect.fn('Sync.assetMetadata.getUpserts')(function* (options: SyncQueryOptions, userId: string) {
        return yield* Effect.sync(() =>
          upsertQuery('asset_metadata', options)
            .select(['assetId', 'key', 'value', 'asset_metadata.updateId'])
            .innerJoin('asset', 'asset.id', 'asset_metadata.assetId')
            .where('asset.ownerId', '=', userId)
            .stream(),
        );
      }),
    },
  };
};

export class SyncRepo extends Context.Service<SyncRepo, ReturnType<typeof makeSyncService>>()(
  'immich/repositories/SyncRepo',
) {
  static readonly make = (db: Kysely<DB>) => SyncRepo.of(makeSyncService(db));

  static readonly layer = Layer.effect(
    SyncRepo,
    Effect.gen(function* () {
      return SyncRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SyncRepo`. */
@Injectable()
export class SyncRepository {
  album: AlbumSync;
  albumAsset: AlbumAssetSync;
  albumAssetExif: AlbumAssetExifSync;
  albumToAsset: AlbumToAssetSync;
  albumUser: AlbumUserSync;
  asset: AssetSync;
  assetExif: AssetExifSync;
  assetEdit: AssetEditSync;
  assetMetadata: AssetMetadataSync;
  authUser: AuthUserSync;
  memory: MemorySync;
  memoryToAsset: MemoryToAssetSync;
  partner: PartnerSync;
  partnerAsset: PartnerAssetsSync;
  partnerAssetExif: PartnerAssetExifsSync;
  partnerStack: PartnerStackSync;
  stack: StackSync;
  user: UserSync;
  userMetadata: UserMetadataSync;

  constructor(@InjectKysely() private db: Kysely<DB>) {
    const effect = SyncRepo.make(this.db);
    this.album = new AlbumSync(this.db, effect);
    this.albumAsset = new AlbumAssetSync(this.db, effect);
    this.albumAssetExif = new AlbumAssetExifSync(this.db, effect);
    this.albumToAsset = new AlbumToAssetSync(this.db, effect);
    this.albumUser = new AlbumUserSync(this.db, effect);
    this.asset = new AssetSync(this.db, effect);
    this.assetExif = new AssetExifSync(this.db, effect);
    this.assetEdit = new AssetEditSync(this.db, effect);
    this.assetMetadata = new AssetMetadataSync(this.db, effect);
    this.authUser = new AuthUserSync(this.db, effect);
    this.memory = new MemorySync(this.db, effect);
    this.memoryToAsset = new MemoryToAssetSync(this.db, effect);
    this.partner = new PartnerSync(this.db, effect);
    this.partnerAsset = new PartnerAssetsSync(this.db, effect);
    this.partnerAssetExif = new PartnerAssetExifsSync(this.db, effect);
    this.partnerStack = new PartnerStackSync(this.db, effect);
    this.stack = new StackSync(this.db, effect);
    this.user = new UserSync(this.db, effect);
    this.userMetadata = new UserMetadataSync(this.db, effect);
    registerEffect(this, effect);
  }
}

export class BaseSync {
  constructor(
    protected db: Kysely<DB>,
    protected effect: Context.Service.Shape<typeof SyncRepo> = SyncRepo.make(db),
  ) {}

  protected auditCleanup<T extends keyof DB>(t: T, days: number) {
    return runPromise(this.effect.auditCleanup(t, days));
  }
}

class AlbumSync extends BaseSync {
  @GenerateSql({ params: [dummyCreateAfterOptions] })
  getCreatedAfter(options: SyncCreatedAfterOptions) {
    return runPromise(this.effect.album.getCreatedAfter(options));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.album.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('album_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.album.getUpserts(options));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getAlbumUsers(albumId: string) {
    return runPromise(this.effect.album.getAlbumUsers(albumId));
  }
}

class AlbumAssetSync extends BaseSync {
  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, albumId: string, userId: string) {
    return runSync(this.effect.albumAsset.getBackfill(options, albumId, userId));
  }

  @GenerateSql({ params: [dummyQueryOptions, { updateId: DummyValue.UUID }], stream: true })
  getUpdates(options: SyncQueryOptions, albumToAssetAck: SyncAck) {
    return runSync(this.effect.albumAsset.getUpdates(options, albumToAssetAck));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getCreates(options: SyncQueryOptions) {
    return runSync(this.effect.albumAsset.getCreates(options));
  }
}

class AlbumAssetExifSync extends BaseSync {
  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, albumId: string) {
    return runSync(this.effect.albumAssetExif.getBackfill(options, albumId));
  }

  @GenerateSql({ params: [dummyQueryOptions, { updateId: DummyValue.UUID }], stream: true })
  getUpdates(options: SyncQueryOptions, albumToAssetAck: SyncAck) {
    return runSync(this.effect.albumAssetExif.getUpdates(options, albumToAssetAck));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getCreates(options: SyncQueryOptions) {
    return runSync(this.effect.albumAssetExif.getCreates(options));
  }
}

class AlbumToAssetSync extends BaseSync {
  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, albumId: string) {
    return runSync(this.effect.albumToAsset.getBackfill(options, albumId));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.albumToAsset.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('album_asset_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.albumToAsset.getUpserts(options));
  }
}

class AlbumUserSync extends BaseSync {
  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, albumId: string) {
    return runSync(this.effect.albumUser.getBackfill(options, albumId));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.albumUser.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('album_user_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.albumUser.getUpserts(options));
  }
}

class AssetSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.asset.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('asset_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.asset.getUpserts(options));
  }
}

class AuthUserSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.authUser.getUpserts(options));
  }
}

class AssetExifSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.assetExif.getUpserts(options));
  }
}

class AssetEditSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.assetEdit.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('asset_edit_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.assetEdit.getUpserts(options));
  }
}

class MemorySync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.memory.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('memory_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.memory.getUpserts(options));
  }
}

class MemoryToAssetSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.memoryToAsset.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('memory_asset_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpsertsV1(options: SyncQueryOptions) {
    return runSync(this.effect.memoryToAsset.getUpsertsV1(options));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.memoryToAsset.getUpserts(options));
  }
}

class PartnerSync extends BaseSync {
  @GenerateSql({ params: [dummyCreateAfterOptions] })
  getCreatedAfter(options: SyncCreatedAfterOptions) {
    return runPromise(this.effect.partner.getCreatedAfter(options));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.partner.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('partner_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.partner.getUpserts(options));
  }
}

class PartnerAssetsSync extends BaseSync {
  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, partnerId: string) {
    return runSync(this.effect.partnerAsset.getBackfill(options, partnerId));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.partnerAsset.getDeletes(options));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.partnerAsset.getUpserts(options));
  }
}

class PartnerAssetExifsSync extends BaseSync {
  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, partnerId: string) {
    return runSync(this.effect.partnerAssetExif.getBackfill(options, partnerId));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.partnerAssetExif.getUpserts(options));
  }
}

class StackSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.stack.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('stack_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.stack.getUpserts(options));
  }
}

class PartnerStackSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.partnerStack.getDeletes(options));
  }

  @GenerateSql({ params: [dummyBackfillOptions, DummyValue.UUID], stream: true })
  getBackfill(options: SyncBackfillOptions, partnerId: string) {
    return runSync(this.effect.partnerStack.getBackfill(options, partnerId));
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.partnerStack.getUpserts(options));
  }
}

class UserSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.user.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('user_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.user.getUpserts(options));
  }
}

class UserMetadataSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getDeletes(options: SyncQueryOptions) {
    return runSync(this.effect.userMetadata.getDeletes(options));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('user_metadata_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions], stream: true })
  getUpserts(options: SyncQueryOptions) {
    return runSync(this.effect.userMetadata.getUpserts(options));
  }
}

class AssetMetadataSync extends BaseSync {
  @GenerateSql({ params: [dummyQueryOptions, DummyValue.UUID], stream: true })
  getDeletes(options: SyncQueryOptions, userId: string) {
    return runSync(this.effect.assetMetadata.getDeletes(options, userId));
  }

  cleanupAuditTable(daysAgo: number) {
    return this.auditCleanup('asset_metadata_audit', daysAgo);
  }

  @GenerateSql({ params: [dummyQueryOptions, DummyValue.UUID], stream: true })
  getUpserts(options: SyncQueryOptions, userId: string) {
    return runSync(this.effect.assetMetadata.getUpserts(options, userId));
  }
}
