import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type ExpressionBuilder, type Insertable, type Kysely, type Updateable, sql } from 'kysely';
import { jsonArrayFrom, jsonObjectFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import { columns } from 'src/database.js';
import { Chunked, ChunkedArray, ChunkedSet, DummyValue, GenerateSql } from 'src/decorators.js';
import { AlbumUserCreateDto, MapAlbumDto } from 'src/dtos/album.dto.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumUserRole } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { AlbumTable } from 'src/schema/tables/album.table.js';
import { asUuid, dummy, withDefaultVisibility } from 'src/utils/database.js';

export interface AlbumAssetCount {
  albumId: string;
  assetCount: number;
  startDate: Date | null;
  endDate: Date | null;
  lastModifiedAssetTimestamp: Date | null;
}

const withAlbumUsers = (authUserId?: string) => (eb: ExpressionBuilder<DB, 'album'>) =>
  jsonArrayFrom(
    eb
      .selectFrom('album_user')
      .innerJoin('user', 'user.id', 'album_user.userId')
      .whereRef('album_user.albumId', '=', 'album.id')
      .select('album_user.role')
      .select((eb) => jsonObjectFrom(eb.selectFrom(dummy).select(columns.user)).$notNull().as('user'))
      .orderBy('album_user.role')
      .$if(!!authUserId, (qb) => qb.orderBy((eb) => eb('album_user.userId', '=', authUserId!), 'desc'))
      .orderBy('user.name', 'asc'),
  )
    .$notNull()
    .as('albumUsers');

const withSharedLink = (eb: ExpressionBuilder<DB, 'album'>) =>
  jsonArrayFrom(
    eb.selectFrom('shared_link').selectAll('shared_link').whereRef('shared_link.albumId', '=', 'album.id'),
  ).as('sharedLinks');

const isAlbumOwned = (ownerId: string) => (eb: ExpressionBuilder<DB, 'album'>) =>
  eb.exists(
    eb
      .selectFrom('album_user')
      .whereRef('album_user.albumId', '=', 'album.id')
      .where('album_user.role', '=', AlbumUserRole.Owner)
      .where('album_user.userId', '=', ownerId),
  );

const buildAlbumBaseQuery = (
  db: Kysely<DB>,
  ownerId: string,
  { isOwned, isShared }: { isOwned?: boolean; isShared?: boolean },
) =>
  db
    .selectFrom('album')
    .innerJoin('album_user', (join) =>
      join.onRef('album_user.albumId', '=', 'album.id').on('album_user.userId', '=', ownerId),
    )
    .where('album.deletedAt', 'is', null)
    .$if(isOwned === true, (qb) => qb.where('album_user.role', '=', sql.lit(AlbumUserRole.Owner)))
    .$if(isOwned === false, (qb) => qb.where('album_user.role', '!=', sql.lit(AlbumUserRole.Owner)))
    .$if(isShared !== undefined, (qb) =>
      qb.where((eb) => {
        const isSharedAlbum = eb.or([
          eb.exists(
            eb
              .selectFrom('album_user as au')
              .whereRef('au.albumId', '=', 'album.id')
              .where('au.role', '!=', sql.lit(AlbumUserRole.Owner)),
          ),
          eb.exists(eb.selectFrom('shared_link').whereRef('shared_link.albumId', '=', 'album.id')),
        ]);
        return isShared ? isSharedAlbum : eb.not(isSharedAlbum);
      }),
    );

const updateThumbnailBuilder = (eb: ExpressionBuilder<DB, 'album'>) =>
  eb
    .selectFrom('album_asset')
    .innerJoin('asset', (join) => join.onRef('album_asset.assetId', '=', 'asset.id').on('asset.deletedAt', 'is', null))
    .whereRef('album_asset.albumId', '=', 'album.id');

const makeAlbum = (db: Kysely<DB>) => ({
  getById: Effect.fn('Album.getById')(function* (id: string, authUserId?: string) {
    return yield* query(db, (db) =>
      db
        .with('album_user', (qb) => qb.selectFrom('album_user').selectAll().where('album_user.albumId', '=', id))
        .selectFrom('album')
        .selectAll('album')
        .where('album.id', '=', id)
        .where('album.deletedAt', 'is', null)
        .select(withAlbumUsers(authUserId))
        .select(withSharedLink)
        .executeTakeFirst(),
    );
  }),

  getByAssetId: Effect.fn('Album.getByAssetId')(function* (ownerId: string, assetId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('album')
        .selectAll('album')
        .innerJoin('album_asset', 'album_asset.albumId', 'album.id')
        .where((eb) =>
          eb.exists(
            eb
              .selectFrom('album_user')
              .whereRef('album_user.albumId', '=', 'album.id')
              .where('album_user.userId', '=', ownerId),
          ),
        )
        .where('album_asset.assetId', '=', assetId)
        .where('album.deletedAt', 'is', null)
        .select(withAlbumUsers(ownerId))
        .orderBy('album.createdAt', 'desc')
        .execute(),
    );
  }),

  getByAssetIds: Effect.fn('Album.getByAssetIds')(function* (ownerId: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return new Map<string, string[]>();
    }

    const results = yield* query(db, (db) =>
      db
        .selectFrom('album')
        .select('album.id')
        .innerJoin('album_asset', 'album_asset.albumId', 'album.id')
        .where((eb) =>
          eb.exists(
            eb
              .selectFrom('album_user')
              .whereRef('album_user.albumId', '=', 'album.id')
              .where('album_user.userId', '=', ownerId),
          ),
        )
        .where('album_asset.assetId', 'in', assetIds)
        .where('album.deletedAt', 'is', null)
        .select('album_asset.assetId')
        .execute(),
    );

    // Group by assetId
    const map = new Map<string, string[]>();
    for (const row of results) {
      const existing = map.get(row.assetId) ?? [];
      existing.push(row.id);
      map.set(row.assetId, existing);
    }

    return map;
  }),

  getMetadataForIds: Effect.fn('Album.getMetadataForIds')(function* (ids: string[]) {
    // Guard against running invalid query when ids list is empty.
    if (ids.length === 0) {
      return [];
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .$call(withDefaultVisibility)
        .innerJoin('album_asset', 'album_asset.assetId', 'asset.id')
        .select('album_asset.albumId as albumId')
        .select((eb) => eb.fn.min(sql<Date>`("asset"."localDateTime" AT TIME ZONE 'UTC'::text)::date`).as('startDate'))
        .select((eb) => eb.fn.max(sql<Date>`("asset"."localDateTime" AT TIME ZONE 'UTC'::text)::date`).as('endDate'))
        // lastModifiedAssetTimestamp is only used in mobile app, please remove if not need
        .select((eb) => eb.fn.max('asset.updatedAt').as('lastModifiedAssetTimestamp'))
        .select((eb) => sql<number>`${eb.fn.count('asset.id')}::int`.as('assetCount'))
        .where('album_asset.albumId', 'in', ids)
        .where('asset.deletedAt', 'is', null)
        .groupBy('album_asset.albumId')
        .execute(),
    );
  }),

  getAll: Effect.fn('Album.getAll')(function* (
    ownerId: string,
    options: { id?: string; isOwned?: boolean; isShared?: boolean; name?: string } = {},
  ) {
    return yield* query(db, (db) =>
      buildAlbumBaseQuery(db, ownerId, options)
        .selectAll('album')
        .select(withAlbumUsers(ownerId))
        .select(withSharedLink)
        .$if(!!options.id, (qb) => qb.where('album.id', '=', options.id!))
        .$if(!!options.name, (qb) => qb.where('album.albumName', '=', options.name!))
        .orderBy('album.createdAt', 'desc')
        .execute(),
    );
  }),

  getAllIds: Effect.fn('Album.getAllIds')(function* (
    ownerId: string,
    options: { isOwned?: boolean; isShared?: boolean } = {},
  ) {
    const rows = yield* query(db, (db) =>
      buildAlbumBaseQuery(db, ownerId, options).select('album.id').orderBy('album.createdAt', 'desc').execute(),
    );
    return rows.map((r) => r.id);
  }),

  restoreAll: Effect.fn('Album.restoreAll')(function* (userId: string) {
    yield* query(db, (db) => db.updateTable('album').set({ deletedAt: null }).where(isAlbumOwned(userId)).execute());
  }),

  softDeleteAll: Effect.fn('Album.softDeleteAll')(function* (userId: string) {
    yield* query(db, (db) =>
      db.updateTable('album').set({ deletedAt: new Date() }).where(isAlbumOwned(userId)).execute(),
    );
  }),

  deleteAll: Effect.fn('Album.deleteAll')(function* (userId: string) {
    yield* query(db, (db) => db.deleteFrom('album').where(isAlbumOwned(userId)).execute());
  }),

  removeAssetsFromAll: Effect.fn('Album.removeAssetsFromAll')(function* (assetIds: string[]) {
    yield* query(db, (db) => db.deleteFrom('album_asset').where('album_asset.assetId', 'in', assetIds).execute());
  }),

  removeAssetIds: Effect.fn('Album.removeAssetIds')(function* (albumId: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return;
    }

    yield* query(db, (db) =>
      db
        .deleteFrom('album_asset')
        .where('album_asset.albumId', '=', albumId)
        .where('album_asset.assetId', 'in', assetIds)
        .execute(),
    );
  }),

  getAssetIds: Effect.fn('Album.getAssetIds')(function* (albumId: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return new Set<string>();
    }

    const results = yield* query(db, (db) =>
      db
        .selectFrom('album_asset')
        .selectAll()
        .where('album_asset.albumId', '=', albumId)
        .where('album_asset.assetId', 'in', assetIds)
        .execute(),
    );
    return new Set(results.map(({ assetId }) => assetId));
  }),

  addAssetIds: Effect.fn('Album.addAssetIds')(function* (albumId: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return;
    }

    yield* query(db, (db) =>
      db
        .insertInto('album_asset')
        .expression((eb) =>
          eb.selectFrom(dummy).select([asUuid(albumId).as('albumId'), sql`unnest(${assetIds}::uuid[])`.as('assetId')]),
        )
        .onConflict((oc) => oc.doNothing())
        .execute(),
    );
  }),

  create: Effect.fn('Album.create')(function* (
    album: Insertable<AlbumTable>,
    assetIds: string[],
    albumUsers: AlbumUserCreateDto[],
    authUserId: string,
  ) {
    if (albumUsers.every((u) => u.role !== AlbumUserRole.Owner)) {
      throw new Error('Album must have an owner');
    }

    const userIds = albumUsers.map((u) => u.userId);
    const roles = albumUsers.map((u) => u.role);

    return yield* query(db, (db) =>
      db
        .with('album', (db) => db.insertInto('album').values(album).returningAll())
        .with('album_user', (db) =>
          db
            .insertInto('album_user')
            .expression((eb) =>
              eb
                .selectFrom('album')
                .select(({ ref }) => [
                  ref('album.id').as('albumId'),
                  sql`unnest(${userIds}::uuid[])`.as('userId'),
                  sql`unnest(${roles}::album_user_role_enum[])`.as('role'),
                ]),
            )
            .returning(['album_user.albumId', 'album_user.userId', 'album_user.role']),
        )
        .with('album_asset', (db) =>
          db
            .insertInto('album_asset')
            .expression((eb) =>
              eb
                .selectFrom('album')
                .select(({ ref }) => [ref('album.id').as('albumId'), sql`unnest(${assetIds}::uuid[])`.as('assetId')]),
            )
            .onConflict((oc) => oc.doNothing())
            .returning(['album_asset.albumId', 'album_asset.assetId']),
        )
        .selectFrom('album')
        .selectAll('album')
        .select(withAlbumUsers(authUserId))
        .executeTakeFirstOrThrow(),
    );
  }),

  update: Effect.fn('Album.update')(function* (id: string, album: Updateable<AlbumTable>, authUserId: string) {
    return yield* query(db, (db) =>
      db
        .updateTable('album')
        .set(album)
        .where('album.id', '=', id)
        .returningAll('album')
        .returning(withSharedLink)
        .returning(withAlbumUsers(authUserId))
        .executeTakeFirstOrThrow(),
    );
  }),

  delete: Effect.fn('Album.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('album').where('id', '=', id).execute());
  }),

  addAssetIdsToAlbums: Effect.fn('Album.addAssetIdsToAlbums')(function* (
    values: { albumId: string; assetId: string }[],
  ) {
    if (values.length === 0) {
      return;
    }
    yield* query(db, (db) =>
      db
        .insertInto('album_asset')
        .values(values)
        // Allow idempotent album sync without failing on existing album memberships.
        .onConflict((oc) => oc.columns(['albumId', 'assetId']).doNothing())
        .execute(),
    );
  }),

  updateThumbnails: Effect.fn('Album.updateThumbnails')(function* () {
    // Subquery for getting a new thumbnail.
    const result = yield* query(db, (db) =>
      db
        .updateTable('album')
        .set((eb) => ({
          albumThumbnailAssetId: updateThumbnailBuilder(eb)
            .select('album_asset.assetId')
            .orderBy('asset.fileCreatedAt', 'desc')
            .limit(sql.lit(1)),
        }))
        .where((eb) =>
          eb.or([
            eb.and([
              eb('albumThumbnailAssetId', 'is', null),
              eb.exists(updateThumbnailBuilder(eb).select(sql`1`.as('1'))), // Has assets
            ]),
            eb.and([
              eb('albumThumbnailAssetId', 'is not', null),
              eb.not(
                eb.exists(
                  updateThumbnailBuilder(eb)
                    .select(sql`1`.as('1'))
                    .whereRef('album.albumThumbnailAssetId', '=', 'album_asset.assetId'), // Has invalid assets
                ),
              ),
            ]),
          ]),
        )
        .execute(),
    );

    return Number(result[0].numUpdatedRows) as number | undefined;
  }),

  getContributorCounts: Effect.fn('Album.getContributorCounts')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('album_asset')
        .innerJoin('asset', 'asset.id', 'assetId')
        .where('asset.deletedAt', 'is', sql.lit(null))
        .where('album_asset.albumId', '=', id)
        .select('asset.ownerId as userId')
        .select((eb) => eb.fn.countAll<number>().as('assetCount'))
        .groupBy('asset.ownerId')
        .orderBy('assetCount', 'desc')
        .execute(),
    );
  }),

  copyAlbums: Effect.fn('Album.copyAlbums')(function* ({
    sourceAssetId,
    targetAssetId,
  }: {
    sourceAssetId: string;
    targetAssetId: string;
  }) {
    return yield* query(db, (db) =>
      db
        .insertInto('album_asset')
        .expression((eb) =>
          eb
            .selectFrom('album_asset')
            .select((eb) => ['album_asset.albumId', eb.val(targetAssetId).as('assetId')])
            .where('album_asset.assetId', '=', sourceAssetId),
        )
        .onConflict((oc) => oc.doNothing())
        .execute(),
    );
  }),
});

export class AlbumRepo extends Context.Service<AlbumRepo, ReturnType<typeof makeAlbum>>()(
  'immich/repositories/AlbumRepo',
) {
  static readonly make = (db: Kysely<DB>) => AlbumRepo.of(makeAlbum(db));

  static readonly layer = Layer.effect(
    AlbumRepo,
    Effect.gen(function* () {
      return AlbumRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AlbumRepo`. */
@Injectable()
export class AlbumRepository {
  private readonly effect: Context.Service.Shape<typeof AlbumRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AlbumRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID] })
  getById(id: string, authUserId?: string) {
    return runPromise(this.effect.getById(id, authUserId));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID] })
  getByAssetId(ownerId: string, assetId: string) {
    return runPromise(this.effect.getByAssetId(ownerId, assetId));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID]] })
  @ChunkedSet({ paramIndex: 1 })
  getByAssetIds(ownerId: string, assetIds: string[]): Promise<Map<string, string[]>> {
    return runPromise(this.effect.getByAssetIds(ownerId, assetIds));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  @ChunkedArray()
  getMetadataForIds(ids: string[]): Promise<AlbumAssetCount[]> {
    return runPromise(this.effect.getMetadataForIds(ids));
  }

  @GenerateSql({ params: [DummyValue.UUID, { isOwned: true, isShared: true }] })
  getAll(
    ownerId: string,
    options: { id?: string; isOwned?: boolean; isShared?: boolean; name?: string } = {},
  ): Promise<MapAlbumDto[]> {
    return runPromise(this.effect.getAll(ownerId, options));
  }

  @GenerateSql({ params: [DummyValue.UUID, { isOwned: true, isShared: true }] })
  getAllIds(ownerId: string, options: { isOwned?: boolean; isShared?: boolean } = {}): Promise<string[]> {
    return runPromise(this.effect.getAllIds(ownerId, options));
  }

  restoreAll(userId: string): Promise<void> {
    return runPromise(this.effect.restoreAll(userId));
  }

  softDeleteAll(userId: string): Promise<void> {
    return runPromise(this.effect.softDeleteAll(userId));
  }

  deleteAll(userId: string): Promise<void> {
    return runPromise(this.effect.deleteAll(userId));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  @Chunked()
  removeAssetsFromAll(assetIds: string[]): Promise<void> {
    return runPromise(this.effect.removeAssetsFromAll(assetIds));
  }

  @Chunked({ paramIndex: 1 })
  removeAssetIds(albumId: string, assetIds: string[]): Promise<void> {
    return runPromise(this.effect.removeAssetIds(albumId, assetIds));
  }

  /**
   * Get asset IDs for the given album ID.
   *
   * @param albumId Album ID to get asset IDs for.
   * @param assetIds Optional list of asset IDs to filter on.
   * @returns Set of Asset IDs for the given album ID.
   */
  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID]] })
  @ChunkedSet({ paramIndex: 1 })
  getAssetIds(albumId: string, assetIds: string[]): Promise<Set<string>> {
    return runPromise(this.effect.getAssetIds(albumId, assetIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID]] })
  addAssetIds(albumId: string, assetIds: string[]): Promise<void> {
    return runPromise(this.effect.addAssetIds(albumId, assetIds));
  }

  @GenerateSql({
    params: [
      { albumName: DummyValue.STRING },
      [],
      [{ userId: DummyValue.UUID, role: AlbumUserRole.Owner }, DummyValue.UUID],
    ],
  })
  create(album: Insertable<AlbumTable>, assetIds: string[], albumUsers: AlbumUserCreateDto[], authUserId: string) {
    return runPromise(this.effect.create(album, assetIds, albumUsers, authUserId));
  }

  update(id: string, album: Updateable<AlbumTable>, authUserId: string) {
    return runPromise(this.effect.update(id, album, authUserId));
  }

  delete(id: string): Promise<void> {
    return runPromise(this.effect.delete(id));
  }

  @Chunked({ chunkSize: 30_000 })
  addAssetIdsToAlbums(values: { albumId: string; assetId: string }[]): Promise<void> {
    return runPromise(this.effect.addAssetIdsToAlbums(values));
  }

  /**
   * Makes sure all thumbnails for albums are updated by:
   * - Removing thumbnails from albums without assets
   * - Removing references of thumbnails to assets outside the album
   * - Setting a thumbnail when none is set and the album contains assets
   *
   * @returns Amount of updated album thumbnails or undefined when unknown
   */
  updateThumbnails(): Promise<number | undefined> {
    return runPromise(this.effect.updateThumbnails());
  }

  /**
   * Get per-user asset contribution counts for a single album.
   * Excludes deleted assets, orders by count desc.
   */
  @GenerateSql({ params: [DummyValue.UUID] })
  getContributorCounts(id: string) {
    return runPromise(this.effect.getContributorCounts(id));
  }

  @GenerateSql({ params: [{ sourceAssetId: DummyValue.UUID, targetAssetId: DummyValue.UUID }] })
  copyAlbums({ sourceAssetId, targetAssetId }: { sourceAssetId: string; targetAssetId: string }) {
    return runPromise(this.effect.copyAlbums({ sourceAssetId, targetAssetId }));
  }
}
