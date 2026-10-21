import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import {
  type ExpressionBuilder,
  type Insertable,
  type Kysely,
  type Selectable,
  type ShallowDehydrateObject,
  type Updateable,
  sql,
} from 'kysely';
import { jsonArrayFrom, jsonObjectFrom } from 'kysely/helpers/postgres';
import { omit } from 'lodash-es';
import { InjectKysely } from 'nestjs-kysely';
import { Album, columns } from 'src/database.js';
import { ChunkedArray, DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumUserRole, SharedLinkType } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { AssetExifTable } from 'src/schema/tables/asset-exif.table.js';
import { AssetTable } from 'src/schema/tables/asset.table.js';
import { SharedLinkTable } from 'src/schema/tables/shared-link.table.js';
import { dummy } from 'src/utils/database.js';

export type SharedLinkSearchOptions = {
  userId: string;
  id?: string;
  albumId?: string;
};

const withSharedAssets = (eb: ExpressionBuilder<DB, 'shared_link'>) => {
  return eb
    .selectFrom('shared_link_asset')
    .whereRef('shared_link.id', '=', 'shared_link_asset.sharedLinkId')
    .innerJoin('asset', 'asset.id', 'shared_link_asset.assetId')
    .where('asset.deletedAt', 'is', null)
    .selectAll('asset')
    .orderBy('asset.fileCreatedAt', 'asc');
};

export const withExifInfo = (eb: ExpressionBuilder<DB, 'asset'>) => {
  return eb
    .selectFrom('asset_exif')
    .select(columns.exif)
    .whereRef('asset_exif.assetId', '=', 'asset.id')
    .as('exifInfo');
};

const withAlbumOwner = (eb: ExpressionBuilder<DB, 'album'>) =>
  jsonArrayFrom(
    eb
      .selectFrom('album_user')
      .innerJoin('user', 'user.id', 'album_user.userId')
      .where('album_user.role', '=', sql.lit(AlbumUserRole.Owner))
      .whereRef('album_user.albumId', '=', 'album.id')
      .select('album_user.role')
      .select((eb) => jsonObjectFrom(eb.selectFrom(dummy).select(columns.user)).$notNull().as('user')),
  )
    .$notNull()
    .as('albumUsers');

const withSharedLinkAlbum = (eb: ExpressionBuilder<DB, 'shared_link'>) => {
  return eb
    .selectFrom('album')
    .selectAll('album')
    .whereRef('album.id', '=', 'shared_link.albumId')
    .where('album.deletedAt', 'is', null);
};

const authBuilder = (db: Kysely<DB>) =>
  db
    .selectFrom('shared_link')
    .leftJoin('album', 'album.id', 'shared_link.albumId')
    .where('album.deletedAt', 'is', null)
    .select((eb) => [
      'shared_link.id',
      'shared_link.userId',
      'shared_link.albumId',
      'shared_link.expiresAt',
      'shared_link.showExif',
      'shared_link.allowUpload',
      'shared_link.allowDownload',
      'shared_link.password',
      jsonObjectFrom(eb.selectFrom('user').select(columns.authUser).whereRef('user.id', '=', 'shared_link.userId')).as(
        'user',
      ),
    ])
    .where((eb) => eb.or([eb('shared_link.type', '=', SharedLinkType.Individual), eb('album.id', 'is not', null)]));

const makeSharedLink = (db: Kysely<DB>) => {
  const getSharedLinks = Effect.fn('SharedLink.getSharedLinks')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('shared_link')
        .selectAll('shared_link')
        .where('shared_link.id', '=', id)
        .leftJoin('shared_link_asset', 'shared_link_asset.sharedLinkId', 'shared_link.id')
        .leftJoinLateral(
          (eb) =>
            eb
              .selectFrom('asset')
              .whereRef('asset.id', '=', 'shared_link_asset.assetId')
              .selectAll('asset')
              .innerJoinLateral(withExifInfo, (join) => join.onTrue())
              .as('assets'),
          (join) => join.onTrue(),
        )
        .select((eb) =>
          eb.fn
            .coalesce(eb.fn.jsonAgg('assets').filterWhere('assets.id', 'is not', null), sql`'[]'`)
            .$castTo<
              (ShallowDehydrateObject<Selectable<AssetTable>> & {
                exifInfo: ShallowDehydrateObject<Selectable<AssetExifTable>>;
              })[]
            >()
            .as('assets'),
        )
        .groupBy('shared_link.id')
        .executeTakeFirstOrThrow(),
    );
  });

  return {
    get: Effect.fn('SharedLink.get')(function* (userId: string, id: string) {
      return yield* query(db, (db) =>
        db
          .selectFrom('shared_link')
          .selectAll('shared_link')
          .select((eb) =>
            jsonArrayFrom(
              withSharedAssets(eb)
                .innerJoinLateral(withExifInfo, (join) => join.onTrue())
                .select((eb) => eb.fn.toJson('exifInfo').as('exifInfo')),
            ).as('assets'),
          )
          .leftJoinLateral(
            (eb) => withSharedLinkAlbum(eb).select(withAlbumOwner).as('album'),
            (join) => join.onTrue(),
          )
          .select((eb) => eb.fn.toJson(eb.table('album')).$castTo<ShallowDehydrateObject<Album> | null>().as('album'))
          .where('shared_link.id', '=', id)
          .where('shared_link.userId', '=', userId)
          .where((eb) =>
            eb.or([eb('shared_link.type', '=', SharedLinkType.Individual), eb('album.id', 'is not', null)]),
          )
          .orderBy('shared_link.createdAt', 'desc')
          .executeTakeFirst(),
      );
    }),

    getAll: Effect.fn('SharedLink.getAll')(function* ({ userId, id, albumId }: SharedLinkSearchOptions) {
      return yield* query(db, (db) =>
        db
          .selectFrom('shared_link')
          .selectAll('shared_link')
          .select((eb) => jsonArrayFrom(withSharedAssets(eb).limit(1)).as('assets'))
          .where('shared_link.userId', '=', userId)
          .leftJoinLateral(
            (eb) => withSharedLinkAlbum(eb).select(withAlbumOwner).as('album'),
            (join) => join.onTrue(),
          )
          .select((eb) => eb.fn.toJson('album').$castTo<ShallowDehydrateObject<Album> | null>().as('album'))
          .where((eb) =>
            eb.or([eb('shared_link.type', '=', SharedLinkType.Individual), eb('album.id', 'is not', null)]),
          )
          .$if(!!albumId, (eb) => eb.where('shared_link.albumId', '=', albumId!))
          .$if(!!id, (eb) => eb.where('shared_link.id', '=', id!))
          .orderBy('shared_link.createdAt', 'desc')
          .execute(),
      );
    }),

    getByKey: Effect.fn('SharedLink.getByKey')(function* (key: Buffer) {
      return yield* query(db, (db) => authBuilder(db).where('shared_link.key', '=', key).executeTakeFirst());
    }),

    getBySlug: Effect.fn('SharedLink.getBySlug')(function* (slug: string) {
      return yield* query(db, (db) => authBuilder(db).where('shared_link.slug', '=', slug).executeTakeFirst());
    }),

    create: Effect.fn('SharedLink.create')(function* (entity: Insertable<SharedLinkTable> & { assetIds?: string[] }) {
      const { id } = yield* query(db, (db) =>
        db.insertInto('shared_link').values(omit(entity, 'assetIds')).returningAll().executeTakeFirstOrThrow(),
      );

      if (entity.assetIds && entity.assetIds.length > 0) {
        yield* query(db, (db) =>
          db
            .insertInto('shared_link_asset')
            .values(entity.assetIds!.map((assetId) => ({ assetId, sharedLinkId: id })))
            .execute(),
        );
      }

      return yield* getSharedLinks(id);
    }),

    update: Effect.fn('SharedLink.update')(function* (
      entity: Updateable<SharedLinkTable> & { id: string; assetIds?: string[] },
    ) {
      const { id } = yield* query(db, (db) =>
        db
          .updateTable('shared_link')
          .set(omit(entity, 'assets', 'album', 'assetIds'))
          .where('shared_link.id', '=', entity.id)
          .returningAll()
          .executeTakeFirstOrThrow(),
      );

      if (entity.assetIds && entity.assetIds.length > 0) {
        yield* query(db, (db) =>
          db
            .insertInto('shared_link_asset')
            .values(entity.assetIds!.map((assetId) => ({ assetId, sharedLinkId: id })))
            .execute(),
        );
      }

      return yield* getSharedLinks(id);
    }),

    remove: Effect.fn('SharedLink.remove')(function* (id: string) {
      yield* query(db, (db) => db.deleteFrom('shared_link').where('shared_link.id', '=', id).execute());
    }),

    addAssets: Effect.fn('SharedLink.addAssets')(function* (id: string, assetIds: string[]) {
      if (assetIds.length === 0) {
        return [];
      }

      return yield* query(db, (db) =>
        db
          .insertInto('shared_link_asset')
          .values(assetIds.map((assetId) => ({ assetId, sharedLinkId: id })))
          .onConflict((oc) => oc.doNothing())
          .returning(['shared_link_asset.assetId'])
          .execute(),
      );
    }),

    getSharedLinks,
  };
};

export class SharedLinkRepo extends Context.Service<SharedLinkRepo, ReturnType<typeof makeSharedLink>>()(
  'immich/repositories/SharedLinkRepo',
) {
  static readonly make = (db: Kysely<DB>) => SharedLinkRepo.of(makeSharedLink(db));

  static readonly layer = Layer.effect(
    SharedLinkRepo,
    Effect.gen(function* () {
      return SharedLinkRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SharedLinkRepo`. */
@Injectable()
export class SharedLinkRepository {
  private readonly effect: Context.Service.Shape<typeof SharedLinkRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = SharedLinkRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID] })
  get(userId: string, id: string) {
    return runPromise(this.effect.get(userId, id));
  }

  @GenerateSql({ params: [{ userId: DummyValue.UUID, albumId: DummyValue.UUID }] })
  getAll({ userId, id, albumId }: SharedLinkSearchOptions) {
    return runPromise(this.effect.getAll({ userId, id, albumId }));
  }

  @GenerateSql({ params: [DummyValue.BUFFER] })
  getByKey(key: Buffer) {
    return runPromise(this.effect.getByKey(key));
  }

  @GenerateSql({ params: [DummyValue.BUFFER] })
  getBySlug(slug: string) {
    return runPromise(this.effect.getBySlug(slug));
  }

  create(entity: Insertable<SharedLinkTable> & { assetIds?: string[] }) {
    return runPromise(this.effect.create(entity));
  }

  update(entity: Updateable<SharedLinkTable> & { id: string; assetIds?: string[] }) {
    return runPromise(this.effect.update(entity));
  }

  remove(id: string): Promise<void> {
    return runPromise(this.effect.remove(id));
  }

  @ChunkedArray({ paramIndex: 1 })
  addAssets(id: string, assetIds: string[]) {
    return runPromise(this.effect.addAssets(id, assetIds));
  }

  // Kept (private) only so the generated SQL docs still cover this query.
  @GenerateSql({ params: [DummyValue.UUID] })
  private getSharedLinks(id: string) {
    return runPromise(this.effect.getSharedLinks(id));
  }
}
