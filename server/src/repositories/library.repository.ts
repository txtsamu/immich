import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Insertable, type Kysely, type Updateable, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { LibraryStatsResponseDto } from 'src/dtos/library.dto.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { AssetType, AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { LibraryTable } from 'src/schema/tables/library.table.js';

export enum AssetSyncResult {
  DO_NOTHING,
  UPDATE,
  OFFLINE,
  CHECK_OFFLINE,
}

const makeLibrary = (db: Kysely<DB>) => ({
  get: Effect.fn('Library.get')(function* (id: string, withDeleted = false) {
    return yield* query(db, (db) =>
      db
        .selectFrom('library')
        .selectAll('library')
        .where('library.id', '=', id)
        .$if(!withDeleted, (qb) => qb.where('library.deletedAt', 'is', null))
        .executeTakeFirst(),
    );
  }),

  getAll: Effect.fn('Library.getAll')(function* (withDeleted = false) {
    return yield* query(db, (db) =>
      db
        .selectFrom('library')
        .selectAll('library')
        .orderBy('createdAt', 'asc')
        .$if(!withDeleted, (qb) => qb.where('library.deletedAt', 'is', null))
        .execute(),
    );
  }),

  getAllDeleted: Effect.fn('Library.getAllDeleted')(function* () {
    return yield* query(db, (db) =>
      db
        .selectFrom('library')
        .selectAll('library')
        .where('library.deletedAt', 'is not', null)
        .orderBy('createdAt', 'asc')
        .execute(),
    );
  }),

  create: Effect.fn('Library.create')(function* (library: Insertable<LibraryTable>) {
    return yield* query(db, (db) => db.insertInto('library').values(library).returningAll().executeTakeFirstOrThrow());
  }),

  delete: Effect.fn('Library.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('library').where('library.id', '=', id).execute());
  }),

  softDelete: Effect.fn('Library.softDelete')(function* (id: string) {
    yield* query(db, (db) =>
      db.updateTable('library').set({ deletedAt: new Date() }).where('library.id', '=', id).execute(),
    );
  }),

  update: Effect.fn('Library.update')(function* (id: string, library: Updateable<LibraryTable>) {
    return yield* query(db, (db) =>
      db.updateTable('library').set(library).where('library.id', '=', id).returningAll().executeTakeFirstOrThrow(),
    );
  }),

  getStatistics: Effect.fn('Library.getStatistics')(function* (id: string) {
    const stats = yield* query(db, (db) =>
      db
        .selectFrom('library')
        .innerJoin('asset', 'asset.libraryId', 'library.id')
        .leftJoin('asset_exif', 'asset_exif.assetId', 'asset.id')
        .select((eb) =>
          eb.fn
            .countAll<number>()
            .filterWhere((eb) =>
              eb.and([eb('asset.type', '=', AssetType.Image), eb('asset.visibility', '!=', AssetVisibility.Hidden)]),
            )
            .as('photos'),
        )
        .select((eb) =>
          eb.fn
            .countAll<number>()
            .filterWhere((eb) =>
              eb.and([eb('asset.type', '=', AssetType.Video), eb('asset.visibility', '!=', AssetVisibility.Hidden)]),
            )
            .as('videos'),
        )
        .select((eb) => eb.fn.coalesce((eb) => eb.fn.sum('asset_exif.fileSizeInByte'), eb.val(0)).as('usage'))
        .where('asset.deletedAt', 'is', null)
        .groupBy('library.id')
        .where('library.id', '=', id)
        .executeTakeFirst(),
    );

    // possibly a new library with 0 assets
    if (!stats) {
      const zero = sql<number>`0::int`;
      return yield* query(db, (db) =>
        db
          .selectFrom('library')
          .select(zero.as('photos'))
          .select(zero.as('videos'))
          .select(zero.as('usage'))
          .select(zero.as('total'))
          .where('library.id', '=', id)
          .executeTakeFirst(),
      );
    }

    return {
      photos: stats.photos,
      videos: stats.videos,
      usage: stats.usage,
      total: stats.photos + stats.videos,
    };
  }),

  streamAssetIds: Effect.fn('Library.streamAssetIds')((libraryId: string) =>
    Effect.sync(() => db.selectFrom('asset').select(['id']).where('libraryId', '=', libraryId).stream()),
  ),
});

export class LibraryRepo extends Context.Service<LibraryRepo, ReturnType<typeof makeLibrary>>()(
  'immich/repositories/LibraryRepo',
) {
  static readonly make = (db: Kysely<DB>) => LibraryRepo.of(makeLibrary(db));

  static readonly layer = Layer.effect(
    LibraryRepo,
    Effect.gen(function* () {
      return LibraryRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `LibraryRepo`. */
@Injectable()
export class LibraryRepository {
  private readonly effect: Context.Service.Shape<typeof LibraryRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = LibraryRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  get(id: string, withDeleted = false) {
    return runPromise(this.effect.get(id, withDeleted));
  }

  @GenerateSql({ params: [] })
  getAll(withDeleted = false) {
    return runPromise(this.effect.getAll(withDeleted));
  }

  @GenerateSql()
  getAllDeleted() {
    return runPromise(this.effect.getAllDeleted());
  }

  create(library: Insertable<LibraryTable>) {
    return runPromise(this.effect.create(library));
  }

  delete(id: string): Promise<void> {
    return runPromise(this.effect.delete(id));
  }

  softDelete(id: string): Promise<void> {
    return runPromise(this.effect.softDelete(id));
  }

  update(id: string, library: Updateable<LibraryTable>) {
    return runPromise(this.effect.update(id, library));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getStatistics(id: string): Promise<LibraryStatsResponseDto | undefined> {
    return runPromise(this.effect.getStatistics(id));
  }

  streamAssetIds(libraryId: string) {
    return runSync(this.effect.streamAssetIds(libraryId));
  }
}
