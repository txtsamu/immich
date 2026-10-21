import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Insertable, type Kysely, type OrderByDirection, type Updateable, sql } from 'kysely';
import { jsonArrayFrom } from 'kysely/helpers/postgres';
import { DateTime } from 'luxon';
import { InjectKysely } from 'nestjs-kysely';
import type { IBulkAsset } from 'src/types.js';
import { Chunked, ChunkedSet, DummyValue, GenerateSql } from 'src/decorators.js';
import { MemorySearchDto } from 'src/dtos/memory.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { AssetOrderWithRandom, AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { MemoryTable } from 'src/schema/tables/memory.table.js';
import { asLocalTime } from 'src/utils/date.js';

const searchBuilder = (db: Kysely<DB>, ownerId: string, dto: MemorySearchDto) =>
  db
    .selectFrom('memory')
    .$if(dto.isSaved !== undefined, (qb) => qb.where('isSaved', '=', dto.isSaved!))
    .$if(dto.type !== undefined, (qb) => qb.where('type', '=', dto.type!))
    .$if(dto.for !== undefined, (qb) =>
      qb
        .where((where) => where.or([where('showAt', 'is', null), where('showAt', '<=', dto.for!)]))
        .where((where) => where.or([where('hideAt', 'is', null), where('hideAt', '>=', dto.for!)])),
    )
    .$if(dto.isUpcoming !== undefined, (qb) => {
      const now = asLocalTime(DateTime.now());
      return dto.isUpcoming
        ? qb.where('showAt', '>', now)
        : qb.where((where) => where.or([where('showAt', 'is', null), where('showAt', '<=', now)]));
    })
    .where('deletedAt', dto.isTrashed ? 'is not' : 'is', null)
    .where('ownerId', '=', ownerId);

const getByIdBuilder = (db: Kysely<DB>, id: string) =>
  db
    .selectFrom('memory')
    .selectAll('memory')
    .select((eb) =>
      jsonArrayFrom(
        eb
          .selectFrom('asset')
          .selectAll('asset')
          .innerJoin('memory_asset', 'asset.id', 'memory_asset.assetId')
          .whereRef('memory_asset.memoriesId', '=', 'memory.id')
          .orderBy('asset.fileCreatedAt', 'asc')
          .where('asset.visibility', '=', sql.lit(AssetVisibility.Timeline))
          .where('asset.deletedAt', 'is', null),
      ).as('assets'),
    )
    .where('id', '=', id)
    .where('deletedAt', 'is', null);

const makeMemory = (db: Kysely<DB>) => ({
  cleanup: Effect.fn('Memory.cleanup')(function* () {
    yield* query(db, (db) =>
      db
        .deleteFrom('memory_asset')
        .using('asset')
        .whereRef('memory_asset.assetId', '=', 'asset.id')
        .where('asset.visibility', '!=', AssetVisibility.Timeline)
        .execute(),
    );

    return yield* query(db, (db) =>
      db
        .deleteFrom('memory')
        .where('createdAt', '<', DateTime.now().minus({ days: 30 }).toJSDate())
        .where('isSaved', '=', false)
        .execute(),
    );
  }),

  searchBuilder: Effect.fn('Memory.searchBuilder')((ownerId: string, dto: MemorySearchDto) =>
    Effect.sync(() => searchBuilder(db, ownerId, dto)),
  ),

  statistics: Effect.fn('Memory.statistics')(function* (ownerId: string, dto: MemorySearchDto) {
    return yield* query(db, (db) =>
      searchBuilder(db, ownerId, dto)
        .select((qb) => qb.fn.countAll<number>().as('total'))
        .executeTakeFirstOrThrow(),
    );
  }),

  search: Effect.fn('Memory.search')(function* (ownerId: string, dto: MemorySearchDto) {
    return yield* query(db, (db) =>
      searchBuilder(db, ownerId, dto)
        .select((eb) =>
          jsonArrayFrom(
            eb
              .selectFrom('asset')
              .selectAll('asset')
              .innerJoin('memory_asset', 'asset.id', 'memory_asset.assetId')
              .whereRef('memory_asset.memoriesId', '=', 'memory.id')
              .where('asset.visibility', '=', sql.lit(AssetVisibility.Timeline))
              .where('asset.deletedAt', 'is', null)
              .orderBy('asset.fileCreatedAt', 'asc'),
          ).as('assets'),
        )
        .selectAll('memory')
        .$call((qb) => {
          if (dto.order === AssetOrderWithRandom.Random) {
            return qb.orderBy(sql`RANDOM()`);
          }

          const direction = (dto.order?.toLowerCase() || 'desc') as OrderByDirection;
          return qb
            .orderBy('showAt', (ob) => (direction === 'asc' ? ob.asc() : ob.desc()).nullsLast())
            .orderBy('memoryAt', direction);
        })
        .$if(dto.id !== undefined, (qb) => qb.where('id', '=', dto.id!))
        .$if(dto.size !== undefined, (qb) => qb.limit(dto.size!))
        .$if(dto.page !== undefined && dto.size !== undefined, (qb) => qb.offset((dto.page! - 1) * dto.size!))
        .execute(),
    );
  }),

  get: Effect.fn('Memory.get')(function* (id: string) {
    return yield* query(db, (db) => getByIdBuilder(db, id).executeTakeFirst());
  }),

  create: Effect.fn('Memory.create')(function* (memory: Insertable<MemoryTable>, assetIds: Set<string>) {
    const id = yield* query(db, (db) =>
      db.transaction().execute(async (tx) => {
        const { id } = await tx.insertInto('memory').values(memory).returning('id').executeTakeFirstOrThrow();

        if (assetIds.size > 0) {
          const values = [...assetIds].map((assetId) => ({ memoriesId: id, assetId }));
          await tx.insertInto('memory_asset').values(values).execute();
        }

        return id;
      }),
    );

    return yield* query(db, (db) => getByIdBuilder(db, id).executeTakeFirstOrThrow());
  }),

  update: Effect.fn('Memory.update')(function* (id: string, memory: Updateable<MemoryTable>) {
    yield* query(db, (db) => db.updateTable('memory').set(memory).where('id', '=', id).execute());
    return yield* query(db, (db) => getByIdBuilder(db, id).executeTakeFirstOrThrow());
  }),

  delete: Effect.fn('Memory.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('memory').where('id', '=', id).execute());
  }),

  getAssetIds: Effect.fn('Memory.getAssetIds')(function* (id: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return new Set<string>();
    }

    const results = yield* query(db, (db) =>
      db
        .selectFrom('memory_asset')
        .select(['assetId'])
        .where('memoriesId', '=', id)
        .where('assetId', 'in', assetIds)
        .execute(),
    );

    return new Set(results.map(({ assetId }) => assetId));
  }),

  addAssetIds: Effect.fn('Memory.addAssetIds')(function* (id: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return;
    }

    yield* query(db, (db) =>
      db
        .insertInto('memory_asset')
        .values(assetIds.map((assetId) => ({ memoriesId: id, assetId })))
        .execute(),
    );
  }),

  removeAssetIds: Effect.fn('Memory.removeAssetIds')(function* (id: string, assetIds: string[]) {
    if (assetIds.length === 0) {
      return;
    }

    yield* query(db, (db) =>
      db.deleteFrom('memory_asset').where('memoriesId', '=', id).where('assetId', 'in', assetIds).execute(),
    );
  }),
});

export class MemoryRepo extends Context.Service<MemoryRepo, ReturnType<typeof makeMemory>>()(
  'immich/repositories/MemoryRepo',
) {
  static readonly make = (db: Kysely<DB>) => MemoryRepo.of(makeMemory(db));

  static readonly layer = Layer.effect(
    MemoryRepo,
    Effect.gen(function* () {
      return MemoryRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `MemoryRepo`. */
@Injectable()
export class MemoryRepository implements IBulkAsset {
  private readonly effect: Context.Service.Shape<typeof MemoryRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = MemoryRepo.make(db);
    registerEffect(this, this.effect);
  }

  cleanup() {
    return runPromise(this.effect.cleanup());
  }

  searchBuilder(ownerId: string, dto: MemorySearchDto) {
    return runSync(this.effect.searchBuilder(ownerId, dto));
  }

  @GenerateSql(
    { params: [DummyValue.UUID, {}] },
    { name: 'date filter', params: [DummyValue.UUID, { for: DummyValue.DATE }] },
  )
  statistics(ownerId: string, dto: MemorySearchDto) {
    return runPromise(this.effect.statistics(ownerId, dto));
  }

  @GenerateSql(
    { params: [DummyValue.UUID, {}] },
    { name: 'date filter', params: [DummyValue.UUID, { for: DummyValue.DATE }] },
    { name: 'upcoming filter', params: [DummyValue.UUID, { isUpcoming: true }] },
    { name: 'not upcoming filter', params: [DummyValue.UUID, { isUpcoming: false }] },
  )
  search(ownerId: string, dto: MemorySearchDto) {
    return runPromise(this.effect.search(ownerId, dto));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  get(id: string) {
    return runPromise(this.effect.get(id));
  }

  create(memory: Insertable<MemoryTable>, assetIds: Set<string>) {
    return runPromise(this.effect.create(memory, assetIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, { ownerId: DummyValue.UUID, isSaved: true }] })
  update(id: string, memory: Updateable<MemoryTable>) {
    return runPromise(this.effect.update(id, memory));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  delete(id: string) {
    return runPromise(this.effect.delete(id));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID]] })
  @ChunkedSet({ paramIndex: 1 })
  getAssetIds(id: string, assetIds: string[]) {
    return runPromise(this.effect.getAssetIds(id, assetIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID]] })
  addAssetIds(id: string, assetIds: string[]) {
    return runPromise(this.effect.addAssetIds(id, assetIds));
  }

  @Chunked({ paramIndex: 1 })
  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID]] })
  removeAssetIds(id: string, assetIds: string[]) {
    return runPromise(this.effect.removeAssetIds(id, assetIds));
  }
}
