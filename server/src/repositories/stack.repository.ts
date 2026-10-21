import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { jsonArrayFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import type { ExpressionBuilder, Insertable, Kysely, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { StackTable } from 'src/schema/tables/stack.table.js';
import { asUuid, withDefaultVisibility } from 'src/utils/database.js';

export interface StackSearch {
  ownerId: string;
  primaryAssetId?: string;
}

const withAssets = (eb: ExpressionBuilder<DB, 'stack'>, withTags = false) => {
  return jsonArrayFrom(
    eb
      .selectFrom('asset')
      .selectAll('asset')
      .innerJoinLateral(
        (eb) =>
          eb
            .selectFrom('asset_exif')
            .select(columns.exif)
            .whereRef('asset_exif.assetId', '=', 'asset.id')
            .as('exifInfo'),
        (join) => join.onTrue(),
      )
      .$if(withTags, (eb) =>
        eb.select((eb) =>
          jsonArrayFrom(
            eb
              .selectFrom('tag')
              .select(columns.tag)
              .innerJoin('tag_asset', 'tag.id', 'tag_asset.tagId')
              .whereRef('tag_asset.assetId', '=', 'asset.id'),
          ).as('tags'),
        ),
      )
      .select((eb) => eb.fn.toJson('exifInfo').as('exifInfo'))
      .where('asset.deletedAt', 'is', null)
      .whereRef('asset.stackId', '=', 'stack.id')
      .$call(withDefaultVisibility)
      .orderBy('asset.fileCreatedAt', 'asc'),
  ).as('assets');
};

const methods = (db: Kysely<DB>) => ({
  search: Effect.fn('Stack.search')(function* (q: StackSearch) {
    return yield* query(db, (db) =>
      db
        .selectFrom('stack')
        .selectAll('stack')
        .select(withAssets)
        .where('stack.ownerId', '=', q.ownerId)
        .$if(!!q.primaryAssetId, (eb) => eb.where('stack.primaryAssetId', '=', q.primaryAssetId!))
        .execute(),
    );
  }),

  create: Effect.fn('Stack.create')(function* (
    entity: Omit<Insertable<StackTable>, 'primaryAssetId'>,
    assetIds: string[],
  ) {
    return yield* query(db, (db) =>
      db.transaction().execute(async (tx) => {
        const stacks = await tx
          .selectFrom('stack')
          .where('stack.ownerId', '=', entity.ownerId)
          .where('stack.primaryAssetId', 'in', assetIds)
          .select('stack.id')
          .select((eb) =>
            jsonArrayFrom(
              eb
                .selectFrom('asset')
                .select('asset.id')
                .whereRef('asset.stackId', '=', 'stack.id')
                .where('asset.deletedAt', 'is', null),
            ).as('assets'),
          )
          .execute();

        const uniqueIds = new Set<string>(assetIds);

        // children
        for (const stack of stacks) {
          if (stack.assets && stack.assets.length > 0) {
            for (const asset of stack.assets) {
              uniqueIds.add(asset.id);
            }
          }
        }

        if (stacks.length > 0) {
          await tx
            .deleteFrom('stack')
            .where(
              'id',
              'in',
              stacks.map((stack) => stack.id),
            )
            .execute();
        }

        const newRecord = await tx
          .insertInto('stack')
          .values({ ...entity, primaryAssetId: assetIds[0] })
          .returning('id')
          .executeTakeFirstOrThrow();

        await tx
          .updateTable('asset')
          .set({
            stackId: newRecord.id,
            updatedAt: new Date(),
          })
          .where('id', 'in', [...uniqueIds])
          .execute();

        return tx
          .selectFrom('stack')
          .selectAll('stack')
          .select(withAssets)
          .where('id', '=', newRecord.id)
          .executeTakeFirstOrThrow();
      }),
    );
  }),

  delete: Effect.fn('Stack.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('stack').where('id', '=', asUuid(id)).execute());
  }),

  deleteAll: Effect.fn('Stack.deleteAll')(function* (ids: string[]) {
    yield* query(db, (db) => db.deleteFrom('stack').where('id', 'in', ids).execute());
  }),

  update: Effect.fn('Stack.update')(function* (id: string, entity: Updateable<StackTable>) {
    return yield* query(db, (db) =>
      db
        .updateTable('stack')
        .set(entity)
        .where('id', '=', asUuid(id))
        .returningAll('stack')
        .returning((eb) => withAssets(eb, true))
        .executeTakeFirstOrThrow(),
    );
  }),

  getById: Effect.fn('Stack.getById')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('stack')
        .selectAll()
        .select((eb) => withAssets(eb, true))
        .where('id', '=', asUuid(id))
        .executeTakeFirst(),
    );
  }),

  getForAssetRemoval: Effect.fn('Stack.getForAssetRemoval')(function* (assetId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .leftJoin('stack', 'stack.id', 'asset.stackId')
        .select(['stackId as id', 'stack.primaryAssetId'])
        .where('asset.id', '=', assetId)
        .executeTakeFirst(),
    );
  }),

  merge: Effect.fn('Stack.merge')(function* ({ sourceId, targetId }: { sourceId: string; targetId: string }) {
    return yield* query(db, (db) =>
      db.updateTable('asset').set({ stackId: targetId }).where('asset.stackId', '=', sourceId).execute(),
    );
  }),
});

export class StackRepo extends Context.Service<StackRepo, ReturnType<typeof methods>>()(
  'immich/repositories/StackRepo',
) {
  static readonly make = (db: Kysely<DB>) => StackRepo.of(methods(db));

  static readonly layer = Layer.effect(
    StackRepo,
    Effect.gen(function* () {
      return StackRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `StackRepo`. */
@Injectable()
export class StackRepository {
  private readonly effect: Context.Service.Shape<typeof StackRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = StackRepo.make(db);
  }

  @GenerateSql({ params: [{ ownerId: DummyValue.UUID }] })
  search(options: StackSearch) {
    return runPromise(this.effect.search(options));
  }

  create(entity: Omit<Insertable<StackTable>, 'primaryAssetId'>, assetIds: string[]) {
    return runPromise(this.effect.create(entity, assetIds));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  delete(id: string): Promise<void> {
    return runPromise(this.effect.delete(id));
  }

  deleteAll(ids: string[]): Promise<void> {
    return runPromise(this.effect.deleteAll(ids));
  }

  update(id: string, entity: Updateable<StackTable>) {
    return runPromise(this.effect.update(id, entity));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getById(id: string) {
    return runPromise(this.effect.getById(id));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID] })
  getForAssetRemoval(assetId: string) {
    return runPromise(this.effect.getForAssetRemoval(assetId));
  }

  @GenerateSql({ params: [{ sourceId: DummyValue.UUID, targetId: DummyValue.UUID }] })
  merge({ sourceId, targetId }: { sourceId: string; targetId: string }) {
    return runPromise(this.effect.merge({ sourceId, targetId }));
  }
}
