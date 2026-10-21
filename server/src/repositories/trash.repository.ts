import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Kysely } from 'kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { AssetStatus } from 'src/enum.js';
import { DB } from 'src/schema/index.js';

const methods = (db: Kysely<DB>) => ({
  /** Returns the lazy stream as-is; it is never buffered. */
  getDeletedIds: Effect.fn('Trash.getDeletedIds')(() =>
    Effect.sync((): AsyncIterableIterator<{ id: string }> =>
      db.selectFrom('asset').select(['id']).where('status', '=', AssetStatus.Deleted).stream(),
    ),
  ),

  restore: Effect.fn('Trash.restore')(function* (userId: string) {
    const { numUpdatedRows } = yield* query(db, (db) =>
      db
        .updateTable('asset')
        .where('ownerId', '=', userId)
        .where('status', '=', AssetStatus.Trashed)
        .set({ status: AssetStatus.Active, deletedAt: null })
        .executeTakeFirst(),
    );

    return Number(numUpdatedRows);
  }),

  empty: Effect.fn('Trash.empty')(function* (userId: string) {
    const { numUpdatedRows } = yield* query(db, (db) =>
      db
        .updateTable('asset')
        .where('ownerId', '=', userId)
        .where('status', '=', AssetStatus.Trashed)
        .set({ status: AssetStatus.Deleted })
        .executeTakeFirst(),
    );

    return Number(numUpdatedRows);
  }),

  restoreAll: Effect.fn('Trash.restoreAll')(function* (ids: string[]) {
    if (ids.length === 0) {
      return 0;
    }

    const { numUpdatedRows } = yield* query(db, (db) =>
      db
        .updateTable('asset')
        .where('status', '=', AssetStatus.Trashed)
        .where('id', 'in', ids)
        .set({ status: AssetStatus.Active, deletedAt: null })
        .executeTakeFirst(),
    );

    return Number(numUpdatedRows);
  }),
});

export class TrashRepo extends Context.Service<TrashRepo, ReturnType<typeof methods>>()(
  'immich/repositories/TrashRepo',
) {
  static readonly make = (db: Kysely<DB>) => TrashRepo.of(methods(db));

  static readonly layer = Layer.effect(
    TrashRepo,
    Effect.gen(function* () {
      return TrashRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `TrashRepo`. */
export class TrashRepository {
  private readonly effect: Context.Service.Shape<typeof TrashRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = TrashRepo.make(db);
  }

  getDeletedIds(): AsyncIterableIterator<{ id: string }> {
    return runSync(this.effect.getDeletedIds());
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  restore(userId: string): Promise<number> {
    return runPromise(this.effect.restore(userId));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  empty(userId: string): Promise<number> {
    return runPromise(this.effect.empty(userId));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  restoreAll(ids: string[]): Promise<number> {
    return runPromise(this.effect.restoreAll(ids));
  }
}
