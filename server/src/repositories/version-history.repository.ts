import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely } from 'kysely';
import { GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { VersionHistoryTable } from 'src/schema/tables/version-history.table.js';

const methods = (db: Kysely<DB>) => ({
  getAll: Effect.fn('VersionHistory.getAll')(function* () {
    return yield* query(db, (db) =>
      db.selectFrom('version_history').selectAll().orderBy('createdAt', 'desc').execute(),
    );
  }),

  getLatest: Effect.fn('VersionHistory.getLatest')(function* () {
    return yield* query(db, (db) =>
      db.selectFrom('version_history').selectAll().orderBy('createdAt', 'desc').executeTakeFirst(),
    );
  }),

  create: Effect.fn('VersionHistory.create')(function* (version: Insertable<VersionHistoryTable>) {
    return yield* query(db, (db) =>
      db.insertInto('version_history').values(version).returningAll().executeTakeFirstOrThrow(),
    );
  }),
});

export class VersionHistoryRepo extends Context.Service<VersionHistoryRepo, ReturnType<typeof methods>>()(
  'immich/repositories/VersionHistoryRepo',
) {
  static readonly make = (db: Kysely<DB>) => VersionHistoryRepo.of(methods(db));

  static readonly layer = Layer.effect(
    VersionHistoryRepo,
    Effect.gen(function* () {
      return VersionHistoryRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `VersionHistoryRepo`. */
@Injectable()
export class VersionHistoryRepository {
  private readonly effect: Context.Service.Shape<typeof VersionHistoryRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = VersionHistoryRepo.make(db);
    registerEffect(this, this.effect);
  }

  @GenerateSql()
  getAll() {
    return runPromise(this.effect.getAll());
  }

  @GenerateSql()
  getLatest() {
    return runPromise(this.effect.getLatest());
  }

  create(version: Insertable<VersionHistoryTable>) {
    return runPromise(this.effect.create(version));
  }
}
