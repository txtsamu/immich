import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Insertable, type Kysely, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { SyncEntityType } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { SessionSyncCheckpointTable } from 'src/schema/tables/sync-checkpoint.table.js';

const makeSyncCheckpointService = (db: Kysely<DB>) => ({
  getAll: Effect.fn('SyncCheckpoint.getAll')(function* (sessionId: string) {
    return yield* query(db, (db) =>
      db.selectFrom('session_sync_checkpoint').select(['type', 'ack']).where('sessionId', '=', sessionId).execute(),
    );
  }),

  upsertAll: Effect.fn('SyncCheckpoint.upsertAll')(function* (items: Insertable<SessionSyncCheckpointTable>[]) {
    return yield* query(db, (db) =>
      db
        .insertInto('session_sync_checkpoint')
        .values(items)
        .onConflict((oc) =>
          oc.columns(['sessionId', 'type']).doUpdateSet((eb) => ({
            ack: eb.ref('excluded.ack'),
          })),
        )
        .execute(),
    );
  }),

  deleteAll: Effect.fn('SyncCheckpoint.deleteAll')(function* (sessionId: string, types?: SyncEntityType[]) {
    return yield* query(db, (db) =>
      db
        .deleteFrom('session_sync_checkpoint')
        .where('sessionId', '=', sessionId)
        .$if(!!types, (qb) => qb.where('type', 'in', types!))
        .execute(),
    );
  }),

  getNow: Effect.fn('SyncCheckpoint.getNow')(function* () {
    return yield* query(db, (db) =>
      db
        .selectNoFrom((eb) => [
          eb.fn<string>('immich_uuid_v7', [sql.raw<Date>("now() - interval '1 millisecond'")]).as('nowId'),
        ])
        .executeTakeFirstOrThrow(),
    );
  }),
});

export class SyncCheckpointRepo extends Context.Service<
  SyncCheckpointRepo,
  ReturnType<typeof makeSyncCheckpointService>
>()('immich/repositories/SyncCheckpointRepo') {
  static readonly make = (db: Kysely<DB>) => SyncCheckpointRepo.of(makeSyncCheckpointService(db));

  static readonly layer = Layer.effect(
    SyncCheckpointRepo,
    Effect.gen(function* () {
      return SyncCheckpointRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SyncCheckpointRepo`. */
@Injectable()
export class SyncCheckpointRepository {
  private readonly effect: Context.Service.Shape<typeof SyncCheckpointRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = SyncCheckpointRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getAll(sessionId: string) {
    return runPromise(this.effect.getAll(sessionId));
  }

  upsertAll(items: Insertable<SessionSyncCheckpointTable>[]) {
    return runPromise(this.effect.upsertAll(items));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  deleteAll(sessionId: string, types?: SyncEntityType[]) {
    return runPromise(this.effect.deleteAll(sessionId, types));
  }

  @GenerateSql()
  getNow() {
    return runPromise(this.effect.getNow());
  }
}
