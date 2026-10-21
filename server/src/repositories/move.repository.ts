import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Insertable, type Kysely, type Updateable, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AssetPathType, type PathType } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { MoveTable } from 'src/schema/tables/move.table.js';

const makeMove = (db: Kysely<DB>) => ({
  create: Effect.fn('Move.create')(function* (entity: Insertable<MoveTable>) {
    return yield* query(db, (db) =>
      db.insertInto('move_history').values(entity).returningAll().executeTakeFirstOrThrow(),
    );
  }),

  getByEntity: Effect.fn('Move.getByEntity')(function* (entityId: string, pathType: PathType) {
    return yield* query(db, (db) =>
      db
        .selectFrom('move_history')
        .selectAll()
        .where('entityId', '=', entityId)
        .where('pathType', '=', pathType)
        .executeTakeFirst(),
    );
  }),

  update: Effect.fn('Move.update')(function* (id: string, entity: Updateable<MoveTable>) {
    return yield* query(db, (db) =>
      db.updateTable('move_history').set(entity).where('id', '=', id).returningAll().executeTakeFirstOrThrow(),
    );
  }),

  delete: Effect.fn('Move.delete')(function* (id: string) {
    return yield* query(db, (db) =>
      db.deleteFrom('move_history').where('id', '=', id).returningAll().executeTakeFirstOrThrow(),
    );
  }),

  cleanMoveHistory: Effect.fn('Move.cleanMoveHistory')(function* () {
    yield* query(db, (db) =>
      db
        .deleteFrom('move_history')
        .where((eb) =>
          eb(
            'move_history.entityId',
            'not in',
            eb.selectFrom('asset').select('id').whereRef('asset.id', '=', 'move_history.entityId'),
          ),
        )
        .where('move_history.pathType', '=', sql.lit(AssetPathType.Original))
        .execute(),
    );
  }),

  cleanMoveHistorySingle: Effect.fn('Move.cleanMoveHistorySingle')(function* (assetId: string) {
    yield* query(db, (db) =>
      db
        .deleteFrom('move_history')
        .where('move_history.pathType', '=', sql.lit(AssetPathType.Original))
        .where('entityId', '=', assetId)
        .execute(),
    );
  }),
});

export class MoveRepo extends Context.Service<MoveRepo, ReturnType<typeof makeMove>>()('immich/repositories/MoveRepo') {
  static readonly make = (db: Kysely<DB>) => MoveRepo.of(makeMove(db));

  static readonly layer = Layer.effect(
    MoveRepo,
    Effect.gen(function* () {
      return MoveRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `MoveRepo`. */
@Injectable()
export class MoveRepository {
  private readonly effect: Context.Service.Shape<typeof MoveRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = MoveRepo.make(db);
    registerEffect(this, this.effect);
  }

  create(entity: Insertable<MoveTable>) {
    return runPromise(this.effect.create(entity));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.STRING] })
  getByEntity(entityId: string, pathType: PathType) {
    return runPromise(this.effect.getByEntity(entityId, pathType));
  }

  update(id: string, entity: Updateable<MoveTable>) {
    return runPromise(this.effect.update(id, entity));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  delete(id: string) {
    return runPromise(this.effect.delete(id));
  }

  cleanMoveHistory(): Promise<void> {
    return runPromise(this.effect.cleanMoveHistory());
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  cleanMoveHistorySingle(assetId: string): Promise<void> {
    return runPromise(this.effect.cleanMoveHistorySingle(assetId));
  }
}
