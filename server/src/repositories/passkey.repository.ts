import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { PasskeyTable } from 'src/schema/tables/passkey.table.js';
import { asUuid } from 'src/utils/database.js';

export type PasskeySearchOptions = {
  id?: string;
  userId?: string;
};

const methods = (db: Kysely<DB>) => ({
  create: Effect.fn('Passkey.create')(function* (dto: Insertable<PasskeyTable>) {
    return yield* query(db, (db) =>
      db.insertInto('passkey').values(dto).returning(columns.passkey).executeTakeFirstOrThrow(),
    );
  }),

  update: Effect.fn('Passkey.update')(function* (id: string, dto: Updateable<PasskeyTable>) {
    return yield* query(db, (db) =>
      db
        .updateTable('passkey')
        .set(dto)
        .where('id', '=', asUuid(id))
        .returning(columns.passkey)
        .executeTakeFirstOrThrow(),
    );
  }),

  delete: Effect.fn('Passkey.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('passkey').where('id', '=', asUuid(id)).execute());
  }),

  getByCredentialId: Effect.fn('Passkey.getByCredentialId')(function* (credentialId: string) {
    return yield* query(db, (db) =>
      db.selectFrom('passkey').selectAll().where('credentialId', '=', credentialId).executeTakeFirst(),
    );
  }),

  get: Effect.fn('Passkey.get')(function* (id: string) {
    return yield* query(db, (db) =>
      db.selectFrom('passkey').select(columns.passkey).where('id', '=', asUuid(id)).executeTakeFirst(),
    );
  }),

  search: Effect.fn('Passkey.search')(function* (options: PasskeySearchOptions) {
    return yield* query(db, (db) =>
      db
        .selectFrom('passkey')
        .select(columns.passkey)
        .$if(!!options.id, (qb) => qb.where('id', '=', asUuid(options.id!)))
        .$if(!!options.userId, (qb) => qb.where('userId', '=', options.userId!))
        .orderBy('createdAt', 'desc')
        .execute(),
    );
  }),
});

export class PasskeyRepo extends Context.Service<PasskeyRepo, ReturnType<typeof methods>>()(
  'immich/repositories/PasskeyRepo',
) {
  static readonly make = (db: Kysely<DB>) => PasskeyRepo.of(methods(db));

  static readonly layer = Layer.effect(
    PasskeyRepo,
    Effect.gen(function* () {
      return PasskeyRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `PasskeyRepo`. */
@Injectable()
export class PasskeyRepository {
  private readonly effect: Context.Service.Shape<typeof PasskeyRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = PasskeyRepo.make(db);
    registerEffect(this, this.effect);
  }

  create(dto: Insertable<PasskeyTable>) {
    return runPromise(this.effect.create(dto));
  }

  update(id: string, dto: Updateable<PasskeyTable>) {
    return runPromise(this.effect.update(id, dto));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  async delete(id: string) {
    await runPromise(this.effect.delete(id));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getByCredentialId(credentialId: string) {
    return runPromise(this.effect.getByCredentialId(credentialId));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  get(id: string) {
    return runPromise(this.effect.get(id));
  }

  @GenerateSql({ params: [{ userId: DummyValue.UUID }] })
  search(options: PasskeySearchOptions) {
    return runPromise(this.effect.search(options));
  }
}
