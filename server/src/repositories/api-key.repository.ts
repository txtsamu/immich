import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { jsonObjectFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { ApiKeyTable } from 'src/schema/tables/api-key.table.js';
import { asUuid } from 'src/utils/database.js';

const methods = (db: Kysely<DB>) => ({
  create: Effect.fn('ApiKey.create')(function* (dto: Insertable<ApiKeyTable>) {
    return yield* query(db, (db) =>
      db.insertInto('api_key').values(dto).returning(columns.apiKey).executeTakeFirstOrThrow(),
    );
  }),

  update: Effect.fn('ApiKey.update')(function* (userId: string, id: string, dto: Updateable<ApiKeyTable>) {
    return yield* query(db, (db) =>
      db
        .updateTable('api_key')
        .set(dto)
        .where('api_key.userId', '=', userId)
        .where('id', '=', asUuid(id))
        .returning(columns.apiKey)
        .executeTakeFirstOrThrow(),
    );
  }),

  delete: Effect.fn('ApiKey.delete')(function* (userId: string, id: string) {
    yield* query(db, (db) =>
      db.deleteFrom('api_key').where('userId', '=', userId).where('id', '=', asUuid(id)).execute(),
    );
  }),

  getKey: Effect.fn('ApiKey.getKey')(function* (hashedToken: Buffer) {
    return yield* query(db, (db) =>
      db
        .selectFrom('api_key')
        .select((eb) => [
          ...columns.authApiKey,
          jsonObjectFrom(
            eb
              .selectFrom('user')
              .select(columns.authUser)
              .whereRef('user.id', '=', 'api_key.userId')
              .where('user.deletedAt', 'is', null),
          ).as('user'),
        ])
        .where('api_key.key', '=', hashedToken)
        .executeTakeFirst(),
    );
  }),

  getById: Effect.fn('ApiKey.getById')(function* (userId: string, id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('api_key')
        .select(columns.apiKey)
        .where('id', '=', asUuid(id))
        .where('userId', '=', userId)
        .executeTakeFirst(),
    );
  }),

  getByUserId: Effect.fn('ApiKey.getByUserId')(function* (userId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('api_key')
        .select(columns.apiKey)
        .where('userId', '=', userId)
        .orderBy('createdAt', 'desc')
        .execute(),
    );
  }),
});

export class ApiKeyRepo extends Context.Service<ApiKeyRepo, ReturnType<typeof methods>>()(
  'immich/repositories/ApiKeyRepo',
) {
  static readonly make = (db: Kysely<DB>) => ApiKeyRepo.of(methods(db));

  static readonly layer = Layer.effect(
    ApiKeyRepo,
    Effect.gen(function* () {
      return ApiKeyRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `ApiKeyRepo`. */
@Injectable()
export class ApiKeyRepository {
  private readonly effect: Context.Service.Shape<typeof ApiKeyRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = ApiKeyRepo.make(db);
  }

  create(dto: Insertable<ApiKeyTable>) {
    return runPromise(this.effect.create(dto));
  }

  update(userId: string, id: string, dto: Updateable<ApiKeyTable>) {
    return runPromise(this.effect.update(userId, id, dto));
  }

  async delete(userId: string, id: string) {
    await runPromise(this.effect.delete(userId, id));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getKey(hashedToken: Buffer) {
    return runPromise(this.effect.getKey(hashedToken));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID] })
  getById(userId: string, id: string) {
    return runPromise(this.effect.getById(userId, id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getByUserId(userId: string) {
    return runPromise(this.effect.getByUserId(userId));
  }
}
