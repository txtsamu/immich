import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely } from 'kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AuthChallengeType } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { AuthChallengeTable } from 'src/schema/tables/auth-challenge.table.js';

const methods = (db: Kysely<DB>) => ({
  create: Effect.fn('AuthChallenge.create')(function* (dto: Insertable<AuthChallengeTable>) {
    return yield* query(db, (db) => db.insertInto('auth_challenge').values(dto).executeTakeFirstOrThrow());
  }),

  consume: Effect.fn('AuthChallenge.consume')(function* ({
    type,
    challenge,
  }: {
    type: AuthChallengeType;
    challenge: string;
  }) {
    return yield* query(db, (db) =>
      db
        .deleteFrom('auth_challenge')
        .where('challenge', '=', challenge)
        .where('type', '=', type)
        .where('expiresAt', '>', new Date())
        .returning(['id', 'userId'])
        .executeTakeFirst(),
    );
  }),

  deleteExpired: Effect.fn('AuthChallenge.deleteExpired')(function* () {
    const result = yield* query(db, (db) =>
      db.deleteFrom('auth_challenge').where('expiresAt', '<=', new Date()).executeTakeFirst(),
    );
    return Number(result.numDeletedRows);
  }),
});

export class AuthChallengeRepo extends Context.Service<AuthChallengeRepo, ReturnType<typeof methods>>()(
  'immich/repositories/AuthChallengeRepo',
) {
  static readonly make = (db: Kysely<DB>) => AuthChallengeRepo.of(methods(db));

  static readonly layer = Layer.effect(
    AuthChallengeRepo,
    Effect.gen(function* () {
      return AuthChallengeRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AuthChallengeRepo`. */
@Injectable()
export class AuthChallengeRepository {
  private readonly effect: Context.Service.Shape<typeof AuthChallengeRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AuthChallengeRepo.make(db);
    registerEffect(this, this.effect);
  }

  create(dto: Insertable<AuthChallengeTable>) {
    return runPromise(this.effect.create(dto));
  }

  @GenerateSql({ params: [{ type: AuthChallengeType.PasskeyRegistration, challenge: DummyValue.STRING }] })
  consume({ type, challenge }: { type: AuthChallengeType; challenge: string }) {
    return runPromise(this.effect.consume({ type, challenge }));
  }

  @GenerateSql()
  deleteExpired() {
    return runPromise(this.effect.deleteExpired());
  }
}
