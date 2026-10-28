import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { jsonObjectFrom } from 'kysely/helpers/postgres';
import { DateTime } from 'luxon';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { SessionTable } from 'src/schema/tables/session.table.js';
import { asUuid } from 'src/utils/database.js';

const makeSession = (db: Kysely<DB>) => ({
  cleanup: Effect.fn('Session.cleanup')(function* () {
    return yield* query(db, (db) =>
      db
        .deleteFrom('session')
        .where((eb) =>
          eb.or([
            eb('updatedAt', '<=', DateTime.now().minus({ days: 90 }).toJSDate()),
            eb.and([eb('expiresAt', 'is not', null), eb('expiresAt', '<=', DateTime.now().toJSDate())]),
          ]),
        )
        .returning(['id', 'deviceOS', 'deviceType'])
        .execute(),
    );
  }),

  get: Effect.fn('Session.get')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('session')
        .select(['id', 'expiresAt', 'pinExpiresAt', 'oauthBearerToken'])
        .where('id', '=', id)
        .executeTakeFirst(),
    );
  }),

  isPendingSyncReset: Effect.fn('Session.isPendingSyncReset')(function* (id: string) {
    const result = yield* query(db, (db) =>
      db.selectFrom('session').select(['isPendingSyncReset']).where('id', '=', id).executeTakeFirst(),
    );
    return result?.isPendingSyncReset ?? false;
  }),

  getByToken: Effect.fn('Session.getByToken')(function* (token: Buffer) {
    return yield* query(db, (db) =>
      db
        .selectFrom('session')
        .select((eb) => [
          ...columns.authSession,
          jsonObjectFrom(
            eb
              .selectFrom('user')
              .select(columns.authUser)
              .whereRef('user.id', '=', 'session.userId')
              .where('user.deletedAt', 'is', null),
          ).as('user'),
        ])
        .where('session.token', '=', token)
        .where((eb) =>
          eb.or([eb('session.expiresAt', 'is', null), eb('session.expiresAt', '>', DateTime.now().toJSDate())]),
        )
        .executeTakeFirst(),
    );
  }),

  getByUserId: Effect.fn('Session.getByUserId')(function* (userId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('session')
        .innerJoin('user', (join) => join.onRef('user.id', '=', 'session.userId').on('user.deletedAt', 'is', null))
        .selectAll('session')
        .where('session.userId', '=', userId)
        .where((eb) =>
          eb.or([eb('session.expiresAt', 'is', null), eb('session.expiresAt', '>', DateTime.now().toJSDate())]),
        )
        .orderBy('session.updatedAt', 'desc')
        .orderBy('session.createdAt', 'desc')
        .execute(),
    );
  }),

  create: Effect.fn('Session.create')(function* (dto: Insertable<SessionTable>) {
    return yield* query(db, (db) => db.insertInto('session').values(dto).returningAll().executeTakeFirstOrThrow());
  }),

  update: Effect.fn('Session.update')(function* (id: string, dto: Updateable<SessionTable>) {
    return yield* query(db, (db) =>
      db.updateTable('session').set(dto).where('session.id', '=', asUuid(id)).returningAll().executeTakeFirstOrThrow(),
    );
  }),

  delete: Effect.fn('Session.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('session').where('id', '=', asUuid(id)).execute());
  }),

  invalidateAll: Effect.fn('Session.invalidateAll')(function* ({
    userId,
    excludeId,
  }: {
    userId: string;
    excludeId?: string;
  }) {
    yield* query(db, (db) =>
      db
        .deleteFrom('session')
        .where('userId', '=', userId)
        .$if(!!excludeId, (qb) => qb.where('id', '!=', excludeId!))
        .execute(),
    );
  }),

  invalidateOAuth: Effect.fn('Session.invalidateOAuth')(function* ({
    oauthSid,
    oauthId,
  }: {
    oauthSid?: string;
    oauthId?: string;
  }) {
    const deletedRows = yield* query(db, async (db) => {
      let deleteQuery = db.deleteFrom('session').returning('session.id');

      if (oauthSid && oauthId) {
        deleteQuery = deleteQuery
          .using('user')
          .whereRef('user.id', '=', 'session.userId')
          .where('session.oauthSid', '=', oauthSid)
          .where('user.oauthId', '=', oauthId);
      } else if (!oauthSid && oauthId) {
        deleteQuery = deleteQuery
          .using('user')
          .whereRef('user.id', '=', 'session.userId')
          .where('user.oauthId', '=', oauthId);
      } else if (oauthSid && !oauthId) {
        deleteQuery = deleteQuery.where('session.oauthSid', '=', oauthSid);
      } else {
        throw new Error('Invalid arguments: at least one of oauthSid or oauthId must be present');
      }

      return deleteQuery.execute();
    });
    return deletedRows.map((row) => row.id);
  }),

  lockAll: Effect.fn('Session.lockAll')(function* (userId: string) {
    yield* query(db, (db) =>
      db.updateTable('session').set({ pinExpiresAt: null }).where('userId', '=', userId).execute(),
    );
  }),

  resetSyncProgress: Effect.fn('Session.resetSyncProgress')(function* (sessionId: string) {
    yield* query(db, (db) =>
      db.transaction().execute((tx) => {
        return Promise.all([
          tx.updateTable('session').set({ isPendingSyncReset: false }).where('id', '=', sessionId).execute(),
          tx.deleteFrom('session_sync_checkpoint').where('sessionId', '=', sessionId).execute(),
        ]);
      }),
    );
  }),
});

export class SessionRepo extends Context.Service<SessionRepo, ReturnType<typeof makeSession>>()(
  'immich/repositories/SessionRepo',
) {
  static readonly make = (db: Kysely<DB>) => SessionRepo.of(makeSession(db));

  static readonly layer = Layer.effect(
    SessionRepo,
    Effect.gen(function* () {
      return SessionRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SessionRepo`. */
@Injectable()
export class SessionRepository {
  private readonly effect: Context.Service.Shape<typeof SessionRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = SessionRepo.make(db);
    registerEffect(this, this.effect);
  }

  cleanup() {
    return runPromise(this.effect.cleanup());
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  get(id: string) {
    return runPromise(this.effect.get(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  isPendingSyncReset(id: string) {
    return runPromise(this.effect.isPendingSyncReset(id));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getByToken(token: Buffer) {
    return runPromise(this.effect.getByToken(token));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getByUserId(userId: string) {
    return runPromise(this.effect.getByUserId(userId));
  }

  create(dto: Insertable<SessionTable>) {
    return runPromise(this.effect.create(dto));
  }

  update(id: string, dto: Updateable<SessionTable>) {
    return runPromise(this.effect.update(id, dto));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  delete(id: string) {
    return runPromise(this.effect.delete(id));
  }

  @GenerateSql({ params: [{ userId: DummyValue.UUID, excludeId: DummyValue.UUID }] })
  invalidateAll({ userId, excludeId }: { userId: string; excludeId?: string }) {
    return runPromise(this.effect.invalidateAll({ userId, excludeId }));
  }

  @GenerateSql({ params: [DummyValue.STRING, DummyValue.STRING] })
  invalidateOAuth({ oauthSid, oauthId }: { oauthSid?: string; oauthId?: string }): Promise<string[]> {
    return runPromise(this.effect.invalidateOAuth({ oauthSid, oauthId }));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  lockAll(userId: string) {
    return runPromise(this.effect.lockAll(userId));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  resetSyncProgress(sessionId: string) {
    return runPromise(this.effect.resetSyncProgress(sessionId));
  }
}
