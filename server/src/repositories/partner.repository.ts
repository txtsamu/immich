import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { jsonObjectFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import type { ExpressionBuilder, Insertable, Kysely, NotNull, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { PartnerTable } from 'src/schema/tables/partner.table.js';

export interface PartnerIds {
  sharedById: string;
  sharedWithId: string;
}

const withSharedBy = (eb: ExpressionBuilder<DB, 'partner'>) => {
  return jsonObjectFrom(
    eb.selectFrom('user as sharedBy').select(columns.user).whereRef('sharedBy.id', '=', 'partner.sharedById'),
  ).as('sharedBy');
};

const withSharedWith = (eb: ExpressionBuilder<DB, 'partner'>) => {
  return jsonObjectFrom(
    eb.selectFrom('user as sharedWith').select(columns.user).whereRef('sharedWith.id', '=', 'partner.sharedWithId'),
  ).as('sharedWith');
};

const builder = (db: Kysely<DB>) =>
  db
    .selectFrom('partner')
    .innerJoin('user as sharedBy', (join) =>
      join.onRef('partner.sharedById', '=', 'sharedBy.id').on('sharedBy.deletedAt', 'is', null),
    )
    .innerJoin('user as sharedWith', (join) =>
      join.onRef('partner.sharedWithId', '=', 'sharedWith.id').on('sharedWith.deletedAt', 'is', null),
    )
    .selectAll('partner')
    .select(withSharedBy)
    .select(withSharedWith);

const methods = (db: Kysely<DB>) => ({
  getAll: Effect.fn('Partner.getAll')(function* (userId: string) {
    return yield* query(db, (db) =>
      builder(db)
        .where((eb) => eb.or([eb('sharedWithId', '=', userId), eb('sharedById', '=', userId)]))
        .execute(),
    );
  }),

  get: Effect.fn('Partner.get')(function* ({ sharedWithId, sharedById }: PartnerIds) {
    return yield* query(db, (db) =>
      builder(db).where('sharedWithId', '=', sharedWithId).where('sharedById', '=', sharedById).executeTakeFirst(),
    );
  }),

  create: Effect.fn('Partner.create')(function* (values: Insertable<PartnerTable>) {
    return yield* query(db, (db) =>
      db
        .insertInto('partner')
        .values(values)
        .returningAll()
        .returning(withSharedBy)
        .returning(withSharedWith)
        .$narrowType<{ sharedWith: NotNull; sharedBy: NotNull }>()
        .executeTakeFirstOrThrow(),
    );
  }),

  update: Effect.fn('Partner.update')(function* (
    { sharedWithId, sharedById }: PartnerIds,
    values: Updateable<PartnerTable>,
  ) {
    return yield* query(db, (db) =>
      db
        .updateTable('partner')
        .set(values)
        .where('sharedWithId', '=', sharedWithId)
        .where('sharedById', '=', sharedById)
        .returningAll()
        .returning(withSharedBy)
        .returning(withSharedWith)
        .$narrowType<{ sharedWith: NotNull; sharedBy: NotNull }>()
        .executeTakeFirstOrThrow(),
    );
  }),

  remove: Effect.fn('Partner.remove')(function* ({ sharedWithId, sharedById }: PartnerIds) {
    yield* query(db, (db) =>
      db.deleteFrom('partner').where('sharedWithId', '=', sharedWithId).where('sharedById', '=', sharedById).execute(),
    );
  }),
});

export class PartnerRepo extends Context.Service<PartnerRepo, ReturnType<typeof methods>>()(
  'immich/repositories/PartnerRepo',
) {
  static readonly make = (db: Kysely<DB>) => PartnerRepo.of(methods(db));

  static readonly layer = Layer.effect(
    PartnerRepo,
    Effect.gen(function* () {
      return PartnerRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `PartnerRepo`. */
@Injectable()
export class PartnerRepository {
  private readonly effect: Context.Service.Shape<typeof PartnerRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = PartnerRepo.make(db);
    registerEffect(this, this.effect);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getAll(userId: string) {
    return runPromise(this.effect.getAll(userId));
  }

  @GenerateSql({ params: [{ sharedWithId: DummyValue.UUID, sharedById: DummyValue.UUID }] })
  get({ sharedWithId, sharedById }: PartnerIds) {
    return runPromise(this.effect.get({ sharedWithId, sharedById }));
  }

  create(values: Insertable<PartnerTable>) {
    return runPromise(this.effect.create(values));
  }

  @GenerateSql({ params: [{ sharedWithId: DummyValue.UUID, sharedById: DummyValue.UUID }, { inTimeline: true }] })
  update({ sharedWithId, sharedById }: PartnerIds, values: Updateable<PartnerTable>) {
    return runPromise(this.effect.update({ sharedWithId, sharedById }, values));
  }

  @GenerateSql({ params: [{ sharedWithId: DummyValue.UUID, sharedById: DummyValue.UUID }] })
  async remove({ sharedWithId, sharedById }: PartnerIds) {
    await runPromise(this.effect.remove({ sharedWithId, sharedById }));
  }
}
