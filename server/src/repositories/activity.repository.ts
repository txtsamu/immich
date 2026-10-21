import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Insertable, type Kysely, type NotNull, sql } from 'kysely';
import { jsonObjectFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { ActivityTable } from 'src/schema/tables/activity.table.js';
import { asUuid, dummy } from 'src/utils/database.js';

export interface ActivitySearch {
  albumId?: string;
  assetId?: string | null;
  userId?: string;
  isLiked?: boolean;
}

const methods = (db: Kysely<DB>) => ({
  search: Effect.fn('Activity.search')(function* (options: ActivitySearch) {
    const { userId, assetId, albumId, isLiked } = options;

    return yield* query(db, (db) =>
      db
        .selectFrom('activity')
        .selectAll('activity')
        .innerJoin('user as user2', (join) =>
          join.onRef('user2.id', '=', 'activity.userId').on('user2.deletedAt', 'is', null),
        )
        .innerJoinLateral(
          (eb) => eb.selectFrom(dummy).select(columns.user2).as('user'),
          (join) => join.onTrue(),
        )
        .select((eb) => eb.fn.toJson('user').as('user'))
        .leftJoin('asset', 'asset.id', 'activity.assetId')
        .$if(!!userId, (qb) => qb.where('activity.userId', '=', userId!))
        .$if(assetId === null, (qb) => qb.where('assetId', 'is', null))
        .$if(!!assetId, (qb) => qb.where('activity.assetId', '=', assetId!))
        .$if(!!albumId, (qb) => qb.where('activity.albumId', '=', albumId!))
        .$if(isLiked !== undefined, (qb) => qb.where('activity.isLiked', '=', isLiked!))
        .where('asset.deletedAt', 'is', null)
        .orderBy('activity.createdAt', 'asc')
        .execute(),
    );
  }),

  create: Effect.fn('Activity.create')(function* (activity: Insertable<ActivityTable>) {
    return yield* query(db, (db) =>
      db
        .insertInto('activity')
        .values(activity)
        .returningAll()
        .returning((eb) =>
          jsonObjectFrom(eb.selectFrom('user').whereRef('user.id', '=', 'activity.userId').select(columns.user)).as(
            'user',
          ),
        )
        .$narrowType<{ user: NotNull }>()
        .executeTakeFirstOrThrow(),
    );
  }),

  delete: Effect.fn('Activity.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('activity').where('id', '=', asUuid(id)).execute());
  }),

  getStatistics: Effect.fn('Activity.getStatistics')(function* ({
    albumId,
    assetId,
  }: {
    albumId: string;
    assetId?: string;
  }) {
    const result: { comments: number; likes: number } = yield* query(db, (db) =>
      db
        .selectFrom('activity')
        .select((eb) => [
          eb.fn.countAll<number>().filterWhere('activity.isLiked', '=', false).as('comments'),
          eb.fn.countAll<number>().filterWhere('activity.isLiked', '=', true).as('likes'),
        ])
        .innerJoin('user', (join) => join.onRef('user.id', '=', 'activity.userId').on('user.deletedAt', 'is', null))
        .leftJoin('asset', 'asset.id', 'activity.assetId')
        .$if(!!assetId, (qb) => qb.where('activity.assetId', '=', assetId!))
        .where('activity.albumId', '=', albumId)
        .where(({ or, and, eb }) =>
          or([
            and([eb('asset.deletedAt', 'is', null), eb('asset.visibility', '!=', sql.lit(AssetVisibility.Locked))]),
            eb('asset.id', 'is', null),
          ]),
        )
        .executeTakeFirstOrThrow(),
    );

    return result;
  }),
});

export class ActivityRepo extends Context.Service<ActivityRepo, ReturnType<typeof methods>>()(
  'immich/repositories/ActivityRepo',
) {
  static readonly make = (db: Kysely<DB>) => ActivityRepo.of(methods(db));

  static readonly layer = Layer.effect(
    ActivityRepo,
    Effect.gen(function* () {
      return ActivityRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `ActivityRepo`. */
@Injectable()
export class ActivityRepository {
  private readonly effect: Context.Service.Shape<typeof ActivityRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = ActivityRepo.make(db);
  }

  @GenerateSql({ params: [{ albumId: DummyValue.UUID }] })
  search(options: ActivitySearch) {
    return runPromise(this.effect.search(options));
  }

  @GenerateSql({ params: [{ albumId: DummyValue.UUID, userId: DummyValue.UUID }] })
  create(activity: Insertable<ActivityTable>) {
    return runPromise(this.effect.create(activity));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  async delete(id: string) {
    await runPromise(this.effect.delete(id));
  }

  @GenerateSql({ params: [{ albumId: DummyValue.UUID, assetId: DummyValue.UUID }] })
  getStatistics({
    albumId,
    assetId,
  }: {
    albumId: string;
    assetId?: string;
  }): Promise<{ comments: number; likes: number }> {
    return runPromise(this.effect.getStatistics({ albumId, assetId }));
  }
}
