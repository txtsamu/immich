import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type ExpressionBuilder, type Insertable, type Kysely, type Updateable, sql } from 'kysely';
import { jsonArrayFrom } from 'kysely/helpers/postgres';
import { DateTime } from 'luxon';
import { InjectKysely } from 'nestjs-kysely';
import type { UserMetadata, UserMetadataItem } from 'src/types.js';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AssetType, AssetVisibility, UserStatus } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { UserTable } from 'src/schema/tables/user.table.js';
import { asUuid } from 'src/utils/database.js';

export interface UserListFilter {
  id?: string;
  withDeleted?: boolean;
}

export interface UserStatsQueryResponse {
  userId: string;
  userName: string;
  photos: number;
  videos: number;
  usage: number;
  usagePhotos: number;
  usageVideos: number;
  quotaSizeInBytes: number | null;
}

export interface UserFindOptions {
  withDeleted?: boolean;
}

const withMetadata = (eb: ExpressionBuilder<DB, 'user'>) => {
  return jsonArrayFrom(
    eb
      .selectFrom('user_metadata')
      .select(['user_metadata.key', 'user_metadata.value'])
      .whereRef('user.id', '=', 'user_metadata.userId'),
  ).as('metadata');
};

const makeUser = (db: Kysely<DB>) => ({
  get: Effect.fn('User.get')(function* (userId: string, options: UserFindOptions) {
    options ||= {};

    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(columns.userAdmin)
        .select(withMetadata)
        .where('user.id', '=', userId)
        .$if(!options.withDeleted, (eb) => eb.where('user.deletedAt', 'is', null))
        .executeTakeFirst(),
    );
  }),

  getMetadata: Effect.fn('User.getMetadata')(function* (userId: string) {
    return yield* query(
      db,
      (db) =>
        db
          .selectFrom('user_metadata')
          .select(['key', 'value'])
          .where('user_metadata.userId', '=', userId)
          .execute() as Promise<UserMetadataItem[]>,
    );
  }),

  getAdmin: Effect.fn('User.getAdmin')(function* () {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(columns.userAdmin)
        .select(withMetadata)
        .where('user.isAdmin', '=', true)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirst(),
    );
  }),

  getFileSamples: Effect.fn('User.getFileSamples')(function* () {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(['id', 'profileImagePath'])
        .where('profileImagePath', '!=', sql.lit(''))
        .limit(sql.lit(3))
        .execute(),
    );
  }),

  hasAdmin: Effect.fn('User.hasAdmin')(function* () {
    const admin = yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select('user.id')
        .where('user.isAdmin', '=', true)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirst(),
    );

    return !!admin;
  }),

  getForPinCode: Effect.fn('User.getForPinCode')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(['user.pinCode', 'user.password'])
        .where('user.id', '=', id)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirstOrThrow(),
    );
  }),

  getForChangePassword: Effect.fn('User.getForChangePassword')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(['user.id', 'user.password'])
        .where('user.id', '=', id)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirstOrThrow(),
    );
  }),

  getByEmail: Effect.fn('User.getByEmail')(function* (email: string, options?: { withPassword?: boolean }) {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(columns.userAdmin)
        .select(withMetadata)
        .$if(!!options?.withPassword, (eb) => eb.select('password'))
        .where('email', '=', email)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirst(),
    );
  }),

  getByStorageLabel: Effect.fn('User.getByStorageLabel')(function* (storageLabel: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(columns.userAdmin)
        .where('user.storageLabel', '=', storageLabel)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirst(),
    );
  }),

  getByOAuthId: Effect.fn('User.getByOAuthId')(function* (oauthId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(columns.userAdmin)
        .select(withMetadata)
        .where('user.oauthId', '=', oauthId)
        .where('user.deletedAt', 'is', null)
        .executeTakeFirst(),
    );
  }),

  getDeletedAfter: Effect.fn('User.getDeletedAfter')(function* (target: DateTime) {
    return yield* query(db, (db) =>
      db.selectFrom('user').select(['id']).where('user.deletedAt', '<', target.toJSDate()).execute(),
    );
  }),

  getList: Effect.fn('User.getList')(function* ({ id, withDeleted }: UserListFilter = {}) {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select(columns.userAdmin)
        .select(withMetadata)
        .$if(!withDeleted, (eb) => eb.where('user.deletedAt', 'is', null))
        .$if(!!id, (eb) => eb.where('user.id', '=', id!))
        .orderBy('createdAt', 'desc')
        .execute(),
    );
  }),

  create: Effect.fn('User.create')(function* (dto: Insertable<UserTable>) {
    return yield* query(db, (db) =>
      db.insertInto('user').values(dto).returning(columns.userAdmin).returning(withMetadata).executeTakeFirstOrThrow(),
    );
  }),

  update: Effect.fn('User.update')(function* (id: string, dto: Updateable<UserTable>) {
    return yield* query(db, (db) =>
      db
        .updateTable('user')
        .set(dto)
        .where('user.id', '=', asUuid(id))
        .where('user.deletedAt', 'is', null)
        .returning(columns.userAdmin)
        .returning(withMetadata)
        .executeTakeFirstOrThrow(),
    );
  }),

  updateAll: Effect.fn('User.updateAll')(function* (dto: Updateable<UserTable>) {
    yield* query(db, (db) => db.updateTable('user').set(dto).execute());
  }),

  restore: Effect.fn('User.restore')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .updateTable('user')
        .set({ status: UserStatus.Active, deletedAt: null })
        .where('user.id', '=', asUuid(id))
        .returning(columns.userAdmin)
        .returning(withMetadata)
        .executeTakeFirstOrThrow(),
    );
  }),

  upsertMetadata: Effect.fn('User.upsertMetadata')(function* <T extends keyof UserMetadata>(
    id: string,
    { key, value }: { key: T; value: UserMetadata[T] },
  ) {
    yield* query(db, (db) =>
      db
        .insertInto('user_metadata')
        .values({ userId: id, key, value })
        .onConflict((oc) =>
          oc.columns(['userId', 'key']).doUpdateSet({
            key,
            value,
          }),
        )
        .execute(),
    );
  }),

  deleteMetadata: Effect.fn('User.deleteMetadata')(function* <T extends keyof UserMetadata>(id: string, key: T) {
    yield* query(db, (db) => db.deleteFrom('user_metadata').where('userId', '=', id).where('key', '=', key).execute());
  }),

  delete: Effect.fn('User.delete')(function* (user: { id: string }, hard?: boolean) {
    return yield* hard
      ? query(db, (db) => db.deleteFrom('user').where('id', '=', user.id).execute())
      : query(db, (db) => db.updateTable('user').set({ deletedAt: new Date() }).where('id', '=', user.id).execute());
  }),

  getUserStats: Effect.fn('User.getUserStats')(function* () {
    return yield* query(db, (db) =>
      db
        .selectFrom('user')
        .leftJoin('asset', (join) => join.onRef('asset.ownerId', '=', 'user.id').on('asset.deletedAt', 'is', null))
        .leftJoin('asset_exif', 'asset_exif.assetId', 'asset.id')
        .select(['user.id as userId', 'user.name as userName', 'user.quotaSizeInBytes'])
        .select((eb) => [
          eb.fn
            .countAll<number>()
            .filterWhere((eb) =>
              eb.and([
                eb('asset.type', '=', sql.lit(AssetType.Image)),
                eb('asset.visibility', '!=', sql.lit(AssetVisibility.Hidden)),
              ]),
            )
            .as('photos'),
          eb.fn
            .countAll<number>()
            .filterWhere((eb) =>
              eb.and([
                eb('asset.type', '=', sql.lit(AssetType.Video)),
                eb('asset.visibility', '!=', sql.lit(AssetVisibility.Hidden)),
              ]),
            )
            .as('videos'),
          eb.fn
            .coalesce(
              eb.fn.sum<number>('asset_exif.fileSizeInByte').filterWhere('asset.libraryId', 'is', null),
              eb.lit(0),
            )
            .as('usage'),
          eb.fn
            .coalesce(
              eb.fn
                .sum<number>('asset_exif.fileSizeInByte')
                .filterWhere((eb) =>
                  eb.and([eb('asset.libraryId', 'is', null), eb('asset.type', '=', sql.lit(AssetType.Image))]),
                ),
              eb.lit(0),
            )
            .as('usagePhotos'),
          eb.fn
            .coalesce(
              eb.fn
                .sum<number>('asset_exif.fileSizeInByte')
                .filterWhere((eb) =>
                  eb.and([eb('asset.libraryId', 'is', null), eb('asset.type', '=', sql.lit(AssetType.Video))]),
                ),
              eb.lit(0),
            )
            .as('usageVideos'),
        ])
        .groupBy('user.id')
        .orderBy('user.createdAt', 'asc')
        .execute(),
    );
  }),

  getCount: Effect.fn('User.getCount')(function* () {
    const result = yield* query(db, (db) =>
      db
        .selectFrom('user')
        .select((eb) => eb.fn.countAll().as('count'))
        .where('user.deletedAt', 'is', null)
        .executeTakeFirstOrThrow(),
    );
    return Number(result.count);
  }),

  updateUsage: Effect.fn('User.updateUsage')(function* (id: string, delta: number) {
    yield* query(db, (db) =>
      db
        .updateTable('user')
        .set({ quotaUsageInBytes: sql`"quotaUsageInBytes" + ${delta}`, updatedAt: new Date() })
        .where('id', '=', asUuid(id))
        .where('user.deletedAt', 'is', null)
        .execute(),
    );
  }),

  syncUsage: Effect.fn('User.syncUsage')(function* (id?: string) {
    yield* query(db, (db) =>
      db
        .updateTable('user')
        .set({
          quotaUsageInBytes: (eb) =>
            eb
              .selectFrom('asset')
              .leftJoin('asset_exif', 'asset_exif.assetId', 'asset.id')
              .select((eb) => eb.fn.coalesce(eb.fn.sum<number>('asset_exif.fileSizeInByte'), eb.lit(0)).as('usage'))
              .where('asset.libraryId', 'is', null)
              .where('asset.ownerId', '=', eb.ref('user.id')),
          updatedAt: new Date(),
        })
        .where('user.deletedAt', 'is', null)
        .$if(id !== undefined, (eb) => eb.where('user.id', '=', asUuid(id!)))
        .execute(),
    );
  }),
});

export class UserRepo extends Context.Service<UserRepo, ReturnType<typeof makeUser>>()('immich/repositories/UserRepo') {
  static readonly make = (db: Kysely<DB>) => UserRepo.of(makeUser(db));

  static readonly layer = Layer.effect(
    UserRepo,
    Effect.gen(function* () {
      return UserRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `UserRepo`. */
@Injectable()
export class UserRepository {
  private readonly effect: Context.Service.Shape<typeof UserRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = UserRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.BOOLEAN] })
  get(userId: string, options: UserFindOptions) {
    return runPromise(this.effect.get(userId, options));
  }

  getMetadata(userId: string) {
    return runPromise(this.effect.getMetadata(userId));
  }

  @GenerateSql()
  getAdmin() {
    return runPromise(this.effect.getAdmin());
  }

  @GenerateSql()
  getFileSamples() {
    return runPromise(this.effect.getFileSamples());
  }

  @GenerateSql()
  hasAdmin(): Promise<boolean> {
    return runPromise(this.effect.hasAdmin());
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForPinCode(id: string) {
    return runPromise(this.effect.getForPinCode(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForChangePassword(id: string) {
    return runPromise(this.effect.getForChangePassword(id));
  }

  @GenerateSql({ params: [DummyValue.EMAIL] })
  getByEmail(email: string, options?: { withPassword?: boolean }) {
    return runPromise(this.effect.getByEmail(email, options));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getByStorageLabel(storageLabel: string) {
    return runPromise(this.effect.getByStorageLabel(storageLabel));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getByOAuthId(oauthId: string) {
    return runPromise(this.effect.getByOAuthId(oauthId));
  }

  @GenerateSql({ params: [DateTime.now().minus({ years: 1 })] })
  getDeletedAfter(target: DateTime) {
    return runPromise(this.effect.getDeletedAfter(target));
  }

  @GenerateSql(
    { name: 'with deleted', params: [{ withDeleted: true }] },
    { name: 'without deleted', params: [{ withDeleted: false }] },
  )
  getList({ id, withDeleted }: UserListFilter = {}) {
    return runPromise(this.effect.getList({ id, withDeleted }));
  }

  create(dto: Insertable<UserTable>) {
    return runPromise(this.effect.create(dto));
  }

  update(id: string, dto: Updateable<UserTable>) {
    return runPromise(this.effect.update(id, dto));
  }

  updateAll(dto: Updateable<UserTable>) {
    return runPromise(this.effect.updateAll(dto));
  }

  restore(id: string) {
    return runPromise(this.effect.restore(id));
  }

  upsertMetadata<T extends keyof UserMetadata>(id: string, { key, value }: { key: T; value: UserMetadata[T] }) {
    return runPromise(this.effect.upsertMetadata(id, { key, value }));
  }

  deleteMetadata<T extends keyof UserMetadata>(id: string, key: T) {
    return runPromise(this.effect.deleteMetadata(id, key));
  }

  delete(user: { id: string }, hard?: boolean) {
    return runPromise(this.effect.delete(user, hard));
  }

  @GenerateSql()
  getUserStats() {
    return runPromise(this.effect.getUserStats());
  }

  @GenerateSql()
  getCount(): Promise<number> {
    return runPromise(this.effect.getCount());
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.NUMBER] })
  updateUsage(id: string, delta: number): Promise<void> {
    return runPromise(this.effect.updateUsage(id, delta));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  syncUsage(id?: string) {
    return runPromise(this.effect.syncUsage(id));
  }
}
