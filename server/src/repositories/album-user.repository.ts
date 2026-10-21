import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely, Updateable } from 'kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumUserRole } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { AlbumUserTable } from 'src/schema/tables/album-user.table.js';

export type AlbumPermissionId = {
  albumId: string;
  userId: string;
};

const methods = (db: Kysely<DB>) => ({
  create: Effect.fn('AlbumUser.create')(function* (albumUser: Insertable<AlbumUserTable>) {
    return yield* query(db, (db) =>
      db.insertInto('album_user').values(albumUser).returning(['userId', 'albumId', 'role']).executeTakeFirstOrThrow(),
    );
  }),

  update: Effect.fn('AlbumUser.update')(function* (
    { userId, albumId }: AlbumPermissionId,
    dto: Updateable<AlbumUserTable>,
  ) {
    yield* query(db, (db) =>
      db.updateTable('album_user').set(dto).where('userId', '=', userId).where('albumId', '=', albumId).execute(),
    );
  }),

  delete: Effect.fn('AlbumUser.delete')(function* ({ userId, albumId }: AlbumPermissionId) {
    yield* query(db, (db) =>
      db.deleteFrom('album_user').where('userId', '=', userId).where('albumId', '=', albumId).execute(),
    );
  }),
});

export class AlbumUserRepo extends Context.Service<AlbumUserRepo, ReturnType<typeof methods>>()(
  'immich/repositories/AlbumUserRepo',
) {
  static readonly make = (db: Kysely<DB>) => AlbumUserRepo.of(methods(db));

  static readonly layer = Layer.effect(
    AlbumUserRepo,
    Effect.gen(function* () {
      return AlbumUserRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AlbumUserRepo`. */
@Injectable()
export class AlbumUserRepository {
  private readonly effect: Context.Service.Shape<typeof AlbumUserRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AlbumUserRepo.make(db);
    registerEffect(this, this.effect);
  }

  @GenerateSql({ params: [{ userId: DummyValue.UUID, albumId: DummyValue.UUID }] })
  create(albumUser: Insertable<AlbumUserTable>) {
    return runPromise(this.effect.create(albumUser));
  }

  @GenerateSql({ params: [{ userId: DummyValue.UUID, albumId: DummyValue.UUID }, { role: AlbumUserRole.Viewer }] })
  async update({ userId, albumId }: AlbumPermissionId, dto: Updateable<AlbumUserTable>) {
    await runPromise(this.effect.update({ userId, albumId }, dto));
  }

  @GenerateSql({ params: [{ userId: DummyValue.UUID, albumId: DummyValue.UUID }] })
  async delete({ userId, albumId }: AlbumPermissionId): Promise<void> {
    await runPromise(this.effect.delete({ userId, albumId }));
  }
}
