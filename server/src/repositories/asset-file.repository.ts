import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Kysely } from 'kysely';
import { AssetFileSearchDto } from 'src/dtos/asset-file.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';

const methods = (db: Kysely<DB>) => ({
  get: Effect.fn('AssetFile.get')(function* (id: string) {
    return yield* query(db, (db) => db.selectFrom('asset_file').where('id', '=', id).selectAll().executeTakeFirst());
  }),

  search: Effect.fn('AssetFile.search')(function* (dto: AssetFileSearchDto) {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset_file')
        .where('assetId', '=', dto.assetId)
        .$if(dto.type !== undefined, (qb) => qb.where('type', '=', dto.type!))
        .$if(dto.isEdited !== undefined, (qb) => qb.where('isEdited', '=', dto.isEdited!))
        .$if(dto.isProgressive !== undefined, (qb) => qb.where('isProgressive', '=', dto.isProgressive!))
        .$if(dto.isTransparent !== undefined, (qb) => qb.where('isTransparent', '=', dto.isTransparent!))
        .selectAll()
        .execute(),
    );
  }),

  delete: Effect.fn('AssetFile.delete')(function* (id: string) {
    const { numDeletedRows } = yield* query(db, (db) =>
      db.deleteFrom('asset_file').where('id', '=', id).executeTakeFirst(),
    );
    return Number(numDeletedRows) === 1;
  }),
});

export class AssetFileRepo extends Context.Service<AssetFileRepo, ReturnType<typeof methods>>()(
  'immich/repositories/AssetFileRepo',
) {
  static readonly make = (db: Kysely<DB>) => AssetFileRepo.of(methods(db));

  static readonly layer = Layer.effect(
    AssetFileRepo,
    Effect.gen(function* () {
      return AssetFileRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AssetFileRepo`. */
@Injectable()
export class AssetFileRepository {
  private readonly effect: Context.Service.Shape<typeof AssetFileRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AssetFileRepo.make(db);
    registerEffect(this, this.effect);
  }

  get(id: string) {
    return runPromise(this.effect.get(id));
  }

  search(dto: AssetFileSearchDto) {
    return runPromise(this.effect.search(dto));
  }

  delete(id: string): Promise<boolean> {
    return runPromise(this.effect.delete(id));
  }
}
