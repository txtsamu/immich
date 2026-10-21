import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Kysely } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { AssetEditActionItem, AssetEditActionItemResponseDto } from 'src/dtos/editing.dto.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';

const methods = (db: Kysely<DB>) => ({
  replaceAll: Effect.fn('AssetEdit.replaceAll')(function* (assetId: string, edits: AssetEditActionItem[]) {
    return yield* query(db, (db): Promise<AssetEditActionItemResponseDto[]> =>
      db.transaction().execute(async (trx) => {
        await trx.deleteFrom('asset_edit').where('assetId', '=', assetId).execute();

        if (edits.length > 0) {
          return trx
            .insertInto('asset_edit')
            .values(edits.map((edit, i) => ({ assetId, sequence: i, ...edit })))
            .returning(['id', 'action', 'parameters'])
            .execute();
        }

        return [];
      }),
    );
  }),

  getAll: Effect.fn('AssetEdit.getAll')(function* (assetId: string) {
    return yield* query(db, (db): Promise<AssetEditActionItemResponseDto[]> =>
      db
        .selectFrom('asset_edit')
        .select(['id', 'action', 'parameters'])
        .where('assetId', '=', assetId)
        .orderBy('sequence', 'asc')
        .execute(),
    );
  }),

  getWithSyncInfo: Effect.fn('AssetEdit.getWithSyncInfo')(function* (assetId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset_edit')
        .select(columns.syncAssetEdit)
        .where('assetId', '=', assetId)
        .orderBy('sequence', 'asc')
        .execute(),
    );
  }),
});

export class AssetEditRepo extends Context.Service<AssetEditRepo, ReturnType<typeof methods>>()(
  'immich/repositories/AssetEditRepo',
) {
  static readonly make = (db: Kysely<DB>) => AssetEditRepo.of(methods(db));

  static readonly layer = Layer.effect(
    AssetEditRepo,
    Effect.gen(function* () {
      return AssetEditRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AssetEditRepo`. */
@Injectable()
export class AssetEditRepository {
  private readonly effect: Context.Service.Shape<typeof AssetEditRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AssetEditRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  replaceAll(assetId: string, edits: AssetEditActionItem[]): Promise<AssetEditActionItemResponseDto[]> {
    return runPromise(this.effect.replaceAll(assetId, edits));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getAll(assetId: string): Promise<AssetEditActionItemResponseDto[]> {
    return runPromise(this.effect.getAll(assetId));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getWithSyncInfo(assetId: string) {
    return runPromise(this.effect.getWithSyncInfo(assetId));
  }
}
