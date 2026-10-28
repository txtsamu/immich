import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Kysely } from 'kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { asUuid, withExif } from 'src/utils/database.js';

const methods = (db: Kysely<DB>) => ({
  getUniqueOriginalPaths: Effect.fn('View.getUniqueOriginalPaths')(function* (userId: string) {
    const results = yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .select((eb) => eb.fn<string>('substring', ['asset.originalPath', eb.val('^(.*/)[^/]*$')]).as('directoryPath'))
        .distinct()
        .where('ownerId', '=', asUuid(userId))
        .where('visibility', '=', AssetVisibility.Timeline)
        .where('deletedAt', 'is', null)
        .where('fileCreatedAt', 'is not', null)
        .where('fileModifiedAt', 'is not', null)
        .where('localDateTime', 'is not', null)
        .orderBy('directoryPath', 'asc')
        .execute(),
    );

    return results.map((row) => row.directoryPath.replaceAll(/\/$/g, ''));
  }),

  getAssetsByOriginalPath: Effect.fn('View.getAssetsByOriginalPath')(function* (userId: string, partialPath: string) {
    const normalizedPath = partialPath.replaceAll(/\/$/g, '');

    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .selectAll('asset')
        .$call(withExif)
        .where('ownerId', '=', asUuid(userId))
        .where('visibility', '=', AssetVisibility.Timeline)
        .where('deletedAt', 'is', null)
        .where('fileCreatedAt', 'is not', null)
        .where('fileModifiedAt', 'is not', null)
        .where('localDateTime', 'is not', null)
        .where('originalPath', 'like', `%${normalizedPath}/%`)
        .where('originalPath', 'not like', `%${normalizedPath}/%/%`)
        .orderBy(
          (eb) => eb.fn('regexp_replace', ['asset.originalPath', eb.val('.*/(.+)'), eb.val(String.raw`\1`)]),
          'asc',
        )
        .execute(),
    );
  }),
});

export class ViewRepo extends Context.Service<ViewRepo, ReturnType<typeof methods>>()('immich/repositories/ViewRepo') {
  static readonly make = (db: Kysely<DB>) => ViewRepo.of(methods(db));

  static readonly layer = Layer.effect(
    ViewRepo,
    Effect.gen(function* () {
      return ViewRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `ViewRepo`. */
export class ViewRepository {
  private readonly effect: Context.Service.Shape<typeof ViewRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = ViewRepo.make(db);
    registerEffect(this, this.effect);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getUniqueOriginalPaths(userId: string) {
    return runPromise(this.effect.getUniqueOriginalPaths(userId));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.STRING] })
  getAssetsByOriginalPath(userId: string, partialPath: string) {
    return runPromise(this.effect.getAssetsByOriginalPath(userId, partialPath));
  }
}
