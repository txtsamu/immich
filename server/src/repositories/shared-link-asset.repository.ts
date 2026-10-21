import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Kysely } from 'kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';

const makeSharedLinkAsset = (db: Kysely<DB>) => ({
  remove: Effect.fn('SharedLinkAsset.remove')(function* (sharedLinkId: string, assetId: string[]) {
    const deleted = yield* query(db, (db) =>
      db
        .deleteFrom('shared_link_asset')
        .where('shared_link_asset.sharedLinkId', '=', sharedLinkId)
        .where('shared_link_asset.assetId', 'in', assetId)
        .returning('assetId')
        .execute(),
    );

    return deleted.map((row) => row.assetId);
  }),

  copySharedLinks: Effect.fn('SharedLinkAsset.copySharedLinks')(function* ({
    sourceAssetId,
    targetAssetId,
  }: {
    sourceAssetId: string;
    targetAssetId: string;
  }) {
    return yield* query(db, (db) =>
      db
        .insertInto('shared_link_asset')
        .expression((eb) =>
          eb
            .selectFrom('shared_link_asset')
            .select((eb) => [eb.val(targetAssetId).as('assetId'), 'shared_link_asset.sharedLinkId'])
            .where('shared_link_asset.assetId', '=', sourceAssetId),
        )
        .onConflict((oc) => oc.doNothing())
        .execute(),
    );
  }),
});

export class SharedLinkAssetRepo extends Context.Service<SharedLinkAssetRepo, ReturnType<typeof makeSharedLinkAsset>>()(
  'immich/repositories/SharedLinkAssetRepo',
) {
  static readonly make = (db: Kysely<DB>) => SharedLinkAssetRepo.of(makeSharedLinkAsset(db));

  static readonly layer = Layer.effect(
    SharedLinkAssetRepo,
    Effect.gen(function* () {
      return SharedLinkAssetRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SharedLinkAssetRepo`. */
export class SharedLinkAssetRepository {
  private readonly effect: Context.Service.Shape<typeof SharedLinkAssetRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = SharedLinkAssetRepo.make(db);
    registerEffect(this, this.effect);
  }

  remove(sharedLinkId: string, assetId: string[]) {
    return runPromise(this.effect.remove(sharedLinkId, assetId));
  }

  @GenerateSql({ params: [{ sourceAssetId: DummyValue.UUID, targetAssetId: DummyValue.UUID }] })
  copySharedLinks({ sourceAssetId, targetAssetId }: { sourceAssetId: string; targetAssetId: string }) {
    return runPromise(this.effect.copySharedLinks({ sourceAssetId, targetAssetId }));
  }
}
