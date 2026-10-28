import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { Kysely } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { registerEffect } from 'src/effect/bridge.js';
import { Db } from 'src/effect/db.js';
import { DatabaseError } from 'src/effect/errors.js';
import { runSync } from 'src/effect/run.js';
import { AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { anyUuid } from 'src/utils/database.js';

const builder = (db: Kysely<DB>) =>
  db
    .selectFrom('asset')
    .innerJoin('asset_exif', 'assetId', 'id')
    .select(['asset.id', 'asset.livePhotoVideoId', 'asset_exif.fileSizeInByte as size'])
    .where('asset.deletedAt', 'is', null);

const downloadAssetIds = (db: Kysely<DB>, ids: string[]) => builder(db).where('asset.id', '=', anyUuid(ids)).stream();

const downloadMotionAssetIds = (db: Kysely<DB>, ids: string[]) =>
  builder(db).select(['asset.originalPath']).where('asset.id', '=', anyUuid(ids)).stream();

const downloadAlbumId = (db: Kysely<DB>, albumId: string) =>
  builder(db)
    .innerJoin('album_asset', 'asset.id', 'album_asset.assetId')
    .where('album_asset.albumId', '=', albumId)
    .stream();

const downloadUserId = (db: Kysely<DB>, userId: string) =>
  builder(db).where('asset.ownerId', '=', userId).where('asset.visibility', '!=', AssetVisibility.Hidden).stream();

const stream = <A>(run: () => A) => Effect.try({ try: run, catch: (cause) => new DatabaseError({ cause }) });

export class DownloadRepo extends Context.Service<
  DownloadRepo,
  {
    /** The returned streams are lazy and unbuffered, exactly as returned by Kysely. */
    downloadAssetIds(ids: string[]): Effect.Effect<ReturnType<typeof downloadAssetIds>, DatabaseError>;
    downloadMotionAssetIds(ids: string[]): Effect.Effect<ReturnType<typeof downloadMotionAssetIds>, DatabaseError>;
    downloadAlbumId(albumId: string): Effect.Effect<ReturnType<typeof downloadAlbumId>, DatabaseError>;
    downloadUserId(userId: string): Effect.Effect<ReturnType<typeof downloadUserId>, DatabaseError>;
  }
>()('immich/repositories/DownloadRepo') {
  static readonly make = (db: Kysely<DB>) =>
    DownloadRepo.of({
      downloadAssetIds: Effect.fn('Download.downloadAssetIds')((ids: string[]) =>
        stream(() => downloadAssetIds(db, ids)),
      ),

      downloadMotionAssetIds: Effect.fn('Download.downloadMotionAssetIds')((ids: string[]) =>
        stream(() => downloadMotionAssetIds(db, ids)),
      ),

      downloadAlbumId: Effect.fn('Download.downloadAlbumId')((albumId: string) =>
        stream(() => downloadAlbumId(db, albumId)),
      ),

      downloadUserId: Effect.fn('Download.downloadUserId')((userId: string) =>
        stream(() => downloadUserId(db, userId)),
      ),
    });

  static readonly layer = Layer.effect(
    DownloadRepo,
    Effect.gen(function* () {
      return DownloadRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `DownloadRepo`. */
@Injectable()
export class DownloadRepository {
  private readonly effect: Context.Service.Shape<typeof DownloadRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = DownloadRepo.make(db);
    registerEffect(this, this.effect);
  }

  downloadAssetIds(ids: string[]) {
    return runSync(this.effect.downloadAssetIds(ids));
  }

  downloadMotionAssetIds(ids: string[]) {
    return runSync(this.effect.downloadMotionAssetIds(ids));
  }

  downloadAlbumId(albumId: string) {
    return runSync(this.effect.downloadAlbumId(albumId));
  }

  downloadUserId(userId: string) {
    return runSync(this.effect.downloadUserId(userId));
  }
}
