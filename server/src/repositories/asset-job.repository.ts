import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Kysely, sql } from 'kysely';
import { jsonArrayFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { AssetFileType, AssetStatus, AssetType, AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import {
  anyUuid,
  asUuid,
  withAudioStream,
  withEdits,
  withExif,
  withExifInner,
  withFiles,
  withVideoFormat,
  withVideoStream,
} from 'src/utils/database.js';
import { mimeTypes } from 'src/utils/mime-types.js';

const makeAssetJob = (db: Kysely<DB>) => {
  const storageTemplateAssetQuery = () =>
    db
      .selectFrom('asset')
      .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
      .select([
        'asset.id',
        'asset.ownerId',
        'asset.type',
        'asset.checksum',
        'asset.originalPath',
        'asset.isExternal',
        'asset.visibility',
        'asset.originalFileName',
        'asset.livePhotoVideoId',
        'asset.fileCreatedAt',
        'asset_exif.timeZone',
        'asset_exif.fileSizeInByte',
        'asset_exif.make',
        'asset_exif.model',
        'asset_exif.lensModel',
      ])
      .select((eb) => withFiles(eb, AssetFileType.Sidecar))
      .where('asset.deletedAt', 'is', null);

  return {
    getForSidecarWriteJob: Effect.fn('AssetJob.getForSidecarWriteJob')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .where('asset.id', '=', asUuid(id))
          .select(['id', 'originalPath'])
          .select((eb) => withFiles(eb, AssetFileType.Sidecar))
          .$call(withExifInner)
          .limit(1)
          .executeTakeFirst(),
      ),
    ),
    getForSidecarCheckJob: Effect.fn('AssetJob.getForSidecarCheckJob')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .where('asset.id', '=', asUuid(id))
          .select(['id', 'originalPath'])
          .select((eb) => withFiles(eb, AssetFileType.Sidecar))
          .limit(1)
          .executeTakeFirst(),
      ),
    ),
    streamForThumbnailJob: Effect.fn('AssetJob.streamForThumbnailJob')(
      (options: { force: boolean | undefined; fullsizeEnabled: boolean }) =>
        Effect.sync(() =>
          db
            .selectFrom('asset')
            .select(['asset.id', 'asset.isEdited'])
            .where('asset.deletedAt', 'is', null)
            .where('asset.visibility', '!=', sql.lit(AssetVisibility.Hidden))
            .$if(!options.force, (qb) =>
              qb
                // If there aren't any entries, metadata extraction hasn't run yet which is required for thumbnails
                .innerJoin('asset_job_status', 'asset_job_status.assetId', 'asset.id')
                .where(({ and, eb, exists, not, or, selectFrom }) => {
                  const file = (type: AssetFileType) =>
                    selectFrom('asset_file').whereRef('assetId', '=', 'asset.id').where('type', '=', sql.lit(type));
                  const conditions = [
                    not(exists(file(AssetFileType.Thumbnail))),
                    not(exists(file(AssetFileType.Preview))),
                    and([
                      eb('asset.isEdited', '=', sql.lit(true)),
                      not(exists(file(AssetFileType.FullSize).where('asset_file.isEdited', '=', sql.lit(true)))),
                    ]),
                    eb('asset.thumbhash', 'is', null),
                  ];
                  if (options.fullsizeEnabled) {
                    const isWebUnsupported = sql.join(
                      Object.keys(mimeTypes.webUnsupportedImage).map((ext) => sql.lit(`%${ext}`)),
                    );
                    conditions.push(
                      and([
                        not(exists(file(AssetFileType.FullSize))),
                        eb(
                          sql`f_unaccent(asset."originalFileName")`,
                          'like',
                          sql`any(array[${isWebUnsupported}]::text[])`,
                        ),
                      ]),
                    );
                  }
                  return or(conditions);
                }),
            )
            .stream(),
        ),
    ),
    getForMigrationJob: Effect.fn('AssetJob.getForMigrationJob')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['asset.id', 'asset.ownerId'])
          .select(withFiles)
          .where('asset.id', '=', id)
          .executeTakeFirst(),
      ),
    ),
    getForGenerateThumbnailJob: Effect.fn('AssetJob.getForGenerateThumbnailJob')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select([
            'asset.id',
            'asset.visibility',
            'asset.originalFileName',
            'asset.originalPath',
            'asset.ownerId',
            'asset.thumbhash',
            'asset.type',
          ])
          .select((eb) =>
            jsonArrayFrom(
              eb
                .selectFrom('asset_file')
                .select(columns.assetFilesForThumbnail)
                .whereRef('asset_file.assetId', '=', 'asset.id')
                .where('asset_file.type', 'in', [
                  AssetFileType.Thumbnail,
                  AssetFileType.Preview,
                  AssetFileType.FullSize,
                ]),
            ).as('files'),
          )
          .select(withEdits)
          .$call(withExifInner)
          .leftJoin('asset_video', 'asset_video.assetId', 'asset.id')
          .select((eb) => withVideoStream(eb).as('videoStream'))
          .select((eb) => withVideoFormat(eb).as('format'))
          .where('asset.id', '=', id)
          .executeTakeFirst(),
      ),
    ),
    getForMetadataExtraction: Effect.fn('AssetJob.getForMetadataExtraction')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select(columns.asset)
          .select((eb) => withFiles(eb, AssetFileType.Sidecar))
          .where('asset.id', '=', id)
          .executeTakeFirst(),
      ),
    ),
    getLockedPropertiesForMetadataExtraction: Effect.fn('AssetJob.getLockedPropertiesForMetadataExtraction')(function* (
      assetId: string,
    ) {
      const row = yield* query(db, (db) =>
        db
          .selectFrom('asset_exif')
          .select('asset_exif.lockedProperties')
          .where('asset_exif.assetId', '=', assetId)
          .executeTakeFirst(),
      );
      return row?.lockedProperties ?? [];
    }),
    getAlbumThumbnailFiles: Effect.fn('AssetJob.getAlbumThumbnailFiles')((id: string, fileType?: AssetFileType) =>
      query(db, (db) =>
        db
          .selectFrom('asset_file')
          .select(columns.assetFiles)
          .where('asset_file.assetId', '=', id)
          .$if(!!fileType, (qb) => qb.where('asset_file.type', '=', fileType!))
          .execute(),
      ),
    ),
    getForSyncAssets: Effect.fn('AssetJob.getForSyncAssets')((ids: string[]) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select([
            'asset.id',
            'asset.isOffline',
            'asset.libraryId',
            'asset.originalPath',
            'asset.status',
            'asset.fileModifiedAt',
          ])
          .where('asset.id', '=', anyUuid(ids))
          .execute(),
      ),
    ),
    getForAssetDeletion: Effect.fn('AssetJob.getForAssetDeletion')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select([
            'asset.id',
            'asset.visibility',
            'asset.libraryId',
            'asset.ownerId',
            'asset.livePhotoVideoId',
            'asset.originalPath',
            'asset.isOffline',
          ])
          .$call(withExif)
          .select(withFiles)
          .leftJoinLateral(
            (eb) =>
              eb
                .selectFrom('stack')
                .whereRef('stack.id', '=', 'asset.stackId')
                .select((eb) => [
                  'stack.id',
                  'stack.primaryAssetId',
                  jsonArrayFrom(
                    eb
                      .selectFrom('asset as stack_asset')
                      .select(['stack_asset.id'])
                      .whereRef('stack_asset.stackId', '=', 'stack.id')
                      .whereRef('stack_asset.id', '!=', 'stack.primaryAssetId')
                      .where('stack_asset.visibility', '=', sql.val(AssetVisibility.Timeline))
                      .where('stack_asset.status', '!=', sql.val(AssetStatus.Deleted)),
                  ).as('assets'),
                ])
                .as('stack_result'),
            (join) => join.onTrue(),
          )
          .select((eb) =>
            eb.fn
              .toJson(eb.table('stack_result'))
              .$castTo<{ id: string; primaryAssetId: string; assets: { id: string }[] } | null>()
              .as('stack'),
          )
          .where('asset.id', '=', id)
          .executeTakeFirst(),
      ),
    ),
    streamForVideoConversion: Effect.fn('AssetJob.streamForVideoConversion')((force?: boolean) =>
      Effect.sync(() =>
        db
          .selectFrom('asset')
          .select(['asset.id'])
          .where('asset.type', '=', sql.lit(AssetType.Video))
          .$if(!force, (qb) =>
            qb.where((eb) =>
              eb.not(
                eb.exists(
                  eb
                    .selectFrom('asset_file')
                    .select('asset_file.id')
                    .whereRef('asset_file.assetId', '=', 'asset.id')
                    .where('asset_file.type', '=', sql.lit(AssetFileType.EncodedVideo)),
                ),
              ),
            ),
          )
          .where('asset.deletedAt', 'is', null)
          .stream(),
      ),
    ),
    getForVideoConversion: Effect.fn('AssetJob.getForVideoConversion')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
          .innerJoin('asset_video', 'asset_video.assetId', 'asset.id')
          .leftJoin('asset_audio', 'asset_audio.assetId', 'asset.id')
          .select(['asset.id', 'asset.ownerId', 'asset.originalPath'])
          .select(withFiles)
          .select((eb) => withAudioStream(eb).as('audioStream'))
          .select((eb) => withVideoStream(eb).$notNull().as('videoStream'))
          .select((eb) => withVideoFormat(eb).$notNull().as('format'))
          .where('asset.id', '=', id)
          .where('asset.type', '=', sql.lit(AssetType.Video))
          .executeTakeFirst(),
      ),
    ),
    streamForMetadataExtraction: Effect.fn('AssetJob.streamForMetadataExtraction')((force?: boolean) =>
      Effect.sync(() =>
        db
          .selectFrom('asset')
          .select(['asset.id'])
          .$if(!force, (qb) =>
            qb
              .leftJoin('asset_job_status', 'asset_job_status.assetId', 'asset.id')
              .where((eb) =>
                eb.or([
                  eb('asset_job_status.metadataExtractedAt', 'is', null),
                  eb('asset_job_status.assetId', 'is', null),
                ]),
              ),
          )
          .where('asset.deletedAt', 'is', null)
          .stream(),
      ),
    ),
    getForStorageTemplateJob: Effect.fn('AssetJob.getForStorageTemplateJob')(
      (id: string, options?: { includeHidden?: boolean }) =>
        query(db, () =>
          storageTemplateAssetQuery()
            .where('asset.id', '=', id)
            .$if(!options?.includeHidden, (qb) => qb.where('asset.visibility', '!=', AssetVisibility.Hidden))
            .executeTakeFirst(),
        ),
    ),
    streamForStorageTemplateJob: Effect.fn('AssetJob.streamForStorageTemplateJob')(() =>
      Effect.sync(() => storageTemplateAssetQuery().where('asset.visibility', '!=', AssetVisibility.Hidden).stream()),
    ),
    streamForDeletedJob: Effect.fn('AssetJob.streamForDeletedJob')((trashedBefore: Date) =>
      Effect.sync(() => db.selectFrom('asset').select(['id']).where('asset.deletedAt', '<=', trashedBefore).stream()),
    ),
    streamForSidecar: Effect.fn('AssetJob.streamForSidecar')((force?: boolean) =>
      Effect.sync(() =>
        db
          .selectFrom('asset')
          .select(['asset.id'])
          .$if(!force, (qb) =>
            qb.where((eb) =>
              eb.not(
                eb.exists(
                  eb
                    .selectFrom('asset_file')
                    .select('asset_file.id')
                    .whereRef('asset_file.assetId', '=', 'asset.id')
                    .where('asset_file.type', '=', AssetFileType.Sidecar),
                ),
              ),
            ),
          )
          .stream(),
      ),
    ),
    streamForMigrationJob: Effect.fn('AssetJob.streamForMigrationJob')(() =>
      Effect.sync(() => db.selectFrom('asset').select(['id']).where('asset.deletedAt', 'is', null).stream()),
    ),
  };
};

export class AssetJobRepo extends Context.Service<AssetJobRepo, ReturnType<typeof makeAssetJob>>()(
  'immich/repositories/AssetJobRepo',
) {
  static readonly make = (db: Kysely<DB>) => AssetJobRepo.of(makeAssetJob(db));

  static readonly layer = Layer.effect(
    AssetJobRepo,
    Effect.gen(function* () {
      return AssetJobRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AssetJobRepo`. */
@Injectable()
export class AssetJobRepository {
  private readonly effect: Context.Service.Shape<typeof AssetJobRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AssetJobRepo.make(db);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForSidecarWriteJob(id: string) {
    return runPromise(this.effect.getForSidecarWriteJob(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForSidecarCheckJob(id: string) {
    return runPromise(this.effect.getForSidecarCheckJob(id));
  }

  @GenerateSql({ params: [{ force: false, fullsizeEnabled: true }], stream: true })
  streamForThumbnailJob(options: { force: boolean | undefined; fullsizeEnabled: boolean }) {
    return runSync(this.effect.streamForThumbnailJob(options));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForMigrationJob(id: string) {
    return runPromise(this.effect.getForMigrationJob(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForGenerateThumbnailJob(id: string) {
    return runPromise(this.effect.getForGenerateThumbnailJob(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForMetadataExtraction(id: string) {
    return runPromise(this.effect.getForMetadataExtraction(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getLockedPropertiesForMetadataExtraction(assetId: string) {
    return runPromise(this.effect.getLockedPropertiesForMetadataExtraction(assetId));
  }

  @GenerateSql({ params: [DummyValue.UUID, AssetFileType.Thumbnail] })
  getAlbumThumbnailFiles(id: string, fileType?: AssetFileType) {
    return runPromise(this.effect.getAlbumThumbnailFiles(id, fileType));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  getForSyncAssets(ids: string[]) {
    return runPromise(this.effect.getForSyncAssets(ids));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForAssetDeletion(id: string) {
    return runPromise(this.effect.getForAssetDeletion(id));
  }

  @GenerateSql({ params: [], stream: true })
  streamForVideoConversion(force?: boolean) {
    return runSync(this.effect.streamForVideoConversion(force));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForVideoConversion(id: string) {
    return runPromise(this.effect.getForVideoConversion(id));
  }

  @GenerateSql({ params: [], stream: true })
  streamForMetadataExtraction(force?: boolean) {
    return runSync(this.effect.streamForMetadataExtraction(force));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForStorageTemplateJob(id: string, options?: { includeHidden?: boolean }) {
    return runPromise(this.effect.getForStorageTemplateJob(id, options));
  }

  @GenerateSql({ params: [], stream: true })
  streamForStorageTemplateJob() {
    return runSync(this.effect.streamForStorageTemplateJob());
  }

  @GenerateSql({ params: [DummyValue.DATE], stream: true })
  streamForDeletedJob(trashedBefore: Date) {
    return runSync(this.effect.streamForDeletedJob(trashedBefore));
  }

  @GenerateSql({ params: [], stream: true })
  streamForSidecar(force?: boolean) {
    return runSync(this.effect.streamForSidecar(force));
  }

  @GenerateSql({ params: [DummyValue.DATE], stream: true })
  streamForMigrationJob() {
    return runSync(this.effect.streamForMigrationJob());
  }
}
