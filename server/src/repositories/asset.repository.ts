import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import {
  type ExpressionBuilder,
  type Insertable,
  type Kysely,
  type NotNull,
  type SelectQueryBuilder,
  type Selectable,
  type ShallowDehydrateObject,
  UpdateResult,
  type Updateable,
  sql,
} from 'kysely';
import { jsonArrayFrom } from 'kysely/helpers/postgres';
import { isEmpty, isUndefined, omitBy } from 'lodash-es';
import { InjectKysely } from 'nestjs-kysely';
import { LockableProperty, Stack } from 'src/database.js';
import { Chunked, ChunkedArray, DummyValue, GenerateSql } from 'src/decorators.js';
import { type AuthDto, isMetadataHidden } from 'src/dtos/auth.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetOrder,
  AssetOrderBy,
  AssetStatus,
  AssetType,
  AssetVisibility,
  CalendarHeatmapType,
} from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { AssetAudioTable, AssetKeyframeTable, AssetVideoTable } from 'src/schema/tables/asset-av.table.js';
import { AssetExifTable } from 'src/schema/tables/asset-exif.table.js';
import { AssetFileTable } from 'src/schema/tables/asset-file.table.js';
import { AssetJobStatusTable } from 'src/schema/tables/asset-job-status.table.js';
import { AssetMetadataTable } from 'src/schema/tables/asset-metadata.table.js';
import { AssetTable } from 'src/schema/tables/asset.table.js';
import {
  anyUuid,
  asUuid,
  removeUndefinedKeys,
  truncatedDate,
  unnest,
  withDefaultVisibility,
  withEdits,
  withExif,
  withFilePath,
  withFiles,
  withLibrary,
  withOwner,
  withTagId,
  withTags,
} from 'src/utils/database.js';
import { globToPostgresRegex } from 'src/utils/misc.js';

export type AssetStats = Record<AssetType, number>;

/** What identifies a file across a move or rename: it keeps its size and modification time. */
export type ExternalFileSignature = { fileModifiedAt: Date; size: number; type: AssetType };

export interface BoundingBox {
  west: number;
  south: number;
  east: number;
  north: number;
}

export interface AssetStatsOptions {
  isFavorite?: boolean;
  isTrashed?: boolean;
  visibility?: AssetVisibility;
}

export interface LivePhotoSearchOptions {
  ownerId: string;
  libraryId?: string | null;
  livePhotoCID: string;
  otherAssetId: string;
  type: AssetType;
}

interface AssetBuilderOptions {
  isFavorite?: boolean;
  isTrashed?: boolean;
  albumId?: string;
  tagId?: string;
  userIds?: string[];
  withStacked?: boolean;
  exifInfo?: boolean;
  status?: AssetStatus;
  assetType?: AssetType;
  visibility?: AssetVisibility;
  withCoordinates?: boolean;
  bbox?: BoundingBox;
}

export interface TimeBucketOptions extends AssetBuilderOptions {
  order?: AssetOrder;
  orderBy?: AssetOrderBy;
}

export interface TimeBucketItem {
  timeBucket: string;
  count: number;
}

export interface YearMonthDay {
  day: number;
  month: number;
  year: number;
}

export interface AssetExploreFieldOptions {
  maxFields: number;
  minAssetsPerField: number;
}

export interface AssetGetByChecksumOptions {
  ownerId: string;
  checksum: Buffer;
  libraryId?: string;
}

export interface GetByIdsRelations {
  exifInfo?: boolean;
  files?: boolean;
  library?: boolean;
  owner?: boolean;
  stack?: { assets?: boolean };
  tags?: boolean;
  edits?: boolean;
}

type UpsertExifOptions = {
  exif: Insertable<AssetExifTable>;
  audio?: Insertable<AssetAudioTable>;
  video?: Insertable<AssetVideoTable>;
  keyframes?: Insertable<AssetKeyframeTable>;
  lockedPropertiesBehavior: 'override' | 'append' | 'skip';
};

const distinctLocked = <T extends LockableProperty[] | null>(eb: ExpressionBuilder<DB, 'asset_exif'>, columns: T) =>
  sql<T>`nullif(array(select distinct unnest(${eb.ref('asset_exif.lockedProperties')} || ${columns})), '{}')`;

const getBoundingCircle = (bbox: BoundingBox) => {
  const { west, south, east, north } = bbox;
  const eastUnwrapped = west <= east ? east : east + 360;
  const centerLongitude = (((west + eastUnwrapped) / 2 + 540) % 360) - 180;
  const centerLatitude = (south + north) / 2;
  const radius = sql<number>`greatest(
    earth_distance(ll_to_earth_public(${centerLatitude}, ${centerLongitude}), ll_to_earth_public(${south}, ${west})),
    earth_distance(ll_to_earth_public(${centerLatitude}, ${centerLongitude}), ll_to_earth_public(${south}, ${east})),
    earth_distance(ll_to_earth_public(${centerLatitude}, ${centerLongitude}), ll_to_earth_public(${north}, ${west})),
    earth_distance(ll_to_earth_public(${centerLatitude}, ${centerLongitude}), ll_to_earth_public(${north}, ${east}))
  )`;

  return { centerLatitude, centerLongitude, radius };
};

const withBoundingBox = <T>(qb: SelectQueryBuilder<DB, 'asset' | 'asset_exif', T>, bbox: BoundingBox) => {
  const { west, south, east, north } = bbox;
  const withLatitude = qb.where('asset_exif.latitude', '>=', south).where('asset_exif.latitude', '<=', north);

  if (west <= east) {
    return withLatitude.where('asset_exif.longitude', '>=', west).where('asset_exif.longitude', '<=', east);
  }

  return withLatitude.where((eb) =>
    eb.or([eb('asset_exif.longitude', '>=', west), eb('asset_exif.longitude', '<=', east)]),
  );
};

const makeAsset = (db: Kysely<DB>) => {
  const getById = Effect.fn('Asset.getById')(
    (id: string, { exifInfo, files, library, owner, stack, tags, edits }: GetByIdsRelations = {}) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .selectAll('asset')
          .where('asset.id', '=', asUuid(id))
          .$if(!!exifInfo, withExif)
          .$if(!!library, (qb) => qb.select(withLibrary))
          .$if(!!owner, (qb) => qb.select(withOwner))
          .$if(!!stack, (qb) =>
            qb
              .leftJoin('stack', 'stack.id', 'asset.stackId')
              .$if(!stack!.assets, (qb) =>
                qb.select((eb) => eb.fn.toJson(eb.table('stack')).$castTo<Stack | null>().as('stack')),
              )
              .$if(!!stack!.assets, (qb) =>
                qb
                  .leftJoinLateral(
                    (eb) =>
                      eb
                        .selectFrom('asset as stacked')
                        .selectAll('stack')
                        .select(
                          sql<
                            ShallowDehydrateObject<Selectable<AssetTable>>[]
                          >`array_agg(to_json(stacked) ORDER BY stacked."fileCreatedAt" ASC)`.as('assets'),
                        )
                        .whereRef('stacked.stackId', '=', 'stack.id')
                        .whereRef('stacked.id', '!=', 'stack.primaryAssetId')
                        .where('stacked.deletedAt', 'is', null)
                        .where('stacked.visibility', '=', AssetVisibility.Timeline)
                        .groupBy('stack.id')
                        .as('stacked_assets'),
                    (join) => join.on('stack.id', 'is not', null),
                  )
                  .select((eb) => eb.fn.toJson(eb.table('stacked_assets')).as('stack')),
              ),
          )
          .$if(!!files, (qb) => qb.select(withFiles))
          .$if(!!tags, (qb) => qb.select(withTags))
          .$if(!!edits, (qb) => qb.select(withEdits))
          .limit(1)
          .executeTakeFirst(),
      ),
  );

  const buildGetForOriginal = (ids: string[], isEdited: boolean) =>
    db
      .selectFrom('asset')
      .select('asset.id')
      .select('originalFileName')
      .where('asset.id', 'in', ids)
      .$if(isEdited, (qb) =>
        qb
          .leftJoin('asset_file', (join) =>
            join
              .onRef('asset.id', '=', 'asset_file.assetId')
              .on('asset_file.isEdited', '=', true)
              .on('asset_file.type', '=', AssetFileType.FullSize),
          )
          .select('asset_file.path as editedPath'),
      )
      .select('originalPath');

  return {
    upsertExif: Effect.fn('Asset.upsertExif')(function* ({
      exif,
      audio,
      video,
      keyframes,
      lockedPropertiesBehavior,
    }: UpsertExifOptions) {
      let builder = db;
      if (audio) {
        (builder as any) = db.with('audio', (qb) =>
          qb
            .insertInto('asset_audio')
            .values(audio)
            .onConflict((oc) =>
              oc.column('assetId').doUpdateSet(({ ref }) => ({
                bitrate: ref('excluded.bitrate'),
                index: ref('excluded.index'),
                profile: ref('excluded.profile'),
                codecName: ref('excluded.codecName'),
              })),
            ),
        );
      }

      if (video) {
        (builder as any) = builder.with('video', (qb) =>
          qb
            .insertInto('asset_video')
            .values(video)
            .onConflict((oc) =>
              oc.column('assetId').doUpdateSet(({ ref }) => ({
                bitrate: ref('excluded.bitrate'),
                frameCount: ref('excluded.frameCount'),
                timeBase: ref('excluded.timeBase'),
                index: ref('excluded.index'),
                profile: ref('excluded.profile'),
                level: ref('excluded.level'),
                colorPrimaries: ref('excluded.colorPrimaries'),
                colorTransfer: ref('excluded.colorTransfer'),
                colorMatrix: ref('excluded.colorMatrix'),
                dvProfile: ref('excluded.dvProfile'),
                dvLevel: ref('excluded.dvLevel'),
                dvBlSignalCompatibilityId: ref('excluded.dvBlSignalCompatibilityId'),
                codecName: ref('excluded.codecName'),
                formatName: ref('excluded.formatName'),
                formatLongName: ref('excluded.formatLongName'),
                pixelFormat: ref('excluded.pixelFormat'),
              })),
            ),
        );
      }

      if (keyframes) {
        (builder as any) = builder.with('keyframe', (qb) =>
          qb
            .insertInto('asset_keyframe')
            .values(keyframes)
            .onConflict((oc) =>
              oc.column('assetId').doUpdateSet(({ ref }) => ({
                pts: ref('excluded.pts'),
                accDuration: ref('excluded.accDuration'),
                ownDuration: ref('excluded.ownDuration'),
                totalDuration: ref('excluded.totalDuration'),
                packetCount: ref('excluded.packetCount'),
                outputFrames: ref('excluded.outputFrames'),
              })),
            ),
        );
      }

      yield* query(db, () =>
        builder
          .insertInto('asset_exif')
          .values(exif)
          .onConflict((oc) =>
            oc.column('assetId').doUpdateSet((eb) => {
              const updateLocked = <T extends keyof AssetExifTable>(col: T) => eb.ref(`excluded.${col}`);
              const skipLocked = <T extends keyof AssetExifTable>(col: T) =>
                eb
                  .case()
                  .when(sql`${col}`, '=', eb.fn.any('asset_exif.lockedProperties'))
                  .then(eb.ref(`asset_exif.${col}`))
                  .else(eb.ref(`excluded.${col}`))
                  .end();
              const ref = lockedPropertiesBehavior === 'skip' ? skipLocked : updateLocked;
              return {
                ...removeUndefinedKeys(
                  {
                    description: ref('description'),
                    exifImageWidth: ref('exifImageWidth'),
                    exifImageHeight: ref('exifImageHeight'),
                    fileSizeInByte: ref('fileSizeInByte'),
                    orientation: ref('orientation'),
                    dateTimeOriginal: ref('dateTimeOriginal'),
                    modifyDate: ref('modifyDate'),
                    timeZone: ref('timeZone'),
                    latitude: ref('latitude'),
                    longitude: ref('longitude'),
                    projectionType: ref('projectionType'),
                    city: ref('city'),
                    livePhotoCID: ref('livePhotoCID'),
                    autoStackId: ref('autoStackId'),
                    state: ref('state'),
                    country: ref('country'),
                    make: ref('make'),
                    model: ref('model'),
                    lensModel: ref('lensModel'),
                    fNumber: ref('fNumber'),
                    focalLength: ref('focalLength'),
                    iso: ref('iso'),
                    exposureTime: ref('exposureTime'),
                    profileDescription: ref('profileDescription'),
                    colorspace: ref('colorspace'),
                    bitsPerSample: ref('bitsPerSample'),
                    rating: ref('rating'),
                    fps: ref('fps'),
                    tags: ref('tags'),
                    lockedProperties:
                      lockedPropertiesBehavior === 'append'
                        ? distinctLocked(eb, exif.lockedProperties ?? null)
                        : ref('lockedProperties'),
                  },
                  exif,
                ),
              };
            }),
          )
          .execute(),
      );
    }),
    updateAllExif: Effect.fn('Asset.updateAllExif')(function* (ids: string[], options: Updateable<AssetExifTable>) {
      if (ids.length === 0) {
        return;
      }

      yield* query(db, (db) =>
        db
          .updateTable('asset_exif')
          .set((eb) => ({
            ...options,
            lockedProperties: distinctLocked(eb, Object.keys(options) as LockableProperty[]),
          }))
          .where('assetId', 'in', ids)
          .execute(),
      );
    }),
    updateDateTimeOriginal: Effect.fn('Asset.updateDateTimeOriginal')(
      (ids: string[], delta?: number, timeZone?: string) =>
        query(db, (db) =>
          db
            .updateTable('asset_exif')
            .set((eb) => ({
              dateTimeOriginal: sql`"dateTimeOriginal" + ${(delta ?? 0) + ' minute'}::interval`,
              timeZone,
              lockedProperties: distinctLocked(eb, ['dateTimeOriginal', 'timeZone']),
            }))
            .where('assetId', 'in', ids)
            .returning(['assetId', 'dateTimeOriginal', 'timeZone'])
            .execute(),
        ),
    ),
    unlockProperties: Effect.fn('Asset.unlockProperties')((assetId: string, properties: LockableProperty[]) =>
      query(db, (db) =>
        db
          .updateTable('asset_exif')
          .where('assetId', '=', assetId)
          .set((eb) => ({
            lockedProperties: sql`nullif(array(select distinct property from unnest(${eb.ref('asset_exif.lockedProperties')}) property where not property = any(${properties})), '{}')`,
          }))
          .execute(),
      ),
    ),
    upsertJobStatus: Effect.fn('Asset.upsertJobStatus')(function* (...jobStatus: Insertable<AssetJobStatusTable>[]) {
      if (jobStatus.length === 0) {
        return;
      }

      const values = jobStatus.map((row) => ({ ...row, assetId: asUuid(row.assetId) }));
      yield* query(db, (db) =>
        db
          .insertInto('asset_job_status')
          .values(values)
          .onConflict((oc) =>
            oc.column('assetId').doUpdateSet((eb) =>
              removeUndefinedKeys(
                {
                  metadataExtractedAt: eb.ref('excluded.metadataExtractedAt'),
                },
                values[0],
              ),
            ),
          )
          .execute(),
      );
    }),
    getMetadata: Effect.fn('Asset.getMetadata')((assetId: string) =>
      query(db, (db) =>
        db.selectFrom('asset_metadata').select(['key', 'value', 'updatedAt']).where('assetId', '=', assetId).execute(),
      ),
    ),
    upsertMetadata: Effect.fn('Asset.upsertMetadata')(function* (
      id: string,
      items: Array<{ key: string; value: Record<string, unknown> }>,
    ) {
      if (items.length === 0) {
        return [];
      }

      return yield* query(db, (db) =>
        db
          .insertInto('asset_metadata')
          .values(items.map((item) => ({ assetId: id, ...item })))
          .onConflict((oc) =>
            oc
              .columns(['assetId', 'key'])
              .doUpdateSet((eb) => ({ key: eb.ref('excluded.key'), value: eb.ref('excluded.value') })),
          )
          .returning(['key', 'value', 'updatedAt'])
          .execute(),
      );
    }),
    upsertBulkMetadata: Effect.fn('Asset.upsertBulkMetadata')((items: Insertable<AssetMetadataTable>[]) =>
      query(db, (db) =>
        db
          .insertInto('asset_metadata')
          .values(items)
          .onConflict((oc) =>
            oc
              .columns(['assetId', 'key'])
              .doUpdateSet((eb) => ({ key: eb.ref('excluded.key'), value: eb.ref('excluded.value') })),
          )
          .returning(['assetId', 'key', 'value', 'updatedAt'])
          .execute(),
      ),
    ),
    getMetadataByKey: Effect.fn('Asset.getMetadataByKey')((assetId: string, key: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset_metadata')
          .select(['key', 'value', 'updatedAt'])
          .where('assetId', '=', assetId)
          .where('key', '=', key)
          .executeTakeFirst(),
      ),
    ),
    deleteMetadataByKey: Effect.fn('Asset.deleteMetadataByKey')(function* (id: string, key: string) {
      yield* query(db, (db) =>
        db.deleteFrom('asset_metadata').where('assetId', '=', id).where('key', '=', key).execute(),
      );
    }),
    deleteBulkMetadata: Effect.fn('Asset.deleteBulkMetadata')(function* (
      items: Array<{ assetId: string; key: string }>,
    ) {
      if (items.length === 0) {
        return;
      }

      yield* query(db, (db) =>
        db.transaction().execute(async (tx) => {
          for (const { assetId, key } of items) {
            await tx.deleteFrom('asset_metadata').where('assetId', '=', assetId).where('key', '=', key).execute();
          }
        }),
      );
    }),
    create: Effect.fn('Asset.create')((asset: Insertable<AssetTable>) =>
      query(db, (db) => db.insertInto('asset').values(asset).returningAll().executeTakeFirstOrThrow()),
    ),
    createAll: Effect.fn('Asset.createAll')(function* (assets: Insertable<AssetTable>[]) {
      if (assets.length === 0) {
        return [];
      }
      const ids = yield* query(db, (db) => db.insertInto('asset').values(assets).returning('id').execute());
      return ids.map(({ id }) => id);
    }),
    getByDayOfYear: Effect.fn('Asset.getByDayOfYear')((ownerIds: string[], { year, day, month }: YearMonthDay) =>
      query(db, (db) =>
        db
          .with('res', (qb) =>
            qb
              .with('today', (qb) =>
                qb
                  .selectFrom((eb) =>
                    eb
                      .fn('generate_series', [
                        sql`(select date_part('year', min(("localDateTime" at time zone 'UTC')::date))::int from asset)`,
                        sql`${year - 1}`,
                      ])
                      .as('year'),
                  )
                  .select((eb) =>
                    eb.fn('make_date', [sql`year::int`, sql`${month}::int`, sql`${day}::int`]).as('date'),
                  ),
              )
              .selectFrom('today')
              .innerJoinLateral(
                (qb) =>
                  qb
                    .selectFrom('asset')
                    .select(['asset.id', 'asset.localDateTime'])
                    .innerJoin('asset_job_status', 'asset.id', 'asset_job_status.assetId')
                    .where(sql`(asset."localDateTime" at time zone 'UTC')::date`, '=', sql`today.date`)
                    .where('asset.ownerId', '=', anyUuid(ownerIds))
                    .where('asset.visibility', '=', AssetVisibility.Timeline)
                    .where((eb) =>
                      eb.exists((qb) =>
                        qb
                          .selectFrom('asset_file')
                          .whereRef('assetId', '=', 'asset.id')
                          .where('asset_file.type', '=', AssetFileType.Preview),
                      ),
                    )
                    .where('asset.deletedAt', 'is', null)
                    .orderBy(sql`(asset."localDateTime" at time zone 'UTC')::date`, 'desc')
                    .limit(20)
                    .as('a'),
                (join) => join.onTrue(),
              )
              .selectAll('a'),
          )
          .selectFrom('res')
          .select(sql<number>`date_part('year', ("localDateTime" at time zone 'UTC')::date)::int`.as('year'))
          .select((eb) => eb.fn.jsonAgg(eb.table('res')).as('assets'))
          .groupBy(sql`("localDateTime" at time zone 'UTC')::date`)
          .orderBy(sql`("localDateTime" at time zone 'UTC')::date`, 'desc')
          .execute(),
      ),
    ),
    getByIds: Effect.fn('Asset.getByIds')((ids: string[]) =>
      query(db, (db) => db.selectFrom('asset').selectAll('asset').where('asset.id', '=', anyUuid(ids)).execute()),
    ),
    getByIdsWithAllRelationsButStacks: Effect.fn('Asset.getByIdsWithAllRelationsButStacks')((ids: string[]) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .selectAll('asset')
          .select(withTags)
          .$call(withExif)
          .where('asset.id', '=', anyUuid(ids))
          .execute(),
      ),
    ),
    deleteAll: Effect.fn('Asset.deleteAll')(function* (ownerId: string) {
      yield* query(db, (db) => db.deleteFrom('asset').where('ownerId', '=', ownerId).execute());
    }),
    getByLibraryIdAndOriginalPath: Effect.fn('Asset.getByLibraryIdAndOriginalPath')(
      (libraryId: string, originalPath: string) =>
        query(db, (db) =>
          db
            .selectFrom('asset')
            .selectAll('asset')
            .where('libraryId', '=', asUuid(libraryId))
            .where('originalPath', '=', originalPath)
            .limit(1)
            .executeTakeFirst(),
        ),
    ),
    getLivePhotoCount: Effect.fn('Asset.getLivePhotoCount')(function* (motionId: string) {
      const [{ count }] = yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .select((eb) => eb.fn.countAll<number>().as('count'))
          .where('livePhotoVideoId', '=', asUuid(motionId))
          .execute(),
      );
      return count;
    }),
    getFileSamples: Effect.fn('Asset.getFileSamples')(() =>
      query(db, (db) => db.selectFrom('asset_file').select(['assetId', 'path']).limit(sql.lit(3)).execute()),
    ),
    getForCopy: Effect.fn('Asset.getForCopy')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['id', 'stackId', 'originalPath', 'isFavorite'])
          .select(withFiles)
          .where('id', '=', asUuid(id))
          .limit(1)
          .executeTakeFirst(),
      ),
    ),
    getById,
    updateAll: Effect.fn('Asset.updateAll')(function* (ids: string[], options: Updateable<AssetTable>) {
      if (ids.length === 0) {
        return;
      }
      yield* query(db, (db) => db.updateTable('asset').set(options).where('id', '=', anyUuid(ids)).execute());
    }),
    updateByLibraryId: Effect.fn('Asset.updateByLibraryId')(function* (
      libraryId: string,
      options: Updateable<AssetTable>,
    ) {
      yield* query(db, (db) =>
        db.updateTable('asset').set(options).where('libraryId', '=', asUuid(libraryId)).execute(),
      );
    }),
    update: Effect.fn('Asset.update')(function* (asset: Updateable<AssetTable> & { id: string }) {
      const value = omitBy(asset, isUndefined);
      delete value.id;
      if (!isEmpty(value)) {
        return yield* query(db, (db) =>
          db
            .with('asset', (qb) => qb.updateTable('asset').set(asset).where('id', '=', asUuid(asset.id)).returningAll())
            .selectFrom('asset')
            .selectAll('asset')
            .$call(withExif)
            .$call((qb) => qb.select(withEdits))
            .executeTakeFirst(),
        );
      }

      return yield* getById(asset.id, { exifInfo: true, edits: true });
    }),
    remove: Effect.fn('Asset.remove')(function* (asset: { id: string }) {
      yield* query(db, (db) => db.deleteFrom('asset').where('id', '=', asUuid(asset.id)).execute());
    }),
    getByChecksum: Effect.fn('Asset.getByChecksum')(({ ownerId, libraryId, checksum }: AssetGetByChecksumOptions) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .selectAll('asset')
          .where('ownerId', '=', asUuid(ownerId))
          .where('checksum', '=', checksum)
          .$call((qb) =>
            libraryId ? qb.where('libraryId', '=', asUuid(libraryId)) : qb.where('libraryId', 'is', null),
          )
          .limit(1)
          .executeTakeFirst(),
      ),
    ),
    getByChecksums: Effect.fn('Asset.getByChecksums')((userId: string, checksums: Buffer[]) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['id', 'checksum', 'deletedAt'])
          .where('ownerId', '=', asUuid(userId))
          .where('checksum', 'in', checksums)
          .execute(),
      ),
    ),
    getUploadAssetIdByChecksum: Effect.fn('Asset.getUploadAssetIdByChecksum')(function* (
      ownerId: string,
      checksum: Buffer,
    ) {
      const asset = yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .select('id')
          .where('ownerId', '=', asUuid(ownerId))
          .where('checksum', '=', checksum)
          .where('libraryId', 'is', null)
          .limit(1)
          .executeTakeFirst(),
      );

      return asset?.id;
    }),
    findLivePhotoMatch: Effect.fn('Asset.findLivePhotoMatch')(function* (options: LivePhotoSearchOptions) {
      const { ownerId, otherAssetId, livePhotoCID, type } = options;
      return yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['asset.id', 'asset.ownerId'])
          .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
          .where('id', '!=', asUuid(otherAssetId))
          .where('ownerId', '=', asUuid(ownerId))
          .where('type', '=', type)
          .where('asset_exif.livePhotoCID', '=', livePhotoCID)
          .limit(1)
          .executeTakeFirst(),
      );
    }),
    getStatistics: Effect.fn('Asset.getStatistics')(
      (ownerId: string, { visibility, isFavorite, isTrashed }: AssetStatsOptions) =>
        query(db, (db) =>
          db
            .selectFrom('asset')
            .select((eb) => eb.fn.countAll<number>().filterWhere('type', '=', AssetType.Audio).as(AssetType.Audio))
            .select((eb) => eb.fn.countAll<number>().filterWhere('type', '=', AssetType.Image).as(AssetType.Image))
            .select((eb) => eb.fn.countAll<number>().filterWhere('type', '=', AssetType.Video).as(AssetType.Video))
            .select((eb) => eb.fn.countAll<number>().filterWhere('type', '=', AssetType.Other).as(AssetType.Other))
            .where('ownerId', '=', asUuid(ownerId))
            .$if(visibility === undefined, withDefaultVisibility)
            .$if(!!visibility, (qb) => qb.where('asset.visibility', '=', visibility!))
            .$if(isFavorite !== undefined, (qb) => qb.where('isFavorite', '=', isFavorite!))
            .$if(!!isTrashed, (qb) => qb.where('asset.status', '!=', AssetStatus.Deleted))
            .where('deletedAt', isTrashed ? 'is not' : 'is', null)
            .executeTakeFirstOrThrow(),
        ),
    ),
    getCalendarHeatmap: Effect.fn('Asset.getCalendarHeatmap')(function* (
      ownerId: string,
      dto: { from: Date; to: Date; type: CalendarHeatmapType },
    ) {
      const dateColumns: Record<CalendarHeatmapType, { order: AssetOrderBy; column: 'createdAt' | 'localDateTime' }> = {
        [CalendarHeatmapType.Upload]: { order: AssetOrderBy.CreatedAt, column: 'createdAt' },
        [CalendarHeatmapType.Taken]: { order: AssetOrderBy.TakenAt, column: 'localDateTime' },
      } as const;

      const { order, column } = dateColumns[dto.type];

      const date = truncatedDate<Date>(order, 'DAY');

      return yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .select(date.as('date'))
          .select((eb) => eb.fn.countAll<number>().as('count'))
          .where('ownerId', '=', asUuid(ownerId))
          .where(column, '>=', dto.from)
          .where(column, '<', dto.to)
          .where('deletedAt', 'is', null)
          .groupBy(date)
          .orderBy('date', 'asc')
          .execute(),
      );
    }),
    getTimeBuckets: Effect.fn('Asset.getTimeBuckets')((options: TimeBucketOptions) =>
      query(
        db,
        (db) =>
          db
            .with('asset', (qb) =>
              qb
                .selectFrom('asset')
                .select(truncatedDate<Date>(options.orderBy).as('timeBucket'))
                .$if(!!options.isTrashed, (qb) => qb.where('asset.status', '!=', AssetStatus.Deleted))
                .where('asset.deletedAt', options.isTrashed ? 'is not' : 'is', null)
                .$if(!!options.bbox, (qb) => {
                  const bbox = options.bbox!;
                  const circle = getBoundingCircle(bbox);

                  const withBoundingCircle = qb
                    .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
                    .where(
                      sql`earth_box(ll_to_earth_public(${circle.centerLatitude}, ${circle.centerLongitude}), ${circle.radius})`,
                      '@>',
                      sql`ll_to_earth_public(asset_exif.latitude, asset_exif.longitude)`,
                    );

                  return withBoundingBox(withBoundingCircle, bbox);
                })
                .$if(options.visibility === undefined, withDefaultVisibility)
                .$if(!!options.visibility, (qb) => qb.where('asset.visibility', '=', options.visibility!))
                .$if(!!options.albumId, (qb) =>
                  qb
                    .innerJoin('album_asset', 'asset.id', 'album_asset.assetId')
                    .where('album_asset.albumId', '=', asUuid(options.albumId!)),
                )
                .$if(!!options.withStacked, (qb) =>
                  qb
                    .leftJoin('stack', (join) =>
                      join.onRef('stack.id', '=', 'asset.stackId').onRef('stack.primaryAssetId', '=', 'asset.id'),
                    )
                    .where((eb) => eb.or([eb('asset.stackId', 'is', null), eb(eb.table('stack'), 'is not', null)])),
                )
                .$if(!!options.userIds, (qb) =>
                  qb.where((eb) => {
                    // TODO this should become a shared `hasAccess` style helper once implement sharing in more places
                    const isOwner = eb('asset.ownerId', '=', anyUuid(options.userIds!));
                    return isOwner;
                  }),
                )
                .$if(options.isFavorite !== undefined, (qb) => qb.where('asset.isFavorite', '=', options.isFavorite!))
                .$if(!!options.assetType, (qb) => qb.where('asset.type', '=', options.assetType!))
                .$if(!!options.tagId, (qb) => withTagId(qb, options.tagId!)),
            )
            .selectFrom('asset')
            .select(sql<string>`("timeBucket" AT TIME ZONE 'UTC')::date::text`.as('timeBucket'))
            .select((eb) => eb.fn.countAll<number>().as('count'))
            .groupBy('timeBucket')
            .orderBy('timeBucket', options.order ?? 'desc')
            .execute() as any as Promise<TimeBucketItem[]>,
      ),
    ),
    getTimeBucket: Effect.fn('Asset.getTimeBucket')(function* (
      timeBucket: string,
      options: TimeBucketOptions,
      auth: AuthDto,
    ) {
      const order = options.order ?? 'desc';
      const timeBucketQuery = db
        .with('cte', (qb) =>
          qb
            .selectFrom('asset')
            .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
            .select((eb) => [
              'asset.duration',
              'asset.id',
              'asset.visibility',
              sql`asset."isFavorite" and asset."ownerId" = ${auth.user.id}`.as('isFavorite'),
              sql`asset.type = 'IMAGE'`.as('isImage'),
              sql`asset."deletedAt" is not null`.as('isTrashed'),
              'asset.livePhotoVideoId',
              sql`extract(epoch from (asset."localDateTime" AT TIME ZONE 'UTC' - asset."fileCreatedAt" at time zone 'UTC'))::real / 3600`.as(
                'localOffsetHours',
              ),
              'asset.ownerId',
              'asset.status',
              sql`asset."fileCreatedAt" at time zone 'utc'`.as('fileCreatedAt'),
              sql`asset."createdAt" at time zone 'utc'`.as('createdAt'),
              eb.fn('encode', ['asset.thumbhash', sql.lit('base64')]).as('thumbhash'),
              'asset_exif.projectionType',
              eb.fn
                .coalesce(
                  eb
                    .case()
                    .when(sql`asset."height" = 0 or asset."width" = 0`)
                    .then(eb.lit(1))
                    .else(sql`round(asset."width"::numeric / asset."height"::numeric, 3)`)
                    .end(),
                  eb.lit(1),
                )
                .as('ratio'),
            ])
            .$if(!isMetadataHidden(auth), (qb) => qb.select(['asset_exif.city', 'asset_exif.country']))
            .$if(!!options.withCoordinates, (qb) => qb.select(['asset_exif.latitude', 'asset_exif.longitude']))
            .where('asset.deletedAt', options.isTrashed ? 'is not' : 'is', null)
            .$if(options.visibility === undefined, withDefaultVisibility)
            .$if(!!options.visibility, (qb) => qb.where('asset.visibility', '=', options.visibility!))
            .$if(!!options.bbox, (qb) => {
              const bbox = options.bbox!;
              const circle = getBoundingCircle(bbox);

              const withBoundingCircle = qb.where(
                sql`earth_box(ll_to_earth_public(${circle.centerLatitude}, ${circle.centerLongitude}), ${circle.radius})`,
                '@>',
                sql`ll_to_earth_public(asset_exif.latitude, asset_exif.longitude)`,
              );

              return withBoundingBox(withBoundingCircle, bbox);
            })
            .where(truncatedDate(options.orderBy), '=', timeBucket.replace(/^[+-]/, ''))
            .$if(!!options.albumId, (qb) =>
              qb.where((eb) =>
                eb.exists(
                  eb
                    .selectFrom('album_asset')
                    .whereRef('album_asset.assetId', '=', 'asset.id')
                    .where('album_asset.albumId', '=', asUuid(options.albumId!)),
                ),
              ),
            )
            .$if(!!options.userIds, (qb) =>
              qb.where((eb) => {
                const isOwner = eb('asset.ownerId', '=', anyUuid(options.userIds!));
                return isOwner;
              }),
            )
            .$if(options.isFavorite !== undefined, (qb) => qb.where('asset.isFavorite', '=', options.isFavorite!))
            .$if(!!options.withStacked, (qb) =>
              qb
                .where((eb) =>
                  eb.not(
                    eb.exists(
                      eb
                        .selectFrom('stack')
                        .whereRef('stack.id', '=', 'asset.stackId')
                        .whereRef('stack.primaryAssetId', '!=', 'asset.id'),
                    ),
                  ),
                )
                .leftJoinLateral(
                  (eb) =>
                    eb
                      .selectFrom('asset as stacked')
                      .select(sql`array[stacked."stackId"::text, count('stacked')::text]`.as('stack'))
                      .whereRef('stacked.stackId', '=', 'asset.stackId')
                      .where('stacked.deletedAt', 'is', null)
                      .where('stacked.visibility', '=', AssetVisibility.Timeline)
                      .groupBy('stacked.stackId')
                      .as('stacked_assets'),
                  (join) => join.onTrue(),
                )
                .select('stack'),
            )
            .$if(!!options.assetType, (qb) => qb.where('asset.type', '=', options.assetType!))
            .$if(!!options.isTrashed, (qb) => qb.where('asset.status', '!=', AssetStatus.Deleted))
            .$if(!!options.tagId, (qb) => withTagId(qb, options.tagId!))
            .orderBy(
              options.orderBy === AssetOrderBy.CreatedAt
                ? sql`"createdAt"`
                : sql`(asset."localDateTime" AT TIME ZONE 'UTC')::date`,
              order,
            )
            .orderBy('asset.fileCreatedAt', order)
            .orderBy('asset.originalFileName', order),
        )
        .with('agg', (qb) =>
          qb
            .selectFrom('cte')
            .select((eb) => [
              eb.fn.coalesce(eb.fn('array_agg', ['duration']), sql.lit('{}')).as('duration'),
              eb.fn.coalesce(eb.fn('array_agg', ['id']), sql.lit('{}')).as('id'),
              eb.fn.coalesce(eb.fn('array_agg', ['visibility']), sql.lit('{}')).as('visibility'),
              eb.fn.coalesce(eb.fn('array_agg', ['isFavorite']), sql.lit('{}')).as('isFavorite'),
              eb.fn.coalesce(eb.fn('array_agg', ['isImage']), sql.lit('{}')).as('isImage'),
              // TODO: isTrashed is redundant as it will always be all true or false depending on the options
              eb.fn.coalesce(eb.fn('array_agg', ['isTrashed']), sql.lit('{}')).as('isTrashed'),
              eb.fn.coalesce(eb.fn('array_agg', ['livePhotoVideoId']), sql.lit('{}')).as('livePhotoVideoId'),
              eb.fn.coalesce(eb.fn('array_agg', ['fileCreatedAt']), sql.lit('{}')).as('fileCreatedAt'),
              eb.fn.coalesce(eb.fn('array_agg', ['localOffsetHours']), sql.lit('{}')).as('localOffsetHours'),
              eb.fn.coalesce(eb.fn('array_agg', ['createdAt']), sql.lit('{}')).as('createdAt'),
              eb.fn.coalesce(eb.fn('array_agg', ['ownerId']), sql.lit('{}')).as('ownerId'),
              eb.fn.coalesce(eb.fn('array_agg', ['projectionType']), sql.lit('{}')).as('projectionType'),
              eb.fn.coalesce(eb.fn('array_agg', ['ratio']), sql.lit('{}')).as('ratio'),
              eb.fn.coalesce(eb.fn('array_agg', ['status']), sql.lit('{}')).as('status'),
              eb.fn.coalesce(eb.fn('array_agg', ['thumbhash']), sql.lit('{}')).as('thumbhash'),
            ])
            .$if(!isMetadataHidden(auth), (qb) =>
              qb.select((eb) => [
                eb.fn.coalesce(eb.fn('array_agg', ['city']), sql.lit('{}')).as('city'),
                eb.fn.coalesce(eb.fn('array_agg', ['country']), sql.lit('{}')).as('country'),
              ]),
            )
            .$if(!!options.withCoordinates, (qb) =>
              qb.select((eb) => [
                eb.fn.coalesce(eb.fn('array_agg', ['latitude']), sql.lit('{}')).as('latitude'),
                eb.fn.coalesce(eb.fn('array_agg', ['longitude']), sql.lit('{}')).as('longitude'),
              ]),
            )
            .$if(!!options.withStacked, (qb) =>
              qb.select((eb) => eb.fn.coalesce(eb.fn('json_agg', ['stack']), sql.lit('[]')).as('stack')),
            ),
        )
        .selectFrom('agg')
        .select(sql<string>`to_json(agg)::text`.as('assets'));

      return yield* query(db, () => timeBucketQuery.executeTakeFirstOrThrow());
    }),
    getAssetIdByCity: Effect.fn('Asset.getAssetIdByCity')(function* (
      ownerId: string,
      { minAssetsPerField, maxFields }: AssetExploreFieldOptions,
    ) {
      const items = yield* query(db, (db) =>
        db
          .with('cities', (qb) =>
            qb
              .selectFrom('asset_exif')
              .select('city')
              .where('city', 'is not', null)
              .groupBy('city')
              .having((eb) => eb.fn('count', [eb.ref('assetId')]), '>=', minAssetsPerField),
          )
          .selectFrom('asset')
          .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
          .innerJoin('cities', 'asset_exif.city', 'cities.city')
          .distinctOn('asset_exif.city')
          .select(['assetId as data', 'asset_exif.city as value'])
          .$narrowType<{ value: NotNull }>()
          .where('ownerId', '=', asUuid(ownerId))
          .where('visibility', '=', AssetVisibility.Timeline)
          .where('type', '=', AssetType.Image)
          .where('deletedAt', 'is', null)
          .limit(maxFields)
          .execute(),
      );

      return { fieldName: 'exifInfo.city', items };
    }),
    getRecentlyCreatedAssetIds: Effect.fn('Asset.getRecentlyCreatedAssetIds')(function* (
      ownerId: string,
      maxAssets: number,
    ) {
      const items = yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['id as data', 'createdAt as value'])
          .where('ownerId', '=', asUuid(ownerId))
          .where('asset.visibility', '=', AssetVisibility.Timeline)
          .where('type', '=', AssetType.Image)
          .where('deletedAt', 'is', null)
          .orderBy('value', 'desc')
          .limit(maxAssets)
          .execute(),
      );

      return { fieldName: 'createdAt', items };
    }),
    upsertFile: Effect.fn('Asset.upsertFile')(function* (
      file: Pick<
        Insertable<AssetFileTable>,
        'assetId' | 'path' | 'type' | 'isEdited' | 'isProgressive' | 'isTransparent'
      >,
    ) {
      yield* query(db, (db) =>
        db
          .insertInto('asset_file')
          .values(file)
          .onConflict((oc) =>
            oc.columns(['assetId', 'type', 'isEdited']).doUpdateSet((eb) => ({
              path: eb.ref('excluded.path'),
            })),
          )
          .execute(),
      );
    }),
    upsertFiles: Effect.fn('Asset.upsertFiles')(function* (
      files: Pick<
        Insertable<AssetFileTable>,
        'assetId' | 'path' | 'type' | 'isEdited' | 'isProgressive' | 'isTransparent'
      >[],
    ) {
      if (files.length === 0) {
        return;
      }

      yield* query(db, (db) =>
        db
          .insertInto('asset_file')
          .values(files)
          .onConflict((oc) =>
            oc.columns(['assetId', 'type', 'isEdited']).doUpdateSet((eb) => ({
              path: eb.ref('excluded.path'),
              isProgressive: eb.ref('excluded.isProgressive'),
              isTransparent: eb.ref('excluded.isTransparent'),
            })),
          )
          .execute(),
      );
    }),
    deleteFile: Effect.fn('Asset.deleteFile')(function* ({
      assetId,
      type,
      edited,
    }: {
      assetId: string;
      type: AssetFileType;
      edited?: boolean;
    }) {
      yield* query(db, (db) =>
        db
          .deleteFrom('asset_file')
          .where('assetId', '=', asUuid(assetId))
          .where('type', '=', type)
          .$if(edited !== undefined, (qb) => qb.where('isEdited', '=', edited!))
          .execute(),
      );
    }),
    deleteFiles: Effect.fn('Asset.deleteFiles')(function* (files: Pick<Selectable<AssetFileTable>, 'id'>[]) {
      if (files.length === 0) {
        return;
      }

      yield* query(db, (db) =>
        db
          .deleteFrom('asset_file')
          .where('id', '=', anyUuid(files.map((file) => file.id)))
          .execute(),
      );
    }),
    detectOfflineExternalAssets: Effect.fn('Asset.detectOfflineExternalAssets')(function* (
      libraryId: string,
      importPaths: string[],
      exclusionPatterns: string[],
    ) {
      const paths = importPaths.map((importPath) => `${importPath}%`);
      const exclusions = exclusionPatterns.map((pattern) => globToPostgresRegex(pattern));

      return yield* query(db, (db) =>
        db
          .updateTable('asset')
          .set({
            isOffline: true,
            deletedAt: new Date(),
          })
          .where('isOffline', '=', false)
          .where('isExternal', '=', true)
          .where('libraryId', '=', asUuid(libraryId))
          .where((eb) =>
            eb.or([
              eb.not(eb.or(paths.map((path) => eb('originalPath', 'like', path)))),
              eb.or(exclusions.map((pattern) => eb('originalPath', '~*', pattern))),
            ]),
          )
          .executeTakeFirstOrThrow(),
      );
    }),
    filterNewExternalAssetPaths: Effect.fn('Asset.filterNewExternalAssetPaths')(function* (
      libraryId: string,
      paths: string[],
    ) {
      const result = yield* query(db, (db) =>
        db
          .selectFrom(unnest(paths).as('path'))
          .select('path')
          .where((eb) =>
            eb.not(
              eb.exists(
                db
                  .selectFrom('asset')
                  .select('originalPath')
                  .whereRef('asset.originalPath', '=', eb.ref('path'))
                  .where('libraryId', '=', asUuid(libraryId))
                  .where('isExternal', '=', true),
              ),
            ),
          )
          .execute(),
      );

      return result.map((row) => row.path as string);
    }),
    findExternalRelinkCandidates: Effect.fn('Asset.findExternalRelinkCandidates')(function* (
      libraryId: string,
      signatures: ExternalFileSignature[],
    ) {
      if (signatures.length === 0) {
        return [];
      }

      const rows = yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .innerJoin('asset_exif', 'asset_exif.assetId', 'asset.id')
          .select([
            'asset.id',
            'asset.originalPath',
            'asset.fileModifiedAt',
            'asset.type',
            'asset.status',
            'asset_exif.fileSizeInByte as size',
          ])
          .where('asset.libraryId', '=', asUuid(libraryId))
          .where('asset.isExternal', '=', true)
          .where('asset.status', '!=', AssetStatus.Deleted)
          .where((eb) =>
            eb.or(
              signatures.map((signature) =>
                eb.and([
                  eb('asset.fileModifiedAt', '=', signature.fileModifiedAt),
                  eb('asset_exif.fileSizeInByte', '=', signature.size),
                  eb('asset.type', '=', signature.type),
                ]),
              ),
            ),
          )
          .execute(),
      );

      return rows.map((row) => ({ ...row, size: Number(row.size) }));
    }),
    getLibraryAssetCount: Effect.fn('Asset.getLibraryAssetCount')(function* (libraryId: string) {
      const { count } = yield* query(db, (db) =>
        db
          .selectFrom('asset')
          .select((eb) => eb.fn.countAll<number>().as('count'))
          .where('libraryId', '=', asUuid(libraryId))
          .executeTakeFirstOrThrow(),
      );

      return count;
    }),
    getForOriginal: Effect.fn('Asset.getForOriginal')((id: string, isEdited: boolean) =>
      query(db, () => buildGetForOriginal([id], isEdited).executeTakeFirstOrThrow()),
    ),
    getForOriginals: Effect.fn('Asset.getForOriginals')((ids: string[], isEdited: boolean) =>
      query(db, () => buildGetForOriginal(ids, isEdited).execute()),
    ),
    getForThumbnail: Effect.fn('Asset.getForThumbnail')((id: string, type: AssetFileType, isEdited: boolean) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .where('asset.id', '=', id)
          .leftJoin('asset_file', (join) =>
            join.onRef('asset.id', '=', 'asset_file.assetId').on('asset_file.type', '=', type),
          )
          .select(['asset.originalPath', 'asset.originalFileName', 'asset_file.path as path'])
          .orderBy('asset_file.isEdited', isEdited ? 'desc' : 'asc')
          .executeTakeFirstOrThrow(),
      ),
    ),
    getForVideo: Effect.fn('Asset.getForVideo')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['asset.originalPath'])
          .select((eb) => withFilePath(eb, AssetFileType.EncodedVideo).as('encodedVideoPath'))
          .where('asset.id', '=', id)
          .where('asset.type', '=', AssetType.Video)
          .executeTakeFirst(),
      ),
    ),
    getForEdit: Effect.fn('Asset.getForEdit')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select(['asset.type', 'asset.livePhotoVideoId', 'asset.originalPath', 'asset.originalFileName'])
          .where('asset.id', '=', id)
          .innerJoin('asset_exif', (join) => join.onRef('asset_exif.assetId', '=', 'asset.id'))
          .select([
            'asset_exif.exifImageWidth',
            'asset_exif.exifImageHeight',
            'asset_exif.orientation',
            'asset_exif.projectionType',
          ])
          .executeTakeFirst(),
      ),
    ),
    getForMetadataExtractionTags: Effect.fn('Asset.getForMetadataExtractionTags')((id: string) =>
      query(db, (db) =>
        db.selectFrom('asset_exif').select('asset_exif.tags').where('asset_exif.assetId', '=', id).executeTakeFirst(),
      ),
    ),
    getForUpdateTags: Effect.fn('Asset.getForUpdateTags')((id: string) =>
      query(db, (db) =>
        db
          .selectFrom('asset')
          .select((eb) =>
            jsonArrayFrom(
              eb
                .selectFrom('tag')
                .select('tag.value')
                .innerJoin('tag_asset', 'tag.id', 'tag_asset.tagId')
                .whereRef('asset.id', '=', 'tag_asset.assetId'),
            ).as('tags'),
          )
          .where('asset.id', '=', id)
          .executeTakeFirstOrThrow(),
      ),
    ),
  };
};

export class AssetRepo extends Context.Service<AssetRepo, ReturnType<typeof makeAsset>>()(
  'immich/repositories/AssetRepo',
) {
  static readonly make = (db: Kysely<DB>) => AssetRepo.of(makeAsset(db));

  static readonly layer = Layer.effect(
    AssetRepo,
    Effect.gen(function* () {
      return AssetRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `AssetRepo`. */
@Injectable()
export class AssetRepository {
  private readonly effect: Context.Service.Shape<typeof AssetRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = AssetRepo.make(db);
    registerEffect(this, this.effect);
  }

  @GenerateSql({
    params: [
      {
        exif: { dateTimeOriginal: DummyValue.DATE, lockedProperties: ['dateTimeOriginal'] },
        lockedPropertiesBehavior: 'append',
      },
    ],
  })
  upsertExif(options: UpsertExifOptions): Promise<void> {
    return runPromise(this.effect.upsertExif(options));
  }

  @GenerateSql({ params: [[DummyValue.UUID], { model: DummyValue.STRING }] })
  @Chunked()
  updateAllExif(ids: string[], options: Updateable<AssetExifTable>): Promise<void> {
    return runPromise(this.effect.updateAllExif(ids, options));
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.NUMBER, DummyValue.STRING] })
  @Chunked()
  updateDateTimeOriginal(ids: string[], delta?: number, timeZone?: string) {
    return runPromise(this.effect.updateDateTimeOriginal(ids, delta, timeZone));
  }

  @GenerateSql({ params: [DummyValue.UUID, ['description']] })
  unlockProperties(assetId: string, properties: LockableProperty[]) {
    return runPromise(this.effect.unlockProperties(assetId, properties));
  }

  upsertJobStatus(...jobStatus: Insertable<AssetJobStatusTable>[]): Promise<void> {
    return runPromise(this.effect.upsertJobStatus(...jobStatus));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getMetadata(assetId: string) {
    return runPromise(this.effect.getMetadata(assetId));
  }

  upsertMetadata(id: string, items: Array<{ key: string; value: Record<string, unknown> }>) {
    if (items.length === 0) {
      return [];
    }

    return runPromise(this.effect.upsertMetadata(id, items));
  }

  upsertBulkMetadata(items: Insertable<AssetMetadataTable>[]) {
    return runPromise(this.effect.upsertBulkMetadata(items));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.STRING] })
  getMetadataByKey(assetId: string, key: string) {
    return runPromise(this.effect.getMetadataByKey(assetId, key));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.STRING] })
  deleteMetadataByKey(id: string, key: string) {
    return runPromise(this.effect.deleteMetadataByKey(id, key));
  }

  @GenerateSql({ params: [[{ assetId: DummyValue.UUID, key: DummyValue.STRING }]] })
  deleteBulkMetadata(items: Array<{ assetId: string; key: string }>) {
    return runPromise(this.effect.deleteBulkMetadata(items));
  }

  create(asset: Insertable<AssetTable>) {
    return runPromise(this.effect.create(asset));
  }

  @ChunkedArray({ chunkSize: 4000 })
  createAll(assets: Insertable<AssetTable>[]) {
    return runPromise(this.effect.createAll(assets));
  }

  @GenerateSql({ params: [DummyValue.UUID, { year: 2000, day: 1, month: 1 }] })
  getByDayOfYear(ownerIds: string[], yearMonthDay: YearMonthDay) {
    return runPromise(this.effect.getByDayOfYear(ownerIds, yearMonthDay));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  @ChunkedArray()
  getByIds(ids: string[]) {
    return runPromise(this.effect.getByIds(ids));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  @ChunkedArray({ paramIndex: 0 })
  getByIdsWithAllRelationsButStacks(ids: string[]) {
    return runPromise(this.effect.getByIdsWithAllRelationsButStacks(ids));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  deleteAll(ownerId: string): Promise<void> {
    return runPromise(this.effect.deleteAll(ownerId));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.STRING] })
  getByLibraryIdAndOriginalPath(libraryId: string, originalPath: string) {
    return runPromise(this.effect.getByLibraryIdAndOriginalPath(libraryId, originalPath));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getLivePhotoCount(motionId: string): Promise<number> {
    return runPromise(this.effect.getLivePhotoCount(motionId));
  }

  @GenerateSql()
  getFileSamples() {
    return runPromise(this.effect.getFileSamples());
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForCopy(id: string) {
    return runPromise(this.effect.getForCopy(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getById(id: string, relations: GetByIdsRelations = {}) {
    return runPromise(this.effect.getById(id, relations));
  }

  @GenerateSql({ params: [[DummyValue.UUID], {}] })
  @Chunked()
  updateAll(ids: string[], options: Updateable<AssetTable>): Promise<void> {
    return runPromise(this.effect.updateAll(ids, options));
  }

  updateByLibraryId(libraryId: string, options: Updateable<AssetTable>): Promise<void> {
    return runPromise(this.effect.updateByLibraryId(libraryId, options));
  }

  update(asset: Updateable<AssetTable> & { id: string }) {
    return runPromise(this.effect.update(asset));
  }

  remove(asset: { id: string }): Promise<void> {
    return runPromise(this.effect.remove(asset));
  }

  @GenerateSql({ params: [{ ownerId: DummyValue.UUID, libraryId: DummyValue.UUID, checksum: DummyValue.BUFFER }] })
  getByChecksum(options: AssetGetByChecksumOptions) {
    return runPromise(this.effect.getByChecksum(options));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.BUFFER]] })
  getByChecksums(userId: string, checksums: Buffer[]) {
    return runPromise(this.effect.getByChecksums(userId, checksums));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.BUFFER] })
  getUploadAssetIdByChecksum(ownerId: string, checksum: Buffer): Promise<string | undefined> {
    return runPromise(this.effect.getUploadAssetIdByChecksum(ownerId, checksum));
  }

  findLivePhotoMatch(options: LivePhotoSearchOptions) {
    return runPromise(this.effect.findLivePhotoMatch(options));
  }

  getStatistics(ownerId: string, options: AssetStatsOptions): Promise<AssetStats> {
    return runPromise(this.effect.getStatistics(ownerId, options));
  }

  @GenerateSql({
    params: [DummyValue.UUID, { from: DummyValue.DATE, to: DummyValue.DATE, type: CalendarHeatmapType.Upload }],
  })
  getCalendarHeatmap(ownerId: string, dto: { from: Date; to: Date; type: CalendarHeatmapType }) {
    return runPromise(this.effect.getCalendarHeatmap(ownerId, dto));
  }

  @GenerateSql({ params: [{}] })
  getTimeBuckets(options: TimeBucketOptions): Promise<TimeBucketItem[]> {
    return runPromise(this.effect.getTimeBuckets(options));
  }

  @GenerateSql({
    params: [DummyValue.TIME_BUCKET, { withStacked: true }, { user: { id: DummyValue.UUID } }],
  })
  getTimeBucket(timeBucket: string, options: TimeBucketOptions, auth: AuthDto) {
    return runPromise(this.effect.getTimeBucket(timeBucket, options, auth));
  }

  @GenerateSql({ params: [DummyValue.UUID, { minAssetsPerField: 5, maxFields: 12 }] })
  getAssetIdByCity(ownerId: string, options: AssetExploreFieldOptions) {
    return runPromise(this.effect.getAssetIdByCity(ownerId, options));
  }

  @GenerateSql({ params: [DummyValue.UUID, 12] })
  getRecentlyCreatedAssetIds(ownerId: string, maxAssets: number) {
    return runPromise(this.effect.getRecentlyCreatedAssetIds(ownerId, maxAssets));
  }

  upsertFile(
    file: Pick<
      Insertable<AssetFileTable>,
      'assetId' | 'path' | 'type' | 'isEdited' | 'isProgressive' | 'isTransparent'
    >,
  ): Promise<void> {
    return runPromise(this.effect.upsertFile(file));
  }

  upsertFiles(
    files: Pick<
      Insertable<AssetFileTable>,
      'assetId' | 'path' | 'type' | 'isEdited' | 'isProgressive' | 'isTransparent'
    >[],
  ): Promise<void> {
    return runPromise(this.effect.upsertFiles(files));
  }

  deleteFile(options: { assetId: string; type: AssetFileType; edited?: boolean }): Promise<void> {
    return runPromise(this.effect.deleteFile(options));
  }

  deleteFiles(files: Pick<Selectable<AssetFileTable>, 'id'>[]): Promise<void> {
    return runPromise(this.effect.deleteFiles(files));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.STRING], [DummyValue.STRING]] })
  detectOfflineExternalAssets(
    libraryId: string,
    importPaths: string[],
    exclusionPatterns: string[],
  ): Promise<UpdateResult> {
    return runPromise(this.effect.detectOfflineExternalAssets(libraryId, importPaths, exclusionPatterns));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.STRING]] })
  filterNewExternalAssetPaths(libraryId: string, paths: string[]): Promise<string[]> {
    return runPromise(this.effect.filterNewExternalAssetPaths(libraryId, paths));
  }

  @GenerateSql({ params: [DummyValue.UUID, [{ fileModifiedAt: DummyValue.DATE, size: 1, type: AssetType.Image }]] })
  findExternalRelinkCandidates(libraryId: string, signatures: ExternalFileSignature[]) {
    return runPromise(this.effect.findExternalRelinkCandidates(libraryId, signatures));
  }

  getLibraryAssetCount(libraryId: string): Promise<number> {
    return runPromise(this.effect.getLibraryAssetCount(libraryId));
  }

  @GenerateSql({ params: [DummyValue.UUID, true] })
  getForOriginal(id: string, isEdited: boolean) {
    return runPromise(this.effect.getForOriginal(id, isEdited));
  }

  @GenerateSql({ params: [[DummyValue.UUID], true] })
  getForOriginals(ids: string[], isEdited: boolean) {
    return runPromise(this.effect.getForOriginals(ids, isEdited));
  }

  @GenerateSql({ params: [DummyValue.UUID, AssetFileType.Preview, true] })
  getForThumbnail(id: string, type: AssetFileType, isEdited: boolean) {
    return runPromise(this.effect.getForThumbnail(id, type, isEdited));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForVideo(id: string) {
    return runPromise(this.effect.getForVideo(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForEdit(id: string) {
    return runPromise(this.effect.getForEdit(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForMetadataExtractionTags(id: string) {
    return runPromise(this.effect.getForMetadataExtractionTags(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForUpdateTags(id: string) {
    return runPromise(this.effect.getForUpdateTags(id));
  }
}
