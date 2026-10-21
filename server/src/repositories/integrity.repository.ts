import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Insertable, type Kysely, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { AssetFileType, IntegrityReport } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { IntegrityReportTable } from 'src/schema/tables/integrity-report.table.js';

export type ReportPaginationOptions = {
  cursor?: string;
  limit: number;
};

const makeIntegrityService = (db: Kysely<DB>) => ({
  create: Effect.fn('Integrity.create')(function* (
    dto: Insertable<IntegrityReportTable> | Insertable<IntegrityReportTable>[],
  ) {
    return yield* query(db, (db) =>
      db
        .insertInto('integrity_report')
        .values(dto)
        .onConflict((oc) =>
          oc.columns(['path', 'type']).doUpdateSet({
            assetId: (eb) => eb.ref('excluded.assetId'),
            fileAssetId: (eb) => eb.ref('excluded.fileAssetId'),
          }),
        )
        .returningAll()
        .executeTakeFirstOrThrow(),
    );
  }),

  getById: Effect.fn('Integrity.getById')(function* (id: string) {
    return yield* query(db, (db) =>
      db.selectFrom('integrity_report').selectAll('integrity_report').where('id', '=', id).executeTakeFirstOrThrow(),
    );
  }),

  getIntegrityReportSummary: Effect.fn('Integrity.getIntegrityReportSummary')(function* () {
    const counts = yield* query(db, (db) =>
      db
        .selectFrom('integrity_report')
        .select(['type', db.fn.countAll<number>().as('count')])
        .groupBy('type')
        .execute(),
    );

    return Object.fromEntries(
      Object.values(IntegrityReport).map((type) => [type, counts.find((count) => count.type === type)?.count || 0]),
    ) as Record<IntegrityReport, number>;
  }),

  getIntegrityReport: Effect.fn('Integrity.getIntegrityReport')(function* (
    pagination: ReportPaginationOptions,
    type: IntegrityReport,
  ) {
    const items = yield* query(db, (db) =>
      db
        .selectFrom('integrity_report')
        .select(['id', 'type', 'path', 'assetId', 'fileAssetId', 'createdAt'])
        .where('type', '=', type)
        .$if(pagination.cursor !== undefined, (eb) => eb.where('id', '<=', pagination.cursor!))
        .orderBy('id', 'desc')
        .limit(pagination.limit + 1)
        .execute(),
    );

    return {
      items: items.slice(0, pagination.limit),
      nextCursor: items.at(pagination.limit)?.id,
    };
  }),

  getAssetPathsByPaths: Effect.fn('Integrity.getAssetPathsByPaths')(function* (paths: string[]) {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .leftJoin('asset_file', (join) =>
          join.onRef('asset.id', '=', 'asset_file.assetId').on('asset_file.type', '=', AssetFileType.EncodedVideo),
        )
        .select(['asset.originalPath', 'asset_file.path as encodedVideoPath'])
        .where((eb) => eb.or([eb('originalPath', 'in', paths), eb('asset_file.path', 'in', paths)]))
        .execute(),
    );
  }),

  getAssetFilePathsByPaths: Effect.fn('Integrity.getAssetFilePathsByPaths')(function* (paths: string[]) {
    return yield* query(db, (db) => db.selectFrom('asset_file').select('path').where('path', 'in', paths).execute());
  }),

  getTrackedPaths: Effect.fn('Integrity.getTrackedPaths')(function* (paths: string[]) {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .select('asset.originalPath as path')
        .where('asset.originalPath', 'in', paths)
        .union((eb) =>
          eb.selectFrom('asset_file').select('asset_file.path as path').where('asset_file.path', 'in', paths),
        )
        .execute(),
    );
  }),

  getAssetCount: Effect.fn('Integrity.getAssetCount')(function* () {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .select((eb) => eb.fn.countAll<number>().as('count'))
        .executeTakeFirstOrThrow(),
    );
  }),

  // The streaming methods below hand back the lazy Kysely stream untouched; nothing is buffered.
  streamAllAssetPaths: Effect.fn('Integrity.streamAllAssetPaths')(function* () {
    return yield* Effect.sync(() =>
      db
        .selectFrom('asset')
        .leftJoin('asset_file', (join) =>
          join.onRef('asset.id', '=', 'asset_file.assetId').on('asset_file.type', '=', AssetFileType.EncodedVideo),
        )
        .select(['originalPath', 'asset_file.path as encodedVideoPath'])
        .stream(),
    );
  }),

  streamAllAssetFilePaths: Effect.fn('Integrity.streamAllAssetFilePaths')(function* () {
    return yield* Effect.sync(() => db.selectFrom('asset_file').select(['path']).stream());
  }),

  streamAssetPathsForMissingFiles: Effect.fn('Integrity.streamAssetPathsForMissingFiles')(function* () {
    return yield* Effect.sync(
      () =>
        db
          .selectFrom((eb) =>
            eb
              .selectFrom('asset')
              .where('asset.deletedAt', 'is', null)
              .select('asset.originalPath as path')
              .select((eb) => [
                eb.ref('asset.id').$castTo<string | null>().as('assetId'),
                sql<string | null>`null::uuid`.as('fileAssetId'),
              ])
              .unionAll(
                eb
                  .selectFrom('asset_file')
                  .select(['path'])
                  .select((eb) => [
                    sql<string | null>`null::uuid`.as('assetId'),
                    eb.ref('asset_file.id').$castTo<string | null>().as('fileAssetId'),
                  ]),
              )
              .as('allPaths'),
          )
          .leftJoin('integrity_report', (join) =>
            join
              .on('integrity_report.type', '=', IntegrityReport.MissingFile)
              .on((eb) =>
                eb.or([
                  eb('integrity_report.assetId', '=', eb.ref('allPaths.assetId')),
                  eb('integrity_report.fileAssetId', '=', eb.ref('allPaths.fileAssetId')),
                ]),
              ),
          )
          .select([
            'allPaths.path as path',
            'allPaths.assetId',
            'allPaths.fileAssetId',
            'integrity_report.id as reportId',
          ])
          .stream() as AsyncIterableIterator<
          { path: string; reportId: string | null } & (
            { assetId: string; fileAssetId: null } | { assetId: null; fileAssetId: string }
          )
        >,
    );
  }),

  streamAssetChecksums: Effect.fn('Integrity.streamAssetChecksums')(function* (startMarker?: Date) {
    return yield* Effect.sync(() =>
      db
        .selectFrom('asset')
        .where('asset.deletedAt', 'is', null)
        .leftJoin('integrity_report', (join) =>
          join
            .onRef('integrity_report.assetId', '=', 'asset.id')
            .on('integrity_report.type', '=', IntegrityReport.ChecksumFail),
        )
        .select([
          'asset.originalPath',
          'asset.checksum',
          'asset.createdAt',
          'asset.id as assetId',
          'integrity_report.id as reportId',
        ])
        .where('asset.isExternal', '=', sql.lit(false))
        .$if(startMarker !== undefined, (qb) => qb.where('asset.createdAt', '>=', startMarker!))
        .orderBy('asset.createdAt', 'asc')
        .stream(),
    );
  }),

  streamIntegrityReports: Effect.fn('Integrity.streamIntegrityReports')(function* (type: IntegrityReport) {
    return yield* Effect.sync(() =>
      db
        .selectFrom('integrity_report')
        .select(['id', 'type', 'path', 'assetId', 'fileAssetId'])
        .where('type', '=', type)
        .orderBy('createdAt', 'desc')
        .stream(),
    );
  }),

  streamIntegrityReportsWithAssetChecksum: Effect.fn('Integrity.streamIntegrityReportsWithAssetChecksum')(function* (
    type: IntegrityReport,
  ) {
    return yield* Effect.sync(() =>
      db
        .selectFrom('integrity_report')
        .select(['integrity_report.id as reportId', 'integrity_report.path'])
        .where('integrity_report.type', '=', type)
        .$if(type === IntegrityReport.ChecksumFail, (eb) =>
          eb.leftJoin('asset', 'integrity_report.path', 'asset.originalPath').select('asset.checksum'),
        )
        .stream(),
    );
  }),

  streamIntegrityReportsByProperty: Effect.fn('Integrity.streamIntegrityReportsByProperty')(function* (
    property?: 'assetId' | 'fileAssetId',
    filterType?: IntegrityReport,
  ) {
    return yield* Effect.sync(() =>
      db
        .selectFrom('integrity_report')
        .select(['id', 'path', 'assetId', 'fileAssetId'])
        .$if(filterType !== undefined, (eb) => eb.where('type', '=', filterType!))
        .$if(property === undefined, (eb) => eb.where('assetId', 'is', null).where('fileAssetId', 'is', null))
        .$if(property !== undefined, (eb) => eb.where(property!, 'is not', null))
        .stream(),
    );
  }),

  deleteById: Effect.fn('Integrity.deleteById')(function* (id: string) {
    return yield* query(db, (db) => db.deleteFrom('integrity_report').where('id', '=', id).execute());
  }),

  deleteByIds: Effect.fn('Integrity.deleteByIds')(function* (ids: string[]) {
    return yield* query(db, (db) => db.deleteFrom('integrity_report').where('id', 'in', ids).execute());
  }),
});

export class IntegrityRepo extends Context.Service<IntegrityRepo, ReturnType<typeof makeIntegrityService>>()(
  'immich/repositories/IntegrityRepo',
) {
  static readonly make = (db: Kysely<DB>) => IntegrityRepo.of(makeIntegrityService(db));

  static readonly layer = Layer.effect(
    IntegrityRepo,
    Effect.gen(function* () {
      return IntegrityRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `IntegrityRepo`. */
@Injectable()
export class IntegrityRepository {
  private readonly effect: Context.Service.Shape<typeof IntegrityRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = IntegrityRepo.make(db);
    registerEffect(this, this.effect);
  }

  create(dto: Insertable<IntegrityReportTable> | Insertable<IntegrityReportTable>[]) {
    return runPromise(this.effect.create(dto));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getById(id: string) {
    return runPromise(this.effect.getById(id));
  }

  @GenerateSql({ params: [] })
  getIntegrityReportSummary() {
    return runPromise(this.effect.getIntegrityReportSummary());
  }

  @GenerateSql({ params: [{ cursor: DummyValue.NUMBER, limit: 100 }, DummyValue.STRING] })
  getIntegrityReport(pagination: ReportPaginationOptions, type: IntegrityReport) {
    return runPromise(this.effect.getIntegrityReport(pagination, type));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getAssetPathsByPaths(paths: string[]) {
    return runPromise(this.effect.getAssetPathsByPaths(paths));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getAssetFilePathsByPaths(paths: string[]) {
    return runPromise(this.effect.getAssetFilePathsByPaths(paths));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  getTrackedPaths(paths: string[]) {
    return runPromise(this.effect.getTrackedPaths(paths));
  }

  @GenerateSql({ params: [] })
  getAssetCount() {
    return runPromise(this.effect.getAssetCount());
  }

  @GenerateSql({ params: [], stream: true })
  streamAllAssetPaths() {
    return runSync(this.effect.streamAllAssetPaths());
  }

  @GenerateSql({ params: [], stream: true })
  streamAllAssetFilePaths() {
    return runSync(this.effect.streamAllAssetFilePaths());
  }

  @GenerateSql({ params: [], stream: true })
  streamAssetPathsForMissingFiles() {
    return runSync(this.effect.streamAssetPathsForMissingFiles());
  }

  @GenerateSql({ params: [DummyValue.DATE], stream: true })
  streamAssetChecksums(startMarker?: Date) {
    return runSync(this.effect.streamAssetChecksums(startMarker));
  }

  @GenerateSql({ params: [DummyValue.STRING], stream: true })
  streamIntegrityReports(type: IntegrityReport) {
    return runSync(this.effect.streamIntegrityReports(type));
  }

  @GenerateSql({ params: [DummyValue.STRING], stream: true })
  streamIntegrityReportsWithAssetChecksum(type: IntegrityReport) {
    return runSync(this.effect.streamIntegrityReportsWithAssetChecksum(type));
  }

  @GenerateSql({ params: [DummyValue.STRING], stream: true })
  streamIntegrityReportsByProperty(property?: 'assetId' | 'fileAssetId', filterType?: IntegrityReport) {
    return runSync(this.effect.streamIntegrityReportsByProperty(property, filterType));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  deleteById(id: string) {
    return runPromise(this.effect.deleteById(id));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  deleteByIds(ids: string[]) {
    return runPromise(this.effect.deleteByIds(ids));
  }
}
