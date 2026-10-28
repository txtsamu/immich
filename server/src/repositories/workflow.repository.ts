import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { jsonArrayFrom, jsonObjectFrom } from 'kysely/helpers/postgres';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { WorkflowGetLogsDto, WorkflowSearchDto } from 'src/dtos/workflow.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { WorkflowLogTable } from 'src/schema/tables/workflow-log.table.js';
import { WorkflowStepTable } from 'src/schema/tables/workflow-step.table.js';
import { WorkflowTable } from 'src/schema/tables/workflow.table.js';
import { withTags } from 'src/utils/database.js';

export type WorkflowStepUpsert = Omit<Insertable<WorkflowStepTable>, 'workflowId' | 'order'>;

const workflowQueryBuilder = (db: Kysely<DB>) =>
  db
    .selectFrom('workflow')
    .select([
      'workflow.id',
      'workflow.name',
      'workflow.description',
      'workflow.trigger',
      'workflow.enabled',
      'workflow.createdAt',
      'workflow.updatedAt',
      'workflow.logging',
    ])
    .select((eb) => [
      jsonArrayFrom(
        eb
          .selectFrom('workflow_step')
          .innerJoin('plugin_method', 'plugin_method.id', 'workflow_step.pluginMethodId')
          .innerJoin('plugin', 'plugin.id', 'plugin_method.pluginId')
          .whereRef('workflow.id', '=', 'workflow_step.workflowId')
          .select([
            'plugin.name as pluginName',
            'plugin_method.name as methodName',
            'workflow_step.config',
            'workflow_step.enabled',
          ])
          .orderBy('workflow_step.order', 'asc'),
      ).as('steps'),
    ]);

const replaceAndReturn = async (tx: Kysely<DB>, workflowId: string, steps?: WorkflowStepUpsert[]) => {
  if (steps) {
    await tx.deleteFrom('workflow_step').where('workflowId', '=', workflowId).execute();
    if (steps.length > 0) {
      await tx
        .insertInto('workflow_step')
        .values(
          steps.map((step, i) => ({
            workflowId,
            enabled: step.enabled ?? true,
            pluginMethodId: step.pluginMethodId,
            config: step.config,
            order: i,
          })),
        )
        .returningAll()
        .execute();
    }
  }

  return workflowQueryBuilder(tx).where('id', '=', workflowId).executeTakeFirstOrThrow();
};

const makeWorkflow = (db: Kysely<DB>) => ({
  search: Effect.fn('Workflow.search')(function* (dto: WorkflowSearchDto & { userId?: string }) {
    return yield* query(db, (db) =>
      workflowQueryBuilder(db)
        .$if(!!dto.id, (qb) => qb.where('id', '=', dto.id!))
        .$if(!!dto.userId, (qb) => qb.where('ownerId', '=', dto.userId!))
        .$if(!!dto.trigger, (qb) => qb.where('trigger', '=', dto.trigger!))
        .$if(dto.enabled !== undefined, (qb) => qb.where('enabled', '=', dto.enabled!))
        .orderBy('createdAt', 'desc')
        .execute(),
    );
  }),

  get: Effect.fn('Workflow.get')(function* (id: string) {
    return yield* query(db, (db) => workflowQueryBuilder(db).where('id', '=', id).executeTakeFirst());
  }),

  getForWorkflowRun: Effect.fn('Workflow.getForWorkflowRun')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('workflow')
        .select(['workflow.id', 'workflow.name', 'workflow.trigger', 'workflow.logging'])
        .select((eb) => [
          jsonArrayFrom(
            eb
              .selectFrom('workflow_step')
              .innerJoin('plugin_method', 'plugin_method.id', 'workflow_step.pluginMethodId')
              .whereRef('workflow_step.workflowId', '=', 'workflow.id')
              .where('workflow_step.enabled', '=', true)
              .orderBy('workflow_step.order', 'asc')
              .select([
                'workflow_step.id',
                'workflow_step.config',
                'plugin_method.pluginId as pluginId',
                'plugin_method.name as methodName',
                'plugin_method.types as types',
                'plugin_method.hostFunctions',
                'plugin_method.allowedHosts',
              ]),
          ).as('steps'),
        ])
        .where('id', '=', id)
        .where('enabled', '=', true)
        .executeTakeFirst(),
    );
  }),

  create: Effect.fn('Workflow.create')(function* (dto: Insertable<WorkflowTable>, steps?: WorkflowStepUpsert[]) {
    return yield* query(db, (db) =>
      db.transaction().execute(async (tx) => {
        const { id } = await tx.insertInto('workflow').values(dto).returning(['id']).executeTakeFirstOrThrow();
        return replaceAndReturn(tx, id, steps);
      }),
    );
  }),

  update: Effect.fn('Workflow.update')(function* (
    id: string,
    dto: Updateable<WorkflowTable>,
    steps?: WorkflowStepUpsert[],
  ) {
    return yield* query(db, (db) =>
      db.transaction().execute(async (tx) => {
        if (dto.logging === false) {
          await tx.deleteFrom('workflow_log').where('workflowId', '=', id).execute();
        }
        if (Object.values(dto).some((prop) => prop !== undefined)) {
          await tx.updateTable('workflow').set(dto).where('id', '=', id).executeTakeFirstOrThrow();
        }
        return replaceAndReturn(tx, id, steps);
      }),
    );
  }),

  getLogs: Effect.fn('Workflow.getLogs')(function* (id: string, dto: WorkflowGetLogsDto) {
    return yield* query(db, (db) =>
      db
        .selectFrom('workflow_log')
        .select([
          'workflow_log.id',
          'workflow_log.createdAt',
          'workflow_log.result',
          'workflow_log.workflowId',
          'workflow_log.workflowStepId',
          'workflow_log.triggerDataId',
        ])
        .where('workflow_log.workflowId', '=', id)
        .select((eb) => [
          jsonObjectFrom(
            eb
              .selectFrom('workflow_step')
              .whereRef('workflow_step.id', '=', 'workflow_log.workflowStepId')
              .innerJoin('plugin_method', 'plugin_method.id', 'workflow_step.pluginMethodId')
              .select(['plugin_method.pluginId', 'plugin_method.name as methodName', 'workflow_step.order']),
          ).as('step'),
        ])
        .$if(dto.result !== undefined, (qb) => qb.where('workflow_log.result', '=', dto.result!))
        .$if(dto.before !== undefined, (qb) => qb.where('workflow_log.createdAt', '<', dto.before!))
        .orderBy('workflow_log.createdAt', 'desc')
        .limit(dto.limit)
        .execute(),
    );
  }),

  log: Effect.fn('Workflow.log')(function* (dto: Insertable<WorkflowLogTable>) {
    return yield* query(db, (db) => db.insertInto('workflow_log').values(dto).execute());
  }),

  updateStep: Effect.fn('Workflow.updateStep')(function* (id: string, dto: Updateable<WorkflowStepTable>) {
    yield* query(db, (db) => db.updateTable('workflow_step').where('workflow_step.id', '=', id).set(dto).execute());
  }),

  delete: Effect.fn('Workflow.delete')(function* (id: string) {
    yield* query(db, (db) => db.deleteFrom('workflow').where('id', '=', id).execute());
  }),

  getForAssetV1: Effect.fn('Workflow.getForAssetV1')(function* (assetId: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .leftJoin('asset_exif', 'asset_exif.assetId', 'asset.id')
        .select((eb) => [
          ...columns.workflowAssetV1,
          withTags,
          jsonObjectFrom(
            eb
              .selectFrom('asset_exif')
              .select([
                'asset_exif.make',
                'asset_exif.model',
                'asset_exif.orientation',
                'asset_exif.dateTimeOriginal',
                'asset_exif.modifyDate',
                'asset_exif.exifImageWidth',
                'asset_exif.exifImageHeight',
                'asset_exif.fileSizeInByte',
                'asset_exif.lensModel',
                'asset_exif.fNumber',
                'asset_exif.focalLength',
                'asset_exif.iso',
                'asset_exif.latitude',
                'asset_exif.longitude',
                'asset_exif.city',
                'asset_exif.state',
                'asset_exif.country',
                'asset_exif.description',
                'asset_exif.fps',
                'asset_exif.exposureTime',
                'asset_exif.livePhotoCID',
                'asset_exif.timeZone',
                'asset_exif.projectionType',
                'asset_exif.profileDescription',
                'asset_exif.colorspace',
                'asset_exif.bitsPerSample',
                'asset_exif.autoStackId',
                'asset_exif.rating',
                'asset_exif.tags',
                'asset_exif.updatedAt',
              ])
              .whereRef('asset_exif.assetId', '=', 'asset.id'),
          ).as('exifInfo'),
        ])
        .where('id', '=', assetId)
        .executeTakeFirstOrThrow(),
    );
  }),
});

export class WorkflowRepo extends Context.Service<WorkflowRepo, ReturnType<typeof makeWorkflow>>()(
  'immich/repositories/WorkflowRepo',
) {
  static readonly make = (db: Kysely<DB>) => WorkflowRepo.of(makeWorkflow(db));

  static readonly layer = Layer.effect(
    WorkflowRepo,
    Effect.gen(function* () {
      return WorkflowRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `WorkflowRepo`. */
@Injectable()
export class WorkflowRepository {
  private readonly effect: Context.Service.Shape<typeof WorkflowRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = WorkflowRepo.make(db);
    registerEffect(this, this.effect);
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  search(dto: WorkflowSearchDto & { userId?: string }) {
    return runPromise(this.effect.search(dto));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  get(id: string) {
    return runPromise(this.effect.get(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getForWorkflowRun(id: string) {
    return runPromise(this.effect.getForWorkflowRun(id));
  }

  create(dto: Insertable<WorkflowTable>, steps?: WorkflowStepUpsert[]) {
    return runPromise(this.effect.create(dto, steps));
  }

  update(id: string, dto: Updateable<WorkflowTable>, steps?: WorkflowStepUpsert[]) {
    return runPromise(this.effect.update(id, dto, steps));
  }

  @GenerateSql({ params: [DummyValue.UUID, { result: undefined }] })
  getLogs(id: string, dto: WorkflowGetLogsDto) {
    return runPromise(this.effect.getLogs(id, dto));
  }

  log(dto: Insertable<WorkflowLogTable>) {
    return runPromise(this.effect.log(dto));
  }

  updateStep(id: string, dto: Updateable<WorkflowStepTable>) {
    return runPromise(this.effect.updateStep(id, dto));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  delete(id: string) {
    return runPromise(this.effect.delete(id));
  }

  getForAssetV1(assetId: string) {
    return runPromise(this.effect.getForAssetV1(assetId));
  }
}
