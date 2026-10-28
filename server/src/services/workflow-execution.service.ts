import { CurrentPlugin } from '@extism/extism';
import {
  WorkflowChanges,
  WorkflowEventData,
  WorkflowEventPayload,
  WorkflowResponse,
  WorkflowTrigger,
} from '@immich/plugin-sdk';
import { HttpException, UnauthorizedException } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import { join } from 'node:path';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { JobOf } from 'src/types.js';
import { DummyValue, OnEvent, OnJob } from 'src/decorators.js';
import { AlbumsAddAssetsDto, CreateAlbumDto, GetAlbumsDto } from 'src/dtos/album.dto.js';
import { BulkIdsDto } from 'src/dtos/asset-ids.response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { PluginManifestDto } from 'src/dtos/plugin-manifest.dto.js';
import { TagBulkAssetsDto } from 'src/dtos/tag.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  BootstrapEventPriority,
  DatabaseLock,
  ImmichEnvironment,
  ImmichWorker,
  JobName,
  JobStatus,
  QueueName,
  WorkflowResult,
  WorkflowType,
} from 'src/enum.js';
import { AlbumService } from 'src/services/album.service.js';
import { AssetService } from 'src/services/asset.service.js';
import { BaseService } from 'src/services/base.service.js';
import { TagService } from 'src/services/tag.service.js';
import { isAllowedHost } from 'src/utils/workflow.js';

const dummy = () => {
  throw new Error(
    `Calling host functions is not allowed without setting methods[].hostFunctions=true in the plugin manifest`,
  );
};

/** Run a call into another (Promise-based) service as an Effect; the error stays the original thrown value. */
const attempt = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => cause });

/** The original thrown value behind a wrapped repository error (`DatabaseError`, `LibraryError`, ...). */
type ExecuteOptions<T extends WorkflowType> = {
  read: (type: T) => Effect.Effect<{ authUserId: string; data: WorkflowEventData<T>; entityId?: string }, unknown>;
  write: (auth: AuthDto, changes: WorkflowChanges<T>) => Effect.Effect<void, unknown>;
};

type AssetTrigger = { userId: string; assetId: string; trigger: WorkflowTrigger };

type HostContext = {
  allowedHosts: string[];
};

export class WorkflowExecutionService extends BaseService {
  private jwtSecret!: string;

  @OnEvent({ name: 'AppBootstrap', priority: BootstrapEventPriority.PluginSync, workers: [ImmichWorker.Microservices] })
  async onPluginSync() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.database.withLock(DatabaseLock.PluginImport, () =>
          runPromise(
            Effect.gen({ self: this }, function* () {
              // TODO avoid importing plugins in each worker
              // Can this use system metadata similar to geocoding?

              const { environment, resourcePaths, plugins } = yield* this.fx.config.getEnv();
              yield* this.importFolder(resourcePaths.corePlugin, {
                force: environment === ImmichEnvironment.Development,
              });

              if (plugins.external.allow && plugins.external.installFolder) {
                yield* this.importFolders(plugins.external.installFolder);
              }
            }),
          ),
        );
      }),
    );
  }

  @OnEvent({ name: 'AppBootstrap', priority: BootstrapEventPriority.PluginLoad, workers: [ImmichWorker.Microservices] })
  async onPluginLoad() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.jwtSecret = yield* this.fx.crypto.randomBytesAsText(32);

        const albumService = BaseService.create(AlbumService, this);
        const tagService = BaseService.create(TagService, this);

        const searchAlbums = this.wrap<[dto: GetAlbumsDto]>((authDto, ctx, args) =>
          albumService.getAll(authDto, ...args),
        );
        const createAlbum = this.wrap<[dto: CreateAlbumDto]>((authDto, ctx, args) =>
          albumService.create(authDto, ...args),
        );
        const addAssetsToAlbum = this.wrap<[id: string, dto: BulkIdsDto]>((authDto, ctx, args) =>
          albumService.addAssets(authDto, ...args),
        );
        const addAssetsToAlbums = this.wrap<[dto: AlbumsAddAssetsDto]>((authDto, ctx, args) =>
          albumService.addAssetsToAlbums(authDto, ...args),
        );
        const httpRequest = this.wrap<
          [
            url: string,
            options?: {
              method?: string;
              headers?: Record<string, string>;
              body?: string;
            },
          ]
        >(async (authDto, context, args) => {
          const hostname = new URL(args[0]).hostname;
          if (!isAllowedHost(hostname, context.allowedHosts)) {
            throw new Error('Hostname did not match any listed in methods[].allowedHosts in the plugin manifest');
          }

          // eslint-disable-next-line unicorn/no-invalid-argument-count
          const res = await fetch(...args);

          return {
            ok: res.ok,
            status: res.status,
            body: await res.text(),
          };
        });
        const bulkTagAssets = this.wrap<[dto: TagBulkAssetsDto]>((authDto, ctx, args) =>
          tagService.bulkTagAssets(authDto, ...args),
        );

        const functions = {
          searchAlbums,
          createAlbum,
          addAssetsToAlbum,
          addAssetsToAlbums,
          httpRequest,
          bulkTagAssets,
        };

        const stubs: typeof functions = {
          searchAlbums: dummy,
          createAlbum: dummy,
          addAssetsToAlbum: dummy,
          addAssetsToAlbums: dummy,
          httpRequest: dummy,
          bulkTagAssets: dummy,
        };

        const plugins = yield* this.fx.plugin.getForLoad();
        for (const { id, name, version, wasmBytes, methods } of plugins) {
          const isMethod = methods.some(({ hostFunctions }) => !hostFunctions);
          if (isMethod) {
            const label = `${name}@${version}`;
            const key = this.getPluginKey({ id, hostFunctions: false });
            yield* Effect.gen({ self: this }, function* () {
              yield* this.fx.plugin.load({ key, label, wasmBytes }, { runInWorker: false, functions: stubs });
              this.logger.log(`Loaded plugin: ${label}`);
            }).pipe(
              Effect.catch((error) => {
                this.logger.error(`Unable to load plugin ${label} (${id})`, unwrap(error));
                return Effect.void;
              }),
            );
          }

          const isMethodWithFunction = methods.some(({ hostFunctions }) => hostFunctions);
          if (!isMethodWithFunction) {
            continue;
          }

          const label = `${name}@${version}/worker`;
          const key = this.getPluginKey({ id, hostFunctions: true });
          yield* Effect.gen({ self: this }, function* () {
            yield* this.fx.plugin.load({ key, label, wasmBytes }, { runInWorker: true, functions });
            this.logger.log(`Loaded plugin with host functions: ${label}`);
          }).pipe(
            Effect.catch((error) => {
              this.logger.error(`Unable to load plugin with host functions ${label} (${id})`, unwrap(error));
              return Effect.void;
            }),
          );
        }
      }),
    );
  }

  private getPluginKey({ id, hostFunctions }: { id: string; hostFunctions: boolean }) {
    return id + (hostFunctions ? '/worker' : '');
  }

  private wrap<T>(fn: (authDto: AuthDto, context: HostContext, args: T) => Promise<unknown>) {
    return async (plugin: CurrentPlugin, offset: bigint) => {
      try {
        const handle = plugin.read(offset);
        if (!handle) {
          return plugin.store(
            JSON.stringify({ success: false, status: 400, message: 'Called host function without input' }),
          );
        }

        const { authToken, args } = handle.json() as { authToken: string; args: T };
        if (!authToken) {
          throw new Error('authToken is required');
        }

        const context = plugin.hostContext<HostContext>();
        const authDto = this.validate(authToken);
        const response = await fn(authDto, context, args);

        return plugin.store(JSON.stringify({ success: true, response }));
      } catch (error: Error | any) {
        if (error instanceof HttpException) {
          this.logger.error(`Plugin host exception: ${error}`);
          return plugin.store(
            JSON.stringify({ success: false, status: error.getStatus(), message: error.getResponse() }),
          );
        }

        this.logger.error(`Plugin host exception: ${error}`, error?.stack);

        return plugin.store(
          JSON.stringify({
            success: false,
            status: 500,
            message: `Internal server error: ${error}`,
          }),
        );
      }
    };
  }

  private importFolders(installFolder: string) {
    return Effect.gen({ self: this }, function* () {
      const entries = yield* this.fx.storage.readdirWithTypes(installFolder);
      for (const entry of entries) {
        if (!entry.isDirectory()) {
          continue;
        }

        yield* this.importFolder(join(installFolder, entry.name));
      }
    }).pipe(
      Effect.catchCause((cause) => {
        this.logger.error(`Failed to import plugins folder ${installFolder}:`, unwrap(Cause.squash(cause)));
        return Effect.void;
      }),
    );
  }

  private importFolder(folder: string, options?: { force?: boolean }) {
    return Effect.gen({ self: this }, function* () {
      const manifestPath = join(folder, 'manifest.json');
      const bytes = yield* this.fx.storage.readFile(manifestPath);
      const contents = bytes.toString('utf8');
      const sha256hash = (yield* this.fx.crypto.hashSha256(contents)) as Buffer;

      if (!options?.force) {
        const match = yield* this.fx.plugin.getByHash(sha256hash);
        if (match) {
          this.logger.log(`Plugin up to date (name=${match.name}@${match.version}, hash=${sha256hash.toString('hex')}`);
          return;
        }
      }

      const dto = JSON.parse(contents);
      const result = PluginManifestDto.schema.safeParse(dto);
      if (!result.success) {
        const issues = result.error.issues.map((issue) => `  - [${issue.path.join('.')}] ${issue.message}`).join('\n');
        this.logger.warn(`Invalid plugin manifest at ${manifestPath}:\n${issues}`);
        return;
      }
      const manifest = result.data;

      const existing = yield* this.fx.plugin.getByName(manifest.name);
      const wasmPath = `${folder}/${manifest.wasmPath}`;
      const wasmBytes = yield* this.fx.storage.readFile(wasmPath);

      const plugin = yield* this.fx.plugin.upsert(
        {
          // NOTE: new properties here need to be added to the on conflict clause in the repository
          enabled: true,
          name: manifest.name,
          title: manifest.title,
          description: manifest.description,
          author: manifest.author,
          version: manifest.version,
          templates: manifest.templates,
          wasmBytes,
          sha256hash,
        },
        manifest.methods,
      );

      if (existing) {
        this.logger.log(
          `Upgraded plugin ${manifest.name} (${plugin.methods.length} methods) from ${existing.version} to ${manifest.version} `,
        );
      } else {
        this.logger.log(
          `Imported plugin ${manifest.name}@${manifest.version} (${plugin.methods.length} methods) from ${folder}`,
        );
      }

      return manifest;
    }).pipe(
      Effect.catchCause(() => {
        this.logger.warn(`Failed to import plugin from ${folder}:`);
        return Effect.succeed(undefined);
      }),
    );
  }

  private validate(authToken: string): AuthDto {
    try {
      const jwt = this.cryptoRepository.verifyJwt<{ userId: string }>(authToken, this.jwtSecret);
      if (!jwt.userId) {
        throw new UnauthorizedException('Invalid token: missing userId');
      }

      return {
        user: {
          id: jwt.userId,
        },
      } as AuthDto;
    } catch (error) {
      this.logger.error('Token validation failed:', error);
      throw new UnauthorizedException('Invalid token');
    }
  }

  private sign(userId: string) {
    return this.cryptoRepository.signJwt({ userId }, this.jwtSecret);
  }

  @OnEvent({ name: 'AssetCreate' })
  onAssetCreate({ asset: { ownerId: userId, id: assetId } }: ArgOf<'AssetCreate'>) {
    return this.onAssetTrigger({ userId, assetId, trigger: WorkflowTrigger.AssetCreate });
  }

  @OnEvent({ name: 'AssetMetadataExtracted' })
  onAssetMetadataExtracted({ userId, assetId, source }: ArgOf<'AssetMetadataExtracted'>) {
    // prevent loops
    // TODO loop detection in job service directly
    if (source === 'sidecar-write') {
      return;
    }

    return this.onAssetTrigger({ userId, assetId, trigger: WorkflowTrigger.AssetMetadataExtraction });
  }

  @OnEvent({ name: 'AssetTag' })
  onAssetTagged({ assetId, userId }: ArgOf<'AssetTag'>) {
    return this.onAssetTrigger({ userId, assetId, trigger: WorkflowTrigger.AssetTagged });
  }

  private async onAssetTrigger({ userId, assetId, trigger }: AssetTrigger) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const items = yield* this.fx.workflow.search({ userId, trigger });
        yield* this.fx.job.queueAll(
          items.map((workflow) => ({
            name: JobName.WorkflowAssetTrigger,
            data: { workflowId: workflow.id, assetId, trigger },
          })),
        );
      }),
    );
  }

  @OnJob({ name: JobName.WorkflowAssetTrigger, queue: QueueName.Workflow })
  handleAssetTrigger({ workflowId, assetId }: JobOf<JobName.WorkflowAssetTrigger>) {
    return runPromise(
      this.execute(workflowId, (type) => {
        const assetService = BaseService.create(AssetService, this);

        switch (type) {
          case WorkflowType.AssetV1: {
            return {
              read: () =>
                Effect.gen({ self: this }, function* () {
                  const asset = yield* this.fx.workflow.getForAssetV1(assetId);
                  return {
                    data: { asset } as any,
                    authUserId: asset.ownerId,
                    entityId: asset.id,
                  };
                }),
              write: (auth, changes) =>
                Effect.gen(function* () {
                  const asset = changes.asset;
                  if (!asset) {
                    return;
                  }

                  yield* attempt(() =>
                    assetService.update(auth, assetId, {
                      isFavorite: asset.isFavorite,
                      visibility: asset.visibility,
                      dateTimeOriginal: asset.exifInfo?.dateTimeOriginal ?? undefined,
                      // TODO allow setting to null
                      longitude: asset.exifInfo?.longitude ?? undefined,
                      // TODO allow setting to null
                      latitude: asset.exifInfo?.latitude ?? undefined,
                      // TODO allow setting to null
                      description: asset.exifInfo?.description ?? undefined,
                      rating: asset.exifInfo?.rating,

                      // TODO add to update dto
                      // make: asset.exifInfo?.make,
                      // model: asset.exifInfo?.model,
                      // city: asset.exifInfo?.city,
                      // state: asset.exifInfo?.state,
                      // country: asset.exifInfo?.country,
                      // lensModel: asset.exifInfo?.lensModel,
                      // fNumber: asset.exifInfo?.fNumber,
                      // fps: asset.exifInfo?.fps,
                      // iso: asset.exifInfo?.iso,
                    }),
                  );
                }),
            } satisfies ExecuteOptions<typeof type>;
          }
        }
      }),
    );
  }

  private execute<T extends WorkflowType>(workflowId: string, getHandler: (type: T) => ExecuteOptions<T> | undefined) {
    return Effect.gen({ self: this }, function* () {
      const workflow = yield* this.fx.workflow.getForWorkflowRun(workflowId);
      if (!workflow) {
        return;
      }

      // TODO infer from steps
      let type: T | undefined;
      for (const targetType of Object.values(WorkflowType)) {
        const isMissing = workflow.steps.some((step) => !step.types.includes(targetType));
        if (!isMissing) {
          type = targetType as unknown as T;
          break;
        }
      }

      if (!type) {
        return yield* Effect.fail(new Error('Unable to infer workflow event type from steps'));
      }

      const handler = getHandler(type);
      if (!handler) {
        this.logger.error(`Misconfigured workflow ${workflowId}: no handler for type ${type}`);
        return;
      }

      const { read, write } = handler;
      const readResult = yield* read(type);
      let data = readResult.data;
      const runId = crypto.randomUUID();

      for (const step of workflow.steps) {
        // 'next' and 'skip' continue with the next step, 'stop' ends the run, 'failed' ends it with a failed job
        const outcome = yield* Effect.gen({ self: this }, function* () {
          const payload: WorkflowEventPayload<typeof type> = {
            trigger: workflow.trigger,
            type,
            config: step.config ?? {},
            workflow: {
              id: workflowId,
              authToken: this.sign(readResult.authUserId),
              stepId: step.id,
            },
            data,
          };

          const context: HostContext = {
            allowedHosts: step.allowedHosts,
          };

          if (step.methodName.startsWith('noop')) {
            return 'skip' as const;
          }

          const result = yield* this.fx.plugin.callMethod<WorkflowResponse<T>>(
            {
              pluginKey: this.getPluginKey({ id: step.pluginId, hostFunctions: step.hostFunctions }),
              methodName: step.methodName,
            },
            payload,
            context,
          );
          if (result?.changes) {
            yield* write(
              {
                user: {
                  id: readResult.authUserId,
                },
                session: {
                  id: DummyValue.UUID,
                  hasElevatedPermission: true,
                },
              } as AuthDto,
              result.changes,
            );
            ({ data } = yield* read(type));
          }

          if (result?.config) {
            yield* this.fx.workflow.updateStep(step.id, { config: result.config });
          }

          const shouldContinue = result?.workflow?.continue ?? true;
          if (!shouldContinue) {
            if (workflow.logging) {
              yield* this.fx.workflow.log({
                workflowId,
                result: WorkflowResult.Halted,
                workflowStepId: step.id,
                triggerDataId: readResult.entityId,
                runId,
              });
            }

            this.logger.debug(`Workflow ${workflowId} run ${runId} stopped on step ${step.id}`);
            return 'stop' as const;
          }

          return 'next' as const;
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen({ self: this }, function* () {
              this.logger.error(`Error executing workflow ${workflowId} run ${runId}:`, unwrap(Cause.squash(cause)));

              if (workflow.logging) {
                yield* this.fx.workflow.log({
                  workflowId,
                  result: WorkflowResult.Error,
                  workflowStepId: step.id,
                  triggerDataId: readResult.entityId,
                  runId,
                });
              }

              return 'failed' as const;
            }),
          ),
        );

        if (outcome === 'stop') {
          return;
        }

        if (outcome === 'failed') {
          return JobStatus.Failed;
        }
      }

      if (workflow.logging) {
        yield* this.fx.workflow.log({
          workflowId,
          result: WorkflowResult.Completed,
          triggerDataId: readResult.entityId,
          runId,
        });
      }

      this.logger.debug(`Workflow ${workflowId} run ${runId} executed successfully`);
    });
  }
}
