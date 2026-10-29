import { BadRequestException, Injectable } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import { Insertable } from 'kysely';
import { R_OK } from 'node:constants';
import { Stats } from 'node:fs';
import path, { isAbsolute, parse } from 'node:path';
import picomatch from 'picomatch';
import type { ExternalFileSignature } from 'src/repositories/asset.repository.js';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { JobOf } from 'src/types.js';
import { JOBS_LIBRARY_PAGINATION_SIZE } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import {
  CreateLibraryDto,
  LibraryResponseDto,
  LibraryStatsResponseDto,
  UpdateLibraryDto,
  ValidateLibraryDto,
  ValidateLibraryImportPathResponseDto,
  ValidateLibraryResponseDto,
  mapLibrary,
} from 'src/dtos/library.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  AssetStatus,
  AssetType,
  ChecksumAlgorithm,
  CronJob,
  DatabaseLock,
  ImmichWorker,
  JobName,
  JobStatus,
  QueueName,
} from 'src/enum.js';
import { AssetSyncResult } from 'src/repositories/library.repository.js';
import { AssetTable } from 'src/schema/tables/asset.table.js';
import { BaseService } from 'src/services/base.service.js';
import { mimeTypes } from 'src/utils/mime-types.js';
import { batched, handlePromiseError } from 'src/utils/misc.js';

/** The original thrown value behind an Effect error that wraps it in `cause` (see `src/effect/errors.ts`). */
/**
 * Sequentially runs `body` for every item of an async iterable, like `for await` with an awaiting body.
 * The iterator is closed when the body fails, matching what `for await` does on an abrupt exit.
 */
const forEachAwait = <T, E, R>(iterable: AsyncIterable<T>, body: (item: T) => Effect.Effect<void, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => iterable[Symbol.asyncIterator]()),
    (iterator) =>
      Effect.gen(function* () {
        while (true) {
          const result = yield* Effect.tryPromise({ try: () => iterator.next(), catch: (cause) => cause });
          if (result.done) {
            return;
          }
          yield* body(result.value);
        }
      }),
    (iterator) =>
      Effect.promise(async () => {
        try {
          await iterator.return?.();
        } catch {
          // ignore errors while closing, the original error wins
        }
      }),
  );

const signatureKey = ({ fileModifiedAt, size, type }: ExternalFileSignature) =>
  `${fileModifiedAt.valueOf()}:${size}:${type}`;

/** How many file signatures one relink candidate query matches at once. */
const RELINK_QUERY_SIZE = 500;

type ExternalFileImport = { asset: Insertable<AssetTable>; signature: ExternalFileSignature };

type RelinkCandidate = {
  id: string;
  originalPath: string;
  fileModifiedAt: Date;
  size: number;
  type: AssetType;
  status: AssetStatus;
};

@Injectable()
export class LibraryService extends BaseService {
  private watchLibraries = false;
  private lock = false;
  private watchers: Record<string, () => Promise<void>> = {};

  @OnEvent({ name: 'ConfigInit', workers: [ImmichWorker.Microservices] })
  async onConfigInit({
    newConfig: {
      library: { watch, scan },
    },
  }: ArgOf<'ConfigInit'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        // This ensures that library watching only occurs in one microservice
        this.lock = yield* this.fx.database.tryLock(DatabaseLock.Library);

        this.watchLibraries = this.lock && watch.enabled;

        if (this.lock) {
          yield* this.fx.cron.create({
            name: CronJob.LibraryScan,
            expression: scan.cronExpression,
            onTick: () =>
              handlePromiseError(runPromise(this.fx.job.queue({ name: JobName.LibraryScanQueueAll })), this.logger),
            start: scan.enabled,
          });
        }

        if (this.watchLibraries) {
          yield* this.watchAllFx();
        }
      }),
    );
  }

  @OnEvent({ name: 'ConfigUpdate', server: true })
  async onConfigUpdate({ newConfig: { library } }: ArgOf<'ConfigUpdate'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!this.lock) {
          return;
        }

        yield* this.fx.cron.update({
          name: CronJob.LibraryScan,
          expression: library.scan.cronExpression,
          start: library.scan.enabled,
        });

        if (library.watch.enabled === this.watchLibraries) {
          return;
        }

        // Watch configuration changed, update accordingly
        this.watchLibraries = library.watch.enabled;
        yield* this.watchLibraries
          ? this.watchAllFx()
          : Effect.tryPromise({ try: () => this.unwatchAll(), catch: (cause) => cause });
      }),
    );
  }

  private readonly watch = Effect.fn('LibraryService.watch')(function* (this: LibraryService, id: string) {
    if (!this.watchLibraries) {
      return false;
    }

    const library = yield* this.findOrFail(id);
    if (library.importPaths.length === 0) {
      return false;
    }

    yield* this.tryUnwatch(id);

    this.logger.log(`Starting to watch library ${library.id} with import path(s) ${library.importPaths}`);

    const matcher = picomatch(`**/*{${mimeTypes.getSupportedFileExtensions().join(',')}}`, {
      nocase: true,
      ignore: library.exclusionPatterns,
    });

    let _resolve: () => void;
    const ready$ = new Promise<void>((resolve) => (_resolve = resolve));

    const handler = async (event: string, path: string) => {
      if (matcher(path)) {
        this.logger.debug(`File ${event} event received for ${path} in library ${library.id}`);
        await runPromise(
          this.fx.job.queue({
            name: JobName.LibrarySyncFiles,
            data: { libraryId: library.id, paths: [path] },
          }),
        );
      } else {
        this.logger.verbose(`Ignoring file ${event} event for ${path} in library ${library.id}`);
      }
    };

    const deletionHandler = async (path: string) => {
      this.logger.debug(`File unlink event received for ${path} in library ${library.id}`);
      await runPromise(
        this.fx.job.queue({
          name: JobName.LibraryRemoveAsset,
          data: { libraryId: library.id, paths: [path] },
        }),
      );
    };

    this.watchers[id] = yield* this.fx.storage.watch(
      library.importPaths,
      {
        usePolling: false,
        ignoreInitial: true,
        ignored: library.exclusionPatterns,
        awaitWriteFinish: {
          stabilityThreshold: 5000,
          pollInterval: 1000,
        },
      },
      {
        onReady: () => _resolve(),
        onAdd: (path) => {
          return handlePromiseError(handler('add', path), this.logger);
        },
        onChange: (path) => {
          return handlePromiseError(handler('change', path), this.logger);
        },
        onUnlink: (path) => {
          return handlePromiseError(deletionHandler(path), this.logger);
        },
        onError: (error) => {
          this.logger.error(`Library watcher for library ${library.id} encountered error: ${error}`);
        },
      },
    );

    // Wait for the watcher to initialize before returning
    yield* Effect.promise(() => ready$);

    return true;
  });

  private tryUnwatch(id: string) {
    return Effect.tryPromise({ try: () => this.unwatch(id), catch: (cause) => cause });
  }

  async unwatch(id: string) {
    if (!Object.hasOwn(this.watchers, id)) {
      return;
    }

    await this.watchers[id]();
    delete this.watchers[id];
  }

  @OnEvent({ name: 'AppShutdown' })
  async onShutdown() {
    await this.unwatchAll();
  }

  private async unwatchAll() {
    if (!this.lock) {
      return false;
    }

    for (const id in this.watchers) {
      await this.unwatch(id);
    }
  }

  async watchAll() {
    return runPromise(this.watchAllFx());
  }

  private watchAllFx() {
    return Effect.gen({ self: this }, function* () {
      if (!this.lock) {
        return false;
      }

      const libraries = yield* this.fx.library.getAll(false);
      for (const library of libraries) {
        yield* this.watch(library.id);
      }
    });
  }

  async getStatistics(id: string): Promise<LibraryStatsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const statistics = yield* this.fx.library.getStatistics(id);
        if (!statistics) {
          return yield* Effect.fail(new BadRequestException(`Library ${id} not found`));
        }
        return statistics;
      }),
    );
  }

  async get(id: string): Promise<LibraryResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const library = yield* this.findOrFail(id);
        return mapLibrary(library);
      }),
    );
  }

  async getAll(): Promise<LibraryResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const libraries = yield* this.fx.library.getAll(false);
        return libraries.map((library) => mapLibrary(library));
      }),
    );
  }

  @OnJob({ name: JobName.LibraryDeleteCheck, queue: QueueName.Library })
  async handleQueueCleanup(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log('Checking for any libraries pending deletion...');
        const pendingDeletions = yield* this.fx.library.getAllDeleted();
        if (pendingDeletions.length > 0) {
          const libraryString = pendingDeletions.length === 1 ? 'library' : 'libraries';
          this.logger.log(`Found ${pendingDeletions.length} ${libraryString} pending deletion, cleaning up...`);

          yield* this.fx.job.queueAll(
            pendingDeletions.map((libraryToDelete) => ({
              name: JobName.LibraryDelete,
              data: { id: libraryToDelete.id },
            })),
          );
        }

        return JobStatus.Success;
      }),
    );
  }

  async create(dto: CreateLibraryDto): Promise<LibraryResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const library = yield* this.fx.library.create({
          ownerId: dto.ownerId,
          name: dto.name ?? 'New External Library',
          importPaths: dto.importPaths ?? [],
          exclusionPatterns: dto.exclusionPatterns ?? [
            '**/@eaDir/**',
            '**/._*',
            '**/#recycle/**',
            '**/#snapshot/**',
            '**/.stversions/**',
            '**/.stfolder/**',
          ],
        });
        return mapLibrary(library);
      }),
    );
  }

  @OnJob({ name: JobName.LibrarySyncFiles, queue: QueueName.Library })
  async handleSyncFiles(job: JobOf<JobName.LibrarySyncFiles>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const library = yield* this.fx.library.get(job.libraryId);
        // We need to check if the library still exists as it could have been deleted after the scan was queued
        if (!library) {
          this.logger.debug(`Library ${job.libraryId} not found, skipping file import`);
          return JobStatus.Failed;
        }
        if (library.deletedAt) {
          this.logger.debug(`Library ${job.libraryId} is deleted, won't import assets into it`);
          return JobStatus.Failed;
        }

        const fileImports: ExternalFileImport[] = [];
        yield* Effect.all(
          job.paths.map((path) =>
            this.processEntity(path, library.ownerId, job.libraryId).pipe(
              Effect.map((fileImport) => {
                fileImports.push(fileImport);
              }),
              Effect.catchCause((cause) => {
                const error = unwrap(Cause.squash(cause));
                this.logger.error(`Error processing ${path} for library ${job.libraryId}: ${error}`);
                return Effect.void;
              }),
            ),
          ),
          { concurrency: 'unbounded' },
        );

        // A file that was moved or renamed is the same asset at a new path, so keep it instead of importing a copy
        const { relinkedIds, remaining } = yield* this.relinkMovedAssets(job.libraryId, fileImports);

        const assetIds = yield* this.fx.asset.createAll(remaining.map(({ asset }) => asset));

        const progressMessage =
          job.progressCounter && job.totalAssets
            ? `(${job.progressCounter} of ${job.totalAssets})`
            : `(${job.progressCounter} done so far)`;

        this.logger.log(`Imported ${assetIds.length} ${progressMessage} file(s) into library ${job.libraryId}`);

        yield* Effect.all(
          assetIds.map((assetId) =>
            this.fx.event.emit('AssetCreate', { asset: { id: assetId, ownerId: library.ownerId } }),
          ),
          { concurrency: 'unbounded' },
        );

        yield* this.queuePostSyncJobsFx([...relinkedIds, ...assetIds]);

        return JobStatus.Success;
      }),
    );
  }

  /**
   * External assets are identified by the checksum of their path, so a file that moved or was renamed shows up as a
   * new path next to an asset whose file is gone. When exactly one asset that is missing from disk has the size and
   * modification time of exactly one new file, that asset is pointed at the new path (keeping its albums, tags and
   * edits). Anything ambiguous is left alone and imported as a new asset.
   */
  private readonly relinkMovedAssets = Effect.fn('LibraryService.relinkMovedAssets')(function* (
    this: LibraryService,
    libraryId: string,
    imports: ExternalFileImport[],
  ) {
    const importsBySignature = new Map<string, ExternalFileImport[]>();
    for (const fileImport of imports) {
      const key = signatureKey(fileImport.signature);
      importsBySignature.set(key, [...(importsBySignature.get(key) ?? []), fileImport]);
    }

    const unambiguous = importsBySignature
      .values()
      .filter((group) => group.length === 1)
      .map(([item]) => item)
      .toArray();
    if (unambiguous.length === 0) {
      return { relinkedIds: [] as string[], remaining: imports };
    }

    const candidates: RelinkCandidate[] = [];
    const signatures = unambiguous.map(({ signature }) => signature);
    for (let i = 0; i < signatures.length; i += RELINK_QUERY_SIZE) {
      const chunk = signatures.slice(i, i + RELINK_QUERY_SIZE);
      candidates.push(...(yield* this.fx.asset.findExternalRelinkCandidates(libraryId, chunk)));
    }
    if (candidates.length === 0) {
      return { relinkedIds: [] as string[], remaining: imports };
    }

    // Only an asset whose file is gone can have moved
    const stats = yield* Effect.all(
      candidates.map((candidate) =>
        this.fx.storage.stat(candidate.originalPath).pipe(Effect.catch(() => Effect.succeed<Stats | null>(null))),
      ),
      { concurrency: 'unbounded' },
    );
    const candidatesBySignature = new Map<string, RelinkCandidate[]>();
    for (const [i, candidate] of candidates.entries()) {
      if (stats[i]) {
        continue;
      }
      const key = signatureKey({
        fileModifiedAt: candidate.fileModifiedAt,
        size: candidate.size,
        type: candidate.type,
      });
      candidatesBySignature.set(key, [...(candidatesBySignature.get(key) ?? []), candidate]);
    }

    const relinkedIds: string[] = [];
    const relinked = new Set<ExternalFileImport>();
    for (const fileImport of unambiguous) {
      const matches = candidatesBySignature.get(signatureKey(fileImport.signature));
      if (matches?.length !== 1) {
        continue;
      }

      const [candidate] = matches;
      const { asset } = fileImport;
      const result = yield* this.fx.asset
        .update({
          id: candidate.id,
          originalPath: asset.originalPath,
          originalFileName: asset.originalFileName,
          checksum: asset.checksum,
          isOffline: false,
          // a file that was trashed by the user stays in the trash, one that was only offlined comes back
          deletedAt: candidate.status === AssetStatus.Trashed ? undefined : null,
        })
        .pipe(
          Effect.map(() => true),
          Effect.catchCause((cause) => {
            this.logger.warn(
              `Could not relink asset ${candidate.id} from ${candidate.originalPath} to ${asset.originalPath}: ${unwrap(Cause.squash(cause))}`,
            );
            return Effect.succeed(false);
          }),
        );
      if (!result) {
        continue;
      }

      this.logger.log(`Asset ${candidate.id} moved from ${candidate.originalPath} to ${asset.originalPath}`);
      relinkedIds.push(candidate.id);
      relinked.add(fileImport);
    }

    return { relinkedIds, remaining: imports.filter((fileImport) => !relinked.has(fileImport)) };
  });

  private readonly validateImportPath = Effect.fn('LibraryService.validateImportPath')(function* (
    this: LibraryService,
    importPath: string,
  ) {
    const validation = new ValidateLibraryImportPathResponseDto();
    validation.importPath = importPath;
    validation.isValid = false;

    if (StorageCore.isImmichPath(importPath)) {
      validation.message = 'Cannot use media upload folder for external libraries';
      return validation;
    }

    if (!isAbsolute(importPath)) {
      validation.message = `Import path must be absolute, try ${path.resolve(importPath)}`;
      return validation;
    }

    const statFailure = yield* Effect.gen({ self: this }, function* () {
      const stat = yield* this.fx.storage.stat(importPath);
      if (stat.isDirectory()) {
        return;
      }

      validation.message = 'Not a directory';
      return validation;
    }).pipe(
      Effect.catchCause((cause) => {
        const error: any = unwrap(Cause.squash(cause));
        if (error.code === 'ENOENT') {
          validation.message = 'Path does not exist (ENOENT)';
          return Effect.succeed(validation);
        }
        validation.message = String(error);
        return Effect.succeed(validation);
      }),
    );
    if (statFailure) {
      return statFailure;
    }

    const isAccess = yield* this.fx.storage.checkFileExists(importPath, R_OK);

    if (!isAccess) {
      validation.message = 'Lacking read permission for folder';
      return validation;
    }

    validation.isValid = true;
    return validation;
  });

  async validate(id: string, dto: ValidateLibraryDto): Promise<ValidateLibraryResponseDto> {
    return runPromise(this.validateFx(id, dto));
  }

  private readonly validateFx = Effect.fn('LibraryService.validate')(function* (
    this: LibraryService,
    id: string,
    dto: ValidateLibraryDto,
  ) {
    const importPaths = yield* Effect.all(
      (dto.importPaths || []).map((importPath) => this.validateImportPath(importPath)),
      { concurrency: 'unbounded' },
    );
    return { importPaths };
  });

  async update(id: string, dto: UpdateLibraryDto): Promise<LibraryResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id);

        if (dto.importPaths) {
          const validation = yield* this.validateFx(id, { importPaths: dto.importPaths });
          if (validation.importPaths) {
            for (const path of validation.importPaths) {
              if (!path.isValid) {
                return yield* Effect.fail(new BadRequestException(`Invalid import path: ${path.message}`));
              }
            }
          }
        }

        const library = yield* this.fx.library.update(id, dto);
        return mapLibrary(library);
      }),
    );
  }

  async delete(id: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id);

        if (this.watchLibraries) {
          yield* this.tryUnwatch(id);
        }

        yield* this.fx.library.softDelete(id);
        yield* this.fx.job.queue({ name: JobName.LibraryDelete, data: { id } });
      }),
    );
  }

  @OnJob({ name: JobName.LibraryDelete, queue: QueueName.Library })
  async handleDeleteLibrary(job: JobOf<JobName.LibraryDelete>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const libraryId = job.id;

        yield* this.fx.asset.updateByLibraryId(libraryId, { deletedAt: new Date() });

        this.logger.debug(`Will delete all assets in library ${libraryId}`);
        let hasAssets = false;
        const assetIds = yield* this.fx.library.streamAssetIds(libraryId);
        yield* forEachAwait(batched(assetIds, JOBS_LIBRARY_PAGINATION_SIZE), (assets) =>
          Effect.gen({ self: this }, function* () {
            this.logger.debug(`Queueing deletion of ${assets.length} asset(s) in library ${libraryId}`);
            yield* this.fx.job.queueAll(
              assets.map((asset) => ({ name: JobName.AssetDelete, data: { id: asset.id, deleteOnDisk: false } })),
            );
            hasAssets = true;
          }),
        );

        if (!hasAssets) {
          this.logger.log(`Deleting library ${libraryId}`);
          yield* this.fx.library.delete(libraryId);
        }

        return JobStatus.Success;
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly processEntity = Effect.fn('LibraryService.processEntity')(function* (
    this: LibraryService,
    filePath: string,
    ownerId: string,
    libraryId: string,
  ) {
    const assetPath = path.normalize(filePath);
    const stat = yield* this.fx.storage.stat(assetPath);

    const type = mimeTypes.isVideo(assetPath) ? AssetType.Video : AssetType.Image;

    return {
      asset: {
        ownerId,
        libraryId,
        checksum: yield* this.fx.crypto.hashSha1(`path:${assetPath}`),
        checksumAlgorithm: ChecksumAlgorithm.sha1Path,
        originalPath: assetPath,

        fileCreatedAt: stat.mtime,
        fileModifiedAt: stat.mtime,
        localDateTime: stat.mtime,
        type,
        originalFileName: parse(assetPath).base,
        isExternal: true,
        livePhotoVideoId: null,
      },
      signature: { fileModifiedAt: stat.mtime, size: stat.size, type },
    } satisfies ExternalFileImport;
  });

  async queuePostSyncJobs(assetIds: string[]) {
    return runPromise(this.queuePostSyncJobsFx(assetIds));
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly queuePostSyncJobsFx = Effect.fn('LibraryService.queuePostSyncJobs')(function* (
    this: LibraryService,
    assetIds: string[],
  ) {
    this.logger.debug(`Queuing sidecar discovery for ${assetIds.length} asset(s)`);

    // We queue a sidecar discovery which, in turn, queues metadata extraction
    yield* this.fx.job.queueAll(
      assetIds.map((assetId) => ({
        name: JobName.SidecarCheck,
        data: { id: assetId, source: 'upload' },
      })),
    );
  });

  async queueScan(id: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id);

        this.logger.log(`Starting to scan library ${id}`);

        yield* this.fx.job.queue({
          name: JobName.LibrarySyncFilesQueueAll,
          data: {
            id,
          },
        });

        yield* this.fx.job.queue({ name: JobName.LibrarySyncAssetsQueueAll, data: { id } });
      }),
    );
  }

  @OnJob({ name: JobName.LibraryScanQueueAll, queue: QueueName.Library })
  async handleQueueScanAll(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Initiating scan of all external libraries...`);

        yield* this.fx.job.queue({ name: JobName.LibraryDeleteCheck, data: {} });

        const libraries = yield* this.fx.library.getAll(true);

        yield* this.fx.job.queueAll(
          libraries.map((library) => ({
            name: JobName.LibrarySyncFilesQueueAll,
            data: {
              id: library.id,
            },
          })),
        );
        yield* this.fx.job.queueAll(
          libraries.map((library) => ({
            name: JobName.LibrarySyncAssetsQueueAll,
            data: {
              id: library.id,
            },
          })),
        );

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.LibrarySyncAssets, queue: QueueName.Library })
  async handleSyncAssets(job: JobOf<JobName.LibrarySyncAssets>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const assets = yield* this.fx.assetJob.getForSyncAssets(job.assetIds);

        const assetIdsToOffline: string[] = [];
        const trashedAssetIdsToOffline: string[] = [];
        const assetIdsToOnline: string[] = [];
        const trashedAssetIdsToOnline: string[] = [];
        const assetIdsToUpdate: string[] = [];

        this.logger.debug(`Checking batch of ${assets.length} existing asset(s) in library ${job.libraryId}`);

        const stats = yield* Effect.all(
          assets.map((asset) =>
            this.fx.storage.stat(asset.originalPath).pipe(Effect.catch(() => Effect.succeed<Stats | null>(null))),
          ),
          { concurrency: 'unbounded' },
        );

        for (let i = 0; i < assets.length; i++) {
          const asset = assets[i];
          const stat = stats[i];
          const action = this.checkExistingAsset(asset, stat);
          switch (action) {
            case AssetSyncResult.DO_NOTHING: {
              break;
            }
            case AssetSyncResult.OFFLINE: {
              if (asset.status === AssetStatus.Trashed) {
                trashedAssetIdsToOffline.push(asset.id);
              } else {
                assetIdsToOffline.push(asset.id);
              }
              break;
            }
            case AssetSyncResult.UPDATE: {
              assetIdsToUpdate.push(asset.id);
              break;
            }
            case AssetSyncResult.CHECK_OFFLINE: {
              const isInImportPath = job.importPaths.some((path) => asset.originalPath.startsWith(path));

              if (!isInImportPath) {
                this.logger.verbose(
                  `Offline asset ${asset.originalPath} is still not in any import path, keeping offline in library ${job.libraryId}`,
                );
                break;
              }

              const isExcluded = job.exclusionPatterns.some((pattern) =>
                picomatch.isMatch(asset.originalPath, pattern, { nocase: true }),
              );

              if (!isExcluded) {
                this.logger.debug(`Offline asset ${asset.originalPath} is now online in library ${job.libraryId}`);
                if (asset.status === AssetStatus.Trashed) {
                  trashedAssetIdsToOnline.push(asset.id);
                } else {
                  assetIdsToOnline.push(asset.id);
                }
                break;
              }

              this.logger.verbose(
                `Offline asset ${asset.originalPath} is in an import path but still covered by exclusion pattern, keeping offline in library ${job.libraryId}`,
              );

              break;
            }
          }
        }

        const promises: Effect.Effect<unknown, unknown>[] = [];
        if (assetIdsToOffline.length > 0) {
          promises.push(this.fx.asset.updateAll(assetIdsToOffline, { isOffline: true, deletedAt: new Date() }));
        }

        if (trashedAssetIdsToOffline.length > 0) {
          promises.push(this.fx.asset.updateAll(trashedAssetIdsToOffline, { isOffline: true }));
        }

        if (assetIdsToOnline.length > 0) {
          promises.push(this.fx.asset.updateAll(assetIdsToOnline, { isOffline: false, deletedAt: null }));
        }

        if (trashedAssetIdsToOnline.length > 0) {
          promises.push(this.fx.asset.updateAll(trashedAssetIdsToOnline, { isOffline: false }));
        }

        if (assetIdsToUpdate.length > 0) {
          promises.push(this.queuePostSyncJobsFx(assetIdsToUpdate));
        }

        yield* Effect.all(promises, { concurrency: 'unbounded' });

        const remainingCount =
          assets.length - assetIdsToOffline.length - assetIdsToUpdate.length - assetIdsToOnline.length;
        const cumulativePercentage = ((100 * job.progressCounter) / job.totalAssets).toFixed(1);
        this.logger.log(
          `Checked existing asset(s): ${assetIdsToOffline.length + trashedAssetIdsToOffline.length} offlined, ${assetIdsToOnline.length + trashedAssetIdsToOnline.length} onlined, ${assetIdsToUpdate.length} updated, ${remainingCount} unchanged of current batch of ${assets.length} (Total progress: ${job.progressCounter} of ${job.totalAssets}, ${cumulativePercentage} %) in library ${job.libraryId}.`,
        );

        return JobStatus.Success;
      }),
    );
  }

  private checkExistingAsset(
    asset: {
      isOffline: boolean;
      libraryId: string | null;
      originalPath: string;
      status: AssetStatus;
      fileModifiedAt: Date;
    },
    stat: Stats | null,
  ): AssetSyncResult {
    if (!stat) {
      // File not found on disk or permission error
      if (asset.isOffline) {
        this.logger.verbose(
          `Asset ${asset.originalPath} is still not accessible, keeping offline in library ${asset.libraryId}`,
        );
        return AssetSyncResult.DO_NOTHING;
      }

      this.logger.debug(
        `Asset ${asset.originalPath} is no longer on disk or is inaccessible because of permissions, marking offline in library ${asset.libraryId}`,
      );
      return AssetSyncResult.OFFLINE;
    }

    if (asset.isOffline && asset.status !== AssetStatus.Deleted) {
      // Only perform the expensive check if the asset is offline
      return AssetSyncResult.CHECK_OFFLINE;
    }

    if (stat.mtime.valueOf() !== asset.fileModifiedAt.valueOf()) {
      this.logger.verbose(`Asset ${asset.originalPath} needs metadata extraction in library ${asset.libraryId}`);

      return AssetSyncResult.UPDATE;
    }

    return AssetSyncResult.DO_NOTHING;
  }

  @OnJob({ name: JobName.LibrarySyncFilesQueueAll, queue: QueueName.Library })
  async handleQueueSyncFiles(job: JobOf<JobName.LibrarySyncFilesQueueAll>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const library = yield* this.fx.library.get(job.id);
        if (!library) {
          this.logger.debug(`Library ${job.id} not found, skipping refresh`);
          return JobStatus.Skipped;
        }

        this.logger.debug(`Validating import paths for library ${library.id}...`);

        const validImportPaths: string[] = [];

        for (const importPath of library.importPaths) {
          const validation = yield* this.validateImportPath(importPath);
          if (validation.isValid) {
            validImportPaths.push(path.normalize(importPath));
          } else {
            this.logger.warn(`Skipping invalid import path: ${importPath}. Reason: ${validation.message}`);
          }
        }

        if (validImportPaths.length === 0) {
          this.logger.warn(`No valid import paths found for library ${library.id}`);

          return JobStatus.Skipped;
        }

        const pathsOnDisk = yield* this.fx.storage.walk({
          pathsToCrawl: validImportPaths,
          includeHidden: false,
          exclusionPatterns: library.exclusionPatterns,
          take: JOBS_LIBRARY_PAGINATION_SIZE,
        });

        let importCount = 0;
        let crawlCount = 0;

        this.logger.log(
          `Starting disk crawl of ${validImportPaths.length} import path(s) for library ${library.id}...`,
        );

        yield* forEachAwait(pathsOnDisk, (pathBatch) =>
          Effect.gen({ self: this }, function* () {
            crawlCount += pathBatch.length;
            const paths = yield* this.fx.asset.filterNewExternalAssetPaths(library.id, pathBatch);

            if (paths.length > 0) {
              importCount += paths.length;

              yield* this.fx.job.queue({
                name: JobName.LibrarySyncFiles,
                data: {
                  libraryId: library.id,
                  paths,
                  progressCounter: crawlCount,
                },
              });
            }

            this.logger.log(
              `Crawled ${crawlCount} file(s) so far: ${paths.length} of current batch of ${pathBatch.length} will be imported to library ${library.id}...`,
            );
          }),
        );

        this.logger.log(
          `Finished disk crawl, ${crawlCount} file(s) found on disk and queued ${importCount} file(s) for import into ${library.id}`,
        );

        yield* this.fx.library.update(job.id, { refreshedAt: new Date() });

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.LibraryRemoveAsset, queue: QueueName.Library })
  async handleAssetRemoval(job: JobOf<JobName.LibraryRemoveAsset>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        // This is only for handling file unlink events via the file watcher
        this.logger.verbose(`Deleting asset(s) ${job.paths} from library ${job.libraryId}`);
        for (const assetPath of job.paths) {
          const asset = yield* this.fx.asset.getByLibraryIdAndOriginalPath(job.libraryId, assetPath);
          if (asset) {
            yield* this.fx.asset.remove(asset);
          }
        }

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.LibrarySyncAssetsQueueAll, queue: QueueName.Library })
  async handleQueueSyncAssets(job: JobOf<JobName.LibrarySyncAssetsQueueAll>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const library = yield* this.fx.library.get(job.id);
        if (!library) {
          return JobStatus.Skipped;
        }

        const assetCount = yield* this.fx.asset.getLibraryAssetCount(job.id);
        if (!assetCount) {
          this.logger.log(`Library ${library.id} is empty, no need to check assets`);
          return JobStatus.Success;
        }

        this.logger.log(
          `Checking ${assetCount} asset(s) against import paths and exclusion patterns in library ${library.id}...`,
        );

        const offlineResult = yield* this.fx.asset.detectOfflineExternalAssets(
          library.id,
          library.importPaths,
          library.exclusionPatterns,
        );

        const affectedAssetCount = Number(offlineResult.numUpdatedRows);

        this.logger.log(
          `${affectedAssetCount} asset(s) out of ${assetCount} were offlined due to import paths and/or exclusion pattern(s) in library ${library.id}`,
        );

        if (affectedAssetCount === assetCount) {
          return JobStatus.Success;
        }

        this.logger.log(`Scanning library ${library.id} for assets missing from disk...`);

        let count = 0;
        const existingAssets = yield* this.fx.library.streamAssetIds(library.id);
        yield* forEachAwait(batched(existingAssets, JOBS_LIBRARY_PAGINATION_SIZE), (assets) =>
          Effect.gen({ self: this }, function* () {
            count += assets.length;

            yield* this.fx.job.queue({
              name: JobName.LibrarySyncAssets,
              data: {
                libraryId: library.id,
                importPaths: library.importPaths,
                exclusionPatterns: library.exclusionPatterns,
                assetIds: assets.map(({ id }) => id),
                progressCounter: count,
                totalAssets: assetCount,
              },
            });

            const completePercentage = ((100 * count) / assetCount).toFixed(1);

            this.logger.log(
              `Queued check of ${count} of ${assetCount} (${completePercentage} %) existing asset(s) so far in library ${library.id}`,
            );
          }),
        );

        this.logger.log(`Finished queuing ${count} asset check(s) for library ${library.id}`);

        return JobStatus.Success;
      }),
    );
  }

  private readonly findOrFail = Effect.fn('LibraryService.findOrFail')(function* (this: LibraryService, id: string) {
    const library = yield* this.fx.library.get(id);
    if (!library) {
      return yield* Effect.fail(new BadRequestException('Library not found'));
    }

    return library;
  });
}
