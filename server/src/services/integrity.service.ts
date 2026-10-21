import { Injectable } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import { createHash } from 'node:crypto';
import { basename } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type {
  IIntegrityDeleteReportTypeJob,
  IIntegrityDeleteReportsJob,
  IIntegrityJob,
  IIntegrityMissingFilesJob,
  IIntegrityPathWithChecksumJob,
  IIntegrityPathWithReportJob,
  IIntegrityUntrackedFilesJob,
} from 'src/types.js';
import { JOBS_LIBRARY_PAGINATION_SIZE } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import {
  IntegrityGetReportDto,
  IntegrityReportResponseDto,
  IntegrityReportSummaryResponseDto,
} from 'src/dtos/integrity.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  AssetStatus,
  CacheControl,
  DatabaseLock,
  ImmichWorker,
  IntegrityReport,
  JobName,
  JobStatus,
  QueueName,
  StorageFolder,
  SystemMetadataKey,
} from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { ImmichFileResponse } from 'src/utils/file.js';
import { batched, handlePromiseError } from 'src/utils/misc.js';

/**
 * Untracked Files:
 *   Files are detected in /data/encoded-video, /data/library, /data/upload
 *     Checked against the asset table
 *   Files are detected in /data/thumbs
 *     Checked against the asset_file table
 *
 *   * Can perform download or delete of files
 *
 * Missing Files:
 *   Paths are queried from asset(originalPath, encodedVideoPath), asset_file(path)
 *     Check whether files exist on disk
 *
 *   * Reports must include origin (asset or asset_file) & ID for further action
 *   * Can perform trash (asset) or delete (asset_file)
 *
 * Checksum Mismatch:
 *   Paths & checksums are queried from asset(originalPath, checksum)
 *     Check whether files match checksum, missing files ignored
 *
 *   * Reports must include origin (as above) for further action
 *   * Can perform download or trash (asset)
 */

/** Runs a Promise-based call (stream pipelines, iterators); the error stays the original thrown value. */
const attempt = <A>(run: () => A | PromiseLike<A>) =>
  Effect.tryPromise({ try: async () => await run(), catch: (cause) => cause });

/** Effect errors wrap the original thrown value in `cause`; error code checks inspect the original. */
/**
 * Mirrors `for await (const item of items)`: the iterator is closed when the loop exits early (the handler
 * returns `true`, like `break`) or fails.
 */
const forEachAsync = <T, E>(items: AsyncIterable<T>, handle: (item: T) => Effect.Effect<boolean | void, E>) => {
  const iterator = items[Symbol.asyncIterator]();
  return Effect.ensuring(
    Effect.gen(function* () {
      while (true) {
        const next = yield* attempt(() => iterator.next());
        if (next.done) {
          break;
        }

        if ((yield* handle(next.value)) === true) {
          break;
        }
      }
    }),
    Effect.promise(async () => {
      try {
        await iterator.return?.();
      } catch {
        // ignored, like the implicit `return()` of `for await`
      }
    }),
  );
};

@Injectable()
export class IntegrityService extends BaseService {
  private integrityLock = false;

  @OnEvent({ name: 'ConfigInit', workers: [ImmichWorker.Microservices] })
  async onConfigInit({
    newConfig: {
      integrityChecks: { untrackedFiles, missingFiles, checksumFiles },
    },
  }: ArgOf<'ConfigInit'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.integrityLock = yield* this.fx.database.tryLock(DatabaseLock.IntegrityCheck);
        if (!this.integrityLock) {
          return;
        }

        yield* this.fx.cron.create({
          name: 'integrityUntrackedFiles',
          expression: untrackedFiles.cronExpression,
          onTick: () =>
            handlePromiseError(
              this.jobRepository.queue({ name: JobName.IntegrityUntrackedFilesQueueAll, data: {} }),
              this.logger,
            ),
          start: untrackedFiles.enabled,
        });

        yield* this.fx.cron.create({
          name: 'integrityMissingFiles',
          expression: missingFiles.cronExpression,
          onTick: () =>
            handlePromiseError(
              this.jobRepository.queue({ name: JobName.IntegrityMissingFilesQueueAll, data: {} }),
              this.logger,
            ),
          start: missingFiles.enabled,
        });

        yield* this.fx.cron.create({
          name: 'integrityChecksumFiles',
          expression: checksumFiles.cronExpression,
          onTick: () =>
            handlePromiseError(
              this.jobRepository.queue({ name: JobName.IntegrityChecksumFiles, data: {} }),
              this.logger,
            ),
          start: checksumFiles.enabled,
        });
      }),
    );
  }

  @OnEvent({ name: 'ConfigUpdate', server: true })
  onConfigUpdate({
    newConfig: {
      integrityChecks: { untrackedFiles, missingFiles, checksumFiles },
    },
  }: ArgOf<'ConfigUpdate'>) {
    if (!this.integrityLock) {
      return;
    }

    this.cronRepository.update({
      name: 'integrityUntrackedFiles',
      expression: untrackedFiles.cronExpression,
      start: untrackedFiles.enabled,
    });

    this.cronRepository.update({
      name: 'integrityMissingFiles',
      expression: missingFiles.cronExpression,
      start: missingFiles.enabled,
    });

    this.cronRepository.update({
      name: 'integrityChecksumFiles',
      expression: checksumFiles.cronExpression,
      start: checksumFiles.enabled,
    });
  }

  getIntegrityReportSummary(): Promise<IntegrityReportSummaryResponseDto> {
    return runPromise(this.fx.integrity.getIntegrityReportSummary());
  }

  getIntegrityReport(dto: IntegrityGetReportDto): Promise<IntegrityReportResponseDto> {
    return runPromise(this.fx.integrity.getIntegrityReport({ cursor: dto.cursor, limit: dto.limit ?? 100 }, dto.type));
  }

  getIntegrityReportCsv(type: IntegrityReport): Readable {
    const items = this.integrityRepository.streamIntegrityReports(type);

    // very rudimentary csv serialiser
    async function* generator() {
      yield 'id,type,assetId,fileAssetId,path\n';

      for await (const item of items) {
        // no expectation of particularly bad filenames
        // but they could potentially have a newline or quote character
        yield `${item.id},${item.type},${item.assetId},${item.fileAssetId},"${item.path.replaceAll('"', '""')}"\n`;
      }
    }

    return Readable.from(generator());
  }

  async getIntegrityReportFile(id: string): Promise<ImmichFileResponse> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { path } = yield* this.fx.integrity.getById(id);

        return new ImmichFileResponse({
          path,
          fileName: basename(path),
          contentType: 'application/octet-stream',
          cacheControl: CacheControl.PrivateWithoutCache,
        });
      }),
    );
  }

  async deleteIntegrityReport(userId: string, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { path, assetId, fileAssetId } = yield* this.fx.integrity.getById(id);

        if (assetId) {
          yield* this.fx.asset.updateAll([assetId], {
            deletedAt: new Date(),
            status: AssetStatus.Trashed,
          });

          yield* this.fx.event.emit('AssetTrashAll', {
            assetIds: [assetId],
            userId,
          });

          yield* this.fx.integrity.deleteById(id);
        } else if (fileAssetId) {
          yield* this.fx.asset.deleteFiles([{ id: fileAssetId }]);
        } else {
          const trackedPaths = yield* this.fx.integrity.getTrackedPaths([path]);
          if (trackedPaths.length === 0) {
            yield* this.fx.storage.unlink(path);
          }
          yield* this.fx.integrity.deleteById(id);
        }
      }),
    );
  }

  private readonly queueRefreshAllUntrackedFilesFx = Effect.fn('IntegrityService.queueRefreshAllUntrackedFiles')(
    function* (this: IntegrityService) {
      this.logger.log(`Checking for out of date untracked file reports...`);

      const reports = yield* this.fx.integrity.streamIntegrityReportsWithAssetChecksum(IntegrityReport.UntrackedFile);

      let total = 0;
      yield* forEachAsync(batched(reports, JOBS_LIBRARY_PAGINATION_SIZE), (batchReports) =>
        Effect.gen({ self: this }, function* () {
          yield* this.fx.job.queue({
            name: JobName.IntegrityUntrackedFilesRefresh,
            data: {
              items: batchReports,
            },
          });

          total += batchReports.length;
          this.logger.log(`Queued report check of ${batchReports.length} report(s) (${total} so far)`);
        }),
      );
    },
  );

  @OnJob({ name: JobName.IntegrityUntrackedFilesQueueAll, queue: QueueName.IntegrityCheck })
  async handleUntrackedFilesQueueAll({ refreshOnly }: IIntegrityJob = {}): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.queueRefreshAllUntrackedFilesFx();

        if (refreshOnly) {
          this.logger.log('Refresh complete.');
          return JobStatus.Success;
        }

        this.logger.log(`Scanning for untracked files...`);

        const assetPaths = yield* this.fx.storage.walk({
          pathsToCrawl: [StorageFolder.EncodedVideo, StorageFolder.Library, StorageFolder.Upload].map((folder) =>
            StorageCore.getBaseFolder(folder),
          ),
          includeHidden: false,
          take: JOBS_LIBRARY_PAGINATION_SIZE,
        });

        const assetFilePaths = yield* this.fx.storage.walk({
          pathsToCrawl: [StorageCore.getBaseFolder(StorageFolder.Thumbnails)],
          includeHidden: false,
          take: JOBS_LIBRARY_PAGINATION_SIZE,
        });

        async function* paths() {
          for await (const batch of assetPaths) {
            yield ['asset', batch] as const;
          }

          for await (const batch of assetFilePaths) {
            yield ['asset_file', batch] as const;
          }
        }

        let total = 0;
        yield* forEachAsync(paths(), ([batchType, batchPaths]) =>
          Effect.gen({ self: this }, function* () {
            yield* this.fx.job.queue({
              name: JobName.IntegrityUntrackedFiles,
              data: {
                type: batchType,
                paths: batchPaths,
              },
            });

            const count = batchPaths.length;
            total += count;

            this.logger.log(`Queued untracked check of ${count} file(s) (${total} so far)`);
          }),
        );

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityUntrackedFiles, queue: QueueName.IntegrityCheck })
  async handleUntrackedFiles({ type, paths }: IIntegrityUntrackedFilesJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Processing batch of ${paths.length} files to check if they are untracked.`);

        const lookupPaths = [...new Set(paths.flatMap((path) => [path, path.normalize('NFC'), path.normalize('NFD')]))];

        const trackedPaths = new Set<string>();
        if (type === 'asset') {
          const assets = yield* this.fx.integrity.getAssetPathsByPaths(lookupPaths);
          for (const { originalPath, encodedVideoPath } of assets) {
            trackedPaths.add(originalPath.normalize('NFC'));

            if (encodedVideoPath) {
              trackedPaths.add(encodedVideoPath.normalize('NFC'));
            }
          }
        } else {
          const assets = yield* this.fx.integrity.getAssetFilePathsByPaths(lookupPaths);
          for (const { path } of assets) {
            trackedPaths.add(path.normalize('NFC'));
          }
        }

        const untrackedFiles = [...new Set(paths)].filter((path) => !trackedPaths.has(path.normalize('NFC')));

        if (untrackedFiles.length > 0) {
          yield* this.fx.integrity.create(
            untrackedFiles.map((path) => ({
              type: IntegrityReport.UntrackedFile,
              path,
            })),
          );
        }

        this.logger.log(`Processed ${paths.length} and found ${untrackedFiles.length} untracked file(s).`);
        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityUntrackedFilesRefresh, queue: QueueName.IntegrityCheck })
  async handleUntrackedRefresh({ items }: IIntegrityPathWithReportJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Processing batch of ${items.length} reports to check if they are out of date.`);

        const tracked = items.length > 0 ? yield* this.fx.integrity.getTrackedPaths(items.map(({ path }) => path)) : [];
        const trackedPaths = new Set(tracked.map(({ path }) => path));

        const results = yield* Effect.all(
          items.map(({ reportId, path }) =>
            Effect.gen({ self: this }, function* () {
              // The path was untracked when the report was written; an asset may reference it now.
              if (trackedPaths.has(path)) {
                return reportId;
              }

              return yield* this.fx.storage.stat(path).pipe(
                Effect.as(undefined),
                Effect.catch(() => Effect.succeed(reportId)),
              );
            }),
          ),
          { concurrency: 'unbounded' },
        );

        const reportIds = results.filter(Boolean) as string[];

        if (reportIds.length > 0) {
          yield* this.fx.integrity.deleteByIds(reportIds);
        }

        this.logger.log(`Processed ${items.length} paths and found ${reportIds.length} report(s) out of date.`);
        return JobStatus.Success;
      }),
    );
  }

  private readonly queueRefreshAllMissingFilesFx = Effect.fn('IntegrityService.queueRefreshAllMissingFiles')(function* (
    this: IntegrityService,
  ) {
    this.logger.log(`Checking for out of date missing file reports...`);

    const reports = yield* this.fx.integrity.streamIntegrityReportsWithAssetChecksum(IntegrityReport.MissingFile);

    let total = 0;
    yield* forEachAsync(batched(reports, JOBS_LIBRARY_PAGINATION_SIZE), (batchReports) =>
      Effect.gen({ self: this }, function* () {
        yield* this.fx.job.queue({
          name: JobName.IntegrityMissingFilesRefresh,
          data: {
            items: batchReports,
          },
        });

        total += batchReports.length;
        this.logger.log(`Queued report check of ${batchReports.length} report(s) (${total} so far)`);
      }),
    );

    this.logger.log('Refresh complete.');
  });

  @OnJob({ name: JobName.IntegrityMissingFilesQueueAll, queue: QueueName.IntegrityCheck })
  async handleMissingFilesQueueAll({ refreshOnly }: IIntegrityJob = {}): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (refreshOnly) {
          yield* this.queueRefreshAllMissingFilesFx();
          return JobStatus.Success;
        }

        this.logger.log(`Scanning for missing files...`);

        const assetPaths = yield* this.fx.integrity.streamAssetPathsForMissingFiles();

        let total = 0;
        yield* forEachAsync(batched(assetPaths, JOBS_LIBRARY_PAGINATION_SIZE), (batchPaths) =>
          Effect.gen({ self: this }, function* () {
            yield* this.fx.job.queue({
              name: JobName.IntegrityMissingFiles,
              data: {
                items: batchPaths,
              },
            });

            total += batchPaths.length;
            this.logger.log(`Queued missing check of ${batchPaths.length} file(s) (${total} so far)`);
          }),
        );

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityMissingFiles, queue: QueueName.IntegrityCheck })
  async handleMissingFiles({ items }: IIntegrityMissingFilesJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Processing batch of ${items.length} files to check if they are missing.`);

        const results = yield* Effect.all(
          items.map((item) =>
            this.fx.storage.stat(item.path).pipe(
              Effect.as({ ...item, exists: true }),
              Effect.catch(() => Effect.succeed({ ...item, exists: false })),
            ),
          ),
          { concurrency: 'unbounded' },
        );

        const outdatedReports = results
          .filter(({ exists, reportId }) => exists && reportId)
          .map(({ reportId }) => reportId!);

        if (outdatedReports.length > 0) {
          yield* this.fx.integrity.deleteByIds(outdatedReports);
        }

        const missingFiles = results.filter(({ exists }) => !exists);
        if (missingFiles.length > 0) {
          yield* this.fx.integrity.create(
            missingFiles.map(({ path, assetId, fileAssetId }) => ({
              type: IntegrityReport.MissingFile,
              path,
              assetId,
              fileAssetId,
            })),
          );
        }

        this.logger.log(`Processed ${items.length} and found ${missingFiles.length} missing file(s).`);
        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityMissingFilesRefresh, queue: QueueName.IntegrityCheck })
  async handleMissingRefresh({ items: paths }: IIntegrityPathWithReportJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Processing batch of ${paths.length} reports to check if they are out of date.`);

        const results = yield* Effect.all(
          paths.map(({ reportId, path }) =>
            this.fx.storage.stat(path).pipe(
              Effect.as(reportId as string | undefined),
              Effect.catch(() => Effect.succeed(undefined)),
            ),
          ),
          { concurrency: 'unbounded' },
        );

        const reportIds = results.filter(Boolean) as string[];

        if (reportIds.length > 0) {
          yield* this.fx.integrity.deleteByIds(reportIds);
        }

        this.logger.log(`Processed ${paths.length} paths and found ${reportIds.length} report(s) out of date.`);
        return JobStatus.Success;
      }),
    );
  }

  private readonly queueRefreshAllChecksumFilesFx = Effect.fn('IntegrityService.queueRefreshAllChecksumFiles')(
    function* (this: IntegrityService) {
      this.logger.log(`Checking for out of date checksum file reports...`);

      const reports = yield* this.fx.integrity.streamIntegrityReportsWithAssetChecksum(IntegrityReport.ChecksumFail);

      let total = 0;
      yield* forEachAsync(batched(reports, JOBS_LIBRARY_PAGINATION_SIZE), (batchReports) =>
        Effect.gen({ self: this }, function* () {
          yield* this.fx.job.queue({
            name: JobName.IntegrityChecksumFilesRefresh,
            data: {
              items: batchReports.map(({ path, reportId, checksum }) => ({
                path,
                reportId,
                checksum: checksum?.toString('hex'),
              })),
            },
          });

          total += batchReports.length;
          this.logger.log(`Queued report check of ${batchReports.length} report(s) (${total} so far)`);
        }),
      );

      this.logger.log('Refresh complete.');
    },
  );

  private readonly checkAssetChecksumFx = Effect.fn('IntegrityService.checkAssetChecksum')(function* (
    this: IntegrityService,
    originalPath: string,
    checksum: Buffer<ArrayBufferLike>,
    assetId: string,
    reportId: string | null,
  ) {
    const hash = createHash('sha1');

    yield* Effect.gen({ self: this }, function* () {
      const source = yield* this.fx.storage.createPlainReadStream(originalPath);
      yield* attempt(() =>
        pipeline([
          source,
          new Writable({
            write(chunk, _encoding, callback) {
              hash.update(chunk);
              callback();
            },
          }),
        ]),
      );

      if (checksum.equals(hash.digest())) {
        if (reportId) {
          yield* this.fx.integrity.deleteById(reportId);
        }
      } else {
        return yield* Effect.fail(new Error('File failed checksum'));
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.gen({ self: this }, function* () {
          const error = unwrap(Cause.squash(cause));
          if ((error as { code?: string }).code === 'ENOENT') {
            if (reportId) {
              yield* this.fx.integrity.deleteById(reportId);
            }

            // missing file; handled by the missing files job
            return;
          }

          this.logger.warn('Failed to process a file: ' + error);
          yield* this.fx.integrity.create({
            path: originalPath,
            type: IntegrityReport.ChecksumFail,
            assetId,
          });
        }),
      ),
    );
  });

  @OnJob({ name: JobName.IntegrityChecksumFiles, queue: QueueName.IntegrityCheck })
  async handleChecksumFiles({ refreshOnly }: IIntegrityJob = {}): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (refreshOnly) {
          yield* this.queueRefreshAllChecksumFilesFx();
          return JobStatus.Success;
        }

        const {
          integrityChecks: {
            checksumFiles: { timeLimit, percentageLimit },
          },
        } = yield* this.getConfigFx({
          withCache: true,
        });

        this.logger.log(
          `Checking file checksums... (will run for up to ${(timeLimit / (60 * 60 * 1000)).toFixed(2)} hours or until ${(percentageLimit * 100).toFixed(2)}% of assets are processed)`,
        );

        let processed = 0;
        const startedAt = Date.now();
        const { count } = yield* this.fx.integrity.getAssetCount();
        const checkpoint = yield* this.fx.systemMetadata.get(SystemMetadataKey.IntegrityChecksumCheckpoint);

        const startMarker = checkpoint?.date ? new Date(checkpoint.date) : undefined;

        const printStats = () => {
          const averageTime = ((Date.now() - startedAt) / processed).toFixed(2);
          const completionProgress = ((processed / count) * 100).toFixed(2);

          this.logger.log(
            `Processed ${processed} files so far... (avg. ${averageTime} ms/asset, ${completionProgress}% of all assets)`,
          );
        };

        let lastCreatedAt: Date | undefined;

        this.logger.log(`Processing assets from ${startMarker?.toISOString() ?? 'beginning'}`);

        const assets = yield* this.fx.integrity.streamAssetChecksums(startMarker);

        yield* forEachAsync(assets, ({ originalPath, checksum, createdAt, assetId, reportId }) =>
          Effect.gen({ self: this }, function* () {
            yield* this.checkAssetChecksumFx(originalPath, checksum, assetId, reportId);

            processed++;

            if (processed % 100 === 0) {
              printStats();
            }

            if (!(Date.now() > startedAt + timeLimit || processed > count * percentageLimit)) {
              return;
            }

            this.logger.log('Reached stop criteria.');
            lastCreatedAt = createdAt;
            return true;
          }),
        );

        yield* this.fx.systemMetadata.set(SystemMetadataKey.IntegrityChecksumCheckpoint, {
          date: lastCreatedAt?.toISOString(),
        });

        printStats();

        if (lastCreatedAt) {
          this.logger.log(`Finished checksum job, will continue from ${lastCreatedAt.toISOString()}.`);
        } else {
          this.logger.log(`Finished checksum job, covered all assets.`);
        }

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityChecksumFilesRefresh, queue: QueueName.IntegrityCheck })
  async handleChecksumRefresh({ items: paths }: IIntegrityPathWithChecksumJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Processing batch of ${paths.length} reports to check if they are out of date.`);

        const results = yield* Effect.all(
          paths.map(({ reportId, path, checksum }) =>
            Effect.gen({ self: this }, function* () {
              if (!checksum) {
                return reportId;
              }

              const hash = createHash('sha1');

              const isMissing = yield* Effect.gen({ self: this }, function* () {
                const source = yield* this.fx.storage.createPlainReadStream(path);
                yield* attempt(() =>
                  pipeline([
                    source,
                    new Writable({
                      write(chunk, _encoding, callback) {
                        hash.update(chunk);
                        callback();
                      },
                    }),
                  ]),
                );
                return false;
              }).pipe(
                Effect.catchCause((cause) =>
                  Effect.succeed((unwrap(Cause.squash(cause)) as { code?: string }).code === 'ENOENT'),
                ),
              );
              if (isMissing) {
                return reportId;
              }

              return Buffer.from(checksum, 'hex').equals(hash.digest()) ? reportId : undefined;
            }),
          ),
          { concurrency: 'unbounded' },
        );

        const reportIds = results.filter(Boolean) as string[];

        if (reportIds.length > 0) {
          yield* this.fx.integrity.deleteByIds(reportIds);
        }

        this.logger.log(`Processed ${paths.length} paths and found ${reportIds.length} report(s) out of date.`);
        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityDeleteReportType, queue: QueueName.IntegrityCheck })
  async handleDeleteAllIntegrityReports({ type }: IIntegrityDeleteReportTypeJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.log(`Deleting all entries for ${type ?? 'all types of'} integrity report`);

        let properties;
        switch (type) {
          case IntegrityReport.ChecksumFail: {
            properties = ['assetId'] as const;
            break;
          }
          case IntegrityReport.MissingFile: {
            properties = ['assetId', 'fileAssetId'] as const;
            break;
          }
          case IntegrityReport.UntrackedFile: {
            properties = [void 0] as const;
            break;
          }
          default: {
            properties = [void 0, 'assetId', 'fileAssetId'] as const;
            break;
          }
        }

        for (const property of properties) {
          const reports = yield* this.fx.integrity.streamIntegrityReportsByProperty(property, type);
          yield* forEachAsync(batched(reports, JOBS_LIBRARY_PAGINATION_SIZE), (batch) =>
            Effect.gen({ self: this }, function* () {
              yield* this.fx.job.queue({
                name: JobName.IntegrityDeleteReports,
                data: {
                  reports: batch,
                },
              });

              this.logger.log(`Queued ${batch.length} reports to delete.`);
            }),
          );
        }

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.IntegrityDeleteReports, queue: QueueName.IntegrityCheck })
  async handleDeleteIntegrityReports({ reports }: IIntegrityDeleteReportsJob): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const byAsset = reports.filter((report) => report.assetId);
        const byFileAsset = reports.filter((report) => report.fileAssetId);
        const byPath = reports.filter((report) => !report.assetId && !report.fileAssetId);

        if (byAsset.length > 0) {
          const ids = byAsset.map(({ assetId }) => assetId!);
          yield* this.fx.asset.updateAll(ids, {
            deletedAt: new Date(),
            status: AssetStatus.Trashed,
          });

          yield* this.fx.event.emit('AssetTrashAll', {
            assetIds: ids,
            userId: '', // we don't notify any users currently
          });

          yield* this.fx.integrity.deleteByIds(byAsset.map(({ id }) => id));
        }

        if (byFileAsset.length > 0) {
          yield* this.fx.asset.deleteFiles(byFileAsset.map(({ fileAssetId }) => ({ id: fileAssetId! })));
        }

        if (byPath.length > 0) {
          const tracked = yield* this.fx.integrity.getTrackedPaths(byPath.map(({ path }) => path));
          const trackedPaths = new Set(tracked.map(({ path }) => path));
          yield* Effect.all(
            byPath
              .filter(({ path }) => !trackedPaths.has(path))
              .map(({ path }) => this.fx.storage.unlink(path).pipe(Effect.catch(() => Effect.void))),
            { concurrency: 'unbounded' },
          );
          yield* this.fx.integrity.deleteByIds(byPath.map(({ id }) => id));
        }

        this.logger.log(`Deleted ${reports.length} reports.`);
        return JobStatus.Success;
      }),
    );
  }
}
