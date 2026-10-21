import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { join } from 'node:path';
import type { JobOf, SystemFlags } from 'src/types.js';
import { ErrorMessages } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { runPromise } from 'src/effect/run.js';
import {
  BootstrapEventPriority,
  DatabaseLock,
  JobName,
  JobStatus,
  QueueName,
  StorageFolder,
  SystemMetadataKey,
} from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { ImmichStartupError } from 'src/utils/misc.js';

const docsMessage = `Please see https://docs.immich.app/administration/system-integrity#folder-checks for more information.`;

/** The original thrown value behind a wrapped repository error (see `runPromise`). */
const unwrapError = (error: unknown) =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;

@Injectable()
export class StorageService extends BaseService {
  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly detectMediaLocation = Effect.fn('Storage.detectMediaLocation')(function* (this: StorageService) {
    const envData = yield* this.fx.config.getEnv();
    if (envData.storage.mediaLocation) {
      return envData.storage.mediaLocation;
    }

    const targets: string[] = [];
    const candidates = ['/data', '/usr/src/app/upload'];

    for (const candidate of candidates) {
      const isExists = yield* this.fx.storage.existsSync(candidate);
      if (isExists) {
        targets.push(candidate);
      }
    }

    if (targets.length === 1) {
      return targets[0];
    }

    return '/usr/src/app/upload';
  });

  @OnEvent({ name: 'AppBootstrap', priority: BootstrapEventPriority.StorageService })
  async onBootstrap() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        StorageCore.setMediaLocation(yield* this.detectMediaLocation());

        yield* this.fx.database.withLock(DatabaseLock.SystemFileMounts, () =>
          runPromise(
            Effect.gen({ self: this }, function* () {
              const flags =
                (yield* this.fx.systemMetadata.get(SystemMetadataKey.SystemFlags)) ||
                ({ mountChecks: {} } as SystemFlags);

              if (!flags.mountChecks) {
                flags.mountChecks = {};
              }
              const { mountChecks } = flags;

              let isUpdated = false;

              this.logger.log(`Verifying system mount folder checks, current state: ${JSON.stringify(flags)}`);

              yield* Effect.gen({ self: this }, function* () {
                // check each folder exists and is writable
                for (const folder of Object.values(StorageFolder)) {
                  if (!mountChecks[folder]) {
                    this.logger.log(`Writing initial mount file for the ${folder} folder`);
                    yield* this.createMountFile(folder);
                  }

                  yield* this.verifyReadAccess(folder);
                  yield* this.verifyWriteAccess(folder);

                  if (mountChecks[folder]) {
                    continue;
                  }

                  mountChecks[folder] = true;
                  isUpdated = true;
                }

                if (isUpdated) {
                  yield* this.fx.systemMetadata.set(SystemMetadataKey.SystemFlags, flags);
                  this.logger.log('Successfully enabled system mount folders checks');
                }

                this.logger.log('Successfully verified system mount folder checks');
              }).pipe(
                Effect.catch((error) =>
                  Effect.gen({ self: this }, function* () {
                    const original = unwrapError(error);
                    const envData = yield* this.fx.config.getEnv();
                    if (envData.storage.ignoreMountCheckErrors) {
                      this.logger.error(original as Error);
                      this.logger.warn('Ignoring mount folder errors');
                    } else {
                      return yield* Effect.fail(original);
                    }
                  }),
                ),
              );
            }),
          ),
        );

        yield* this.fx.database.withLock(DatabaseLock.MediaLocation, () =>
          runPromise(
            Effect.gen({ self: this }, function* () {
              const current = StorageCore.getMediaLocation();
              const samples = yield* this.fx.asset.getFileSamples();
              const savedValue = yield* this.fx.systemMetadata.get(SystemMetadataKey.MediaLocation);
              if (samples.length > 0) {
                const path = samples[0].path;

                let previous = savedValue?.location || '';

                if (!previous && (yield* this.fx.config.getEnv()).storage.mediaLocation) {
                  previous = current;
                }

                if (!previous) {
                  previous = path.startsWith('upload/') ? 'upload' : '/usr/src/app/upload';
                }

                if (previous !== current) {
                  this.logger.log(`Media location changed (from=${previous}, to=${current})`);

                  if (!path.startsWith(previous)) {
                    return yield* Effect.fail(new Error(ErrorMessages.InconsistentMediaLocation));
                  }

                  this.logger.warn(
                    `Detected a change to media location, performing an automatic migration of file paths from ${previous} to ${current}, this may take awhile`,
                  );
                  yield* this.fx.database.migrateFilePaths(previous, current);
                }
              }

              // Only set MediaLocation in systemMetadataRepository if needed
              if (savedValue?.location !== current) {
                yield* this.fx.systemMetadata.set(SystemMetadataKey.MediaLocation, { location: current });
              }
            }),
          ),
        );
      }),
    );
  }

  @OnJob({ name: JobName.FileDelete, queue: QueueName.BackgroundTask })
  async handleDeleteFiles(job: JobOf<JobName.FileDelete>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { files } = job;

        // TODO: one job per file
        for (const file of files) {
          if (!file) {
            continue;
          }

          yield* this.fx.storage.unlink(file).pipe(
            Effect.catch((error) =>
              Effect.sync(() => {
                this.logger.warn('Unable to remove file from disk', unwrapError(error) as any);
              }),
            ),
          );
        }

        return JobStatus.Success;
      }),
    );
  }

  private readonly verifyReadAccess = Effect.fn('Storage.verifyReadAccess')(function* (
    this: StorageService,
    folder: StorageFolder,
  ) {
    const { internalPath, externalPath } = this.getMountFilePaths(folder);
    yield* this.fx.storage.readFile(internalPath).pipe(
      Effect.catch((error) => {
        this.logger.error(`Failed to read (${internalPath}): ${unwrapError(error)}`);
        return Effect.fail(
          new ImmichStartupError(`Failed to read: "${externalPath} (${internalPath}) - ${docsMessage}"`),
        );
      }),
    );
  });

  private readonly createMountFile = Effect.fn('Storage.createMountFile')(function* (
    this: StorageService,
    folder: StorageFolder,
  ) {
    const { folderPath, internalPath, externalPath } = this.getMountFilePaths(folder);
    yield* Effect.gen({ self: this }, function* () {
      yield* this.fx.storage.mkdirSync(folderPath);
      yield* this.fx.storage.createFile(internalPath, Buffer.from(Date.now().toString()));
    }).pipe(
      Effect.catch((error) => {
        const original = unwrapError(error);
        if ((original as NodeJS.ErrnoException).code === 'EEXIST') {
          this.logger.warn('Found existing mount file, skipping creation');
          return Effect.void;
        }
        this.logger.error(`Failed to create ${internalPath}: ${original}`);
        return Effect.fail(new ImmichStartupError(`Failed to create "${externalPath} - ${docsMessage}"`));
      }),
    );
  });

  private readonly verifyWriteAccess = Effect.fn('Storage.verifyWriteAccess')(function* (
    this: StorageService,
    folder: StorageFolder,
  ) {
    const { internalPath, externalPath } = this.getMountFilePaths(folder);
    yield* this.fx.storage.overwriteFile(internalPath, Buffer.from(Date.now().toString())).pipe(
      Effect.catch((error) => {
        this.logger.error(`Failed to write ${internalPath}: ${unwrapError(error)}`);
        return Effect.fail(new ImmichStartupError(`Failed to write "${externalPath} - ${docsMessage}"`));
      }),
    );
  });

  private getMountFilePaths(folder: StorageFolder) {
    const folderPath = StorageCore.getBaseFolder(folder);
    const internalPath = join(folderPath, '.immich');
    const externalPath = `<UPLOAD_LOCATION>/${folder}/.immich`;

    return { folderPath, internalPath, externalPath };
  }
}
