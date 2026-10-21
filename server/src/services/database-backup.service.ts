import { BadRequestException, Injectable, Optional } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import { debounce } from 'lodash-es';
import { DateTime } from 'luxon';
import path, { basename } from 'node:path';
import { Duplex, PassThrough, Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { coerce, satisfies } from 'semver';
import type { ArgOf } from 'src/repositories/event.repository.js';
import { serverVersion } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { DatabaseBackupListResponseDto } from 'src/dtos/database-backup.dto.js';
import { effectOf } from 'src/effect/bridge.js';
import { runPromise } from 'src/effect/run.js';
import { CacheControl, DatabaseLock, ImmichWorker, JobName, JobStatus, QueueName, StorageFolder } from 'src/enum.js';
import { MaintenanceHealthRepository } from 'src/maintenance/maintenance-health.repository.js';
import { ConfigRepo, ConfigRepository } from 'src/repositories/config.repository.js';
import { CronRepo, CronRepository } from 'src/repositories/cron.repository.js';
import { DatabaseRepo, DatabaseRepository } from 'src/repositories/database.repository.js';
import { JobRepository } from 'src/repositories/job.repository.js';
import { LoggingRepo, LoggingRepository } from 'src/repositories/logging.repository.js';
import { ProcessRepo, ProcessRepository } from 'src/repositories/process.repository.js';
import { StorageRepo, StorageRepository } from 'src/repositories/storage.repository.js';
import { SystemMetadataRepo, SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { UserRepo, UserRepository } from 'src/repositories/user.repository.js';
import { getConfigFx } from 'src/utils/config.js';
import {
  UnsupportedPostgresError,
  findDatabaseBackupVersion,
  isFailedDatabaseBackupName,
  isValidDatabaseBackupName,
  isValidDatabaseRoutineBackupName,
} from 'src/utils/database-backups.js';
import { ImmichFileResponse } from 'src/utils/file.js';
import { handlePromiseError } from 'src/utils/misc.js';

/** Effect errors wrap the original thrown value in `cause`; services log and rethrow the original. */
const unwrapCause = (error: unknown) =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;

@Injectable()
export class DatabaseBackupService {
  constructor(
    private readonly logger: LoggingRepository,
    private readonly storageRepository: StorageRepository,
    private readonly configRepository: ConfigRepository,
    private readonly systemMetadataRepository: SystemMetadataRepository,
    private readonly processRepository: ProcessRepository,
    private readonly databaseRepository: DatabaseRepository,
    private readonly userRepository: UserRepository,
    @Optional()
    private readonly cronRepository: CronRepository,
    @Optional()
    private readonly jobRepository: JobRepository,
    @Optional()
    private readonly maintenanceHealthRepository: MaintenanceHealthRepository,
  ) {
    this.logger.setContext(this.constructor.name);
  }

  private backupLock = false;

  private fxRepos?: {
    logger: ReturnType<typeof effectOf<typeof LoggingRepo>>;
    storage: ReturnType<typeof effectOf<typeof StorageRepo>>;
    config: ReturnType<typeof effectOf<typeof ConfigRepo>>;
    systemMetadata: ReturnType<typeof effectOf<typeof SystemMetadataRepo>>;
    process: ReturnType<typeof effectOf<typeof ProcessRepo>>;
    database: ReturnType<typeof effectOf<typeof DatabaseRepo>>;
    user: ReturnType<typeof effectOf<typeof UserRepo>>;
  };

  /**
   * The repositories as Effect services. This service does not extend `BaseService`, and the cron, job and
   * maintenance health repositories are optional, so those are lifted at their (guarded) call sites instead.
   */
  private get fx() {
    this.fxRepos ??= {
      logger: effectOf(LoggingRepo, this.logger),
      storage: effectOf(StorageRepo, this.storageRepository),
      config: effectOf(ConfigRepo, this.configRepository),
      systemMetadata: effectOf(SystemMetadataRepo, this.systemMetadataRepository),
      process: effectOf(ProcessRepo, this.processRepository),
      database: effectOf(DatabaseRepo, this.databaseRepository),
      user: effectOf(UserRepo, this.userRepository),
    };
    return this.fxRepos;
  }

  @OnEvent({ name: 'ConfigInit', workers: [ImmichWorker.Microservices] })
  async onConfigInit({
    newConfig: {
      backup: { database },
    },
  }: ArgOf<'ConfigInit'>) {
    if (!this.cronRepository || !this.jobRepository) {
      return;
    }

    const cronRepository = this.cronRepository;
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.backupLock = yield* this.fx.database.tryLock(DatabaseLock.BackupDatabase);

        if (this.backupLock) {
          yield* effectOf(CronRepo, cronRepository).create({
            name: 'backupDatabase',
            expression: database.cronExpression,
            onTick: () => handlePromiseError(this.jobRepository.queue({ name: JobName.DatabaseBackup }), this.logger),
            start: database.enabled,
          });
        }
      }),
    );
  }

  @OnEvent({ name: 'ConfigUpdate', server: true })
  onConfigUpdate({ newConfig: { backup } }: ArgOf<'ConfigUpdate'>) {
    if (!this.cronRepository || !this.jobRepository || !this.backupLock) {
      return;
    }

    this.cronRepository.update({
      name: 'backupDatabase',
      expression: backup.database.cronExpression,
      start: backup.database.enabled,
    });
  }

  @OnJob({ name: JobName.DatabaseBackup, queue: QueueName.BackupDatabase })
  async handleBackupDatabase(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const failed = yield* this.createDatabaseBackupFx().pipe(
          Effect.as(false),
          Effect.catch((error) =>
            unwrapCause(error) instanceof UnsupportedPostgresError ? Effect.succeed(true) : Effect.fail(error),
          ),
        );
        if (failed) {
          return JobStatus.Failed;
        }

        yield* this.cleanupDatabaseBackupsFx();
        return JobStatus.Success;
      }),
    );
  }

  async buildPostgresLaunchArguments(
    bin: 'pg_dump' | 'pg_dumpall' | 'psql',
    options: {
      singleTransaction?: boolean;
    } = {},
  ): Promise<{
    bin: string;
    args: string[];
    databaseUsername: string;
    databasePassword: string;
    databaseVersion: string;
    databaseMajorVersion?: number;
  }> {
    return runPromise(this.buildPostgresLaunchArgumentsFx(bin, options));
  }

  private readonly buildPostgresLaunchArgumentsFx = Effect.fn('DatabaseBackupService.buildPostgresLaunchArguments')(
    function* (
      this: DatabaseBackupService,
      bin: 'pg_dump' | 'pg_dumpall' | 'psql',
      options: {
        singleTransaction?: boolean;
      } = {},
    ) {
      const {
        database: { config: databaseConfig },
      } = yield* this.fx.config.getEnv();
      const isUrlConnection = databaseConfig.connectionType === 'url';

      const databaseVersion = yield* this.fx.database.getPostgresVersion();
      const databaseSemver = coerce(databaseVersion);
      const databaseMajorVersion = databaseSemver?.major;

      const args: string[] = [];
      let databaseUsername;
      let databasePassword;

      if (isUrlConnection) {
        if (bin !== 'pg_dump') {
          args.push('--dbname');
        }

        let url = databaseConfig.url;
        if (URL.canParse(databaseConfig.url)) {
          const parsedUrl = new URL(databaseConfig.url);
          // remove known bad parameters
          parsedUrl.searchParams.delete('uselibpqcompat');

          databaseUsername = parsedUrl.username || parsedUrl.searchParams.get('user');
          databasePassword = parsedUrl.password;

          url = parsedUrl.href;
        }

        // assume typical values if we can't parse URL or not present
        databaseUsername ??= 'postgres';
        databasePassword ??= '';

        args.push(url);
      } else {
        databaseUsername = databaseConfig.username;
        databasePassword = databaseConfig.password;

        args.push(
          '--username',
          databaseUsername,
          '--host',
          databaseConfig.host,
          '--port',
          databaseConfig.port.toString(),
        );

        switch (bin) {
          case 'pg_dump': {
            break;
          }
          case 'pg_dumpall': {
            args.push('--database');
            break;
          }
          case 'psql': {
            args.push('--dbname');
            break;
          }
        }

        args.push(databaseConfig.database);
      }

      switch (bin) {
        case 'pg_dump':
        case 'pg_dumpall': {
          args.push('--clean', '--if-exists');
          break;
        }
        case 'psql': {
          if (options.singleTransaction) {
            args.push(
              // don't commit any transaction on failure
              '--single-transaction',
              // exit with non-zero code on error
              '--set',
              'ON_ERROR_STOP=on',
            );
          }

          args.push(
            // used for progress monitoring
            '--echo-all',
            '--output=/dev/null',
          );
          break;
        }
      }

      if (!databaseMajorVersion || !databaseSemver || !satisfies(databaseSemver, '>=14.0.0 <19.0.0')) {
        this.logger.error(`Database Restore Failure: Unsupported PostgreSQL version: ${databaseVersion}`);
        return yield* Effect.fail(new UnsupportedPostgresError(databaseVersion));
      }

      return {
        bin: `/usr/lib/postgresql/${databaseMajorVersion}/bin/${bin}`,
        args,
        databaseUsername,
        databasePassword,
        databaseVersion,
        databaseMajorVersion,
      };
    },
  );

  async createDatabaseBackup(filenamePrefix: string = ''): Promise<string> {
    return runPromise(this.createDatabaseBackupFx(filenamePrefix));
  }

  private readonly createDatabaseBackupFx = Effect.fn('DatabaseBackupService.createDatabaseBackup')(function* (
    this: DatabaseBackupService,
    filenamePrefix: string = '',
  ) {
    this.logger.debug(`Database Backup Started`);

    const { bin, args, databasePassword, databaseVersion, databaseMajorVersion } =
      yield* this.buildPostgresLaunchArgumentsFx('pg_dump');

    this.logger.log(`Database Backup Starting. Database Version: ${databaseMajorVersion}`);

    const filename = `${filenamePrefix}immich-db-backup-${DateTime.now().toFormat("yyyyLLdd'T'HHmmss")}-v${serverVersion.toString()}-pg${databaseVersion.split(' ', 1)[0]}.sql.gz`;
    const backupFilePath = path.join(StorageCore.getBaseFolder(StorageFolder.Backups), filename);
    const temporaryFilePath = `${backupFilePath}.tmp`;

    let pgdump: Duplex | undefined;
    let gzip: Duplex | undefined;

    yield* Effect.gen({ self: this }, function* () {
      pgdump = yield* this.fx.process.spawnDuplexStream(bin, args, {
        env: {
          PATH: process.env.PATH,
          PGPASSWORD: databasePassword,
        },
      });

      gzip = yield* this.fx.process.spawnDuplexStream('gzip', ['--rsyncable']);
      const fileStream = yield* this.fx.storage.createWriteStream(temporaryFilePath);

      const source = pgdump;
      const middle = gzip;
      yield* Effect.tryPromise({ try: () => pipeline(source, middle, fileStream), catch: (cause) => cause });
      yield* this.fx.storage.rename(temporaryFilePath, backupFilePath);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.gen({ self: this }, function* () {
          const error = unwrapCause(Cause.squash(cause));
          this.logger.error(`Database Backup Failure: ${error}`);
          pgdump?.destroy();
          gzip?.destroy();
          yield* this.fx.storage
            .unlink(temporaryFilePath)
            .pipe(
              Effect.catch((error) =>
                Effect.sync(() => this.logger.error(`Failed to delete failed backup file: ${unwrapCause(error)}`)),
              ),
            );
          return yield* Effect.fail(error);
        }),
      ),
    );

    this.logger.log(`Database Backup Success`);
    return backupFilePath;
  });

  async uploadBackup(file: Express.Multer.File): Promise<void> {
    const backupsFolder = StorageCore.getBaseFolder(StorageFolder.Backups);
    const fn = basename(file.originalname);
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!isValidDatabaseBackupName(fn)) {
          return yield* Effect.fail(new BadRequestException('Invalid backup name!'));
        }

        const filePath = path.join(backupsFolder, `uploaded-${fn}`);
        yield* this.fx.storage.createOrOverwriteFile(filePath, file.buffer);
      }),
    );
  }

  downloadBackup(fileName: string): ImmichFileResponse {
    if (!isValidDatabaseBackupName(fileName)) {
      throw new BadRequestException('Invalid backup name!');
    }

    const filePath = path.join(StorageCore.getBaseFolder(StorageFolder.Backups), fileName);

    return {
      path: filePath,
      fileName,
      cacheControl: CacheControl.PrivateWithoutCache,
      contentType: fileName.endsWith('.gz') ? 'application/gzip' : 'application/sql',
    };
  }

  async listBackups(): Promise<DatabaseBackupListResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const backupsFolder = StorageCore.getBaseFolder(StorageFolder.Backups);
        const files = yield* this.fx.storage.readdir(backupsFolder);
        const timezone = DateTime.local().zoneName;

        const validFiles = files
          .filter((fn) => isValidDatabaseBackupName(fn))
          .toSorted((a, b) => (a.startsWith('uploaded-') === b.startsWith('uploaded-') ? a.localeCompare(b) : 1))
          .toReversed();

        const backups = yield* Effect.all(
          validFiles.map((filename) =>
            Effect.gen({ self: this }, function* () {
              const stats = yield* this.fx.storage.stat(path.join(backupsFolder, filename));
              return { filename, filesize: stats.size, timezone };
            }),
          ),
          { concurrency: 'unbounded' },
        );

        return {
          backups,
        };
      }),
    );
  }

  async deleteBackup(files: string[]): Promise<void> {
    const backupsFolder = StorageCore.getBaseFolder(StorageFolder.Backups);

    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (files.some((filename) => !isValidDatabaseBackupName(filename))) {
          return yield* Effect.fail(new BadRequestException('Invalid backup name!'));
        }

        yield* Effect.all(
          files.map((filename) => this.fx.storage.unlink(path.join(backupsFolder, filename))),
          { concurrency: 'unbounded' },
        );
      }),
    );
  }

  async cleanupDatabaseBackups() {
    return runPromise(this.cleanupDatabaseBackupsFx());
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly cleanupDatabaseBackupsFx = Effect.fn('DatabaseBackupService.cleanupDatabaseBackups')(function* (
    this: DatabaseBackupService,
  ) {
    this.logger.debug(`Database Backup Cleanup Started`);
    const {
      backup: { database: config },
    } = yield* getConfigFx(
      {
        configRepo: this.fx.config,
        metadataRepo: this.fx.systemMetadata,
        logger: this.fx.logger,
      },
      {
        withCache: false,
      },
    );

    const backupsFolder = StorageCore.getBaseFolder(StorageFolder.Backups);
    const files = yield* this.fx.storage.readdir(backupsFolder);
    const backups = files
      .filter((filename) => isValidDatabaseRoutineBackupName(filename))
      .toSorted()
      .toReversed();
    const failedBackups = files.filter((filename) => isFailedDatabaseBackupName(filename));

    const toDelete = backups.slice(config.keepLastAmount);
    toDelete.push(...failedBackups);

    for (const file of toDelete) {
      yield* this.fx.storage.unlink(path.join(backupsFolder, file));
    }

    this.logger.debug(`Database Backup Cleanup Finished, deleted ${toDelete.length} backups`);
  });

  async restoreDatabaseBackup(
    filename: string,
    progressCb?: (action: 'backup' | 'restore' | 'migrations' | 'rollback', progress: number) => void,
  ): Promise<void> {
    return runPromise(this.restoreDatabaseBackupFx(filename, progressCb));
  }

  private readonly restoreDatabaseBackupFx = Effect.fn('DatabaseBackupService.restoreDatabaseBackup')(function* (
    this: DatabaseBackupService,
    filename: string,
    progressCb?: (action: 'backup' | 'restore' | 'migrations' | 'rollback', progress: number) => void,
  ) {
    this.logger.debug(`Database Restore Started`);

    let isComplete = false;
    yield* Effect.gen({ self: this }, function* () {
      if (!isValidDatabaseBackupName(filename)) {
        return yield* Effect.fail(new Error('Invalid backup file format!'));
      }

      const backupFilePath = path.join(StorageCore.getBaseFolder(StorageFolder.Backups), filename);
      yield* this.fx.storage.stat(backupFilePath); // => check file exists

      let isPgClusterDump = false;
      const version = findDatabaseBackupVersion(filename);
      if (version && satisfies(version, '<= 2.4')) {
        isPgClusterDump = true;
      }

      const { bin, args, databaseUsername, databasePassword, databaseMajorVersion } =
        yield* this.buildPostgresLaunchArgumentsFx('psql', {
          singleTransaction: !isPgClusterDump,
        });

      progressCb?.('backup', 0.05);

      const restorePointFilePath = yield* this.createDatabaseBackupFx('restore-point-');

      this.logger.log(`Database Restore Starting. Database Version: ${databaseMajorVersion}`);

      let inputStream: Readable;
      if (backupFilePath.endsWith('.gz')) {
        const fileStream = yield* this.fx.storage.createPlainReadStream(backupFilePath);
        const gunzip = yield* this.fx.storage.createGunzip();
        fileStream.pipe(gunzip);
        inputStream = gunzip;
      } else {
        inputStream = yield* this.fx.storage.createPlainReadStream(backupFilePath);
      }

      const sqlStream = Readable.from(sql(inputStream, databaseUsername, isPgClusterDump));
      const psql = yield* this.fx.process.spawnDuplexStream(bin, args, {
        env: {
          PATH: process.env.PATH,
          PGPASSWORD: databasePassword,
        },
      });

      const [progressSource, progressSink] = createSqlProgressStreams((progress) => {
        if (isComplete) {
          return;
        }

        this.logger.log(`Restore progress ~ ${(progress * 100).toFixed(2)}%`);
        progressCb?.('restore', progress);
      });

      yield* Effect.tryPromise({
        try: () =>
          pipeline(sqlStream, createSqlOwnerTransformStream(databaseUsername), progressSource, psql, progressSink),
        catch: (cause) => cause,
      });

      yield* Effect.gen({ self: this }, function* () {
        progressCb?.('migrations', 0.9);

        yield* this.fx.database.runMigrations();

        const hasAdmin = yield* this.fx.user.hasAdmin();
        if (!hasAdmin) {
          return yield* Effect.fail(new Error('Server health check failed, no admin exists.'));
        }

        yield* Effect.tryPromise({
          try: async () => await this.maintenanceHealthRepository.checkApiHealth(),
          catch: (cause) => cause,
        });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen({ self: this }, function* () {
            const error = unwrapCause(Cause.squash(cause));
            progressCb?.('rollback', 0);

            const fileStream = yield* this.fx.storage.createPlainReadStream(restorePointFilePath);
            const gunzip = yield* this.fx.storage.createGunzip();
            fileStream.pipe(gunzip);
            inputStream = gunzip;

            const sqlStream = Readable.from(sqlRollback(inputStream, databaseUsername));
            const psql = yield* this.fx.process.spawnDuplexStream(bin, args, {
              env: {
                PATH: process.env.PATH,
                PGPASSWORD: databasePassword,
              },
            });

            const [progressSource, progressSink] = createSqlProgressStreams((progress) => {
              if (isComplete) {
                return;
              }

              this.logger.log(`Rollback progress ~ ${(progress * 100).toFixed(2)}%`);
              progressCb?.('rollback', progress);
            });

            yield* Effect.tryPromise({
              try: () => pipeline(sqlStream, progressSource, psql, progressSink),
              catch: (cause) => cause,
            });

            return yield* Effect.fail(error);
          }),
        ),
      );
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.gen({ self: this }, function* () {
          const error = unwrapCause(Cause.squash(cause));
          this.logger.error(`Database Restore Failure: ${error}`);
          return yield* Effect.fail(error);
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          isComplete = true;
        }),
      ),
    );

    this.logger.log(`Database Restore Success`);
  });
}

const SQL_DROP_CONNECTIONS = `
  -- drop all other database connections
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid();
`;

const SQL_RESET_SCHEMA = (username: string) => `
  -- re-create the default schema
  DROP SCHEMA public CASCADE;
  CREATE SCHEMA public;

  -- restore access to schema
  GRANT ALL ON SCHEMA public TO "${username}";
  GRANT ALL ON SCHEMA public TO public;
`;

async function* sql(inputStream: Readable, databaseUsername: string, isPgClusterDump: boolean) {
  yield SQL_DROP_CONNECTIONS;
  yield isPgClusterDump
    ? // it is likely the dump contains SQL to try to drop the currently active
      // database to ensure we have a fresh slate; if the `postgres` database exists
      // then prefer to switch before continuing otherwise this will just silently fail
      String.raw`
        \c postgres
      `
    : SQL_RESET_SCHEMA(databaseUsername);

  for await (const chunk of inputStream) {
    yield chunk;
  }
}

async function* sqlRollback(inputStream: Readable, databaseUsername: string) {
  yield SQL_DROP_CONNECTIONS;
  yield SQL_RESET_SCHEMA(databaseUsername);

  for await (const chunk of inputStream) {
    yield chunk;
  }
}

function createSqlProgressStreams(cb: (progress: number) => void) {
  const STDIN_START_MARKER = new TextEncoder().encode('FROM stdin');
  const STDIN_END_MARKER = new TextEncoder().encode(String.raw`\.`);

  let isReadingStdin = false;
  let sequenceIdx = 0;

  let linesSent = 0;
  let linesProcessed = 0;

  const startedAt = +Date.now();
  const cbDebounced = debounce(
    () => {
      const progress = source.writableEnded
        ? Math.min(1, linesProcessed / linesSent)
        : // progress simulation while we're in an indeterminate state
          Math.min(0.3, 0.1 + (Date.now() - startedAt) / 1e4);
      cb(progress);
    },
    100,
    {
      maxWait: 100,
    },
  );

  let lastByte = -1;
  const source = new PassThrough({
    transform(chunk, _encoding, callback) {
      for (const byte of chunk) {
        if (!isReadingStdin && byte === 10 && lastByte !== 10) {
          linesSent += 1;
        }

        lastByte = byte;

        const sequence = isReadingStdin ? STDIN_END_MARKER : STDIN_START_MARKER;
        if (sequence[sequenceIdx] === byte) {
          sequenceIdx += 1;

          if (sequence.length === sequenceIdx) {
            sequenceIdx = 0;
            isReadingStdin = !isReadingStdin;
          }
        } else {
          sequenceIdx = 0;
        }
      }

      cbDebounced();

      this.push(chunk);
      callback();
    },
  });

  const sink = new Writable({
    write(chunk, _encoding, callback) {
      for (const byte of chunk) {
        if (byte === 10) {
          linesProcessed++;
        }
      }

      cbDebounced();
      callback();
    },
  });

  return [source, sink];
}

function createSqlOwnerTransformStream(databaseUsername: string) {
  const OWNER_MARKER_START = new TextEncoder().encode('OWNER TO ');
  const DATA_MARKER_START = new TextEncoder().encode('FROM stdin');
  const LINE_END = new TextEncoder().encode(';');

  const owner = new TextEncoder().encode(`"${databaseUsername}"`);

  let ownerSequenceIndex = 0;

  let replacingOwnerIndex = 0;
  let replacingOwner = false;

  let readingDataIndex = 0;

  let dataPart = false;

  return new PassThrough({
    transform(chunk, _encoding, callback) {
      let result = chunk;
      if (!dataPart) {
        for (let index = 0; index < result.length; index++) {
          if (replacingOwner) {
            if (result[index] === LINE_END[0]) {
              result = Buffer.concat([result.slice(0, index), owner.slice(replacingOwnerIndex), result.slice(index)]);
              replacingOwnerIndex = owner.length;
            } else {
              result[index] = owner[replacingOwnerIndex];
              replacingOwnerIndex++;
            }
          }

          if (replacingOwnerIndex === owner.length) {
            replacingOwner = false;
          }

          if (result[index] === OWNER_MARKER_START[ownerSequenceIndex]) {
            ownerSequenceIndex++;
          } else {
            ownerSequenceIndex = 0;
          }

          if (ownerSequenceIndex === OWNER_MARKER_START.length) {
            ownerSequenceIndex = 0;
            replacingOwner = true;
            replacingOwnerIndex = 0;
          }

          if (result[index] === DATA_MARKER_START[readingDataIndex]) {
            readingDataIndex++;
          } else {
            readingDataIndex = 0;
          }

          if (readingDataIndex === DATA_MARKER_START.length) {
            dataPart = true;
            break;
          }
        }
      }

      this.push(result);
      callback();
    },
  });
}
