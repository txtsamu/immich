import { schemaDiff, schemaFromCode, schemaFromDatabase } from '@immich/sql-tools';
import { Injectable } from '@nestjs/common';
import AsyncLock from 'async-lock';
import { Context, Effect, Layer } from 'effect';
import { Kysely, sql } from 'kysely';
import { FileMigrationProvider, Migrator } from 'kysely/migration';
import { InjectKysely } from 'nestjs-kysely';
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { diff } from 'semver';
import type { ExtensionVersion, VectorExtension } from 'src/types.js';
import {
  EXTENSION_NAMES,
  POSTGRES_VERSION_RANGE,
  VECTORCHORD_VERSION_RANGE,
  VECTOR_EXTENSIONS,
  VECTOR_VERSION_RANGE,
  serverVersion,
} from 'src/constants.js';
import { GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { DatabaseError, LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { DatabaseExtension, DatabaseLock } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { immich_uuid_v7 } from 'src/schema/functions.js';
// eslint-disable-next-line import-x/no-duplicates
import 'src/schema/index.js'; // make sure all schema definitions are imported for schemaFromCode
// eslint-disable-next-line import-x/no-duplicates
import { DB } from 'src/schema/index.js';

export let cachedVectorExtension: VectorExtension | undefined;
export async function getVectorExtension(runner: Kysely<DB>): Promise<VectorExtension> {
  if (cachedVectorExtension) {
    return cachedVectorExtension;
  }

  cachedVectorExtension = new ConfigRepository().getEnv().database.vectorExtension;
  if (cachedVectorExtension) {
    return cachedVectorExtension;
  }

  const query = `SELECT name FROM pg_available_extensions WHERE name IN (${VECTOR_EXTENSIONS.map((ext) => `'${ext}'`).join(', ')})`;
  const { rows: availableExtensions } = await sql.raw<{ name: VectorExtension }>(query).execute(runner);
  const extensionNames = new Set(availableExtensions.map((row) => row.name));
  cachedVectorExtension = VECTOR_EXTENSIONS.find((ext) => extensionNames.has(ext));
  if (!cachedVectorExtension) {
    throw new Error(`No vector extension found. Available extensions: ${VECTOR_EXTENSIONS.join(', ')}`);
  }
  return cachedVectorExtension;
}

const acquireLock = (lock: DatabaseLock, connection: Kysely<DB>) =>
  sql`SELECT pg_advisory_lock(${lock})`.execute(connection);

const acquireTryLock = async (lock: DatabaseLock, connection: Kysely<DB>): Promise<boolean> => {
  const { rows } = await sql<{
    pg_try_advisory_lock: boolean;
  }>`SELECT pg_try_advisory_lock(${lock})`.execute(connection);
  return rows[0].pg_try_advisory_lock;
};

const releaseLock = (lock: DatabaseLock, connection: Kysely<DB>) =>
  sql`SELECT pg_advisory_unlock(${lock})`.execute(connection);

type SchemaDrift = ReturnType<typeof schemaDiff>;

type DatabaseDeps = {
  db: Kysely<DB>;
  logger: LoggingRepository;
  configRepository: ConfigRepository;
};

export class DatabaseRepo extends Context.Service<
  DatabaseRepo,
  {
    shutdown(): Effect.Effect<void, DatabaseError>;
    getVectorExtension(): Effect.Effect<VectorExtension, DatabaseError>;
    getExtensionVersions(extensions: readonly DatabaseExtension[]): Effect.Effect<ExtensionVersion[], DatabaseError>;
    getExtensionVersionRange(extension: VectorExtension): Effect.Effect<string, Error>;
    getPostgresVersion(): Effect.Effect<string, DatabaseError>;
    getPostgresVersionRange(): Effect.Effect<string>;
    createExtension(extension: DatabaseExtension): Effect.Effect<void, DatabaseError>;
    dropExtension(extension: DatabaseExtension): Effect.Effect<void, DatabaseError>;
    updateVectorExtension(
      extension: VectorExtension,
      targetVersion?: string,
    ): Effect.Effect<void, DatabaseError | Error>;
    vacuum(options?: { analyze?: boolean; table?: keyof DB }): Effect.Effect<void>;
    reindex(table: keyof DB, options?: { concurrently?: boolean }): Effect.Effect<unknown, DatabaseError>;
    getMigrations(): Effect.Effect<{ name: string; timestamp: string }[], DatabaseError>;
    getSchemaDrift(): Effect.Effect<SchemaDrift, LibraryError>;
    runMigrations(): Effect.Effect<number, DatabaseError>;
    migrateFilePaths(sourceFolder: string, targetFolder: string): Effect.Effect<void, DatabaseError>;
    withLock<R>(lock: DatabaseLock, callback: () => Promise<R>): Effect.Effect<R, DatabaseError>;
    tryLock(lock: DatabaseLock): Effect.Effect<boolean, DatabaseError>;
    isBusy(lock: DatabaseLock): Effect.Effect<boolean>;
    wait(lock: DatabaseLock): Effect.Effect<void, LibraryError>;
  }
>()('immich/repositories/DatabaseRepo') {
  static readonly make = ({ db, logger, configRepository }: DatabaseDeps) => {
    // in-process lock queue; advisory locks alone cannot give `isBusy`/`wait`
    const asyncLock = new AsyncLock();

    const getExtensionVersions = Effect.fn('Database.getExtensionVersions')(function* (
      extensions: readonly DatabaseExtension[],
    ) {
      const { rows } = yield* query(db, (db) =>
        sql<ExtensionVersion>`
      SELECT name, default_version as "availableVersion", installed_version as "installedVersion"
      FROM pg_available_extensions
      WHERE name in (${sql.join(extensions)})
    `.execute(db),
      );
      return rows;
    });

    const getDatabaseName = Effect.fn('Database.getDatabaseName')(function* () {
      const { rows } = yield* query(db, (db) => sql<{ db: string }>`SELECT current_database() as db`.execute(db));
      return rows[0].db;
    });

    const createMigrator = (): Migrator =>
      new Migrator({
        db,
        migrationLockTableName: 'kysely_migrations_lock',
        allowUnorderedMigrations: configRepository.isDev(),
        migrationTableName: 'kysely_migrations',
        provider: new FileMigrationProvider({
          fs: { readdir },
          path: { join },
          import: (filePath) => import(filePath),
          migrationFolder: join(import.meta.dirname, '..', 'schema/migrations'),
        }),
      });

    return DatabaseRepo.of({
      shutdown: Effect.fn('Database.shutdown')(function* () {
        yield* query(db, (db) => db.destroy());
      }),

      getVectorExtension: Effect.fn('Database.getVectorExtension')(() => query(db, (db) => getVectorExtension(db))),

      getExtensionVersions,

      getExtensionVersionRange: Effect.fn('Database.getExtensionVersionRange')(function* (extension: VectorExtension) {
        switch (extension) {
          case DatabaseExtension.VectorChord: {
            return VECTORCHORD_VERSION_RANGE;
          }
          case DatabaseExtension.Vector: {
            return VECTOR_VERSION_RANGE;
          }
          default: {
            return yield* Effect.fail(new Error(`Unsupported vector extension: '${extension}'`));
          }
        }
      }),

      getPostgresVersion: Effect.fn('Database.getPostgresVersion')(function* () {
        const { rows } = yield* query(db, (db) => sql<{ server_version: string }>`SHOW server_version`.execute(db));
        return rows[0].server_version;
      }),

      getPostgresVersionRange: Effect.fn('Database.getPostgresVersionRange')(() =>
        Effect.succeed(POSTGRES_VERSION_RANGE),
      ),

      createExtension: Effect.fn('Database.createExtension')(function* (extension: DatabaseExtension) {
        logger.log(`Creating ${EXTENSION_NAMES[extension]} extension`);
        yield* query(db, (db) => sql`CREATE EXTENSION IF NOT EXISTS ${sql.raw(extension)} CASCADE`.execute(db));
        if (extension !== DatabaseExtension.VectorChord) {
          return;
        }

        const dbName = sql.id(yield* getDatabaseName());
        yield* query(db, (db) => sql`ALTER DATABASE ${dbName} SET vchordrq.probes = 1`.execute(db));
        yield* query(db, (db) => sql`SET vchordrq.probes = 1`.execute(db));
      }),

      dropExtension: Effect.fn('Database.dropExtension')(function* (extension: DatabaseExtension) {
        logger.log(`Dropping ${EXTENSION_NAMES[extension]} extension`);
        yield* query(db, (db) => sql`DROP EXTENSION IF EXISTS ${sql.raw(extension)}`.execute(db));
      }),

      updateVectorExtension: Effect.fn('Database.updateVectorExtension')(function* (
        extension: VectorExtension,
        targetVersion?: string,
      ) {
        const [{ availableVersion, installedVersion }] = yield* getExtensionVersions([extension]);
        if (!installedVersion) {
          return yield* Effect.fail(new Error(`${EXTENSION_NAMES[extension]} extension is not installed`));
        }

        if (!availableVersion) {
          return yield* Effect.fail(new Error(`No available version for ${EXTENSION_NAMES[extension]} extension`));
        }
        targetVersion ??= availableVersion;

        if (!diff(installedVersion, targetVersion)) {
          return;
        }

        // vector indexes from versions that still had machine learning would block the extension update
        yield* query(db, (db) =>
          Promise.all([
            db.schema.dropIndex('clip_index').ifExists().execute(),
            db.schema.dropIndex('face_index').ifExists().execute(),
          ]),
        );

        const version = targetVersion;
        yield* query(db, (db) => sql`ALTER EXTENSION ${sql.raw(extension)} UPDATE TO ${sql.lit(version)}`.execute(db));
      }),

      vacuum: Effect.fn('Database.vacuum')(function* ({
        analyze = false,
        table,
      }: { analyze?: boolean; table?: keyof DB } = {}) {
        yield* query(db, (db) =>
          sql`VACUUM ${sql.raw(analyze ? 'ANALYZE' : '')} ${sql.raw(table ?? '')}`.execute(db),
        ).pipe(
          Effect.catch((error) =>
            Effect.sync(() => {
              logger.warn(`Failed to vacuum ${table || 'database'}: ${error.cause}`);
              logger.warn('If using Docker, consider increasing shm_size for the database.');
            }),
          ),
        );
      }),

      reindex: Effect.fn('Database.reindex')((table: keyof DB, { concurrently = false } = {}) =>
        query(db, (db) =>
          sql`REINDEX TABLE ${sql.raw(concurrently ? 'CONCURRENTLY' : '')} ${sql.raw(table)}`.execute(db),
        ),
      ),

      getMigrations: Effect.fn('Database.getMigrations')(() =>
        query(db, (db) =>
          db.selectFrom('kysely_migrations').select(['name', 'timestamp']).orderBy('name', 'asc').execute(),
        ),
      ),

      getSchemaDrift: Effect.fn('Database.getSchemaDrift')(function* () {
        const source = yield* Effect.try({
          try: () =>
            schemaFromCode({
              overrides: true,
              namingStrategy: 'default',
              uuidFunction: (version) => (version === 7 ? `${immich_uuid_v7.name}()` : 'uuid_generate_v4()'),
            }),
          catch: (cause) => new LibraryError({ cause }),
        });
        const { database } = configRepository.getEnv();
        const target = yield* Effect.tryPromise({
          try: () => schemaFromDatabase({ connection: database.config }),
          catch: (cause) => new LibraryError({ cause }),
        });

        return yield* Effect.try({
          try: () =>
            schemaDiff(source, target, {
              tables: { ignoreExtra: true },
              constraints: { ignoreExtra: false },
              indexes: { ignoreExtra: true },
              triggers: { ignoreExtra: true },
              columns: { ignoreExtra: true },
              functions: { ignoreExtra: false },
              parameters: { ignoreExtra: true },
              extensions: { ignoreExtra: true },
            }),
          catch: (cause) => new LibraryError({ cause }),
        });
      }),

      runMigrations: Effect.fn('Database.runMigrations')(function* () {
        logger.log('Running migrations');

        const migrator = yield* Effect.try({ try: createMigrator, catch: (cause) => new DatabaseError({ cause }) });

        const { error, results = [] } = yield* query(db, () => migrator.migrateToLatest());

        for (const result of results) {
          if (result.status === 'Success') {
            logger.log(`Migration "${result.migrationName}" succeeded`);
          } else if (result.status === 'Error') {
            logger.warn(`Migration "${result.migrationName}" failed`);
          }
        }

        if (error) {
          logger.error(`Migrations failed: ${error}`);

          const missing =
            error instanceof Error ? error.message.match(/previously executed migration (.+) is missing/u) : null;
          if (missing) {
            return yield* Effect.fail(
              new DatabaseError({
                cause: new Error(
                  `Migration "${missing[1]}" was already applied to this database but is not in this version of Immich (${serverVersion}). ` +
                    `This usually means the database was migrated by a newer version. Downgrades are not supported.`,
                  { cause: error },
                ),
              }),
            );
          }

          return yield* Effect.fail(new DatabaseError({ cause: error }));
        }

        logger.log('Finished running migrations');
        return results.length;
      }),

      migrateFilePaths: Effect.fn('Database.migrateFilePaths')(function* (sourceFolder: string, targetFolder: string) {
        // remove trailing slashes
        if (sourceFolder.endsWith('/')) {
          sourceFolder = sourceFolder.slice(0, -1);
        }

        if (targetFolder.endsWith('/')) {
          targetFolder = targetFolder.slice(0, -1);
        }

        // escaping regex special characters with a backslash
        const sourceRegex = '^' + sourceFolder.replaceAll(/[-[\]{}()*+?.,\\^$|#\s]/g, String.raw`\$&`);
        const source = sql.raw(`'${sourceRegex}'`);
        const target = sql.lit(targetFolder);

        yield* query(db, (db) =>
          db.transaction().execute(async (tx) => {
            await tx
              .updateTable('asset')
              .set((eb) => ({
                originalPath: eb.fn('REGEXP_REPLACE', ['originalPath', source, target]),
              }))
              .execute();

            await tx
              .updateTable('asset_file')
              .set((eb) => ({ path: eb.fn('REGEXP_REPLACE', ['path', source, target]) }))
              .execute();

            await tx
              .updateTable('user')
              .set((eb) => ({ profileImagePath: eb.fn('REGEXP_REPLACE', ['profileImagePath', source, target]) }))
              .execute();
          }),
        );
      }),

      // The in-process AsyncLock, the dedicated connection and the finally-release are kept as one unit: splitting
      // them across fibers would change lock ordering. Errors from `callback` and from the lock queries both surface
      // as `DatabaseError`; the adapter rethrows the original value either way.
      withLock: Effect.fn('Database.withLock')(function* <R>(lock: DatabaseLock, callback: () => Promise<R>) {
        return yield* query(db, async () => {
          let res;
          await asyncLock.acquire(DatabaseLock[lock], async () => {
            await db.connection().execute(async (connection) => {
              try {
                await acquireLock(lock, connection);
                res = await callback();
              } finally {
                await releaseLock(lock, connection);
              }
            });
          });

          return res as R;
        });
      }),

      tryLock: Effect.fn('Database.tryLock')((lock: DatabaseLock) =>
        query(db, (db) => db.connection().execute(async (connection) => acquireTryLock(lock, connection))),
      ),

      isBusy: Effect.fn('Database.isBusy')((lock: DatabaseLock) =>
        Effect.sync(() => asyncLock.isBusy(DatabaseLock[lock])),
      ),

      wait: Effect.fn('Database.wait')(function* (lock: DatabaseLock) {
        yield* Effect.tryPromise({
          try: async () => {
            await asyncLock.acquire(DatabaseLock[lock], () => {});
          },
          catch: (cause) => new LibraryError({ cause }),
        });
      }),
    });
  };

  static readonly layer = (deps: Omit<DatabaseDeps, 'db'>) =>
    Layer.effect(
      DatabaseRepo,
      Effect.gen(function* () {
        return DatabaseRepo.make({ ...deps, db: yield* Db });
      }),
    );
}

/** Promise-facing adapter for the Nest services. Delegates to `DatabaseRepo`. */
@Injectable()
export class DatabaseRepository {
  private readonly effect: Context.Service.Shape<typeof DatabaseRepo>;

  constructor(
    @InjectKysely() db: Kysely<DB>,
    private logger: LoggingRepository,
    configRepository: ConfigRepository,
  ) {
    this.logger.setContext(DatabaseRepository.name);
    this.effect = DatabaseRepo.make({ db, logger: this.logger, configRepository });
    registerEffect(this, this.effect);
  }

  shutdown(): Promise<void> {
    return runPromise(this.effect.shutdown());
  }

  getVectorExtension(): Promise<VectorExtension> {
    return runPromise(this.effect.getVectorExtension());
  }

  @GenerateSql({ params: [[DatabaseExtension.Vector]] })
  getExtensionVersions(extensions: readonly DatabaseExtension[]): Promise<ExtensionVersion[]> {
    return runPromise(this.effect.getExtensionVersions(extensions));
  }

  getExtensionVersionRange(extension: VectorExtension): string {
    return runSync(this.effect.getExtensionVersionRange(extension));
  }

  @GenerateSql()
  getPostgresVersion(): Promise<string> {
    return runPromise(this.effect.getPostgresVersion());
  }

  getPostgresVersionRange(): string {
    return runSync(this.effect.getPostgresVersionRange());
  }

  createExtension(extension: DatabaseExtension): Promise<void> {
    return runPromise(this.effect.createExtension(extension));
  }

  dropExtension(extension: DatabaseExtension): Promise<void> {
    return runPromise(this.effect.dropExtension(extension));
  }

  updateVectorExtension(extension: VectorExtension, targetVersion?: string): Promise<void> {
    return runPromise(this.effect.updateVectorExtension(extension, targetVersion));
  }

  vacuum(options: { analyze?: boolean; table?: keyof DB } = {}): Promise<void> {
    return runPromise(this.effect.vacuum(options));
  }

  reindex(table: keyof DB, options: { concurrently?: boolean } = {}): Promise<unknown> {
    return runPromise(this.effect.reindex(table, options));
  }

  getMigrations() {
    return runPromise(this.effect.getMigrations());
  }

  getSchemaDrift() {
    return runPromise(this.effect.getSchemaDrift());
  }

  runMigrations(): Promise<number> {
    return runPromise(this.effect.runMigrations());
  }

  migrateFilePaths(sourceFolder: string, targetFolder: string): Promise<void> {
    return runPromise(this.effect.migrateFilePaths(sourceFolder, targetFolder));
  }

  withLock<R>(lock: DatabaseLock, callback: () => Promise<R>): Promise<R> {
    return runPromise(this.effect.withLock(lock, callback));
  }

  tryLock(lock: DatabaseLock): Promise<boolean> {
    return runPromise(this.effect.tryLock(lock));
  }

  isBusy(lock: DatabaseLock): boolean {
    return runSync(this.effect.isBusy(lock));
  }

  wait(lock: DatabaseLock): Promise<void> {
    return runPromise(this.effect.wait(lock));
  }
}
