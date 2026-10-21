import { schemaDiff, schemaFromCode, schemaFromDatabase } from '@immich/sql-tools';
import { Injectable } from '@nestjs/common';
import AsyncLock from 'async-lock';
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

@Injectable()
export class DatabaseRepository {
  private readonly asyncLock = new AsyncLock();

  constructor(
    @InjectKysely() private db: Kysely<DB>,
    private logger: LoggingRepository,
    private configRepository: ConfigRepository,
  ) {
    this.logger.setContext(DatabaseRepository.name);
  }

  async shutdown() {
    await this.db.destroy();
  }

  getVectorExtension(): Promise<VectorExtension> {
    return getVectorExtension(this.db);
  }

  @GenerateSql({ params: [[DatabaseExtension.Vector]] })
  async getExtensionVersions(extensions: readonly DatabaseExtension[]): Promise<ExtensionVersion[]> {
    const { rows } = await sql<ExtensionVersion>`
      SELECT name, default_version as "availableVersion", installed_version as "installedVersion"
      FROM pg_available_extensions
      WHERE name in (${sql.join(extensions)})
    `.execute(this.db);
    return rows;
  }

  getExtensionVersionRange(extension: VectorExtension): string {
    switch (extension) {
      case DatabaseExtension.VectorChord: {
        return VECTORCHORD_VERSION_RANGE;
      }
      case DatabaseExtension.Vector: {
        return VECTOR_VERSION_RANGE;
      }
      default: {
        throw new Error(`Unsupported vector extension: '${extension}'`);
      }
    }
  }

  @GenerateSql()
  async getPostgresVersion(): Promise<string> {
    const { rows } = await sql<{ server_version: string }>`SHOW server_version`.execute(this.db);
    return rows[0].server_version;
  }

  getPostgresVersionRange(): string {
    return POSTGRES_VERSION_RANGE;
  }

  async createExtension(extension: DatabaseExtension): Promise<void> {
    this.logger.log(`Creating ${EXTENSION_NAMES[extension]} extension`);
    await sql`CREATE EXTENSION IF NOT EXISTS ${sql.raw(extension)} CASCADE`.execute(this.db);
    if (extension !== DatabaseExtension.VectorChord) {
      return;
    }

    const dbName = sql.id(await this.getDatabaseName());
    await sql`ALTER DATABASE ${dbName} SET vchordrq.probes = 1`.execute(this.db);
    await sql`SET vchordrq.probes = 1`.execute(this.db);
  }

  async dropExtension(extension: DatabaseExtension): Promise<void> {
    this.logger.log(`Dropping ${EXTENSION_NAMES[extension]} extension`);
    await sql`DROP EXTENSION IF EXISTS ${sql.raw(extension)}`.execute(this.db);
  }

  async updateVectorExtension(extension: VectorExtension, targetVersion?: string): Promise<void> {
    const [{ availableVersion, installedVersion }] = await this.getExtensionVersions([extension]);
    if (!installedVersion) {
      throw new Error(`${EXTENSION_NAMES[extension]} extension is not installed`);
    }

    if (!availableVersion) {
      throw new Error(`No available version for ${EXTENSION_NAMES[extension]} extension`);
    }
    targetVersion ??= availableVersion;

    if (!diff(installedVersion, targetVersion)) {
      return;
    }

    // vector indexes from versions that still had machine learning would block the extension update
    await Promise.all([
      this.db.schema.dropIndex('clip_index').ifExists().execute(),
      this.db.schema.dropIndex('face_index').ifExists().execute(),
    ]);

    await sql`ALTER EXTENSION ${sql.raw(extension)} UPDATE TO ${sql.lit(targetVersion)}`.execute(this.db);
  }

  async vacuum({ analyze = false, table }: { analyze?: boolean; table?: keyof DB } = {}): Promise<void> {
    try {
      await sql`VACUUM ${sql.raw(analyze ? 'ANALYZE' : '')} ${sql.raw(table ?? '')}`.execute(this.db);
    } catch (error) {
      this.logger.warn(`Failed to vacuum ${table || 'database'}: ${error}`);
      this.logger.warn('If using Docker, consider increasing shm_size for the database.');
    }
  }

  reindex(table: keyof DB, { concurrently = false } = {}): Promise<unknown> {
    return sql`REINDEX TABLE ${sql.raw(concurrently ? 'CONCURRENTLY' : '')} ${sql.raw(table)}`.execute(this.db);
  }

  private async getDatabaseName(): Promise<string> {
    const { rows } = await sql<{ db: string }>`SELECT current_database() as db`.execute(this.db);
    return rows[0].db;
  }

  getMigrations() {
    return this.db.selectFrom('kysely_migrations').select(['name', 'timestamp']).orderBy('name', 'asc').execute();
  }

  async getSchemaDrift() {
    const source = schemaFromCode({
      overrides: true,
      namingStrategy: 'default',
      uuidFunction: (version) => (version === 7 ? `${immich_uuid_v7.name}()` : 'uuid_generate_v4()'),
    });
    const { database } = this.configRepository.getEnv();
    const target = await schemaFromDatabase({ connection: database.config });

    const drift = schemaDiff(source, target, {
      tables: { ignoreExtra: true },
      constraints: { ignoreExtra: false },
      indexes: { ignoreExtra: true },
      triggers: { ignoreExtra: true },
      columns: { ignoreExtra: true },
      functions: { ignoreExtra: false },
      parameters: { ignoreExtra: true },
      extensions: { ignoreExtra: true },
    });

    return drift;
  }

  async runMigrations(): Promise<number> {
    this.logger.log('Running migrations');

    const migrator = this.createMigrator();

    const { error, results = [] } = await migrator.migrateToLatest();

    for (const result of results) {
      if (result.status === 'Success') {
        this.logger.log(`Migration "${result.migrationName}" succeeded`);
      } else if (result.status === 'Error') {
        this.logger.warn(`Migration "${result.migrationName}" failed`);
      }
    }

    if (error) {
      this.logger.error(`Migrations failed: ${error}`);

      const missing =
        error instanceof Error ? error.message.match(/previously executed migration (.+) is missing/u) : null;
      if (missing) {
        throw new Error(
          `Migration "${missing[1]}" was already applied to this database but is not in this version of Immich (${serverVersion}). ` +
            `This usually means the database was migrated by a newer version. Downgrades are not supported.`,
          { cause: error },
        );
      }

      throw error;
    }

    this.logger.log('Finished running migrations');
    return results.length;
  }

  async migrateFilePaths(sourceFolder: string, targetFolder: string): Promise<void> {
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

    await this.db.transaction().execute(async (tx) => {
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
    });
  }

  async withLock<R>(lock: DatabaseLock, callback: () => Promise<R>): Promise<R> {
    let res;
    await this.asyncLock.acquire(DatabaseLock[lock], async () => {
      await this.db.connection().execute(async (connection) => {
        try {
          await this.acquireLock(lock, connection);
          res = await callback();
        } finally {
          await this.releaseLock(lock, connection);
        }
      });
    });

    return res as R;
  }

  tryLock(lock: DatabaseLock): Promise<boolean> {
    return this.db.connection().execute(async (connection) => this.acquireTryLock(lock, connection));
  }

  isBusy(lock: DatabaseLock): boolean {
    return this.asyncLock.isBusy(DatabaseLock[lock]);
  }

  async wait(lock: DatabaseLock): Promise<void> {
    await this.asyncLock.acquire(DatabaseLock[lock], () => {});
  }

  private async acquireLock(lock: DatabaseLock, connection: Kysely<DB>): Promise<void> {
    await sql`SELECT pg_advisory_lock(${lock})`.execute(connection);
  }

  private async acquireTryLock(lock: DatabaseLock, connection: Kysely<DB>): Promise<boolean> {
    const { rows } = await sql<{
      pg_try_advisory_lock: boolean;
    }>`SELECT pg_try_advisory_lock(${lock})`.execute(connection);
    return rows[0].pg_try_advisory_lock;
  }

  private async releaseLock(lock: DatabaseLock, connection: Kysely<DB>): Promise<void> {
    await sql`SELECT pg_advisory_unlock(${lock})`.execute(connection);
  }

  private createMigrator(): Migrator {
    return new Migrator({
      db: this.db,
      migrationLockTableName: 'kysely_migrations_lock',
      allowUnorderedMigrations: this.configRepository.isDev(),
      migrationTableName: 'kysely_migrations',
      provider: new FileMigrationProvider({
        fs: { readdir },
        path: { join },
        import: (filePath) => import(filePath),
        migrationFolder: join(import.meta.dirname, '..', 'schema/migrations'),
      }),
    });
  }
}
