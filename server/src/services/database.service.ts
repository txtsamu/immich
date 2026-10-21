import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { coerce, eq, gt, lt, satisfies } from 'semver';
import type { VectorExtension } from 'src/types.js';
import { EXTENSION_NAMES, ErrorMessages, VECTOR_EXTENSIONS } from 'src/constants.js';
import { OnEvent } from 'src/decorators.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import { BootstrapEventPriority, DatabaseExtension, DatabaseLock } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';

type CreateFailedArgs = { name: string; extension: string };
type UpdateFailedArgs = { name: string; extension: string; availableVersion: string };
type DropFailedArgs = { name: string; extension: string };
type NightlyVersionArgs = { name: string; extension: string; version: string };
type OutOfRangeArgs = { name: string; extension: string; version: string; range: string };
type InvalidDowngradeArgs = { name: string; extension: string; installedVersion: string; availableVersion: string };

/** The original thrown value behind an Effect error that wraps it in `cause` (see `src/effect/errors.ts`). */
const messages = {
  notInstalled: (name: string) =>
    `The ${name} extension is not available in this Postgres instance.
    If using a container image, ensure the image has the extension installed.`,
  nightlyVersion: ({ name, extension, version }: NightlyVersionArgs) => `
    The ${name} extension version is ${version}, which means it is a nightly release.

    Please run 'DROP EXTENSION IF EXISTS ${extension}' and switch to a release version.
    See https://docs.immich.app/guides/database-queries for how to query the database.`,
  outOfRange: ({ name, version, range }: OutOfRangeArgs) =>
    `The ${name} extension version is ${version}, but Immich only supports ${range}.
    Please change ${name} to a compatible version in the Postgres instance.`,
  createFailed: ({ name, extension }: CreateFailedArgs) =>
    `Failed to activate ${name} extension.
    Please ensure the Postgres instance has ${name} installed.

    If the Postgres instance already has ${name} installed, Immich may not have the necessary permissions to activate it.
    In this case, please run 'CREATE EXTENSION IF NOT EXISTS ${extension} CASCADE' manually as a superuser.
    See https://docs.immich.app/guides/database-queries for how to query the database.`,
  updateFailed: ({ name, extension, availableVersion }: UpdateFailedArgs) =>
    `The ${name} extension can be updated to ${availableVersion}.
    Immich attempted to update the extension, but failed to do so.
    This may be because Immich does not have the necessary permissions to update the extension.

    Please run 'ALTER EXTENSION ${extension} UPDATE' manually as a superuser.
    See https://docs.immich.app/guides/database-queries for how to query the database.`,
  dropFailed: ({ name, extension }: DropFailedArgs) =>
    `The ${name} extension is no longer needed, but could not be dropped.
    This may be because Immich does not have the necessary permissions to drop the extension.

    Please run 'DROP EXTENSION ${extension};' manually as a superuser.
    See https://docs.immich.app/guides/database-queries for how to query the database.`,
  invalidDowngrade: ({ name, installedVersion, availableVersion }: InvalidDowngradeArgs) =>
    `The database currently has ${name} ${installedVersion} activated, but the Postgres instance only has ${availableVersion} available.
    This most likely means the extension was downgraded.
    If ${name} ${installedVersion} is compatible with Immich, please ensure the Postgres instance has this available.`,
};

@Injectable()
export class DatabaseService extends BaseService {
  @OnEvent({ name: 'AppBootstrap', priority: BootstrapEventPriority.DatabaseService })
  async onBootstrap() {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const version = yield* this.fx.database.getPostgresVersion();
        const current = coerce(version);
        const postgresRange = yield* this.fx.database.getPostgresVersionRange();
        if (!current || !satisfies(current, postgresRange)) {
          return yield* Effect.fail(
            new Error(
              `Invalid PostgreSQL version. Found ${version}, but needed ${postgresRange}. Please use a supported version.`,
            ),
          );
        }

        yield* this.fx.database.withLock(DatabaseLock.Migrations, () =>
          // the lock callback is Promise based: run the Effect and rethrow the original error
          runPromise(
            Effect.gen({ self: this }, function* () {
              const extension = yield* this.fx.database.getVectorExtension();
              const name = EXTENSION_NAMES[extension];
              const extensionRange = yield* this.fx.database.getExtensionVersionRange(extension);

              const extensionVersions = yield* this.fx.database.getExtensionVersions(VECTOR_EXTENSIONS);
              const { installedVersion, availableVersion } = extensionVersions.find((v) => v.name === extension) ?? {};
              if (!availableVersion) {
                return yield* Effect.fail(new Error(messages.notInstalled(name)));
              }

              if ([availableVersion, installedVersion].some((version) => version && eq(version, '0.0.0'))) {
                return yield* Effect.fail(new Error(messages.nightlyVersion({ name, extension, version: '0.0.0' })));
              }

              if (!satisfies(availableVersion, extensionRange)) {
                return yield* Effect.fail(
                  new Error(messages.outOfRange({ name, extension, version: availableVersion, range: extensionRange })),
                );
              }

              if (!installedVersion) {
                yield* this.createExtension(extension);
              }

              if (installedVersion && gt(availableVersion, installedVersion)) {
                yield* this.updateExtension(extension, availableVersion);
              } else if (installedVersion && !satisfies(installedVersion, extensionRange)) {
                return yield* Effect.fail(
                  new Error(messages.outOfRange({ name, extension, version: installedVersion, range: extensionRange })),
                );
              } else if (installedVersion && lt(availableVersion, installedVersion)) {
                return yield* Effect.fail(
                  new Error(messages.invalidDowngrade({ name, extension, availableVersion, installedVersion })),
                );
              }

              for (const { name: dbName, installedVersion } of extensionVersions) {
                const isDepended = dbName === DatabaseExtension.Vector && extension === DatabaseExtension.VectorChord;
                if (dbName !== extension && installedVersion && !isDepended) {
                  yield* this.dropExtension(dbName);
                }
              }

              const preparation: Effect.Effect<unknown, unknown>[] = [];
              const { database } = this.configRepository.getEnv();
              if (!database.skipMigrations) {
                const migrationCount = yield* this.fx.database.runMigrations();
                preparation.push(this.checkSchemaDrift());
                if (migrationCount > 0) {
                  preparation.push(this.fx.database.vacuum({ analyze: true }));
                }
              }
              yield* Effect.all(preparation, { concurrency: 'unbounded' });
            }),
          ),
        );
      }),
    );
  }

  private checkSchemaDrift() {
    return Effect.gen({ self: this }, function* () {
      this.logger.log('Checking for schema drift');
      const drift = yield* this.fx.database.getSchemaDrift();
      if (drift.items.length === 0) {
        this.logger.log('No schema drift detected');
      } else {
        this.logger.warn(`${ErrorMessages.SchemaDrift} or run \`immich-admin schema-check\``);
        for (const warning of drift.asHuman()) {
          this.logger.warn(`  - ${warning}`);
        }
      }
    });
  }

  private createExtension = Effect.fn('DatabaseService.createExtension')(function* (
    this: DatabaseService,
    extension: DatabaseExtension,
  ) {
    yield* this.fx.database.createExtension(extension).pipe(
      Effect.tapError(() =>
        Effect.sync(() => {
          const name = EXTENSION_NAMES[extension];
          this.logger.fatal(messages.createFailed({ name, extension }));
        }),
      ),
    );
  });

  private updateExtension = Effect.fn('DatabaseService.updateExtension')(function* (
    this: DatabaseService,
    extension: VectorExtension,
    availableVersion: string,
  ) {
    this.logger.log(`Updating ${EXTENSION_NAMES[extension]} extension to ${availableVersion}`);
    yield* this.fx.database.updateVectorExtension(extension, availableVersion).pipe(
      Effect.tapError(() =>
        Effect.sync(() => {
          this.logger.warn(messages.updateFailed({ name: EXTENSION_NAMES[extension], extension, availableVersion }));
        }),
      ),
    );
  });

  private dropExtension = Effect.fn('DatabaseService.dropExtension')(function* (
    this: DatabaseService,
    extension: DatabaseExtension,
  ) {
    yield* this.fx.database.dropExtension(extension).pipe(
      Effect.catch((error) =>
        Effect.sync(() => {
          const name = EXTENSION_NAMES[extension];
          this.logger.warn(messages.dropFailed({ name, extension }), unwrap(error));
        }),
      ),
    );
  });
}
