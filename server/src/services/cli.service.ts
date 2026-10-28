import { schemaDiff } from '@immich/sql-tools';
import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { isAbsolute, join } from 'node:path';
import { SALT_ROUNDS } from 'src/constants.js';
import { MaintenanceAuthDto } from 'src/dtos/maintenance.dto.js';
import { UserAdminResponseDto, mapUserAdmin } from 'src/dtos/user.dto.js';
import { runPromise } from 'src/effect/run.js';
import { MaintenanceAction, SystemMetadataKey } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { createMaintenanceLoginUrl, generateMaintenanceSecret } from 'src/utils/maintenance.js';
import { getExternalDomain } from 'src/utils/misc.js';

export type SchemaReport = {
  migrations: MigrationStatus[];
  drift: ReturnType<typeof schemaDiff>;
};

type MigrationStatus = {
  name: string;
  status: 'applied' | 'missing' | 'deleted';
};

@Injectable()
export class CliService extends BaseService {
  async schemaReport(): Promise<SchemaReport> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const allFiles = yield* this.fx.storage.readdir(join(import.meta.dirname, '../schema/migrations'));
        const files = allFiles.filter((file) => file.endsWith('.js')).map((file) => file.slice(0, -3));
        const rows = yield* this.fx.database.getMigrations();
        const filesSet = new Set(files);
        const rowsSet = new Set(rows.map((item) => item.name));
        const combined = [...filesSet, ...rowsSet].toSorted();

        const migrations: MigrationStatus[] = [];

        for (const name of combined) {
          if (filesSet.has(name) && rowsSet.has(name)) {
            migrations.push({ name, status: 'applied' });
            continue;
          }

          if (filesSet.has(name) && !rowsSet.has(name)) {
            migrations.push({ name, status: 'missing' });
            continue;
          }

          if (!filesSet.has(name) && rowsSet.has(name)) {
            migrations.push({ name, status: 'deleted' });
          }
        }

        const drift = yield* this.fx.database.getSchemaDrift();

        return { migrations, drift };
      }),
    );
  }

  async listUsers(): Promise<UserAdminResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const users = yield* this.fx.user.getList({ withDeleted: true });
        return users.map((user) => mapUserAdmin(user));
      }),
    );
  }

  async resetAdminPassword(
    ask: (admin: UserAdminResponseDto) => Promise<{ newPassword: string | undefined; invalidateSessions: boolean }>,
  ) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const admin = yield* this.fx.user.getAdmin();
        if (!admin) {
          return yield* Effect.fail(new Error('Admin account does not exist'));
        }

        const { newPassword: providedPassword, invalidateSessions } = yield* Effect.tryPromise({
          try: () => ask(mapUserAdmin(admin)),
          catch: (cause) => cause,
        });
        const password = providedPassword || (yield* this.fx.crypto.randomBytesAsText(24));
        const hashedPassword = yield* this.fx.crypto.hashBcrypt(password, SALT_ROUNDS);

        yield* this.fx.user.update(admin.id, { password: hashedPassword });

        if (invalidateSessions) {
          yield* this.fx.session.invalidateAll({ userId: admin.id });
        }

        return { admin, password, provided: !!providedPassword };
      }),
    );
  }

  async disablePasswordLogin(): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        config.passwordLogin.enabled = false;
        yield* this.updateConfigFx(config);
      }),
    );
  }

  async enablePasswordLogin(): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        config.passwordLogin.enabled = true;
        yield* this.updateConfigFx(config);
      }),
    );
  }

  async disableMaintenanceMode(): Promise<{ alreadyDisabled: boolean }> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const currentState = (yield* this.fx.systemMetadata.get(SystemMetadataKey.MaintenanceMode)) ?? {
          isMaintenanceMode: false as const,
        };

        if (!currentState.isMaintenanceMode) {
          return {
            alreadyDisabled: true,
          };
        }

        const state = { isMaintenanceMode: false as const };
        yield* this.fx.systemMetadata.set(SystemMetadataKey.MaintenanceMode, state);
        yield* this.fx.app.sendOneShotAppRestart(state);

        return {
          alreadyDisabled: false,
        };
      }),
    );
  }

  async enableMaintenanceMode(): Promise<{ authUrl: string; alreadyEnabled: boolean }> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { server } = yield* this.getConfigFx({ withCache: true });
        const baseUrl = getExternalDomain(server);

        const payload: MaintenanceAuthDto = {
          username: 'cli-admin',
        };

        const state = (yield* this.fx.systemMetadata.get(SystemMetadataKey.MaintenanceMode)) ?? {
          isMaintenanceMode: false as const,
        };

        if (state.isMaintenanceMode) {
          return {
            authUrl: yield* Effect.tryPromise({
              try: () => createMaintenanceLoginUrl(baseUrl, payload, state.secret),
              catch: (cause) => cause,
            }),
            alreadyEnabled: true,
          };
        }

        const secret = generateMaintenanceSecret();

        yield* this.fx.systemMetadata.set(SystemMetadataKey.MaintenanceMode, {
          isMaintenanceMode: true,
          secret,
          action: {
            action: MaintenanceAction.Start,
          },
        });

        yield* this.fx.app.sendOneShotAppRestart({
          isMaintenanceMode: true,
        });

        return {
          authUrl: yield* Effect.tryPromise({
            try: () => createMaintenanceLoginUrl(baseUrl, payload, secret),
            catch: (cause) => cause,
          }),
          alreadyEnabled: false,
        };
      }),
    );
  }

  async grantAdminAccess(email: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.fx.user.getByEmail(email);
        if (!user) {
          return yield* Effect.fail(new Error('User does not exist'));
        }

        yield* this.fx.user.update(user.id, { isAdmin: true });
      }),
    );
  }

  async revokeAdminAccess(email: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.fx.user.getByEmail(email);
        if (!user) {
          return yield* Effect.fail(new Error('User does not exist'));
        }

        yield* this.fx.user.update(user.id, { isAdmin: false });
      }),
    );
  }

  async disableOAuthLogin(): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        config.oauth.enabled = false;
        yield* this.updateConfigFx(config);
      }),
    );
  }

  async enableOAuthLogin(): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        config.oauth.enabled = true;
        yield* this.updateConfigFx(config);
      }),
    );
  }

  async getSampleFilePaths(): Promise<string[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const [assets, users] = yield* Effect.all([this.fx.asset.getFileSamples(), this.fx.user.getFileSamples()], {
          concurrency: 'unbounded',
        });

        const paths: string[] = Array.from(users, (user) => user.profileImagePath);

        for (const asset of assets) {
          paths.push(asset.path);
        }

        return paths.filter(Boolean) as string[];
      }),
    );
  }

  async migrateFilePaths({
    oldValue,
    newValue,
    confirm,
  }: {
    oldValue: string;
    newValue: string;
    confirm: (data: { sourceFolder: string; targetFolder: string }) => Promise<boolean>;
  }): Promise<boolean> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        let sourceFolder = oldValue;
        if (sourceFolder.startsWith('./')) {
          sourceFolder = sourceFolder.slice(2);
        }

        const targetFolder = newValue;
        if (!isAbsolute(targetFolder)) {
          return yield* Effect.fail(new Error('Target media location must be an absolute path'));
        }

        const confirmed = yield* Effect.tryPromise({
          try: () => confirm({ sourceFolder, targetFolder }),
          catch: (cause) => cause,
        });
        if (!confirmed) {
          return false;
        }

        yield* this.fx.database.migrateFilePaths(sourceFolder, targetFolder);

        return true;
      }),
    );
  }

  cleanup() {
    return runPromise(this.fx.database.shutdown());
  }
}
