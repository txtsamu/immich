import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { isEqual } from 'lodash-es';
import type { ArgOf } from 'src/repositories/event.repository.js';
import { OnEvent } from 'src/decorators.js';
import {
  AdminConfigDto,
  PublicConfigDto,
  UserConfigDto,
  defaults,
  mapAdminConfig,
  mapPublicConfig,
  mapUserConfig,
} from 'src/dtos/config.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import { BootstrapEventPriority } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { clearConfigCache } from 'src/utils/config.js';
import { toPlainObject } from 'src/utils/object.js';

/** The original thrown value behind an Effect error that wraps it in `cause` (see `src/effect/errors.ts`). */
@Injectable()
export class SystemConfigService extends BaseService {
  @OnEvent({ name: 'AppBootstrap', priority: BootstrapEventPriority.SystemConfig })
  async onBootstrap() {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        yield* this.fx.event.emit('ConfigInit', { newConfig: config });
      }),
    );
  }

  async getAdminConfig(): Promise<AdminConfigDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        return mapAdminConfig(config);
      }),
    );
  }

  getAdminConfigDefaults(): AdminConfigDto {
    return mapAdminConfig(defaults);
  }

  async getUserConfig(): Promise<UserConfigDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        return mapUserConfig(config);
      }),
    );
  }

  getUserConfigDefaults(): UserConfigDto {
    return mapUserConfig(defaults);
  }

  async getPublicConfig(): Promise<PublicConfigDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        return mapPublicConfig(config);
      }),
    );
  }

  getPublicConfigDefaults(): PublicConfigDto {
    return mapPublicConfig(defaults);
  }

  @OnEvent({ name: 'ConfigInit', priority: -100 })
  onConfigInit({ newConfig: { logging } }: ArgOf<'ConfigInit'>) {
    const { logLevel: envLevel } = this.configRepository.getEnv();
    const configLevel = logging.enabled && logging.level;
    const level = envLevel ?? configLevel;
    this.logger.setLogLevel(level);
    this.logger.log(`LogLevel=${level} ${envLevel ? '(set via IMMICH_LOG_LEVEL)' : '(set via system config)'}`);
  }

  @OnEvent({ name: 'ConfigUpdate', server: true })
  onConfigUpdate({ newConfig }: ArgOf<'ConfigUpdate'>) {
    this.onConfigInit({ newConfig });
    clearConfigCache();
  }

  @OnEvent({ name: 'ConfigValidate' })
  onConfigValidate({ newConfig, oldConfig }: ArgOf<'ConfigValidate'>) {
    const { logLevel } = this.configRepository.getEnv();
    if (logLevel && !isEqual(toPlainObject(newConfig.logging), oldConfig.logging)) {
      throw new Error('Logging cannot be changed while the environment variable IMMICH_LOG_LEVEL is set.');
    }
  }

  async updateAdminConfig(dto: AdminConfigDto): Promise<AdminConfigDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { configFile } = this.configRepository.getEnv();
        if (configFile) {
          return yield* Effect.fail(
            new BadRequestException('Cannot update configuration while IMMICH_CONFIG_FILE is in use'),
          );
        }

        const oldConfig = yield* this.getConfigFx({ withCache: false });

        yield* this.fx.event.emit('ConfigValidate', { newConfig: toPlainObject(dto), oldConfig }).pipe(
          Effect.catch((error_) => {
            const error = unwrap(error_);
            this.logger.warn(`Unable to save system config due to a validation error: ${error}`);
            return Effect.fail(new BadRequestException(error instanceof Error ? error.message : (error as string)));
          }),
        );

        const newConfig = yield* this.updateConfigFx(dto);

        yield* this.fx.event.emit('ConfigUpdate', { newConfig, oldConfig });

        return mapAdminConfig(newConfig);
      }),
    );
  }

  async getCustomCss(): Promise<string> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { theme } = yield* this.getConfigFx({ withCache: false });
        return theme.customCss;
      }),
    );
  }
}
