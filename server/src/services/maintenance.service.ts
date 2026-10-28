import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { MaintenanceModeState } from 'src/types.js';
import { OnEvent } from 'src/decorators.js';
import {
  MaintenanceAuthDto,
  MaintenanceDetectInstallResponseDto,
  MaintenanceStatusResponseDto,
  SetMaintenanceModeDto,
} from 'src/dtos/maintenance.dto.js';
import { runPromise } from 'src/effect/run.js';
import { MaintenanceAction, SystemMetadataKey } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import {
  createMaintenanceLoginUrl,
  detectPriorInstallFx,
  generateMaintenanceSecret,
  signMaintenanceJwt,
} from 'src/utils/maintenance.js';
import { getExternalDomain } from 'src/utils/misc.js';

/**
 * This service is available outside of maintenance mode to manage maintenance mode
 */
@Injectable()
export class MaintenanceService extends BaseService {
  private getMaintenanceModeFx() {
    const { fx } = this;
    return Effect.gen(function* () {
      const state = yield* fx.systemMetadata.get(SystemMetadataKey.MaintenanceMode);
      return state ?? ({ isMaintenanceMode: false } as MaintenanceModeState);
    });
  }

  getMaintenanceMode(): Promise<MaintenanceModeState> {
    return runPromise(this.getMaintenanceModeFx());
  }

  getMaintenanceStatus(): MaintenanceStatusResponseDto {
    return {
      active: false,
      action: MaintenanceAction.End,
    };
  }

  detectPriorInstall(): Promise<MaintenanceDetectInstallResponseDto> {
    return runPromise(detectPriorInstallFx(this.fx.storage));
  }

  async startMaintenance(action: SetMaintenanceModeDto, username: string): Promise<{ jwt: string }> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const secret = generateMaintenanceSecret();
        yield* this.fx.systemMetadata.set(SystemMetadataKey.MaintenanceMode, {
          isMaintenanceMode: true,
          secret,
          action,
        });

        yield* this.fx.event.emit('AppRestart', { isMaintenanceMode: true });

        return {
          jwt: yield* Effect.tryPromise({
            try: () =>
              signMaintenanceJwt(secret, {
                username,
              }),
            catch: (cause) => cause,
          }),
        };
      }),
    );
  }

  async startRestoreFlow(): Promise<{ jwt: string }> {
    return this.startMaintenance(
      {
        action: MaintenanceAction.SelectDatabaseRestore,
      },
      'admin',
    );
  }

  @OnEvent({ name: 'AppRestart', server: true })
  onRestart(event: ArgOf<'AppRestart'>, ack?: (ok: 'ok') => void): void {
    this.logger.log(`Restarting due to event... ${JSON.stringify(event)}`);

    ack?.('ok');
    this.appRepository.exitApp();
  }

  async createLoginUrl(auth: MaintenanceAuthDto, secret?: string): Promise<string> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { server } = yield* this.getConfigFx({ withCache: true });
        const baseUrl = getExternalDomain(server);

        if (!secret) {
          const state = yield* this.getMaintenanceModeFx();
          if (!state.isMaintenanceMode) {
            return yield* Effect.fail(new Error('Not in maintenance mode'));
          }

          secret = state.secret;
        }

        const loginSecret = secret;
        return yield* Effect.tryPromise({
          try: () => createMaintenanceLoginUrl(baseUrl, auth, loginSecret),
          catch: (cause) => cause,
        });
      }),
    );
  }
}
