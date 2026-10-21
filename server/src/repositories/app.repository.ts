import { Injectable } from '@nestjs/common';
import { createAdapter } from '@socket.io/redis-adapter';
import { Context, Effect, Layer } from 'effect';
import { Redis } from 'ioredis';
import { Server as SocketIO } from 'socket.io';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { ExitCode } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { AppRestartEvent } from 'src/repositories/event.repository.js';

export class AppRepo extends Context.Service<
  AppRepo,
  {
    exitApp(): Effect.Effect<void>;
    setCloseFn(fn: () => Promise<void>): Effect.Effect<void>;
    sendOneShotAppRestart(state: AppRestartEvent): Effect.Effect<void, LibraryError>;
  }
>()('immich/repositories/AppRepo') {
  /** Each instance owns its own `closeFn` slot, like the per-instance field on the repository it replaces. */
  static readonly make = () => {
    let closeFn: (() => Promise<void>) | undefined;

    return AppRepo.of({
      exitApp: Effect.fn('App.exitApp')(() =>
        Effect.sync(() => {
          /* eslint-disable unicorn/no-process-exit */
          void closeFn?.().finally(() => process.exit(ExitCode.AppRestart));

          // in exceptional circumstance, the application may hang
          setTimeout(() => process.exit(ExitCode.AppRestart), 2000);
          /* eslint-enable unicorn/no-process-exit */
        }),
      ),

      setCloseFn: Effect.fn('App.setCloseFn')((fn: () => Promise<void>) =>
        Effect.sync(() => {
          closeFn = fn;
        }),
      ),

      sendOneShotAppRestart: Effect.fn('App.sendOneShotAppRestart')((state: AppRestartEvent) =>
        Effect.tryPromise({
          try: async () => {
            const server = new SocketIO();
            const { redis } = new ConfigRepository().getEnv();
            const pubClient = new Redis({ ...redis, lazyConnect: true });
            const subClient = pubClient.duplicate();

            await Promise.all([pubClient.connect(), subClient.connect()]);

            server.adapter(createAdapter(pubClient, subClient));

            // => corresponds to notification.service.ts#onAppRestart
            server.emit('AppRestartV1', state, async () => {
              const responses = await server.serverSideEmitWithAck('AppRestart', state);
              if (responses.some((response) => response !== 'ok')) {
                throw new Error("One or more node(s) returned a non-'ok' response to our restart request!");
              }

              pubClient.disconnect();
              subClient.disconnect();
            });
          },
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),
    });
  };

  static readonly layer = Layer.sync(AppRepo, () => AppRepo.make());
}

/**
 * Promise-facing adapter for the Nest services. Delegates to `AppRepo`.
 * `effect` is optional and created lazily so the class keeps the same (all-optional) private surface as before,
 * which the specs rely on when mocking it.
 */
@Injectable()
export class AppRepository {
  private effect?: Context.Service.Shape<typeof AppRepo>;

  constructor() {
    this.effect = AppRepo.make();
    registerEffect(this, this.effect);
  }

  exitApp() {
    runSync((this.effect ??= AppRepo.make()).exitApp());
  }

  setCloseFn(fn: () => Promise<void>) {
    runSync((this.effect ??= AppRepo.make()).setCloseFn(fn));
  }

  async sendOneShotAppRestart(state: AppRestartEvent): Promise<void> {
    await runPromise((this.effect ??= AppRepo.make()).sendOneShotAppRestart(state));
  }
}
