import { Injectable } from '@nestjs/common';
import { SchedulerRegistry } from '@nestjs/schedule';
import { CronJob, CronTime } from 'cron';
import { Context, Effect } from 'effect';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runSync } from 'src/effect/run.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';

type CronBase = {
  name: string;
  start?: boolean;
};

export type CronCreate = CronBase & {
  expression: string;
  onTick: () => void;
};

export type CronUpdate = CronBase & {
  expression?: string;
};

export class CronRepo extends Context.Service<
  CronRepo,
  {
    create(options: CronCreate): Effect.Effect<void, LibraryError>;
    update(options: CronUpdate): Effect.Effect<void, LibraryError>;
  }
>()('immich/repositories/CronRepo') {
  static readonly make = (schedulerRegistry: SchedulerRegistry) =>
    CronRepo.of({
      create: Effect.fn('Cron.create')(function* ({ name, expression, onTick, start = true }: CronCreate) {
        yield* Effect.try({
          try: () => {
            const job = new CronJob<null, null>(
              expression,
              onTick,
              // function to run onComplete
              undefined,
              // whether it should start directly
              start,
              // timezone
              undefined,
              // context
              undefined,
              // runOnInit
              undefined,
              // utcOffset
              undefined,
              // prevents memory leaking by automatically stopping when the node process finishes
              true,
            );

            schedulerRegistry.addCronJob(name, job);
          },
          catch: (cause) => new LibraryError({ cause }),
        });
      }),

      update: Effect.fn('Cron.update')(function* ({ name, expression, start }: CronUpdate) {
        yield* Effect.try({
          try: () => {
            const job = schedulerRegistry.getCronJob(name);
            if (expression) {
              job.setTime(new CronTime(expression));
            }
            if (start !== undefined) {
              if (start) {
                job.start();
              } else {
                void job.stop();
              }
            }
          },
          catch: (cause) => new LibraryError({ cause }),
        });
      }),
    });
}

/** Adapter for the Nest services. Delegates to `CronRepo`; stays synchronous. */
@Injectable()
export class CronRepository {
  private readonly effect: Context.Service.Shape<typeof CronRepo>;

  constructor(
    schedulerRegistry: SchedulerRegistry,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(CronRepository.name);
    this.effect = CronRepo.make(schedulerRegistry);
    registerEffect(this, this.effect);
  }

  create(options: CronCreate): void {
    runSync(this.effect.create(options));
  }

  update(options: CronUpdate): void {
    runSync(this.effect.update(options));
  }
}
