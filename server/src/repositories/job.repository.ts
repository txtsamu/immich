import { getQueueToken } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { ModuleRef, Reflector } from '@nestjs/core';
import { JobsOptions, Queue, Worker } from 'bullmq';
import { Context, Effect } from 'effect';
import type { JobCounts, JobItem, JobOf } from 'src/types.js';
import { JobConfig } from 'src/decorators.js';
import { QueueJobResponseDto, QueueJobSearchDto } from 'src/dtos/queue.dto.js';
import { HandlerError, LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { ImmichWorker, JobName, JobStatus, MetadataKey, QueueCleanType, QueueJobStatus, QueueName } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { EventRepository } from 'src/repositories/event.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { ImmichStartupError, getKeyByValue, getMethodNames } from 'src/utils/misc.js';

type JobMapItem = {
  jobName: JobName;
  queueName: QueueName;
  handler: (job: JobOf<any>) => Promise<JobStatus>;
  label: string;
};

const WORKER_WATCH_INTERVAL_MS = 30_000;

const lib = (cause: unknown) => new LibraryError({ cause });

type JobDeps = {
  moduleRef: ModuleRef;
  configRepository: ConfigRepository;
  eventRepository: EventRepository;
  logger: LoggingRepository;
};

export class JobRepo extends Context.Service<
  JobRepo,
  {
    setup(services: (new (...args: any[]) => unknown)[]): Effect.Effect<void, ImmichStartupError | LibraryError>;
    startWorkers(): Effect.Effect<void, LibraryError>;
    watchWorkers(): Effect.Effect<void>;
    teardown(): Effect.Effect<void>;
    run(item: JobItem): Effect.Effect<JobStatus, HandlerError>;
    setConcurrency(queueName: QueueName, concurrency: number): Effect.Effect<void>;
    isActive(name: QueueName): Effect.Effect<boolean, LibraryError>;
    isPaused(name: QueueName): Effect.Effect<boolean, LibraryError>;
    pause(name: QueueName): Effect.Effect<void, LibraryError>;
    resume(name: QueueName): Effect.Effect<void, LibraryError>;
    empty(name: QueueName): Effect.Effect<void, LibraryError>;
    clear(name: QueueName, type: QueueCleanType): Effect.Effect<string[], LibraryError>;
    getJobCounts(name: QueueName): Effect.Effect<JobCounts, LibraryError>;
    queueAll(items: JobItem[]): Effect.Effect<void, LibraryError>;
    queue(item: JobItem): Effect.Effect<void, LibraryError>;
    waitForQueueCompletion(...queues: QueueName[]): Effect.Effect<void, LibraryError>;
    searchJobs(name: QueueName, dto: QueueJobSearchDto): Effect.Effect<QueueJobResponseDto[], LibraryError>;
    removeJob(name: JobName, jobID: string): Effect.Effect<void, LibraryError>;
  }
>()('immich/repositories/JobRepo') {
  static readonly make = ({ moduleRef, configRepository, eventRepository, logger }: JobDeps) => {
    // mutable runtime state, owned by this service instance
    const workers: Partial<Record<QueueName, Worker>> = {};
    const handlers: Partial<Record<JobName, JobMapItem>> = {};
    let workerWatcher: ReturnType<typeof setInterval> | undefined;
    let microservicesPresent = true;

    const getQueue = (queue: QueueName): Queue => moduleRef.get<Queue>(getQueueToken(queue), { strict: false });

    const getQueueName = (name: JobName) => (handlers[name] as JobMapItem).queueName;

    const getJobOptions = (item: JobItem): JobsOptions | null => {
      switch (item.name) {
        case JobName.NotifyAlbumUpdate: {
          return {
            jobId: `${item.data.id}/${item.data.recipientId}`,
            delay: item.data?.delay,
          };
        }
        case JobName.StorageTemplateMigrationSingle: {
          return { jobId: item.data.id };
        }
        case JobName.VersionCheck: {
          return { deduplication: { id: JobName.VersionCheck } };
        }
        case JobName.DatabaseBackup: {
          return { deduplication: { id: JobName.DatabaseBackup } };
        }
        default: {
          return null;
        }
      }
    };

    const isActive = Effect.fn('Job.isActive')(function* (name: QueueName) {
      const count = yield* Effect.tryPromise({ try: () => getQueue(name).getActiveCount(), catch: lib });
      return count > 0;
    });

    const checkWorkers = Effect.fn('Job.checkWorkers')(function* () {
      const suffix = `:w:${ImmichWorker.Microservices}`;
      const found = yield* Effect.tryPromise({
        try: () => getQueue(QueueName.BackgroundTask).getWorkers(),
        catch: lib,
      }).pipe(Effect.option);
      if (found._tag === 'None') {
        return;
      }

      const isPresent = found.value.some((worker) => worker.rawname?.endsWith(suffix));
      if (microservicesPresent !== isPresent) {
        if (isPresent) {
          logger.log('Microservices worker connected.');
        } else {
          logger.warn(
            'No microservices worker is connected. Background jobs will not be processed until one is running.',
          );
        }
      }
      microservicesPresent = isPresent;
    });

    const queueAll = Effect.fn('Job.queueAll')(function* (items: JobItem[]) {
      if (items.length === 0) {
        return;
      }

      // the queue calls are started together (as the original did) and awaited as one
      const pending = yield* Effect.try({
        try: () => {
          const promises: Array<Promise<unknown>> = [];
          const itemsByQueue = {} as Record<string, (JobItem & { data: any; options: JobsOptions | undefined })[]>;
          for (const item of items) {
            const queueName = getQueueName(item.name);
            const job = {
              name: item.name,
              data: item.data || {},
              options: getJobOptions(item) || undefined,
            } as JobItem & { data: any; options: JobsOptions | undefined };

            if (job.options?.jobId || job.options?.deduplication) {
              // need to use add() instead of addBulk() for jobId/deduplication to take effect
              promises.push(getQueue(queueName).add(item.name, item.data, job.options));
            } else {
              itemsByQueue[queueName] ||= [];
              itemsByQueue[queueName].push(job);
            }
          }

          for (const [queueName, jobs] of Object.entries(itemsByQueue)) {
            const queue = getQueue(queueName as QueueName);
            promises.push(queue.addBulk(jobs));
          }

          return promises;
        },
        catch: lib,
      });

      yield* Effect.tryPromise({ try: () => Promise.all(pending), catch: lib });
    });

    return JobRepo.of({
      setup: Effect.fn('Job.setup')(function* (services) {
        // discovery
        for (const Service of services) {
          const found = yield* Effect.try({
            try: () => {
              const reflector = moduleRef.get(Reflector, { strict: false });
              const instance = moduleRef.get<any>(Service);
              const entries: Array<{ handler: any; config: JobConfig; instance: any }> = [];
              for (const methodName of getMethodNames(instance)) {
                const handler = instance[methodName];
                const config = reflector.get<JobConfig>(MetadataKey.JobConfig, handler);
                if (config) {
                  entries.push({ handler, config, instance });
                }
              }
              return entries;
            },
            catch: lib,
          });

          for (const { handler, config, instance } of found) {
            const { name: jobName, queue: queueName } = config;
            const label = `${Service.name}.${handler.name}`;

            // one handler per job
            if (Object.hasOwn(handlers, jobName)) {
              const jobKey = getKeyByValue(JobName, jobName);
              const errorMessage = `Failed to add job handler for ${label}`;
              logger.error(`${errorMessage}. JobName.${jobKey} is already handled by ${handlers[jobName]!.label}.`);
              return yield* Effect.fail(new ImmichStartupError(errorMessage));
            }

            handlers[jobName] = {
              label,
              jobName,
              queueName,
              handler: handler.bind(instance),
            };

            logger.verbose(`Added job handler: ${jobName} => ${label}`);
          }
        }

        // no missing handlers
        for (const [jobKey, jobName] of Object.entries(JobName)) {
          const item = handlers[jobName];
          if (!item) {
            const errorMessage = `Failed to find job handler for Job.${jobKey} ("${jobName}")`;
            logger.error(
              `${errorMessage}. Make sure to add the @OnJob({ name: JobName.${jobKey}, queue: QueueName.XYZ }) decorator for the new job.`,
            );
            return yield* Effect.fail(new ImmichStartupError(errorMessage));
          }
        }
      }),

      startWorkers: Effect.fn('Job.startWorkers')(function* () {
        yield* Effect.try({
          try: () => {
            const { bull } = configRepository.getEnv();
            for (const queueName of Object.values(QueueName)) {
              logger.debug(`Starting worker for queue: ${queueName}`);
              workers[queueName] = new Worker(
                queueName,
                (job) => eventRepository.emit('JobRun', queueName, job as JobItem),
                { ...bull.config, concurrency: 1, name: ImmichWorker.Microservices },
              );
            }
          },
          catch: lib,
        });
      }),

      watchWorkers: Effect.fn('Job.watchWorkers')(function* () {
        // the interval callback is fire-and-forget, exactly like the original `void this.checkWorkers()`
        yield* Effect.sync(() => {
          workerWatcher ??= setInterval(() => void Effect.runFork(checkWorkers()), WORKER_WATCH_INTERVAL_MS);
        });
      }),

      teardown: Effect.fn('Job.teardown')(function* () {
        yield* Effect.sync(() => {
          if (!workerWatcher) {
            return;
          }

          clearInterval(workerWatcher);
          workerWatcher = undefined;
        });
      }),

      run: Effect.fn('Job.run')(function* ({ name, data }: JobItem) {
        const item = handlers[name as JobName];
        if (!item) {
          logger.warn(`Skipping unknown job: "${name}"`);
          return JobStatus.Skipped;
        }

        return yield* Effect.tryPromise({
          try: async () => item.handler(data),
          catch: (cause) => new HandlerError({ cause }),
        });
      }),

      setConcurrency: Effect.fn('Job.setConcurrency')(function* (queueName: QueueName, concurrency: number) {
        yield* Effect.sync(() => {
          const worker = workers[queueName];
          if (!worker) {
            logger.warn(`Unable to set queue concurrency, worker not found: '${queueName}'`);
            return;
          }

          worker.concurrency = concurrency;
        });
      }),

      isActive,

      isPaused: Effect.fn('Job.isPaused')((name: QueueName) =>
        Effect.tryPromise({ try: async () => getQueue(name).isPaused(), catch: lib }),
      ),

      pause: Effect.fn('Job.pause')((name: QueueName) =>
        Effect.tryPromise({ try: async () => getQueue(name).pause(), catch: lib }),
      ),

      resume: Effect.fn('Job.resume')((name: QueueName) =>
        Effect.tryPromise({ try: async () => getQueue(name).resume(), catch: lib }),
      ),

      empty: Effect.fn('Job.empty')((name: QueueName) =>
        Effect.tryPromise({ try: async () => getQueue(name).drain(), catch: lib }),
      ),

      clear: Effect.fn('Job.clear')((name: QueueName, type: QueueCleanType) =>
        Effect.tryPromise({ try: async () => getQueue(name).clean(0, 1000, type), catch: lib }),
      ),

      getJobCounts: Effect.fn('Job.getJobCounts')((name: QueueName) =>
        Effect.tryPromise({
          try: async () =>
            (await getQueue(name).getJobCounts(
              'active',
              'completed',
              'failed',
              'delayed',
              'waiting',
            )) as unknown as JobCounts,
          catch: lib,
        }),
      ),

      queueAll,

      queue: Effect.fn('Job.queue')((item: JobItem) => queueAll([item])),

      waitForQueueCompletion: Effect.fn('Job.waitForQueueCompletion')(function* (...queues: QueueName[]) {
        const getPending = Effect.fn('Job.getPending')(function* () {
          const results = yield* Effect.forEach(
            queues,
            (name) => isActive(name).pipe(Effect.map((pending) => ({ pending, name }))),
            { concurrency: 'unbounded' },
          );
          return results.filter(({ pending }) => pending).map(({ name }) => name);
        });

        let pending = yield* getPending();

        while (pending.length > 0) {
          logger.verbose(`Waiting for ${pending[0]} queue to stop...`);
          yield* Effect.sleep(1000);
          pending = yield* getPending();
        }
      }),

      searchJobs: Effect.fn('Job.searchJobs')(function* (name: QueueName, dto: QueueJobSearchDto) {
        const jobs = yield* Effect.tryPromise({
          try: async () => getQueue(name).getJobs(dto.status ?? Object.values(QueueJobStatus), 0, 1000),
          catch: lib,
        });
        return jobs.map((job): QueueJobResponseDto => {
          const { id, name, timestamp, data } = job;
          return { id, name: name as JobName, timestamp, data };
        });
      }),

      // todo: remove this when asset notifications no longer need it.
      removeJob: Effect.fn('Job.removeJob')(function* (name: JobName, jobID: string) {
        const existingJob = yield* Effect.tryPromise({
          try: async () => getQueue(getQueueName(name)).getJob(jobID),
          catch: lib,
        });
        if (existingJob) {
          yield* Effect.tryPromise({ try: () => existingJob.remove(), catch: lib });
        }
      }),
    });
  };
}

/** Promise-facing adapter for the Nest services. Delegates to `JobRepo`. */
@Injectable()
export class JobRepository {
  private readonly effect: Context.Service.Shape<typeof JobRepo>;

  constructor(
    moduleRef: ModuleRef,
    configRepository: ConfigRepository,
    eventRepository: EventRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(JobRepository.name);
    this.effect = JobRepo.make({ moduleRef, configRepository, eventRepository, logger: this.logger });
  }

  setup(services: (new (...args: any[]) => unknown)[]) {
    runSync(this.effect.setup(services));
  }

  startWorkers() {
    runSync(this.effect.startWorkers());
  }

  watchWorkers() {
    runSync(this.effect.watchWorkers());
  }

  teardown() {
    runSync(this.effect.teardown());
  }

  run(item: JobItem) {
    return runPromise(this.effect.run(item));
  }

  setConcurrency(queueName: QueueName, concurrency: number) {
    runSync(this.effect.setConcurrency(queueName, concurrency));
  }

  isActive(name: QueueName): Promise<boolean> {
    return runPromise(this.effect.isActive(name));
  }

  isPaused(name: QueueName): Promise<boolean> {
    return runPromise(this.effect.isPaused(name));
  }

  pause(name: QueueName) {
    return runPromise(this.effect.pause(name));
  }

  resume(name: QueueName) {
    return runPromise(this.effect.resume(name));
  }

  empty(name: QueueName) {
    return runPromise(this.effect.empty(name));
  }

  clear(name: QueueName, type: QueueCleanType) {
    return runPromise(this.effect.clear(name, type));
  }

  getJobCounts(name: QueueName): Promise<JobCounts> {
    return runPromise(this.effect.getJobCounts(name));
  }

  queueAll(items: JobItem[]): Promise<void> {
    return runPromise(this.effect.queueAll(items));
  }

  queue(item: JobItem): Promise<void> {
    return runPromise(this.effect.queue(item));
  }

  waitForQueueCompletion(...queues: QueueName[]): Promise<void> {
    return runPromise(this.effect.waitForQueueCompletion(...queues));
  }

  searchJobs(name: QueueName, dto: QueueJobSearchDto): Promise<QueueJobResponseDto[]> {
    return runPromise(this.effect.searchJobs(name, dto));
  }

  /** @deprecated */
  // todo: remove this when asset notifications no longer need it.
  public removeJob(name: JobName, jobID: string): Promise<void> {
    return runPromise(this.effect.removeJob(name, jobID));
  }
}
