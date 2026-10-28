import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { ConcurrentQueueName, JobItem } from 'src/types.js';
import { OnEvent } from 'src/decorators.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { SystemConfig } from 'src/dtos/config.dto.js';
import {
  QueueResponseLegacyDto,
  QueuesResponseLegacyDto,
  mapQueueLegacy,
  mapQueuesLegacy,
} from 'src/dtos/queue-legacy.dto.js';
import {
  QueueCommandDto,
  QueueDeleteDto,
  QueueJobResponseDto,
  QueueJobSearchDto,
  QueueResponseDto,
  QueueUpdateDto,
} from 'src/dtos/queue.dto.js';
import { runPromise } from 'src/effect/run.js';
import {
  BootstrapEventPriority,
  CronJob,
  DatabaseLock,
  ImmichWorker,
  JobName,
  QueueCleanType,
  QueueCommand,
  QueueName,
} from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { handlePromiseError } from 'src/utils/misc.js';

const asNightlyTasksCron = (config: SystemConfig) => {
  const [hours, minutes] = config.nightlyTasks.startTime.split(':').map(Number);
  return `${minutes} ${hours} * * *`;
};

@Injectable()
export class QueueService extends BaseService {
  private services: (new (...args: any[]) => unknown)[] = [];
  private nightlyJobsLock = false;

  @OnEvent({ name: 'ConfigInit' })
  async onConfigInit({ newConfig: config }: ArgOf<'ConfigInit'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if ((yield* this.fx.config.getWorker()) === ImmichWorker.Microservices) {
          this.updateConcurrency(config);
          return;
        }

        this.nightlyJobsLock = yield* this.fx.database.tryLock(DatabaseLock.NightlyJobs);
        if (!this.nightlyJobsLock) {
          return;
        }

        const cronExpression = asNightlyTasksCron(config);
        this.logger.debug(`Scheduling nightly jobs for ${cronExpression}`);
        yield* this.fx.cron.create({
          name: CronJob.NightlyJobs,
          expression: cronExpression,
          start: true,
          onTick: () => handlePromiseError(this.handleNightlyJobs(), this.logger),
        });
      }),
    );
  }

  @OnEvent({ name: 'ConfigUpdate', server: true })
  onConfigUpdate({ newConfig: config }: ArgOf<'ConfigUpdate'>) {
    if (this.worker === ImmichWorker.Microservices) {
      this.updateConcurrency(config);
      return;
    }

    if (!this.nightlyJobsLock) {
      return;
    }

    const cronExpression = asNightlyTasksCron(config);
    this.logger.debug(`Scheduling nightly jobs for ${cronExpression}`);
    this.cronRepository.update({ name: CronJob.NightlyJobs, expression: cronExpression, start: true });
  }

  @OnEvent({ name: 'AppBootstrap', priority: BootstrapEventPriority.JobService })
  onBootstrap() {
    this.jobRepository.setup(this.services);
    if (this.worker === ImmichWorker.Microservices) {
      this.jobRepository.startWorkers();
    } else if (this.worker === ImmichWorker.Api) {
      this.jobRepository.watchWorkers();
    }
  }

  @OnEvent({ name: 'AppShutdown' })
  onShutdown() {
    this.jobRepository.teardown();
  }

  private updateConcurrency(config: SystemConfig) {
    this.logger.debug(`Updating queue concurrency settings`);
    for (const queueName of Object.values(QueueName)) {
      const concurrency = this.isConcurrentQueue(queueName) ? config.job[queueName].concurrency : 1;
      this.logger.debug(`Setting ${queueName} concurrency to ${concurrency}`);
      this.jobRepository.setConcurrency(queueName, concurrency);
    }
  }

  setServices(services: (new (...args: any[]) => unknown)[]) {
    this.services = services;
  }

  async runCommandLegacy(name: QueueName, dto: QueueCommandDto): Promise<QueueResponseLegacyDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.logger.debug(`Handling command: queue=${name},command=${dto.command},force=${dto.force}`);

        switch (dto.command) {
          case QueueCommand.Start: {
            yield* this.start(name, dto);
            break;
          }

          case QueueCommand.Pause: {
            yield* this.fx.job.pause(name);
            break;
          }

          case QueueCommand.Resume: {
            yield* this.fx.job.resume(name);
            break;
          }

          case QueueCommand.Empty: {
            yield* this.fx.job.empty(name);
            break;
          }

          case QueueCommand.ClearFailed: {
            const failedJobs = yield* this.fx.job.clear(name, QueueCleanType.Failed);
            this.logger.debug(`Cleared failed jobs: ${failedJobs}`);
            break;
          }
        }

        const response = yield* this.getByName(name);

        return mapQueueLegacy(response);
      }),
    );
  }

  async getAll(_auth: AuthDto): Promise<QueueResponseDto[]> {
    return runPromise(
      Effect.all(
        Object.values(QueueName).map((name) => this.getByName(name)),
        { concurrency: 'unbounded' },
      ),
    );
  }

  async getAllLegacy(auth: AuthDto): Promise<QueuesResponseLegacyDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const responses = yield* Effect.tryPromise({ try: () => this.getAll(auth), catch: (cause) => cause });
        return mapQueuesLegacy(responses);
      }),
    );
  }

  get(auth: AuthDto, name: QueueName): Promise<QueueResponseDto> {
    return runPromise(this.getByName(name));
  }

  async update(auth: AuthDto, name: QueueName, dto: QueueUpdateDto): Promise<QueueResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (dto.isPaused === true) {
          if (name === QueueName.BackgroundTask) {
            return yield* Effect.fail(new BadRequestException(`The BackgroundTask queue cannot be paused`));
          }
          yield* this.fx.job.pause(name);
        } else if (dto.isPaused === false) {
          yield* this.fx.job.resume(name);
        }

        return yield* this.getByName(name);
      }),
    );
  }

  searchJobs(auth: AuthDto, name: QueueName, dto: QueueJobSearchDto): Promise<QueueJobResponseDto[]> {
    return runPromise(this.fx.job.searchJobs(name, dto));
  }

  async emptyQueue(auth: AuthDto, name: QueueName, dto: QueueDeleteDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.job.empty(name);
        if (dto.failed) {
          yield* this.fx.job.clear(name, QueueCleanType.Failed);
        }
      }),
    );
  }

  private readonly getByName = Effect.fn('Queue.getByName')(function* (this: QueueService, name: QueueName) {
    const [statistics, isPaused] = yield* Effect.all([this.fx.job.getJobCounts(name), this.fx.job.isPaused(name)], {
      concurrency: 'unbounded',
    });
    return { name, isPaused, statistics: { ...statistics, paused: 0 } } satisfies QueueResponseDto;
  });

  private readonly start = Effect.fn('Queue.start')(function* (
    this: QueueService,
    name: QueueName,
    { force }: QueueCommandDto,
  ) {
    const isActive = yield* this.fx.job.isActive(name);
    if (isActive) {
      return yield* Effect.fail(new BadRequestException(`Job is already running`));
    }

    yield* this.fx.event.emit('QueueStart', { name });

    switch (name) {
      case QueueName.VideoConversion: {
        yield* this.fx.job.queue({ name: JobName.AssetEncodeVideoQueueAll, data: { force } });
        return;
      }

      case QueueName.StorageTemplateMigration: {
        yield* this.fx.job.queue({ name: JobName.StorageTemplateMigration });
        return;
      }

      case QueueName.Migration: {
        yield* this.fx.job.queue({ name: JobName.FileMigrationQueueAll });
        return;
      }

      case QueueName.MetadataExtraction: {
        yield* this.fx.job.queue({ name: JobName.AssetExtractMetadataQueueAll, data: { force } });
        return;
      }

      case QueueName.Sidecar: {
        yield* this.fx.job.queue({ name: JobName.SidecarQueueAll, data: { force } });
        return;
      }

      case QueueName.ThumbnailGeneration: {
        yield* this.fx.job.queue({ name: JobName.AssetGenerateThumbnailsQueueAll, data: { force } });
        return;
      }

      case QueueName.Library: {
        yield* this.fx.job.queue({ name: JobName.LibraryScanQueueAll, data: { force } });
        return;
      }

      case QueueName.BackupDatabase: {
        yield* this.fx.job.queue({ name: JobName.DatabaseBackup, data: { force } });
        return;
      }

      default: {
        return yield* Effect.fail(new BadRequestException(`Invalid job name: ${name}`));
      }
    }
  });

  private isConcurrentQueue(name: QueueName): name is ConcurrentQueueName {
    return ![QueueName.StorageTemplateMigration, QueueName.BackupDatabase].includes(name);
  }

  async handleNightlyJobs() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        const jobs: JobItem[] = [];

        if (config.nightlyTasks.databaseCleanup) {
          jobs.push(
            { name: JobName.AssetDeleteCheck },
            { name: JobName.UserDeleteCheck },
            { name: JobName.MemoryCleanup },
            { name: JobName.SessionCleanup },
            { name: JobName.AuthChallengeCleanup },
            { name: JobName.HlsSessionCleanup },
            { name: JobName.AuditTableCleanup },
          );
        }

        if (config.nightlyTasks.generateMemories) {
          jobs.push({ name: JobName.MemoryGenerate });
        }

        if (config.nightlyTasks.syncQuotaUsage) {
          jobs.push({ name: JobName.UserSyncUsage });
        }

        if (config.nightlyTasks.missingThumbnails) {
          jobs.push({ name: JobName.AssetGenerateThumbnailsQueueAll, data: { force: false } });
        }

        yield* this.fx.job.queueAll(jobs);
      }),
    );
  }
}
