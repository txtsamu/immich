import { Effect } from 'effect';
import { snakeCase } from 'lodash-es';
import type { ArgOf, ArgsOf } from 'src/repositories/event.repository.js';
import { OnEvent } from 'src/decorators.js';
import { runPromise } from 'src/effect/run.js';
import { ImmichWorker, JobStatus } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';

export class TelemetryService extends BaseService {
  @OnEvent({ name: 'AppBootstrap', workers: [ImmichWorker.Api] })
  async onBootstrap(): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const userCount = yield* this.fx.user.getCount();
        this.telemetryRepository.api.addToGauge('immich.users.total', userCount);
      }),
    );
  }

  @OnEvent({ name: 'UserCreate' })
  onUserCreate() {
    this.telemetryRepository.api.addToGauge(`immich.users.total`, 1);
  }

  @OnEvent({ name: 'UserTrash' })
  onUserTrash() {
    this.telemetryRepository.api.addToGauge(`immich.users.total`, -1);
  }

  @OnEvent({ name: 'UserRestore' })
  onUserRestore() {
    this.telemetryRepository.api.addToGauge(`immich.users.total`, 1);
  }

  @OnEvent({ name: 'JobStart' })
  onJobStart(...[queueName]: ArgsOf<'JobStart'>) {
    const queueMetric = `immich.queues.${snakeCase(queueName)}.active`;
    this.telemetryRepository.jobs.addToGauge(queueMetric, 1);
  }

  @OnEvent({ name: 'JobSuccess' })
  onJobSuccess({ job, response }: ArgOf<'JobSuccess'>) {
    if (!(response && Object.values(JobStatus).includes(response as JobStatus))) {
      return;
    }

    const jobMetric = `immich.jobs.${snakeCase(job.name)}.${response}`;
    this.telemetryRepository.jobs.addToCounter(jobMetric, 1);
  }

  @OnEvent({ name: 'JobError' })
  onJobError({ job }: ArgOf<'JobError'>) {
    const jobMetric = `immich.jobs.${snakeCase(job.name)}.${JobStatus.Failed}`;
    this.telemetryRepository.jobs.addToCounter(jobMetric, 1);
  }

  @OnEvent({ name: 'JobComplete' })
  onJobComplete(...[queueName]: ArgsOf<'JobComplete'>) {
    const queueMetric = `immich.queues.${snakeCase(queueName)}.active`;
    this.telemetryRepository.jobs.addToGauge(queueMetric, -1);
  }

  @OnEvent({ name: 'QueueStart' })
  onQueueStart({ name }: ArgOf<'QueueStart'>) {
    this.telemetryRepository.jobs.addToCounter(`immich.queues.${snakeCase(name)}.started`, 1);
  }
}
