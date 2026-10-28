import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { DateTime } from 'luxon';
import { SemVer, diff, intersects, lt } from 'semver';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { VersionCheckMetadata } from 'src/types.js';
import { serverVersion } from 'src/constants.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { ReleaseEventV1, ReleaseType, ServerVersionResponseDto } from 'src/dtos/server.dto.js';
import { runPromise } from 'src/effect/run.js';
import {
  CronJob,
  DatabaseLock,
  ImmichWorker,
  JobName,
  JobStatus,
  QueueName,
  ReleaseChannel,
  SystemMetadataKey,
} from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { handlePromiseError } from 'src/utils/misc.js';

const asNotification = (
  channel: ReleaseChannel,
  { checkedAt, releaseVersion }: VersionCheckMetadata,
): ReleaseEventV1 => {
  return {
    // can't use gt because it's broken for release candidates F https://github.com/npm/node-semver/issues/483
    isAvailable: intersects(`>${serverVersion}`, releaseVersion, {
      includePrerelease: channel === ReleaseChannel.ReleaseCandidate,
    }),
    checkedAt,
    serverVersion: ServerVersionResponseDto.fromSemVer(serverVersion),
    releaseVersion: ServerVersionResponseDto.fromSemVer(new SemVer(releaseVersion)),
    type: diff(serverVersion, releaseVersion) as ReleaseType,
  };
};

@Injectable()
export class VersionService extends BaseService {
  @OnEvent({ name: 'AppBootstrap', workers: [ImmichWorker.Microservices] })
  async onBootstrap(): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const hasLock = yield* this.fx.database.tryLock(DatabaseLock.VersionCheck);
        if (hasLock) {
          yield* Effect.tryPromise({ try: () => this.handleVersionCheck(), catch: (cause) => cause });

          const randomMinute = Math.floor(Math.random() * 60);
          const expression = `${randomMinute} * * * *`;
          this.logger.debug(`Scheduling version check for cron ${expression}`);
          yield* this.fx.cron.create({
            name: CronJob.VersionCheck,
            expression,
            onTick: () => handlePromiseError(this.handleQueueVersionCheck(), this.logger),
          });
        }

        yield* this.fx.database.withLock(DatabaseLock.VersionHistory, () =>
          runPromise(
            Effect.gen({ self: this }, function* () {
              const previous = yield* this.fx.version.getLatest();
              const current = serverVersion.toString();

              if (!previous) {
                yield* this.fx.version.create({ version: current });
                return;
              }

              if (previous.version === current) {
                return;
              }

              const previousVersion = new SemVer(previous.version);

              this.logger.log(`Adding ${current} to upgrade history`);
              yield* this.fx.version.create({ version: current });

              const isNeedsNewMemories = lt(previousVersion, '1.129.0');
              if (isNeedsNewMemories) {
                yield* this.fx.job.queue({ name: JobName.MemoryGenerate });
              }
            }),
          ),
        );
      }),
    );
  }

  getVersion() {
    return ServerVersionResponseDto.fromSemVer(serverVersion);
  }

  getVersionHistory() {
    return runPromise(this.fx.version.getAll());
  }

  @OnEvent({ name: 'ConfigUpdate' })
  async onConfigUpdate({ oldConfig, newConfig }: ArgOf<'ConfigUpdate'>) {
    if (!oldConfig.newVersionCheck.enabled && newConfig.newVersionCheck.enabled) {
      await this.handleQueueVersionCheck();
    }
  }

  async handleQueueVersionCheck() {
    return runPromise(this.fx.job.queue({ name: JobName.VersionCheck, data: {} }).pipe(Effect.asVoid));
  }

  @OnJob({ name: JobName.VersionCheck, queue: QueueName.BackgroundTask })
  async handleVersionCheck(): Promise<JobStatus> {
    try {
      return await runPromise(
        Effect.gen({ self: this }, function* () {
          if (!(yield* this.fx.config.isProduction())) {
            return JobStatus.Skipped;
          }

          this.logger.debug('Running version check');

          const { newVersionCheck } = yield* this.getConfigFx({ withCache: true });
          if (!newVersionCheck.enabled) {
            return JobStatus.Skipped;
          }

          const versionCheck = yield* this.fx.systemMetadata.get(SystemMetadataKey.VersionCheckState);
          if (versionCheck?.checkedAt) {
            const lastUpdate = DateTime.fromISO(versionCheck.checkedAt);
            const elapsedTime = DateTime.now().diff(lastUpdate).as('seconds');
            if (elapsedTime < 50) {
              return JobStatus.Skipped;
            }
          }

          const { version: releaseVersion, published_at: publishedAt } = yield* this.fx.serverInfo.getLatestRelease(
            newVersionCheck.channel,
          );
          const metadata: VersionCheckMetadata = { checkedAt: DateTime.utc().toISO(), releaseVersion };

          yield* this.fx.systemMetadata.set(SystemMetadataKey.VersionCheckState, metadata);

          // can't use gt because it's broken for release candidates F https://github.com/npm/node-semver/issues/483
          if (
            intersects(`>${serverVersion}`, releaseVersion, {
              includePrerelease: newVersionCheck.channel === ReleaseChannel.ReleaseCandidate,
            })
          ) {
            this.logger.log(`Found ${releaseVersion}, released at ${new Date(publishedAt).toLocaleString()}`);
            yield* this.fx.websocket.clientBroadcast(
              'on_new_release',
              asNotification(newVersionCheck.channel, metadata),
            );
          }

          return JobStatus.Success;
        }),
      );
    } catch (error: Error | any) {
      this.logger.warn(`Unable to run version check: ${error}\n${error?.stack}`);
      return JobStatus.Failed;
    }
  }

  @OnEvent({ name: 'WebsocketConnect' })
  async onWebsocketConnection({ userId }: ArgOf<'WebsocketConnect'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.websocket.clientSend(
          'on_server_version',
          userId,
          ServerVersionResponseDto.fromSemVer(serverVersion),
        );

        const { newVersionCheck } = yield* this.getConfigFx({ withCache: true });
        if (!newVersionCheck.enabled) {
          return;
        }

        const metadata = yield* this.fx.systemMetadata.get(SystemMetadataKey.VersionCheckState);
        if (metadata) {
          yield* this.fx.websocket.clientSend(
            'on_new_release',
            userId,
            asNotification(newVersionCheck.channel, metadata),
          );
        }
      }),
    );
  }
}
