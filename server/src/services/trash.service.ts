import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { OnEvent, OnJob } from 'src/decorators.js';
import { BulkIdsDto } from 'src/dtos/asset-ids.response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { TrashResponseDto } from 'src/dtos/trash.dto.js';
import { runPromise } from 'src/effect/run.js';
import { JobName, JobStatus, Permission, QueueName } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { batched } from 'src/utils/misc.js';

@Injectable()
export class TrashService extends BaseService {
  async restoreAssets(auth: AuthDto, dto: BulkIdsDto): Promise<TrashResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { ids } = dto;
        if (ids.length === 0) {
          return { count: 0 };
        }

        yield* this.requireAccessFx({ auth, permission: Permission.AssetDelete, ids });
        yield* this.fx.trash.restoreAll(ids);
        yield* this.fx.event.emit('AssetRestoreAll', { assetIds: ids, userId: auth.user.id });

        this.logger.log(`Restored ${ids.length} asset(s) from trash`);

        return { count: ids.length };
      }),
    );
  }

  async restore(auth: AuthDto): Promise<TrashResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const count = yield* this.fx.trash.restore(auth.user.id);
        if (count > 0) {
          this.logger.log(`Restored ${count} asset(s) from trash`);
        }
        return { count };
      }),
    );
  }

  async empty(auth: AuthDto): Promise<TrashResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const count = yield* this.fx.trash.empty(auth.user.id);
        if (count > 0) {
          yield* this.fx.job.queue({ name: JobName.AssetEmptyTrash, data: {} });
        }
        return { count };
      }),
    );
  }

  @OnEvent({ name: 'AssetDeleteAll' })
  async onAssetsDelete() {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.job.queue({ name: JobName.AssetEmptyTrash, data: {} });
      }),
    );
  }

  @OnJob({ name: JobName.AssetEmptyTrash, queue: QueueName.BackgroundTask })
  async handleEmptyTrash() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        let count = 0;
        const deletedIds = yield* this.fx.trash.getDeletedIds();
        // the stream is consumed by hand (instead of `for await`) so each queueAll is an Effect
        const batches = batched(deletedIds);
        const loop = Effect.gen({ self: this }, function* () {
          while (true) {
            const next = yield* Effect.tryPromise({ try: () => batches.next(), catch: (cause) => cause });
            if (next.done) {
              break;
            }

            const assets = next.value;
            yield* this.fx.job.queueAll(
              assets.map(({ id }) => ({
                name: JobName.AssetDelete,
                data: { id, deleteOnDisk: true, deleteOffline: true },
              })),
            );
            count += assets.length;
          }
        });
        // `for await` closes the iterator on abrupt exit, keep that behavior
        yield* loop.pipe(
          Effect.onError(() =>
            Effect.tryPromise({ try: () => batches.return(undefined), catch: (cause) => cause }).pipe(Effect.ignore),
          ),
        );

        this.logger.log(`Queued ${count} asset(s) for deletion from the trash`);

        return JobStatus.Success;
      }),
    );
  }
}
