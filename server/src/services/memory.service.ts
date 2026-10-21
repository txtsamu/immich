import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { DateTime } from 'luxon';
import { Memory } from 'src/database.js';
import { OnJob } from 'src/decorators.js';
import { BulkIdResponseDto, BulkIdsDto } from 'src/dtos/asset-ids.response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  MemoryCreateDto,
  MemoryResponseDto,
  MemorySearchDto,
  MemoryUpdateDto,
  mapMemory,
} from 'src/dtos/memory.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import { DatabaseLock, JobName, MemoryType, Permission, QueueName, SystemMetadataKey } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { addAssets, removeAssets } from 'src/utils/asset.util.js';

const DAYS = 3;

/** The original thrown value behind a wrapped repository error (see `runPromise`). */
@Injectable()
export class MemoryService extends BaseService {
  @OnJob({ name: JobName.MemoryGenerate, queue: QueueName.BackgroundTask })
  async onMemoriesCreate() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const users = yield* this.fx.user.getList({ withDeleted: false });

        yield* this.fx.database.withLock(DatabaseLock.MemoryCreation, () =>
          runPromise(
            Effect.gen({ self: this }, function* () {
              const state = yield* this.fx.systemMetadata.get(SystemMetadataKey.MemoriesState);
              const start = DateTime.utc().startOf('day').minus({ days: DAYS });
              const lastOnThisDayDate = state?.lastOnThisDayDate ? DateTime.fromISO(state.lastOnThisDayDate) : start;

              // generate a memory +/- X days from today
              for (let i = 0; i <= DAYS * 2; i++) {
                const target = start.plus({ days: i });
                if (lastOnThisDayDate >= target) {
                  continue;
                }

                this.logger.log(`Creating memories for ${target.toISO()}`);
                yield* Effect.all(
                  users.map((owner) => this.createOnThisDayMemories(owner.id, target)),
                  { concurrency: 'unbounded' },
                ).pipe(
                  Effect.catch((error) =>
                    Effect.sync(() =>
                      this.logger.error(`Failed to create memories for ${target.toISO()}: ${unwrap(error)}`),
                    ),
                  ),
                );
                // update system metadata even when there is an error to minimize the chance of duplicates
                yield* this.fx.systemMetadata.set(SystemMetadataKey.MemoriesState, {
                  ...state,
                  lastOnThisDayDate: target.toISO(),
                });
              }
            }),
          ),
        );
      }),
    );
  }

  private readonly createOnThisDayMemories = Effect.fn('Memory.createOnThisDayMemories')(function* (
    this: MemoryService,
    ownerId: string,
    target: DateTime,
  ) {
    const showAt = target.startOf('day').toISO();
    const hideAt = target.endOf('day').toISO();
    const memories = yield* this.fx.asset.getByDayOfYear([ownerId], target);
    yield* Effect.all(
      memories.map(({ year, assets }) =>
        this.fx.memory.create(
          {
            ownerId,
            type: MemoryType.OnThisDay,
            data: { year },
            memoryAt: target.set({ year }).toISO()!,
            showAt,
            hideAt,
          },
          new Set(assets.map(({ id }) => id)),
        ),
      ),
      { concurrency: 'unbounded' },
    );
  });

  @OnJob({ name: JobName.MemoryCleanup, queue: QueueName.BackgroundTask })
  async onMemoriesCleanup() {
    return runPromise(this.fx.memory.cleanup().pipe(Effect.asVoid));
  }

  async search(auth: AuthDto, dto: MemorySearchDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const memories = yield* this.fx.memory.search(auth.user.id, dto);
        return memories
          .filter((memory: Memory) => memory.assets && memory.assets.length > 0)
          .map((memory: Memory) => mapMemory(memory, auth));
      }),
    );
  }

  statistics(auth: AuthDto, dto: MemorySearchDto) {
    return runPromise(this.fx.memory.statistics(auth.user.id, dto));
  }

  async get(auth: AuthDto, id: string): Promise<MemoryResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.MemoryRead, ids: [id] });
        const memory = yield* this.findOrFail(id);
        return mapMemory(memory, auth);
      }),
    );
  }

  async create(auth: AuthDto, dto: MemoryCreateDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const assetIds = dto.assetIds || [];
        const allowedAssetIds = yield* this.checkAccessFx({
          auth,
          permission: Permission.AssetUpdate,
          ids: assetIds,
        });
        const memory = yield* this.fx.memory.create(
          {
            ownerId: auth.user.id,
            type: dto.type,
            data: dto.data,
            isSaved: dto.isSaved,
            memoryAt: dto.memoryAt,
            showAt: dto.showAt,
            hideAt: dto.hideAt,
            seenAt: dto.seenAt,
          },
          allowedAssetIds,
        );

        return mapMemory(memory, auth);
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: MemoryUpdateDto): Promise<MemoryResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.MemoryUpdate, ids: [id] });

        const memory = yield* this.fx.memory.update(id, {
          isSaved: dto.isSaved,
          memoryAt: dto.memoryAt,
          seenAt: dto.seenAt,
        });

        return mapMemory(memory, auth);
      }),
    );
  }

  async remove(auth: AuthDto, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.MemoryDelete, ids: [id] });
        yield* this.fx.memory.delete(id);
      }),
    );
  }

  async addAssets(auth: AuthDto, id: string, dto: BulkIdsDto): Promise<BulkIdResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.MemoryRead, ids: [id] });

        const repos = { access: this.accessRepository, bulk: this.memoryRepository };
        const results = yield* Effect.tryPromise({
          try: () =>
            addAssets(auth, repos, {
              parentId: id,
              assetIds: dto.ids,
              permission: Permission.AssetUpdate,
            }),
          catch: (cause) => cause,
        });

        const hasSuccess = results.some(({ success }) => success);
        if (hasSuccess) {
          yield* this.fx.memory.update(id, { updatedAt: new Date() });
        }

        return results;
      }),
    );
  }

  async removeAssets(auth: AuthDto, id: string, dto: BulkIdsDto): Promise<BulkIdResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.MemoryUpdate, ids: [id] });

        const repos = { access: this.accessRepository, bulk: this.memoryRepository };
        const results = yield* Effect.tryPromise({
          try: () =>
            removeAssets(auth, repos, {
              parentId: id,
              assetIds: dto.ids,
              canAlwaysRemove: Permission.MemoryDelete,
            }),
          catch: (cause) => cause,
        });

        const hasSuccess = results.some(({ success }) => success);
        if (hasSuccess) {
          yield* this.fx.memory.update(id, { id, updatedAt: new Date() });
        }

        return results;
      }),
    );
  }

  private readonly findOrFail = Effect.fn('Memory.findOrFail')(function* (this: MemoryService, id: string) {
    const value = yield* this.fx.memory.get(id);
    if (!value) {
      return yield* Effect.fail(new BadRequestException('Memory not found'));
    }

    return value;
  });
}
