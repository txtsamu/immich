import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { Insertable } from 'kysely';
import { OnJob } from 'src/decorators.js';
import { BulkIdResponseDto, BulkIdsDto } from 'src/dtos/asset-ids.response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  TagBulkAssetsDto,
  TagBulkAssetsResponseDto,
  TagCreateDto,
  TagResponseDto,
  TagUpdateDto,
  TagUpsertDto,
  mapTag,
} from 'src/dtos/tag.dto.js';
import { runPromise } from 'src/effect/run.js';
import { JobName, JobStatus, Permission, QueueName } from 'src/enum.js';
import { TagAssetTable } from 'src/schema/tables/tag-asset.table.js';
import { BaseService } from 'src/services/base.service.js';
import { addAssetsFx, removeAssetsFx } from 'src/utils/asset.util.js';
import { updateLockedColumns } from 'src/utils/database.js';
import { upsertTagsFx } from 'src/utils/tag.js';

@Injectable()
export class TagService extends BaseService {
  async getAll(auth: AuthDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const tags = yield* this.fx.tag.getAll(auth.user.id);
        return tags.map((tag) => mapTag(tag));
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<TagResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.TagRead, ids: [id] });
        const tag = yield* this.findOrFail(id);
        return mapTag(tag);
      }),
    );
  }

  async create(auth: AuthDto, dto: TagCreateDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        let parent;
        if (dto.parentId) {
          yield* this.requireAccessFx({ auth, permission: Permission.TagRead, ids: [dto.parentId] });
          parent = yield* this.fx.tag.get(dto.parentId);
          if (!parent) {
            return yield* Effect.fail(new BadRequestException('Tag not found'));
          }
        }

        const userId = auth.user.id;
        const value = parent ? `${parent.value}/${dto.name}` : dto.name;
        const duplicate = yield* this.fx.tag.getByValue(userId, value);
        if (duplicate) {
          return yield* Effect.fail(new BadRequestException(`A tag with that name already exists`));
        }

        const { color } = dto;
        const tag = yield* this.fx.tag.create({ userId, value, color, parentId: parent?.id });

        return mapTag(tag);
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: TagUpdateDto): Promise<TagResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.TagUpdate, ids: [id] });

        const { name, color } = dto;
        const existing = yield* this.findOrFail(id);

        let value;
        if (name) {
          const parts = existing.value.split('/');
          parts[parts.length - 1] = name;
          value = parts.join('/');
        } else {
          value = existing.value;
        }

        const assetIds = value === existing.value ? [] : yield* this.fx.tag.getAssetIdsByTagId(id);
        const tag = yield* this.fx.tag.update(id, { value, color });
        yield* this.syncAssetTags(assetIds);
        return mapTag(tag);
      }),
    );
  }

  async upsert(auth: AuthDto, dto: TagUpsertDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const tags = yield* upsertTagsFx(this.fx.tag, { userId: auth.user.id, tags: dto.tags });
        return tags.map((tag) => mapTag(tag));
      }),
    );
  }

  async remove(auth: AuthDto, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.TagDelete, ids: [id] });

        const assetIds = yield* this.fx.tag.getAssetIdsByTagId(id);
        yield* this.fx.tag.delete(id);
        yield* this.syncAssetTags(assetIds);
      }),
    );
  }

  async bulkTagAssets(auth: AuthDto, dto: TagBulkAssetsDto): Promise<TagBulkAssetsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const [tagIds, assetIds] = yield* Effect.all(
          [
            this.checkAccessFx({ auth, permission: Permission.TagAsset, ids: dto.tagIds }),
            this.checkAccessFx({ auth, permission: Permission.AssetUpdate, ids: dto.assetIds }),
          ],
          { concurrency: 'unbounded' },
        );

        const items: Insertable<TagAssetTable>[] = [];
        for (const tagId of tagIds) {
          for (const assetId of assetIds) {
            items.push({ tagId, assetId });
          }
        }

        const results = yield* this.fx.tag.upsertAssetIds(items);
        for (const assetId of new Set(results.map((item) => item.assetId))) {
          yield* this.updateTags(assetId);
          yield* this.fx.event.emit('AssetTag', { assetId, userId: auth.user.id });
        }

        return { count: results.length };
      }),
    );
  }

  async addAssets(auth: AuthDto, id: string, dto: BulkIdsDto): Promise<BulkIdResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.TagAsset, ids: [id] });

        const results = yield* addAssetsFx(
          auth,
          { access: this.fx.access, bulk: this.fx.tag },
          { parentId: id, assetIds: dto.ids, permission: Permission.AssetUpdate },
        );

        for (const { id: assetId, success } of results) {
          if (!success) {
            continue;
          }

          yield* this.updateTags(assetId);
          yield* this.fx.event.emit('AssetTag', { assetId, userId: auth.user.id });
        }

        return results;
      }),
    );
  }

  async removeAssets(auth: AuthDto, id: string, dto: BulkIdsDto): Promise<BulkIdResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.TagAsset, ids: [id] });

        const results = yield* removeAssetsFx(
          auth,
          { access: this.fx.access, bulk: this.fx.tag },
          { parentId: id, assetIds: dto.ids, canAlwaysRemove: Permission.TagDelete },
        );

        for (const { id: assetId, success } of results) {
          if (!success) {
            continue;
          }

          yield* this.updateTags(assetId);
          yield* this.fx.event.emit('AssetUntag', { assetId });
        }

        return results;
      }),
    );
  }

  @OnJob({ name: JobName.TagCleanup, queue: QueueName.BackgroundTask })
  async handleTagCleanup() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.tag.deleteEmptyTags();
        return JobStatus.Success;
      }),
    );
  }

  private readonly findOrFail = Effect.fn('Tag.findOrFail')(function* (this: TagService, id: string) {
    const value = yield* this.fx.tag.get(id);
    if (!value) {
      return yield* Effect.fail(new BadRequestException('Tag not found'));
    }

    return value;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly updateTags = Effect.fn('Tag.updateTags')(function* (this: TagService, assetId: string) {
    const { tags } = yield* this.fx.asset.getForUpdateTags(assetId);
    yield* this.fx.asset.upsertExif({
      exif: updateLockedColumns({ assetId, tags: tags.map(({ value }) => value) }),
      lockedPropertiesBehavior: 'append',
    });
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly syncAssetTags = Effect.fn('Tag.syncAssetTags')(function* (this: TagService, assetIds: string[]) {
    if (assetIds.length === 0) {
      return;
    }

    for (const assetId of assetIds) {
      yield* this.updateTags(assetId);
    }

    yield* this.fx.job.queueAll(assetIds.map((id) => ({ name: JobName.SidecarWrite, data: { id } })));
  });
}
