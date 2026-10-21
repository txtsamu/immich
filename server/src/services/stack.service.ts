import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { BulkIdsDto } from 'src/dtos/asset-ids.response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { StackCreateDto, StackResponseDto, StackSearchDto, StackUpdateDto, mapStack } from 'src/dtos/stack.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { UUIDAssetIDParamDto } from 'src/validation.js';

@Injectable()
export class StackService extends BaseService {
  async search(auth: AuthDto, dto: StackSearchDto): Promise<StackResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const stacks = yield* this.fx.stack.search({
          ownerId: auth.user.id,
          primaryAssetId: dto.primaryAssetId,
        });

        return stacks.map((stack) => mapStack(stack, { auth }));
      }),
    );
  }

  async create(auth: AuthDto, dto: StackCreateDto): Promise<StackResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetUpdate, ids: dto.assetIds });

        const stack = yield* this.fx.stack.create({ ownerId: auth.user.id }, dto.assetIds);

        yield* this.fx.event.emit('StackCreate', { stackId: stack.id, userId: auth.user.id });

        return mapStack(stack, { auth });
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<StackResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.StackRead, ids: [id] });
        const stack = yield* this.findOrFail(id);
        return mapStack(stack, { auth });
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: StackUpdateDto): Promise<StackResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.StackUpdate, ids: [id] });
        const stack = yield* this.findOrFail(id);
        if (dto.primaryAssetId && stack.assets.every(({ id }) => id !== dto.primaryAssetId)) {
          return yield* Effect.fail(new BadRequestException('Primary asset must be in the stack'));
        }

        const updatedStack = yield* this.fx.stack.update(id, { id, primaryAssetId: dto.primaryAssetId });

        yield* this.fx.event.emit('StackUpdate', { stackId: id, userId: auth.user.id });

        return mapStack(updatedStack, { auth });
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.StackDelete, ids: [id] });
        yield* this.fx.stack.delete(id);
        yield* this.fx.event.emit('StackDelete', { stackId: id, userId: auth.user.id });
      }),
    );
  }

  async deleteAll(auth: AuthDto, dto: BulkIdsDto): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.StackDelete, ids: dto.ids });
        yield* this.fx.stack.deleteAll(dto.ids);
        yield* this.fx.event.emit('StackDeleteAll', { stackIds: dto.ids, userId: auth.user.id });
      }),
    );
  }

  async removeAsset(auth: AuthDto, dto: UUIDAssetIDParamDto): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const { id: stackId, assetId } = dto;
        yield* this.requireAccessFx({ auth, permission: Permission.StackUpdate, ids: [stackId] });

        const stack = yield* this.fx.stack.getForAssetRemoval(assetId);

        if (!stack?.id || stack.id !== stackId) {
          return yield* Effect.fail(new BadRequestException('Asset not in stack'));
        }

        if (stack.primaryAssetId === assetId) {
          return yield* Effect.fail(new BadRequestException("Cannot remove stack's primary asset"));
        }

        yield* this.fx.asset.update({ id: assetId, stackId: null });
        yield* this.fx.event.emit('StackUpdate', { stackId, userId: auth.user.id });
      }),
    );
  }

  private findOrFail = Effect.fn('StackService.findOrFail')(function* (this: StackService, id: string) {
    const stack = yield* this.fx.stack.getById(id);
    if (!stack) {
      return yield* Effect.fail(new BadRequestException('Asset stack not found'));
    }
    return stack;
  });
}
