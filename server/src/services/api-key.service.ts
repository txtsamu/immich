import { BadRequestException, ForbiddenException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { ApiKey } from 'src/database.js';
import { ApiKeyCreateDto, ApiKeyCreateResponseDto, ApiKeyResponseDto, ApiKeyUpdateDto } from 'src/dtos/api-key.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { isGranted } from 'src/utils/access.js';

@Injectable()
export class ApiKeyService extends BaseService {
  async create(auth: AuthDto, dto: ApiKeyCreateDto): Promise<ApiKeyCreateResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const token = yield* this.fx.crypto.randomBytesAsText(32);
        const hashed = yield* this.fx.crypto.hashSha256(token);

        if (auth.apiKey && !isGranted({ requested: dto.permissions, current: auth.apiKey.permissions })) {
          return yield* Effect.fail(new BadRequestException('Cannot grant permissions you do not have'));
        }

        const entity = yield* this.fx.apiKey.create({
          key: hashed,
          name: dto.name || 'API Key',
          userId: auth.user.id,
          permissions: dto.permissions,
        });
        const apiKey = this.map(entity);

        return { ...apiKey, secret: token, apiKey };
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: ApiKeyUpdateDto): Promise<ApiKeyResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const exists = yield* this.fx.apiKey.getById(auth.user.id, id);
        if (!exists) {
          return yield* Effect.fail(new BadRequestException('API Key not found'));
        }

        if (
          auth.apiKey &&
          dto.permissions &&
          !isGranted({ requested: dto.permissions, current: auth.apiKey.permissions })
        ) {
          return yield* Effect.fail(new BadRequestException('Cannot grant permissions you do not have'));
        }

        const key = yield* this.fx.apiKey.update(auth.user.id, id, {
          name: dto.name,
          permissions: dto.permissions,
        });

        return this.map(key);
      }),
    );
  }

  async rotate(auth: AuthDto, id: string): Promise<ApiKeyCreateResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const existing = yield* this.fx.apiKey.getById(auth.user.id, id);
        if (!existing) {
          // findOrFail appended " not found" to the entity name
          return yield* Effect.fail(new BadRequestException('API Key not found not found'));
        }

        if (
          auth.apiKey &&
          !isGranted({ requested: existing.permissions as Permission[], current: auth.apiKey.permissions })
        ) {
          return yield* Effect.fail(
            new BadRequestException('Cannot rotate an API Key with permissions you do not have'),
          );
        }

        const token = yield* this.fx.crypto.randomBytesAsText(32);
        const hashed = yield* this.fx.crypto.hashSha256(token);
        const newKey = yield* this.fx.apiKey.update(auth.user.id, id, { key: hashed });
        const apiKey = this.map(newKey);

        return { ...apiKey, secret: token, apiKey };
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const exists = yield* this.fx.apiKey.getById(auth.user.id, id);
        if (!exists) {
          return yield* Effect.fail(new BadRequestException('API Key not found'));
        }

        yield* this.fx.apiKey.delete(auth.user.id, id);
      }),
    );
  }

  async getMine(auth: AuthDto): Promise<ApiKeyResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!auth.apiKey) {
          return yield* Effect.fail(new ForbiddenException('Not authenticated with an API Key'));
        }

        const key = yield* this.fx.apiKey.getById(auth.user.id, auth.apiKey.id);
        if (!key) {
          return yield* Effect.fail(new BadRequestException('API Key not found'));
        }

        return this.map(key);
      }),
    );
  }

  async getById(auth: AuthDto, id: string): Promise<ApiKeyResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const key = yield* this.fx.apiKey.getById(auth.user.id, id);
        if (!key) {
          return yield* Effect.fail(new BadRequestException('API Key not found'));
        }
        return this.map(key);
      }),
    );
  }

  async getAll(auth: AuthDto): Promise<ApiKeyResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const keys = yield* this.fx.apiKey.getByUserId(auth.user.id);
        return keys.map((key) => this.map(key));
      }),
    );
  }

  private map(entity: ApiKey): ApiKeyResponseDto {
    return {
      id: entity.id,
      name: entity.name,
      createdAt: entity.createdAt,
      updatedAt: entity.updatedAt,
      permissions: entity.permissions as Permission[],
    };
  }
}
