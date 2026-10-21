import { BadRequestException, ForbiddenException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { SALT_ROUNDS } from 'src/constants.js';
import { AssetStatsDto, AssetStatsResponseDto, mapStats } from 'src/dtos/asset.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { CalendarHeatmapDto, CalendarHeatmapResponseDto } from 'src/dtos/calendar-heatmap.dto.js';
import { SessionResponseDto, mapSession } from 'src/dtos/session.dto.js';
import { UserPreferencesResponseDto, UserPreferencesUpdateDto, mapPreferences } from 'src/dtos/user-preferences.dto.js';
import {
  UserAdminCreateDto,
  UserAdminDeleteDto,
  UserAdminResponseDto,
  UserAdminSearchDto,
  UserAdminUpdateDto,
  mapUserAdmin,
} from 'src/dtos/user.dto.js';
import { runPromise } from 'src/effect/run.js';
import { JobName, UserMetadataKey, UserStatus } from 'src/enum.js';
import { UserFindOptions } from 'src/repositories/user.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { getCalendarHeatmap } from 'src/services/shared/user-methods.js';
import { getPreferences, getPreferencesPartial, mergePreferences } from 'src/utils/preferences.js';

@Injectable()
export class UserAdminService extends BaseService {
  async search(auth: AuthDto, dto: UserAdminSearchDto): Promise<UserAdminResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const users = yield* this.fx.user.getList({
          id: dto.id,
          withDeleted: dto.withDeleted,
        });
        return users.map((user) => mapUserAdmin(user));
      }),
    );
  }

  async create(dto: UserAdminCreateDto): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { notify, ...userDto } = dto;
        const config = yield* this.getConfigFx({ withCache: false });
        if (!config.oauth.enabled && !userDto.password) {
          return yield* Effect.fail(new BadRequestException('password is required'));
        }

        const user = yield* this.createUserFx(userDto);

        yield* this.fx.event.emit('UserSignup', {
          notify: !!notify,
          id: user.id,
          password: userDto.password,
        });

        return mapUserAdmin(user);
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.findOrFail(id, { withDeleted: true });
        return mapUserAdmin(user);
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: UserAdminUpdateDto): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.findOrFail(id, {});

        if (dto.isAdmin !== undefined && dto.isAdmin !== auth.user.isAdmin && auth.user.id === id) {
          return yield* Effect.fail(new BadRequestException('Admin status can only be changed by another admin'));
        }

        if (dto.quotaSizeInBytes && user.quotaSizeInBytes !== dto.quotaSizeInBytes) {
          yield* this.fx.user.syncUsage(id);
        }

        if (dto.email) {
          const duplicate = yield* this.fx.user.getByEmail(dto.email);
          if (duplicate && duplicate.id !== id) {
            this.logger.debug('Email already in use by another account');
            return yield* Effect.fail(new BadRequestException('Email is not available'));
          }
        }

        if (dto.storageLabel) {
          const duplicate = yield* this.fx.user.getByStorageLabel(dto.storageLabel);
          if (duplicate && duplicate.id !== id) {
            return yield* Effect.fail(new BadRequestException('Storage label already in use by another account'));
          }
        }

        if (dto.password) {
          dto.password = yield* this.fx.crypto.hashBcrypt(dto.password, SALT_ROUNDS);
        }

        if (dto.pinCode) {
          dto.pinCode = yield* this.fx.crypto.hashBcrypt(dto.pinCode, SALT_ROUNDS);
        }

        if (dto.storageLabel === '') {
          dto.storageLabel = null;
        }

        const updatedUser = yield* this.fx.user.update(id, { ...dto, updatedAt: new Date() });

        return mapUserAdmin(updatedUser);
      }),
    );
  }

  async delete(auth: AuthDto, id: string, dto: UserAdminDeleteDto): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { force } = dto;
        yield* this.findOrFail(id, {});
        if (auth.user.id === id) {
          return yield* Effect.fail(new ForbiddenException('Cannot delete your own account'));
        }

        yield* this.fx.album.softDeleteAll(id);

        const status = force ? UserStatus.Removing : UserStatus.Deleted;
        const user = yield* this.fx.user.update(id, { status, deletedAt: new Date() });

        yield* this.fx.event.emit('UserTrash', user);

        if (force) {
          yield* this.fx.job.queue({ name: JobName.UserDelete, data: { id: user.id, force } });
        }

        return mapUserAdmin(user);
      }),
    );
  }

  async restore(auth: AuthDto, id: string): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id, { withDeleted: true });
        yield* this.fx.album.restoreAll(id);
        const user = yield* this.fx.user.restore(id);
        yield* this.fx.event.emit('UserRestore', user);
        return mapUserAdmin(user);
      }),
    );
  }

  async getCalendarHeatmap(auth: AuthDto, id: string, dto: CalendarHeatmapDto): Promise<CalendarHeatmapResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id, { withDeleted: false });
        return yield* Effect.tryPromise({
          try: () => getCalendarHeatmap(id, dto, { asset: this.assetRepository }),
          catch: (cause) => cause,
        });
      }),
    );
  }

  async getSessions(auth: AuthDto, id: string): Promise<SessionResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sessions = yield* this.fx.session.getByUserId(id);
        return sessions.map((session) => mapSession(session));
      }),
    );
  }

  async getStatistics(auth: AuthDto, id: string, dto: AssetStatsDto): Promise<AssetStatsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const stats = yield* this.fx.asset.getStatistics(id, dto);
        return mapStats(stats);
      }),
    );
  }

  async getPreferences(auth: AuthDto, id: string): Promise<UserPreferencesResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id, { withDeleted: true });
        const metadata = yield* this.fx.user.getMetadata(id);
        return mapPreferences(getPreferences(metadata));
      }),
    );
  }

  async updatePreferences(auth: AuthDto, id: string, dto: UserPreferencesUpdateDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(id, { withDeleted: false });
        const metadata = yield* this.fx.user.getMetadata(id);
        const newPreferences = mergePreferences(getPreferences(metadata), dto);

        yield* this.fx.user.upsertMetadata(id, {
          key: UserMetadataKey.Preferences,
          value: getPreferencesPartial(newPreferences),
        });

        return mapPreferences(newPreferences);
      }),
    );
  }

  private readonly findOrFail = Effect.fn('UserAdmin.findOrFail')(function* (
    this: UserAdminService,
    id: string,
    options: UserFindOptions,
  ) {
    const value = yield* this.fx.user.get(id, options);
    if (!value) {
      return yield* Effect.fail(new BadRequestException('User not found'));
    }

    return value;
  });
}
