import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { Effect } from 'effect';
import { Updateable } from 'kysely';
import { DateTime } from 'luxon';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { JobOf, UserMetadataItem } from 'src/types.js';
import { SALT_ROUNDS } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { CalendarHeatmapDto, CalendarHeatmapResponseDto } from 'src/dtos/calendar-heatmap.dto.js';
import { LicenseKeyDto, LicenseResponseDto } from 'src/dtos/license.dto.js';
import { OnboardingDto, OnboardingResponseDto } from 'src/dtos/onboarding.dto.js';
import { UserPreferencesResponseDto, UserPreferencesUpdateDto, mapPreferences } from 'src/dtos/user-preferences.dto.js';
import { CreateProfileImageResponseDto } from 'src/dtos/user-profile.dto.js';
import { UserAdminResponseDto, UserResponseDto, UserUpdateMeDto, mapUser, mapUserAdmin } from 'src/dtos/user.dto.js';
import { runPromise } from 'src/effect/run.js';
import { CacheControl, JobName, JobStatus, QueueName, StorageFolder, UserMetadataKey } from 'src/enum.js';
import { UserFindOptions } from 'src/repositories/user.repository.js';
import { UserTable } from 'src/schema/tables/user.table.js';
import { BaseService } from 'src/services/base.service.js';
import { getCalendarHeatmap } from 'src/services/shared/user-methods.js';
import { ImmichFileResponse } from 'src/utils/file.js';
import { mimeTypes } from 'src/utils/mime-types.js';
import { getPreferences, getPreferencesPartial, mergePreferences } from 'src/utils/preferences.js';
import { generateProfileImage } from 'src/utils/profile-image.js';

/** The value originally thrown, for errors that wrap it in `cause` (see `src/effect/errors.ts`). */
const originalError = (error: unknown) =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;

@Injectable()
export class UserService extends BaseService {
  async search(auth: AuthDto): Promise<UserResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });

        let users;
        if (auth.user.isAdmin || config.server.publicUsers) {
          users = yield* this.fx.user.getList({ withDeleted: false });
        } else {
          const authUser = yield* this.fx.user.get(auth.user.id, {});
          users = authUser ? [authUser] : [];
        }

        return users.map((user) => mapUser(user));
      }),
    );
  }

  async getMe(auth: AuthDto): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.fx.user.get(auth.user.id, {});
        if (!user) {
          return yield* Effect.fail(new BadRequestException('User not found'));
        }

        return mapUserAdmin(user);
      }),
    );
  }

  getCalendarHeatmap(auth: AuthDto, dto: CalendarHeatmapDto): Promise<CalendarHeatmapResponseDto> {
    return getCalendarHeatmap(auth.user.id, dto, { asset: this.assetRepository });
  }

  async updateMe({ user }: AuthDto, dto: UserUpdateMeDto): Promise<UserAdminResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (dto.email) {
          const duplicate = yield* this.fx.user.getByEmail(dto.email);
          if (duplicate && duplicate.id !== user.id) {
            this.logger.warn('Email already in use by another account');
            return yield* Effect.fail(new BadRequestException('Email is not available'));
          }
        }

        const update: Updateable<UserTable> = {
          email: dto.email,
          name: dto.name,
          avatarColor: dto.avatarColor,
        };

        if (dto.password) {
          const hashedPassword = yield* this.fx.crypto.hashBcrypt(dto.password, SALT_ROUNDS);
          update.password = hashedPassword;
          update.shouldChangePassword = false;
        }

        const updatedUser = yield* this.fx.user.update(user.id, update);

        return mapUserAdmin(updatedUser);
      }),
    );
  }

  async getMyPreferences(auth: AuthDto): Promise<UserPreferencesResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const metadata = yield* this.fx.user.getMetadata(auth.user.id);
        return mapPreferences(getPreferences(metadata));
      }),
    );
  }

  async updateMyPreferences(auth: AuthDto, dto: UserPreferencesUpdateDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const metadata = yield* this.fx.user.getMetadata(auth.user.id);
        const updated = mergePreferences(getPreferences(metadata), dto);

        yield* this.fx.user.upsertMetadata(auth.user.id, {
          key: UserMetadataKey.Preferences,
          value: getPreferencesPartial(updated),
        });

        return mapPreferences(updated);
      }),
    );
  }

  async get(id: string): Promise<UserResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.findOrFail(id, { withDeleted: false });
        return mapUser(user);
      }),
    );
  }

  async createProfileImage(auth: AuthDto, file: Express.Multer.File): Promise<CreateProfileImageResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { profileImagePath: oldPath } = yield* this.findOrFail(auth.user.id, { withDeleted: false });

        const profileImagePath: string = yield* Effect.gen({ self: this }, function* () {
          const config = yield* this.getConfigFx({ withCache: true });
          return yield* Effect.tryPromise({
            try: () =>
              generateProfileImage(
                { media: this.mediaRepository, crypto: this.cryptoRepository, storageCore: this.storageCore },
                config,
                auth.user.id,
                file.path,
              ),
            catch: (cause) => cause,
          });
        }).pipe(
          Effect.catch((error) =>
            Effect.gen({ self: this }, function* () {
              yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: [file.path] } });
              return yield* Effect.fail(
                new BadRequestException('Unable to process profile image', { cause: originalError(error) }),
              );
            }),
          ),
        );

        const user = yield* this.fx.user.update(auth.user.id, {
          profileImagePath,
          profileChangedAt: new Date(),
        });

        const toDelete = [file.path, ...(oldPath ? [oldPath] : [])];
        yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: toDelete } });

        return {
          userId: user.id,
          profileImagePath: user.profileImagePath,
          profileChangedAt: user.profileChangedAt,
        };
      }),
    );
  }

  async deleteProfileImage(auth: AuthDto): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.findOrFail(auth.user.id, { withDeleted: false });
        if (user.profileImagePath === '') {
          return yield* Effect.fail(new BadRequestException("Can't delete a missing profile Image"));
        }
        yield* this.fx.user.update(auth.user.id, { profileImagePath: '', profileChangedAt: new Date() });
        yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: [user.profileImagePath] } });
      }),
    );
  }

  async getProfileImage(id: string): Promise<ImmichFileResponse> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.fx.user.get(id, {});
        if (!user || !user.profileImagePath) {
          this.logger.debug('User or profile image not found');
          return yield* Effect.fail(new NotFoundException());
        }

        return new ImmichFileResponse({
          path: user.profileImagePath,
          contentType: mimeTypes.lookup(user.profileImagePath),
          cacheControl: CacheControl.None,
        });
      }),
    );
  }

  async getLicense(auth: AuthDto): Promise<LicenseResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const metadata = yield* this.fx.user.getMetadata(auth.user.id);

        const license = metadata.find(
          (item): item is UserMetadataItem<UserMetadataKey.License> => item.key === UserMetadataKey.License,
        );
        if (!license) {
          return yield* Effect.fail(new NotFoundException());
        }
        return { ...license.value, activatedAt: new Date(license.value.activatedAt) };
      }),
    );
  }

  async deleteLicense({ user }: AuthDto): Promise<void> {
    return runPromise(this.fx.user.deleteMetadata(user.id, UserMetadataKey.License));
  }

  async setLicense(auth: AuthDto, license: LicenseKeyDto): Promise<LicenseResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!license.licenseKey.startsWith('IMCL-') && !license.licenseKey.startsWith('IMSV-')) {
          return yield* Effect.fail(new BadRequestException('Invalid license key'));
        }

        const { licensePublicKey } = yield* this.fx.config.getEnv();

        const isClientLicenseValid = yield* this.fx.crypto.verifySha256(
          license.licenseKey,
          license.activationKey,
          licensePublicKey.client,
        );

        const isServerLicenseValid = yield* this.fx.crypto.verifySha256(
          license.licenseKey,
          license.activationKey,
          licensePublicKey.server,
        );

        if (!isClientLicenseValid && !isServerLicenseValid) {
          return yield* Effect.fail(new BadRequestException('Invalid license key'));
        }

        const activatedAt = new Date();

        yield* this.fx.user.upsertMetadata(auth.user.id, {
          key: UserMetadataKey.License,
          value: { ...license, activatedAt: activatedAt.toISOString() },
        });

        return { ...license, activatedAt };
      }),
    );
  }

  async getOnboarding(auth: AuthDto): Promise<OnboardingResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const metadata = yield* this.fx.user.getMetadata(auth.user.id);

        const onboardingData = metadata.find(
          (item): item is UserMetadataItem<UserMetadataKey.Onboarding> => item.key === UserMetadataKey.Onboarding,
        )?.value;

        if (!onboardingData) {
          return { isOnboarded: false };
        }

        return {
          isOnboarded: onboardingData.isOnboarded,
        };
      }),
    );
  }

  async deleteOnboarding({ user }: AuthDto): Promise<void> {
    return runPromise(this.fx.user.deleteMetadata(user.id, UserMetadataKey.Onboarding));
  }

  async setOnboarding(auth: AuthDto, onboarding: OnboardingDto): Promise<OnboardingResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.user.upsertMetadata(auth.user.id, {
          key: UserMetadataKey.Onboarding,
          value: {
            isOnboarded: onboarding.isOnboarded,
          },
        });

        return {
          isOnboarded: onboarding.isOnboarded,
        };
      }),
    );
  }

  @OnEvent({ name: 'AssetCreate' })
  async onAssetCreate({ asset, file }: ArgOf<'AssetCreate'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (file) {
          yield* this.fx.user.updateUsage(asset.ownerId, file.size);
        }
      }),
    );
  }

  @OnJob({ name: JobName.UserSyncUsage, queue: QueueName.BackgroundTask })
  async handleUserSyncUsage(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.user.syncUsage();
        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.UserDeleteCheck, queue: QueueName.BackgroundTask })
  async handleUserDeleteCheck(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        const users = yield* this.fx.user.getDeletedAfter(DateTime.now().minus({ days: config.user.deleteDelay }));
        yield* this.fx.job.queueAll(users.map((user) => ({ name: JobName.UserDelete, data: { id: user.id } })));
        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.UserDelete, queue: QueueName.BackgroundTask })
  async handleUserDelete({ id, force }: JobOf<JobName.UserDelete>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        const user = yield* this.fx.user.get(id, { withDeleted: true });
        if (!user) {
          return;
        }

        // just for extra protection here
        if (!force && !this.isReadyForDeletion(user, config.user.deleteDelay)) {
          this.logger.warn(`Skipped user that was not ready for deletion: id=${id}`);
          return;
        }

        this.logger.log(`Deleting user: ${user.id}`);

        const folders = [
          StorageCore.getLibraryFolder(user),
          StorageCore.getFolderLocation(StorageFolder.Upload, user.id),
          StorageCore.getFolderLocation(StorageFolder.Profile, user.id),
          StorageCore.getFolderLocation(StorageFolder.Thumbnails, user.id),
          StorageCore.getFolderLocation(StorageFolder.EncodedVideo, user.id),
        ];

        for (const folder of folders) {
          this.logger.warn(`Removing user from filesystem: ${folder}`);
          yield* this.fx.storage.unlinkDir(folder, { recursive: true, force: true });
        }

        this.logger.warn(`Removing user from database: ${user.id}`);
        yield* this.fx.album.deleteAll(user.id);
        yield* this.fx.user.delete(user, true);

        yield* this.fx.event.emit('UserDelete', user);
      }),
    );
  }

  private isReadyForDeletion(user: { id: string; deletedAt?: Date | null }, delayUntilDeletion: number): boolean {
    if (!user.deletedAt) {
      return false;
    }

    return DateTime.now().minus({ days: delayUntilDeletion }) > DateTime.fromJSDate(user.deletedAt);
  }

  private findOrFail = Effect.fn('UserService.findOrFail')(function* (
    this: UserService,
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
