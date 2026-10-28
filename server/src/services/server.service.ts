import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { Effect } from 'effect';
import { serverVersion } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent } from 'src/decorators.js';
import { LicenseKeyDto, LicenseResponseDto } from 'src/dtos/license.dto.js';
import {
  ServerAboutResponseDto,
  ServerApkLinksDto,
  ServerConfigDto,
  ServerFeaturesDto,
  ServerMediaTypesResponseDto,
  ServerPingResponse,
  ServerStatsResponseDto,
  ServerStorageResponseDto,
  UsageByUserDto,
} from 'src/dtos/server.dto.js';
import { runPromise } from 'src/effect/run.js';
import { StorageFolder, SystemMetadataKey } from 'src/enum.js';
import { UserStatsQueryResponse } from 'src/repositories/user.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { asHumanReadable } from 'src/utils/bytes.js';
import { mimeTypes } from 'src/utils/mime-types.js';

@Injectable()
export class ServerService extends BaseService {
  @OnEvent({ name: 'AppBootstrap' })
  async onBootstrap(): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const featureFlags = yield* Effect.tryPromise({ try: () => this.getFeatures(), catch: (cause) => cause });
        if (featureFlags.configFile) {
          yield* this.fx.systemMetadata.set(SystemMetadataKey.AdminOnboarding, {
            isOnboarded: true,
          });
        }
        this.logger.log(
          `Feature Flags: ${JSON.stringify(yield* Effect.tryPromise({ try: () => this.getFeatures(), catch: (cause) => cause }))}`,
        );
      }),
    );
  }

  async getAboutInfo(): Promise<ServerAboutResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const version = `v${serverVersion.toString()}`;
        const { buildMetadata } = yield* this.fx.config.getEnv();
        const buildVersions = yield* this.fx.serverInfo.getBuildVersions();
        const licensed = yield* this.fx.systemMetadata.get(SystemMetadataKey.License);

        return {
          version,
          versionUrl: `https://github.com/immich-app/immich/releases/tag/${version}`,
          licensed: !!licensed,
          ...buildMetadata,
          ...buildVersions,
        };
      }),
    );
  }

  getApkLinks(): ServerApkLinksDto {
    const baseUrl = `https://github.com/immich-app/immich/releases/download/v${serverVersion.toString()}`;
    return {
      arm64v8a: `${baseUrl}/app-arm64-v8a-release.apk`,
      armeabiv7a: `${baseUrl}/app-armeabi-v7a-release.apk`,
      universal: `${baseUrl}/app-release.apk`,
      x86_64: `${baseUrl}/app-x86_64-release.apk`,
    };
  }

  async getStorage(): Promise<ServerStorageResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const libraryBase = StorageCore.getBaseFolder(StorageFolder.Library);
        const diskInfo = yield* this.fx.storage.checkDiskUsage(libraryBase);

        const usagePercentage = (((diskInfo.total - diskInfo.free) / diskInfo.total) * 100).toFixed(2);

        const serverInfo = new ServerStorageResponseDto();
        serverInfo.diskAvailable = asHumanReadable(diskInfo.available);
        serverInfo.diskSize = asHumanReadable(diskInfo.total);
        serverInfo.diskUse = asHumanReadable(diskInfo.total - diskInfo.free);
        serverInfo.diskAvailableRaw = diskInfo.available;
        serverInfo.diskSizeRaw = diskInfo.total;
        serverInfo.diskUseRaw = diskInfo.total - diskInfo.free;
        serverInfo.diskUsagePercentage = Number(usagePercentage);
        return serverInfo;
      }),
    );
  }

  ping(): ServerPingResponse {
    return { res: 'pong' };
  }

  async getFeatures(): Promise<ServerFeaturesDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { reverseGeocoding, map, trash, oauth, passwordLogin, notifications, ffmpeg } = yield* this.getConfigFx({
          withCache: false,
        });
        const { configFile } = yield* this.fx.config.getEnv();

        return {
          map: map.enabled,
          reverseGeocoding: reverseGeocoding.enabled,
          sidecar: true,
          search: true,
          trash: trash.enabled,
          oauth: oauth.enabled,
          oauthAutoLaunch: oauth.autoLaunch,
          passwordLogin: passwordLogin.enabled,
          configFile: !!configFile,
          email: notifications.smtp.enabled,
          realtimeTranscoding: ffmpeg.realtime.enabled,
        };
      }),
    );
  }

  async getSystemConfig(): Promise<ServerConfigDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: false });
        const isInitialized = !(yield* this.isSetupAvailableFx());
        const onboarding = yield* this.fx.systemMetadata.get(SystemMetadataKey.AdminOnboarding);

        return {
          loginPageMessage: config.server.loginPageMessage,
          trashDays: config.trash.days,
          userDeleteDelay: config.user.deleteDelay,
          oauthButtonText: config.oauth.buttonText,
          oauthAccountManagementUrl: config.oauth.accountManagementUrl,
          isInitialized,
          isOnboarded: onboarding?.isOnboarded || false,
          externalDomain: config.server.externalDomain,
          publicUsers: config.server.publicUsers,
          mapDarkStyleUrl: config.map.darkStyle,
          mapLightStyleUrl: config.map.lightStyle,
          maintenanceMode: false,
        };
      }),
    );
  }

  async getStatistics(): Promise<ServerStatsResponseDto> {
    const userStats: UserStatsQueryResponse[] = await runPromise(this.fx.user.getUserStats());
    const serverStats = new ServerStatsResponseDto();
    serverStats.photos ??= 0;
    serverStats.videos ??= 0;
    serverStats.usage ??= 0;
    serverStats.usagePhotos ??= 0;
    serverStats.usageVideos ??= 0;
    serverStats.usageByUser ??= [];

    for (const user of userStats) {
      const usage = new UsageByUserDto();
      usage.userId = user.userId;
      usage.userName = user.userName;
      usage.photos = user.photos;
      usage.videos = user.videos;
      usage.usage = user.usage;
      usage.usagePhotos = user.usagePhotos;
      usage.usageVideos = user.usageVideos;
      usage.quotaSizeInBytes = user.quotaSizeInBytes;

      serverStats.photos += usage.photos;
      serverStats.videos += usage.videos;
      serverStats.usage += usage.usage;
      serverStats.usagePhotos += usage.usagePhotos;
      serverStats.usageVideos += usage.usageVideos;

      serverStats.usageByUser.push(usage);
    }

    return serverStats;
  }

  getSupportedMediaTypes(): ServerMediaTypesResponseDto {
    return {
      video: Object.keys(mimeTypes.video),
      image: Object.keys(mimeTypes.image),
      sidecar: Object.keys(mimeTypes.sidecar),
    };
  }

  async deleteLicense(): Promise<void> {
    return runPromise(this.fx.systemMetadata.delete(SystemMetadataKey.License));
  }

  async getLicense(): Promise<LicenseResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const license = yield* this.fx.systemMetadata.get(SystemMetadataKey.License);
        if (!license) {
          return yield* Effect.fail(new NotFoundException());
        }
        return license;
      }),
    );
  }

  async setLicense(dto: LicenseKeyDto): Promise<LicenseResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!dto.licenseKey.startsWith('IMSV-')) {
          return yield* Effect.fail(new BadRequestException('Invalid license key'));
        }
        const { licensePublicKey } = yield* this.fx.config.getEnv();
        const isLicenseValid = yield* this.fx.crypto.verifySha256(
          dto.licenseKey,
          dto.activationKey,
          licensePublicKey.server,
        );
        if (!isLicenseValid) {
          return yield* Effect.fail(new BadRequestException('Invalid license key'));
        }

        const licenseData = { ...dto, activatedAt: new Date() };

        yield* this.fx.systemMetadata.set(SystemMetadataKey.License, licenseData);

        return licenseData;
      }),
    );
  }
}
