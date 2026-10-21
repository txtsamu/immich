import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import {
  AdminOnboardingResponseDto,
  AdminOnboardingUpdateDto,
  ReverseGeocodingStateResponseDto,
  VersionCheckStateResponseDto,
} from 'src/dtos/system-metadata.dto.js';
import { runPromise } from 'src/effect/run.js';
import { SystemMetadataKey } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';

@Injectable()
export class SystemMetadataService extends BaseService {
  async getAdminOnboarding(): Promise<AdminOnboardingResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const value = yield* this.fx.systemMetadata.get(SystemMetadataKey.AdminOnboarding);
        return { isOnboarded: false, ...value };
      }),
    );
  }

  async updateAdminOnboarding(dto: AdminOnboardingUpdateDto): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.systemMetadata.set(SystemMetadataKey.AdminOnboarding, {
          isOnboarded: dto.isOnboarded,
        });
      }),
    );
  }

  async getReverseGeocodingState(): Promise<ReverseGeocodingStateResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const value = yield* this.fx.systemMetadata.get(SystemMetadataKey.ReverseGeocodingState);
        return { lastUpdate: null, lastImportFileName: null, ...value };
      }),
    );
  }

  async getVersionCheckState(): Promise<VersionCheckStateResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const value = yield* this.fx.systemMetadata.get(SystemMetadataKey.VersionCheckState);
        return { checkedAt: null, releaseVersion: null, ...value };
      }),
    );
  }
}
