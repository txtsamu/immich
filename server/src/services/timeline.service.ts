import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AuthDto, isMetadataHidden } from 'src/dtos/auth.dto.js';
import { TimeBucketAssetDto, TimeBucketDto, TimeBucketsResponseDto } from 'src/dtos/time-bucket.dto.js';
import { runPromise } from 'src/effect/run.js';
import { AssetVisibility, Permission } from 'src/enum.js';
import { TimeBucketOptions } from 'src/repositories/asset.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { requireElevatedPermission } from 'src/utils/access.js';
import { getMyPartnerIds } from 'src/utils/asset.util.js';

@Injectable()
export class TimelineService extends BaseService {
  async getTimeBuckets(auth: AuthDto, dto: TimeBucketDto): Promise<TimeBucketsResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.timeBucketChecks(auth, dto);
        const timeBucketOptions = yield* this.buildTimeBucketOptions(auth, dto);
        return yield* this.fx.asset.getTimeBuckets(timeBucketOptions);
      }),
    );
  }

  // pre-jsonified response
  async getTimeBucket(auth: AuthDto, dto: TimeBucketAssetDto): Promise<string> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.timeBucketChecks(auth, dto);
        const timeBucketOptions = yield* this.buildTimeBucketOptions(auth, { ...dto });

        // TODO: use id cursor for pagination
        const bucket = yield* this.fx.asset.getTimeBucket(dto.timeBucket, timeBucketOptions, auth);
        return bucket.assets;
      }),
    );
  }

  private buildTimeBucketOptions = Effect.fn('TimelineService.buildTimeBucketOptions')(function* (
    this: TimelineService,
    auth: AuthDto,
    dto: TimeBucketDto,
  ) {
    const { userId, ...options } = dto;
    let userIds: string[] | undefined;

    if (userId) {
      userIds = [userId];
      if (dto.withPartners) {
        const partnerIds = yield* Effect.tryPromise({
          try: () =>
            getMyPartnerIds({
              userId: auth.user.id,
              repository: this.partnerRepository,
              timelineEnabled: true,
            }),
          catch: (cause) => cause,
        });
        userIds.push(...partnerIds);
      }
    }

    const timeBucketOptions: TimeBucketOptions = { ...options, userIds };
    return timeBucketOptions;
  });

  private timeBucketChecks = Effect.fn('TimelineService.timeBucketChecks')(function* (
    this: TimelineService,
    auth: AuthDto,
    dto: TimeBucketDto,
  ) {
    if (dto.visibility === AssetVisibility.Locked) {
      yield* Effect.try({ try: () => requireElevatedPermission(auth), catch: (cause) => cause });
    }

    if (dto.albumId) {
      yield* this.requireAccessFx({ auth, permission: Permission.AlbumRead, ids: [dto.albumId] });
    } else {
      dto.userId ||= auth.user.id;
    }

    if (dto.userId) {
      yield* this.requireAccessFx({ auth, permission: Permission.TimelineRead, ids: [dto.userId] });
    }

    const includesOtherUsers = dto.withPartners || (dto.userId && dto.userId !== auth.user.id);
    if (includesOtherUsers) {
      dto.visibility ??= AssetVisibility.Timeline;
      if (dto.visibility !== AssetVisibility.Timeline || dto.isTrashed || dto.isFavorite !== undefined) {
        return yield* Effect.fail(
          new BadRequestException(
            'Only non-archived, non-trashed, non-favorited, non-locked assets can be requested for other users',
          ),
        );
      }
    }

    if (dto.tagId) {
      yield* this.requireAccessFx({ auth, permission: Permission.TagRead, ids: [dto.tagId] });
    }

    if (isMetadataHidden(auth)) {
      dto.withCoordinates = false;
    }
  });
}
