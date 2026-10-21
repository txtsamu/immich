import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { Activity } from 'src/database.js';
import {
  ActivityCreateDto,
  ActivityDto,
  ActivityResponseDto,
  ActivitySearchDto,
  ActivityStatisticsResponseDto,
  MaybeDuplicate,
  ReactionLevel,
  ReactionType,
  mapActivity,
} from 'src/dtos/activity.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';

@Injectable()
export class ActivityService extends BaseService {
  async getAll(auth: AuthDto, dto: ActivitySearchDto): Promise<ActivityResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumRead, ids: [dto.albumId] });
        const activities = yield* this.fx.activity.search({
          userId: dto.userId,
          albumId: dto.albumId,
          assetId: dto.level === ReactionLevel.ALBUM ? null : dto.assetId,
          isLiked: dto.type && dto.type === ReactionType.LIKE,
        });

        return activities.map((activity) => mapActivity(activity));
      }),
    );
  }

  async getStatistics(auth: AuthDto, dto: ActivityDto): Promise<ActivityStatisticsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumRead, ids: [dto.albumId] });
        return yield* this.fx.activity.getStatistics({ albumId: dto.albumId, assetId: dto.assetId });
      }),
    );
  }

  async create(auth: AuthDto, dto: ActivityCreateDto): Promise<MaybeDuplicate<ActivityResponseDto>> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.ActivityCreate, ids: [dto.albumId] });

        const common = {
          userId: auth.user.id,
          assetId: dto.assetId,
          albumId: dto.albumId,
        };

        let activity: Activity | undefined;
        let isDuplicate = false;

        if (dto.type === ReactionType.LIKE) {
          delete dto.comment;
          [activity] = yield* this.fx.activity.search({
            ...common,
            // `null` will search for an album like
            assetId: dto.assetId ?? null,
            isLiked: true,
          });
          isDuplicate = !!activity;
        }

        if (!activity) {
          activity = yield* this.fx.activity.create({
            ...common,
            isLiked: dto.type === ReactionType.LIKE,
            comment: dto.comment,
          });
        }

        return { duplicate: isDuplicate, value: mapActivity(activity) };
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.ActivityDelete, ids: [id] });
        yield* this.fx.activity.delete(id);
      }),
    );
  }
}
