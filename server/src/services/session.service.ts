import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { DateTime } from 'luxon';
import type { ArgOf } from 'src/repositories/event.repository.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  SessionCreateDto,
  SessionCreateResponseDto,
  SessionResponseDto,
  SessionUpdateDto,
  mapSession,
} from 'src/dtos/session.dto.js';
import { runPromise } from 'src/effect/run.js';
import { JobName, JobStatus, Permission, QueueName } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';

@Injectable()
export class SessionService extends BaseService {
  @OnJob({ name: JobName.SessionCleanup, queue: QueueName.BackgroundTask })
  async handleCleanup(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sessions = yield* this.fx.session.cleanup();
        for (const session of sessions) {
          this.logger.verbose(`Deleted expired session token: ${session.deviceOS}/${session.deviceType}`);
        }

        this.logger.log(`Deleted ${sessions.length} expired session tokens`);

        return JobStatus.Success;
      }),
    );
  }

  async create(auth: AuthDto, dto: SessionCreateDto): Promise<SessionCreateResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!auth.session) {
          return yield* Effect.fail(new BadRequestException('This endpoint can only be used with a session token'));
        }

        const token = yield* this.fx.crypto.randomBytesAsText(32);
        const hashed = yield* this.fx.crypto.hashSha256(token);
        const session = yield* this.fx.session.create({
          parentId: auth.session.id,
          userId: auth.user.id,
          expiresAt: dto.duration ? DateTime.now().plus({ seconds: dto.duration }).toJSDate() : null,
          deviceType: dto.deviceType,
          deviceOS: dto.deviceOS,
          token: hashed,
        });

        return { ...mapSession(session), token };
      }),
    );
  }

  async getAll(auth: AuthDto): Promise<SessionResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sessions = yield* this.fx.session.getByUserId(auth.user.id);
        return sessions.map((session) => mapSession(session, auth.session?.id));
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: SessionUpdateDto): Promise<SessionResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.SessionUpdate, ids: [id] });

        if (Object.values(dto).filter((prop) => prop !== undefined).length === 0) {
          return yield* Effect.fail(new BadRequestException('No fields to update'));
        }

        const session = yield* this.fx.session.update(id, {
          isPendingSyncReset: dto.isPendingSyncReset,
        });

        return mapSession(session);
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AuthDeviceDelete, ids: [id] });
        yield* this.fx.session.delete(id);
      }),
    );
  }

  async deleteAll(auth: AuthDto): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const userId = auth.user.id;
        const currentSessionId = auth.session?.id;
        yield* this.fx.session.invalidateAll({ userId, excludeId: currentSessionId });
      }),
    );
  }

  async lock(auth: AuthDto, id: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.SessionLock, ids: [id] });
        yield* this.fx.session.update(id, { pinExpiresAt: null });
      }),
    );
  }

  @OnEvent({ name: 'AuthChangePassword' })
  async onAuthChangePassword({ userId, currentSessionId }: ArgOf<'AuthChangePassword'>): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.session.invalidateAll({ userId, excludeId: currentSessionId });
      }),
    );
  }
}
