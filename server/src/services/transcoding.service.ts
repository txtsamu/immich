import { Injectable } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import { ChildProcess } from 'node:child_process';
import { join } from 'node:path';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { VideoInterfaces } from 'src/types.js';
import {
  HLS_BACKPRESSURE_PAUSE_SEGMENTS,
  HLS_BACKPRESSURE_RESUME_SEGMENTS,
  HLS_CLEANUP_INTERVAL_MS,
  HLS_CRF,
  HLS_INACTIVITY_TIMEOUT_MS,
  HLS_LEASE_DURATION_MS,
  HLS_SEGMENT_DURATION,
  HLS_SEGMENT_FILENAME_REGEX,
  HLS_VARIANTS,
} from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import { DatabaseLock, ImmichWorker, JobName, QueueName, TranscodeTarget } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { isVideoStreamSessionPkConstraint } from 'src/utils/database.js';
import { BaseConfig } from 'src/utils/media.js';

type Session = {
  assetId: string;
  expiresAt: Date;
  id: string;
  lastActivityTime: Date;
  lastClientRequestedSegment: number | null;
  lastCompletedSegment: number | null;
  ownerId: string;
  paused: boolean;
  process: ChildProcess | null;
  starting: boolean;
  startSegment: number | null;
  variantIndex: number | null;
};

/** The value originally thrown, for errors that wrap it in `cause` (see `src/effect/errors.ts`). */
@Injectable()
export class TranscodingService extends BaseService {
  private sessions = new Map<string, Session>();
  private videoInterfaces: VideoInterfaces = { dri: [], mali: false };
  private cleanupInterval: NodeJS.Timeout | null = null;

  @OnEvent({ name: 'AppBootstrap', workers: [ImmichWorker.Microservices] })
  async onBootstrap() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const [videoInterfaces] = yield* Effect.all(
          [
            Effect.tryPromise({ try: () => this.storageCore.getVideoInterfaces(), catch: (cause) => cause }),
            this.removeExpiredSessions(),
          ],
          { concurrency: 'unbounded' },
        );
        this.videoInterfaces = videoInterfaces;
      }),
    );
  }

  @OnEvent({ name: 'AppShutdown', workers: [ImmichWorker.Microservices] })
  onShutdown() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    return Promise.all(this.sessions.values().map(({ id }) => this.onSessionEnd({ sessionId: id })));
  }

  @OnJob({ name: JobName.HlsSessionCleanup, queue: QueueName.BackgroundTask })
  onHlsSessionCleanup() {
    return runPromise(this.removeExpiredSessions());
  }

  @OnEvent({ name: 'HlsSessionRequest', server: true, workers: [ImmichWorker.Microservices] })
  async onSessionRequest({ assetId, sessionId, ownerId }: ArgOf<'HlsSessionRequest'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const expiresAt = new Date(Date.now() + HLS_LEASE_DURATION_MS);
        yield* this.fx.videoStream.createSession({ id: sessionId, assetId, expiresAt });
        this.sessions.set(sessionId, {
          assetId,
          expiresAt,
          id: sessionId,
          lastActivityTime: new Date(),
          lastClientRequestedSegment: null,
          lastCompletedSegment: null,
          ownerId,
          paused: false,
          process: null,
          starting: false,
          startSegment: null,
          variantIndex: null,
        });
        this.cleanupInterval ??= setInterval(
          () => void runPromise(this.removeInactiveSessions()),
          HLS_CLEANUP_INTERVAL_MS,
        );
        yield* this.fx.websocket.serverSend('HlsSessionResult', { sessionId });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen({ self: this }, function* () {
            const error = unwrap(Cause.squash(cause));
            // If insertion failed due to a PK constraint, another worker has already created a session for this ID.
            if (isVideoStreamSessionPkConstraint(error)) {
              return;
            }

            this.logger.error(`Failed to create HLS session ${sessionId}: ${error}`);
            yield* this.fx.websocket.serverSend('HlsSessionResult', {
              sessionId,
              error: 'Failed to create HLS session',
            });
          }),
        ),
      ),
    );
  }

  @OnEvent({ name: 'HlsSessionEnd', server: true, workers: [ImmichWorker.Microservices] })
  async onSessionEnd({ sessionId }: ArgOf<'HlsSessionEnd'>) {
    return runPromise(this.endSession(sessionId));
  }

  @OnEvent({ name: 'HlsHeartbeat', server: true, workers: [ImmichWorker.Microservices] })
  async onHeartbeat({ sessionId, segmentIndex }: ArgOf<'HlsHeartbeat'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const session = this.sessions.get(sessionId);
        if (!session) {
          return;
        }

        session.lastActivityTime = new Date();

        if (segmentIndex !== undefined) {
          session.lastClientRequestedSegment = segmentIndex;
          this.applyBackpressure(session);
        }

        const remaining = session.expiresAt.getTime() - Date.now();
        // eslint-disable-next-line unicorn/prefer-early-return
        if (remaining < HLS_LEASE_DURATION_MS / 2) {
          session.expiresAt = new Date(Date.now() + HLS_LEASE_DURATION_MS);
          yield* this.fx.videoStream.extendSession(sessionId, session.expiresAt);
        }
      }),
    );
  }

  @OnEvent({ name: 'HlsSegmentRequest', server: true, workers: [ImmichWorker.Microservices] })
  async onSegmentRequest({ sessionId, variantIndex, segmentIndex }: ArgOf<'HlsSegmentRequest'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const session = this.sessions.get(sessionId);
        if (!session) {
          return;
        }

        session.variantIndex ??= variantIndex;
        session.startSegment ??= segmentIndex;
        const curSegment =
          session.lastCompletedSegment === null ? session.startSegment : session.lastCompletedSegment + 1;
        const isNeedsRestart =
          session.variantIndex !== variantIndex || segmentIndex < session.startSegment || segmentIndex > curSegment + 1;
        if (isNeedsRestart) {
          this.stopTranscode(session);
          session.variantIndex = variantIndex;
          session.startSegment = segmentIndex;
        } else if (session.process) {
          this.resumeTranscode(session);
          return;
        } else if (session.starting) {
          this.logger.debug(`Session ${sessionId} is already starting a transcode, skipping duplicate start request`);
          return;
        }

        session.starting = true;
        const process = yield* this.startTranscode(session, variantIndex, segmentIndex).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              session.starting = false;
            }),
          ),
        );
        if (process) {
          session.process = process;
        }
      }),
    );
  }

  private applyBackpressure(session: Session) {
    if (session.lastCompletedSegment === null || session.lastClientRequestedSegment === null) {
      return;
    }
    const lead = session.lastCompletedSegment - session.lastClientRequestedSegment;
    this.logger.debug(`Session ${session.id} lead is ${lead} segments`);
    if (!session.paused && lead > HLS_BACKPRESSURE_PAUSE_SEGMENTS) {
      this.pauseTranscode(session);
    } else if (session.paused && lead < HLS_BACKPRESSURE_RESUME_SEGMENTS) {
      this.resumeTranscode(session);
    }
  }

  private startTranscode = Effect.fn('TranscodingService.startTranscode')(function* (
    this: TranscodingService,
    session: Session,
    variantIndex: number,
    startSegment: number,
  ) {
    const { ffmpeg } = yield* this.getConfigFx({ withCache: true });

    const asset = yield* this.fx.videoStream.getForTranscoding(session.assetId);
    if (!asset) {
      this.logger.error(`Asset ${session.assetId} not found for HLS transcoding`);
      return;
    }

    if (session.variantIndex !== variantIndex || session.startSegment !== startSegment) {
      return;
    }

    const variant = HLS_VARIANTS[variantIndex];
    if (!variant) {
      this.logger.error(`Variant ${variantIndex} out of range for asset ${session.assetId}`);
      yield* this.failSession(session, `Invalid variant index ${variantIndex}`);
      return;
    }

    const variantDir = StorageCore.getHlsVariantFolder({
      ownerId: session.ownerId,
      sessionId: session.id,
      variantIndex,
    });
    yield* this.fx.storage.mkdirSync(variantDir);

    // Encoder runs at fps = packetCount × timeBase / totalDuration with
    // gop = ceil(SEGMENT_DURATION × fps). To start segment K's content at
    // exactly cfr slot K × gop, seek to the midpoint between slots K×gop−1 and
    // K×gop. accurate_seek's "discard < target" then keeps the source frame
    // that quantizes to slot K×gop and discards the one quantizing to K×gop−1.
    const fps = (asset.packets.packetCount * asset.videoStream.timeBase) / asset.packets.totalDuration;
    const gop = Math.ceil(HLS_SEGMENT_DURATION * fps);
    const seekSeconds = startSegment > 0 ? (startSegment * gop - 0.5) / fps : 0;

    const config = yield* Effect.try({
      try: () =>
        BaseConfig.create(
          {
            ...ffmpeg,
            targetVideoCodec: variant.codec,
            targetResolution: String(variant.resolution),
            maxBitrate: `${Math.round(variant.bitrate / 1000)}k`,
            gopSize: gop,
            crf: HLS_CRF[variant.codec],
          },
          this.videoInterfaces,
          { strictGop: true, lowLatency: true },
        ),
      catch: (cause) => cause,
    }).pipe(
      Effect.catch((error: any) =>
        Effect.gen({ self: this }, function* () {
          this.logger.error(
            `Failed to create transcode config for variant ${variantIndex} asset ${session.assetId}: ${error?.message ?? error}`,
          );
          yield* this.failSession(session, `Failed to start transcode: ${error?.message ?? 'unknown error'}`);
          return;
        }),
      ),
    );
    if (!config) {
      return;
    }
    const args = config.getHlsCommand(
      {
        initFilename: 'init.mp4',
        inputPath: asset.originalPath,
        packetCount: asset.packets.packetCount,
        playlistFilename: join(variantDir, 'playlist.m3u8'),
        seekSeconds,
        segmentDuration: HLS_SEGMENT_DURATION,
        segmentFilename: join(variantDir, 'seg_%d.m4s'),
        startSegment,
        target: TranscodeTarget.All,
        timeBase: asset.videoStream.timeBase,
        totalDuration: asset.packets.totalDuration,
      },
      asset.videoStream,
      asset.audioStream ?? undefined,
    );
    this.logger.log(
      `Starting HLS transcode for asset ${session.assetId} variant ${variantIndex} with command: ffmpeg ${args.join(' ')}`,
    );
    const process = this.processRepository.spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    this.attachProcessHandlers(process, session, variantIndex);
    return process;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private failSession = Effect.fn('TranscodingService.failSession')(function* (
    this: TranscodingService,
    session: Session,
    error: string,
  ) {
    yield* this.fx.websocket.serverSend('HlsSessionResult', { sessionId: session.id, error });
    yield* this.endSession(session.id);
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private endSession = Effect.fn('TranscodingService.endSession')(function* (
    this: TranscodingService,
    sessionId: string,
  ) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      return;
    }
    this.sessions.delete(sessionId);
    if (this.cleanupInterval && this.sessions.size === 0) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    this.stopTranscode(session);
    yield* this.removeSessionDir(session);
    yield* this.fx.videoStream.deleteSession(sessionId);
  });

  private attachProcessHandlers(process: ChildProcess, session: Session, variantIndex: number) {
    let stderr = '';
    const variantDir = StorageCore.getHlsVariantFolder({
      ownerId: session.ownerId,
      sessionId: session.id,
      variantIndex,
    });

    // hlsenc writes each segment as `seg_K.m4s.tmp` then renames to
    // `seg_K.m4s`. The rename event fires the moment the renamed file is
    // observable — the only signal we need to tell the API worker the
    // segment is ready to serve.
    const watcher = this.storageRepository.watchDir(variantDir, (eventType, filename) => {
      if (eventType !== 'rename' || !filename || session.process !== process) {
        return;
      }
      const match = HLS_SEGMENT_FILENAME_REGEX.exec(filename);
      if (!match) {
        return;
      }
      const segmentIndex = Number.parseInt(match[1]);
      const expected = session.lastCompletedSegment === null ? session.startSegment : session.lastCompletedSegment + 1;
      // Ignore stale events from old process after seek
      if (expected === null || segmentIndex !== expected) {
        return;
      }
      session.lastCompletedSegment = segmentIndex;
      this.websocketRepository.serverSend('HlsSegmentResult', {
        sessionId: session.id,
        variantIndex,
        segmentIndex,
      });
      this.applyBackpressure(session);
    });
    watcher.on('error', (error) => {
      this.logger.error(`watcher error for ${variantDir}: ${error}`);
    });

    process.stderr!.on('data', (chunk: Buffer) => {
      if (session.process !== process) {
        return;
      }
      stderr += chunk.toString();
    });

    process.on('exit', (code) => {
      watcher.close();
      if (session.process !== process || session.variantIndex !== variantIndex) {
        return;
      }
      session.paused = false;
      session.process = null;
      session.lastCompletedSegment = null;

      if (!code) {
        return;
      }

      this.logger.error(
        `FFmpeg exited with code ${code} for variant ${variantIndex} asset ${session.assetId}\n${stderr}`,
      );
      void runPromise(this.failSession(session, `Transcoding process exited unexpectedly with code ${code}`)).catch(
        (error) => this.logger.error(`Failed to end session ${session.id} after ffmpeg exit: ${error}`),
      );
    });
  }

  private stopTranscode(session: Session) {
    if (!session.process) {
      return;
    }
    // SIGTERM makes it rename .tmp segments to .m4s even if they're still incomplete
    session.process.kill('SIGKILL');
    session.process = null;
    session.lastCompletedSegment = null;
    session.paused = false;
    this.logger.debug(`Stopped transcoding for session ${session.id}`);
  }

  private pauseTranscode(session: Session) {
    if (session.paused || !session.process) {
      return;
    }
    session.process.kill('SIGSTOP');
    session.paused = true;
    this.logger.debug(`Paused transcoding for session ${session.id}`);
  }

  private resumeTranscode(session: Session) {
    if (!session.paused || !session.process) {
      return;
    }
    session.process.kill('SIGCONT');
    session.paused = false;
    this.logger.debug(`Resumed transcoding for session ${session.id}`);
  }

  private removeSessionDir = Effect.fn('TranscodingService.removeSessionDir')(function* (
    this: TranscodingService,
    session: { ownerId: string; id: string },
  ) {
    const dir = StorageCore.getHlsSessionFolder({ ownerId: session.ownerId, sessionId: session.id });
    yield* this.fx.storage.unlinkDir(dir, { recursive: true, force: true }).pipe(
      Effect.catch((error_) => {
        const error = unwrap(error_);
        if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') {
          return Effect.fail(error);
        }
        this.logger.warn(`Session dir ${dir} does not exist.`);
        return Effect.void;
      }),
    );
  });

  private removeInactiveSessions = Effect.fn('TranscodingService.removeInactiveSessions')(function* (
    this: TranscodingService,
  ) {
    const cutoff = Date.now() - HLS_INACTIVITY_TIMEOUT_MS;
    const inactiveSessions = this.sessions
      .values()
      .filter((s) => s.lastActivityTime.getTime() < cutoff)
      .toArray();
    yield* Effect.forEach(
      inactiveSessions,
      (session) =>
        Effect.gen({ self: this }, function* () {
          yield* this.fx.websocket.serverSend('HlsSessionEnd', { sessionId: session.id });
          yield* this.endSession(session.id);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.sync(() => {
              this.logger.error(`Failed to sweep inactive HLS session ${session.id}: ${unwrap(Cause.squash(cause))}`);
            }),
          ),
        ),
      { concurrency: 'unbounded' },
    );
  });

  private removeExpiredSessions = Effect.fn('TranscodingService.removeExpiredSessions')(function* (
    this: TranscodingService,
  ) {
    yield* this.fx.database.withLock(DatabaseLock.HlsSessionCleanup, () =>
      runPromise(
        Effect.gen({ self: this }, function* () {
          const expiredSessions = yield* this.fx.videoStream.getExpiredSessions();
          yield* Effect.all(
            expiredSessions.map((session) =>
              Effect.gen({ self: this }, function* () {
                yield* this.removeSessionDir(session);
                yield* this.fx.videoStream.deleteSession(session.id);
              }),
            ),
            { concurrency: 'unbounded' },
          );
        }),
      ),
    );
  });
}
