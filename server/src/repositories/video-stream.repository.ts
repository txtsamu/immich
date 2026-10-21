import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely } from 'kysely';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { DatabaseError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import {
  VideoStreamSegmentTable,
  VideoStreamSessionTable,
  VideoStreamVariantTable,
} from 'src/schema/tables/video-stream.table.js';
import { withAudioStream, withVideoFormat, withVideoPackets, withVideoStream } from 'src/utils/database.js';

const createSession = (db: Kysely<DB>, session: Insertable<VideoStreamSessionTable>) =>
  db.insertInto('video_stream_session').values(session).returning(['id']).executeTakeFirstOrThrow();

const createVariant = (db: Kysely<DB>, variant: Insertable<VideoStreamVariantTable>) =>
  db.insertInto('video_stream_variant').values(variant).returning(['id']).executeTakeFirstOrThrow();

const getSession = (db: Kysely<DB>, id: string) =>
  db
    .selectFrom('video_stream_session')
    .selectAll()
    .where('id', '=', id)
    .where('expiresAt', '>', new Date())
    .executeTakeFirst();

const getVariant = (db: Kysely<DB>, id: string) =>
  db.selectFrom('video_stream_variant').selectAll().where('id', '=', id).executeTakeFirst();

const getSegment = (db: Kysely<DB>, variantId: string, index: number) =>
  db
    .selectFrom('video_stream_segment')
    .selectAll()
    .where('variantId', '=', variantId)
    .where('index', '=', index)
    .executeTakeFirst();

const getExpiredSessions = (db: Kysely<DB>) =>
  db
    .selectFrom('video_stream_session')
    .innerJoin('asset', 'asset.id', 'video_stream_session.assetId')
    .select(['video_stream_session.id', 'asset.ownerId'])
    .where('video_stream_session.expiresAt', '<=', new Date())
    .execute();

const getForMainPlaylist = (db: Kysely<DB>, id: string) =>
  db
    .selectFrom('asset')
    .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
    .where('asset.id', '=', id)
    .innerJoin('asset_video', 'asset.id', 'asset_video.assetId')
    .innerJoin('asset_keyframe', 'asset.id', 'asset_keyframe.assetId')
    .select((eb) => withVideoStream(eb).$notNull().as('videoStream'))
    .select((eb) => withVideoPackets(eb).$notNull().as('packets'))
    .executeTakeFirst();

const getForMediaPlaylist = (db: Kysely<DB>, id: string, sessionId: string) =>
  db
    .selectFrom('asset')
    .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
    .innerJoin('video_stream_session', 'asset.id', 'video_stream_session.assetId')
    .where('asset.id', '=', id)
    .where('video_stream_session.id', '=', sessionId)
    .where('video_stream_session.expiresAt', '>', new Date())
    .innerJoin('asset_video', 'asset.id', 'asset_video.assetId')
    .innerJoin('asset_keyframe', 'asset.id', 'asset_keyframe.assetId')
    .select((eb) => withVideoStream(eb).$notNull().as('videoStream'))
    .select((eb) => withVideoPackets(eb).$notNull().as('packets'))
    .executeTakeFirst();

const getForTranscoding = (db: Kysely<DB>, id: string) =>
  db
    .selectFrom('asset')
    .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
    .where('asset.id', '=', id)
    .leftJoin('asset_audio', 'asset.id', 'asset_audio.assetId')
    .innerJoin('asset_video', 'asset.id', 'asset_video.assetId')
    .innerJoin('asset_keyframe', 'asset.id', 'asset_keyframe.assetId')
    .select('asset.originalPath')
    .select((eb) => withAudioStream(eb).as('audioStream'))
    .select((eb) => withVideoStream(eb).$notNull().as('videoStream'))
    .select((eb) => withVideoFormat(eb).$notNull().as('format'))
    .select((eb) => withVideoPackets(eb).$notNull().as('packets'))
    .executeTakeFirst();

/** Effect result of a query function: its resolved value, failing with `DatabaseError`. */
type Result<F extends (...args: any[]) => PromiseLike<any>> = Effect.Effect<Awaited<ReturnType<F>>, DatabaseError>;

export class VideoStreamRepo extends Context.Service<
  VideoStreamRepo,
  {
    createSession(session: Insertable<VideoStreamSessionTable>): Result<typeof createSession>;
    createVariant(variant: Insertable<VideoStreamVariantTable>): Result<typeof createVariant>;
    createSegment(segment: Insertable<VideoStreamSegmentTable>): Effect.Effect<void, DatabaseError>;
    getSession(id: string): Result<typeof getSession>;
    getVariant(id: string): Result<typeof getVariant>;
    getSegment(variantId: string, index: number): Result<typeof getSegment>;
    getExpiredSessions(): Result<typeof getExpiredSessions>;
    extendSession(id: string, expiresAt: Date): Effect.Effect<void, DatabaseError>;
    deleteSession(id: string): Effect.Effect<void, DatabaseError>;
    getForMainPlaylist(id: string): Result<typeof getForMainPlaylist>;
    getForMediaPlaylist(id: string, sessionId: string): Result<typeof getForMediaPlaylist>;
    getForTranscoding(id: string): Result<typeof getForTranscoding>;
  }
>()('immich/repositories/VideoStreamRepo') {
  static readonly make = (db: Kysely<DB>) =>
    VideoStreamRepo.of({
      createSession: Effect.fn('VideoStream.createSession')((session: Insertable<VideoStreamSessionTable>) =>
        query(db, (db) => createSession(db, session)),
      ),

      createVariant: Effect.fn('VideoStream.createVariant')((variant: Insertable<VideoStreamVariantTable>) =>
        query(db, (db) => createVariant(db, variant)),
      ),

      createSegment: Effect.fn('VideoStream.createSegment')(function* (segment: Insertable<VideoStreamSegmentTable>) {
        yield* query(db, (db) => db.insertInto('video_stream_segment').values(segment).execute());
      }),

      getSession: Effect.fn('VideoStream.getSession')((id: string) => query(db, (db) => getSession(db, id))),

      getVariant: Effect.fn('VideoStream.getVariant')((id: string) => query(db, (db) => getVariant(db, id))),

      getSegment: Effect.fn('VideoStream.getSegment')((variantId: string, index: number) =>
        query(db, (db) => getSegment(db, variantId, index)),
      ),

      getExpiredSessions: Effect.fn('VideoStream.getExpiredSessions')(() => query(db, (db) => getExpiredSessions(db))),

      extendSession: Effect.fn('VideoStream.extendSession')(function* (id: string, expiresAt: Date) {
        yield* query(db, (db) =>
          db.updateTable('video_stream_session').set({ expiresAt }).where('id', '=', id).execute(),
        );
      }),

      deleteSession: Effect.fn('VideoStream.deleteSession')(function* (id: string) {
        yield* query(db, (db) => db.deleteFrom('video_stream_session').where('id', '=', id).execute());
      }),

      getForMainPlaylist: Effect.fn('VideoStream.getForMainPlaylist')((id: string) =>
        query(db, (db) => getForMainPlaylist(db, id)),
      ),

      getForMediaPlaylist: Effect.fn('VideoStream.getForMediaPlaylist')((id: string, sessionId: string) =>
        query(db, (db) => getForMediaPlaylist(db, id, sessionId)),
      ),

      getForTranscoding: Effect.fn('VideoStream.getForTranscoding')((id: string) =>
        query(db, (db) => getForTranscoding(db, id)),
      ),
    });

  static readonly layer = Layer.effect(
    VideoStreamRepo,
    Effect.gen(function* () {
      return VideoStreamRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `VideoStreamRepo`. */
@Injectable()
export class VideoStreamRepository {
  private readonly effect: Context.Service.Shape<typeof VideoStreamRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = VideoStreamRepo.make(db);
  }

  createSession(session: Insertable<VideoStreamSessionTable>) {
    return runPromise(this.effect.createSession(session));
  }

  createVariant(variant: Insertable<VideoStreamVariantTable>) {
    return runPromise(this.effect.createVariant(variant));
  }

  async createSegment(segment: Insertable<VideoStreamSegmentTable>) {
    await runPromise(this.effect.createSegment(segment));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getSession(id: string) {
    return runPromise(this.effect.getSession(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getVariant(id: string) {
    return runPromise(this.effect.getVariant(id));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.NUMBER] })
  getSegment(variantId: string, index: number) {
    return runPromise(this.effect.getSegment(variantId, index));
  }

  @GenerateSql()
  getExpiredSessions() {
    return runPromise(this.effect.getExpiredSessions());
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.DATE] })
  async extendSession(id: string, expiresAt: Date) {
    await runPromise(this.effect.extendSession(id, expiresAt));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  async deleteSession(id: string) {
    await runPromise(this.effect.deleteSession(id));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  async getForMainPlaylist(id: string) {
    return runPromise(this.effect.getForMainPlaylist(id));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID] })
  async getForMediaPlaylist(id: string, sessionId: string) {
    return runPromise(this.effect.getForMediaPlaylist(id, sessionId));
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  async getForTranscoding(id: string) {
    return runPromise(this.effect.getForTranscoding(id));
  }
}
