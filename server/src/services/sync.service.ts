import { BadRequestException, ForbiddenException, Injectable } from '@nestjs/common';
import { Effect, Stream } from 'effect';
import { Insertable } from 'kysely';
import { DateTime, Duration } from 'luxon';
import { Writable } from 'node:stream';
import type { SyncAck } from 'src/types.js';
import { OnJob } from 'src/decorators.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  SyncAckDeleteDto,
  SyncAckSetDto,
  SyncAssetV2,
  SyncItem,
  SyncStreamDto,
  syncAlbumV2ToV1,
} from 'src/dtos/sync.dto.js';
import { runPromise } from 'src/effect/run.js';
import { JobName, MemoryType, QueueName, SyncEntityType, SyncRequestType } from 'src/enum.js';
import { SyncQueryOptions } from 'src/repositories/sync.repository.js';
import { SessionSyncCheckpointTable } from 'src/schema/tables/sync-checkpoint.table.js';
import { BaseService } from 'src/services/base.service.js';
import { hexOrBufferToBase64 } from 'src/utils/bytes.js';
import { ClientDisconnectedError, waitForDrain } from 'src/utils/response.js';
import { SerializeOptions, fromAck, serialize, toAck } from 'src/utils/sync.js';

type CheckpointMap = Partial<Record<SyncEntityType, SyncAck>>;
type AssetLike = Omit<SyncAssetV2, 'checksum' | 'thumbhash'> & {
  checksum: Buffer<ArrayBufferLike>;
  thumbhash: Buffer<ArrayBufferLike> | null;
};

const COMPLETE_ID = 'complete';
const MAX_DAYS = 30;
const MAX_DURATION = Duration.fromObject({ days: MAX_DAYS });

const mapSyncAssetV2 = ({ checksum, thumbhash, ...data }: AssetLike): SyncAssetV2 => ({
  ...data,
  checksum: hexOrBufferToBase64(checksum),
  thumbhash: thumbhash ? hexOrBufferToBase64(thumbhash) : null,
});

const isEntityBackfillComplete = (createId: string, checkpoint: SyncAck | undefined): boolean =>
  createId === checkpoint?.updateId && checkpoint.extraId === COMPLETE_ID;

const getStartId = (createId: string, checkpoint: SyncAck | undefined): string | undefined =>
  createId === checkpoint?.updateId ? checkpoint?.extraId : undefined;

export const send = async <T extends keyof SyncItem, D extends SyncItem[T]>(
  response: Writable,
  item: SerializeOptions<T, D>,
) => {
  if (response.destroyed || response.writableEnded) {
    throw new ClientDisconnectedError();
  }

  // indicates back pressure, so we wait for 'drain' event
  if (!response.write(serialize(item))) {
    await waitForDrain(response);
  }
};

type SyncEffect = Effect.Effect<void, unknown>;

/** `send` as an Effect: the write happens when the Effect runs, and it fails with the original error (e.g. `ClientDisconnectedError`). */
const sendFx = <T extends keyof SyncItem, D extends SyncItem[T]>(response: Writable, item: SerializeOptions<T, D>) =>
  Effect.tryPromise({ try: () => send(response, item), catch: (cause) => cause });

/**
 * Consumes a lazy DB cursor one row at a time, running `f` (and so its backpressure-aware writes) before the next row
 * is pulled; nothing is buffered. Like `for await`, the cursor's `return()` is called when `f` fails or the fiber is
 * interrupted (e.g. the client disconnected), which closes the underlying DB cursor.
 */
const forEachRow = <T, E, X, E2>(rows: Effect.Effect<AsyncIterable<T>, E>, f: (row: T) => Effect.Effect<X, E2>) =>
  Effect.flatMap(rows, (iterable) =>
    Stream.fromAsyncIterable(iterable, (cause: unknown) => cause).pipe(Stream.runForEach(f)),
  );

const sendEntityBackfillCompleteAck = (response: Writable, ackType: SyncEntityType, id: string) =>
  sendFx(response, { type: SyncEntityType.SyncAckV1, data: {}, ackType, ids: [id, COMPLETE_ID] });

export const SYNC_TYPES_ORDER = [
  SyncRequestType.AuthUsersV1,
  SyncRequestType.AuthUsersV2,
  SyncRequestType.UsersV1,
  SyncRequestType.PartnersV1,
  SyncRequestType.AssetsV1,
  SyncRequestType.AssetsV2,
  SyncRequestType.StacksV1,
  SyncRequestType.PartnerAssetsV1,
  SyncRequestType.PartnerAssetsV2,
  SyncRequestType.PartnerStacksV1,
  SyncRequestType.AlbumAssetsV1,
  SyncRequestType.AlbumAssetsV2,
  SyncRequestType.AlbumsV1,
  SyncRequestType.AlbumsV2,
  SyncRequestType.AlbumUsersV1,
  SyncRequestType.AlbumToAssetsV1,
  SyncRequestType.AssetExifsV1,
  SyncRequestType.AlbumAssetExifsV1,
  SyncRequestType.PartnerAssetExifsV1,
  SyncRequestType.MemoriesV1,
  SyncRequestType.MemoriesV2,
  SyncRequestType.MemoryToAssetsV1,
  SyncRequestType.MemoryToAssetsV2,
  SyncRequestType.UserMetadataV1,
  SyncRequestType.AssetMetadataV1,
  SyncRequestType.AssetEditsV1,
];

const throwSessionRequired = () => {
  throw new ForbiddenException('Sync endpoints cannot be used with API keys');
};

@Injectable()
export class SyncService extends BaseService {
  getAcks(auth: AuthDto) {
    const sessionId = auth.session?.id;
    if (!sessionId) {
      return throwSessionRequired();
    }

    return runPromise(this.fx.syncCheckpoint.getAll(sessionId));
  }

  async setAcks(auth: AuthDto, dto: SyncAckSetDto) {
    const sessionId = auth.session?.id;
    if (!sessionId) {
      return throwSessionRequired();
    }

    return runPromise(
      Effect.gen({ self: this }, function* () {
        const checkpoints: Record<string, Insertable<SessionSyncCheckpointTable>> = {};
        for (const ack of dto.acks) {
          const { type } = fromAck(ack);
          if (type === SyncEntityType.SyncResetV1) {
            yield* this.fx.session.resetSyncProgress(sessionId);
            return;
          }
          // TODO proper ack validation via class validator
          if (!Object.values(SyncEntityType).includes(type)) {
            return yield* Effect.fail(new BadRequestException(`Invalid ack type: ${type}`));
          }

          // TODO pick the latest ack for each type, instead of using the last one
          checkpoints[type] = { sessionId, type, ack };
        }

        yield* this.fx.syncCheckpoint.upsertAll(Object.values(checkpoints));
      }),
    );
  }

  async deleteAcks(auth: AuthDto, dto: SyncAckDeleteDto) {
    const sessionId = auth.session?.id;
    if (!sessionId) {
      return throwSessionRequired();
    }

    await runPromise(this.fx.syncCheckpoint.deleteAll(sessionId, dto.types));
  }

  async stream(auth: AuthDto, response: Writable, dto: SyncStreamDto) {
    try {
      await this.streamInternal(auth, response, dto);
    } catch (error) {
      if (error instanceof ClientDisconnectedError) {
        this.logger.debug('Client closed the connection');
        return;
      }

      throw error;
    }
  }

  private async streamInternal(auth: AuthDto, response: Writable, dto: SyncStreamDto) {
    const session = auth.session;
    if (!session) {
      return throwSessionRequired();
    }

    await runPromise(
      Effect.gen({ self: this }, function* () {
        if (dto.reset) {
          yield* this.fx.session.resetSyncProgress(session.id);
        }

        const isPendingSyncReset = yield* this.fx.session.isPendingSyncReset(session.id);
        if (isPendingSyncReset) {
          yield* sendFx(response, { type: SyncEntityType.SyncResetV1, ids: ['reset'], data: {} });
          response.end();
          return;
        }

        const checkpoints = yield* this.fx.syncCheckpoint.getAll(session.id);
        const checkpointMap: CheckpointMap = Object.fromEntries(
          checkpoints.map(({ type, ack }) => [type, fromAck(ack)]),
        );

        if (this.needsFullSync(checkpointMap)) {
          yield* sendFx(response, { type: SyncEntityType.SyncResetV1, ids: ['reset'], data: {} });
          response.end();
          return;
        }

        const { nowId } = yield* this.fx.syncCheckpoint.getNow();
        const options: SyncQueryOptions = { nowId, userId: auth.user.id };

        const handlers: Record<SyncRequestType, () => SyncEffect> = {
          // deprecated handlers
          [SyncRequestType.AssetsV1]: () => this.syncAssetsV1(),
          [SyncRequestType.PartnerAssetsV1]: () => this.syncPartnerAssetsV1(),
          [SyncRequestType.AlbumAssetsV1]: () => this.syncAlbumAssetsV1(),

          [SyncRequestType.AuthUsersV1]: () => this.syncAuthUsersV1(options, response, checkpointMap),
          [SyncRequestType.AuthUsersV2]: () => this.syncAuthUsersV2(options, response, checkpointMap),
          [SyncRequestType.UsersV1]: () => this.syncUsersV1(options, response, checkpointMap),
          [SyncRequestType.PartnersV1]: () => this.syncPartnersV1(options, response, checkpointMap),
          [SyncRequestType.AssetsV2]: () => this.syncAssetsV2(options, response, checkpointMap),
          [SyncRequestType.AssetExifsV1]: () => this.syncAssetExifsV1(options, response, checkpointMap),
          [SyncRequestType.AssetEditsV1]: () => this.syncAssetEditsV1(options, response, checkpointMap),
          [SyncRequestType.PartnerAssetsV2]: () =>
            this.syncPartnerAssetsV2(options, response, checkpointMap, session.id),
          [SyncRequestType.AssetMetadataV1]: () => this.syncAssetMetadataV1(options, response, checkpointMap, auth),
          [SyncRequestType.PartnerAssetExifsV1]: () =>
            this.syncPartnerAssetExifsV1(options, response, checkpointMap, session.id),
          [SyncRequestType.AlbumsV1]: () => this.syncAlbumsV1(options, response, checkpointMap),
          [SyncRequestType.AlbumsV2]: () => this.syncAlbumsV2(options, response, checkpointMap),
          [SyncRequestType.AlbumUsersV1]: () => this.syncAlbumUsersV1(options, response, checkpointMap, session.id),
          [SyncRequestType.AlbumAssetsV2]: () => this.syncAlbumAssetsV2(options, response, checkpointMap, session.id),
          [SyncRequestType.AlbumToAssetsV1]: () =>
            this.syncAlbumToAssetsV1(options, response, checkpointMap, session.id),
          [SyncRequestType.AlbumAssetExifsV1]: () =>
            this.syncAlbumAssetExifsV1(options, response, checkpointMap, session.id),
          [SyncRequestType.MemoriesV1]: () => this.syncMemoriesV1(options, response, checkpointMap),
          [SyncRequestType.MemoriesV2]: () => this.syncMemoriesV2(options, response, checkpointMap),
          [SyncRequestType.MemoryToAssetsV1]: () => this.syncMemoryAssetsV1(options, response, checkpointMap),
          [SyncRequestType.MemoryToAssetsV2]: () => this.syncMemoryAssetsV2(options, response, checkpointMap),
          [SyncRequestType.StacksV1]: () => this.syncStackV1(options, response, checkpointMap),
          [SyncRequestType.PartnerStacksV1]: () =>
            this.syncPartnerStackV1(options, response, checkpointMap, session.id),
          [SyncRequestType.UserMetadataV1]: () => this.syncUserMetadataV1(options, response, checkpointMap),
        } as const;

        for (const type of SYNC_TYPES_ORDER) {
          if (!dto.types.includes(type)) {
            continue;
          }

          const handler = handlers[type as keyof typeof handlers];
          yield* handler();
        }

        yield* sendFx(response, { type: SyncEntityType.SyncCompleteV1, ids: [nowId], data: {} });

        response.end();
      }),
    );
  }

  @OnJob({ name: JobName.AuditTableCleanup, queue: QueueName.BackgroundTask })
  async onAuditTableCleanup() {
    const pruneThreshold = MAX_DAYS + 1;

    // `cleanupAuditTable` only exists on the Promise adapter's per-table classes (the Effect service exposes a
    // generic `auditCleanup(table, days)`), so these stay Promise calls wrapped with tryPromise.
    const repo = this.syncRepository;
    const cleanups = [
      () => repo.album.cleanupAuditTable(pruneThreshold),
      () => repo.albumUser.cleanupAuditTable(pruneThreshold),
      () => repo.albumToAsset.cleanupAuditTable(pruneThreshold),
      () => repo.asset.cleanupAuditTable(pruneThreshold),
      () => repo.assetMetadata.cleanupAuditTable(pruneThreshold),
      () => repo.assetEdit.cleanupAuditTable(pruneThreshold),
      () => repo.memory.cleanupAuditTable(pruneThreshold),
      () => repo.memoryToAsset.cleanupAuditTable(pruneThreshold),
      () => repo.partner.cleanupAuditTable(pruneThreshold),
      () => repo.stack.cleanupAuditTable(pruneThreshold),
      () => repo.user.cleanupAuditTable(pruneThreshold),
      () => repo.userMetadata.cleanupAuditTable(pruneThreshold),
    ];
    await runPromise(
      Effect.gen(function* () {
        for (const cleanup of cleanups) {
          yield* Effect.tryPromise({ try: cleanup, catch: (cause) => cause });
        }
      }),
    );
  }

  private needsFullSync(checkpointMap: CheckpointMap) {
    const completeAck = checkpointMap[SyncEntityType.SyncCompleteV1];
    if (!completeAck) {
      return false;
    }

    const milliseconds = Number.parseInt(completeAck.updateId.replaceAll('-', '').slice(0, 12), 16);

    return DateTime.fromMillis(milliseconds) < DateTime.now().minus(MAX_DURATION);
  }

  private syncAuthUsersV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    const upsertType = SyncEntityType.AuthUserV1;
    return forEachRow(
      this.fx.sync.authUser.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
      ({ updateId, profileImagePath, ...data }) =>
        sendFx(response, {
          type: upsertType,
          ids: [updateId],
          data: { ...data, oauthId: data.oauthId ?? '', hasProfileImage: !!profileImagePath },
        }),
    );
  }

  private syncAuthUsersV2(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    const upsertType = SyncEntityType.AuthUserV2;
    return forEachRow(
      this.fx.sync.authUser.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
      ({ updateId, profileImagePath, ...data }) =>
        sendFx(response, {
          type: upsertType,
          ids: [updateId],
          data: { ...data, hasProfileImage: !!profileImagePath },
        }),
    );
  }

  private syncUsersV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.UserDeleteV1;
      yield* forEachRow(
        this.fx.sync.user.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.UserV1;
      yield* forEachRow(
        this.fx.sync.user.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, profileImagePath, ...data }) =>
          sendFx(response, {
            type: upsertType,
            ids: [updateId],
            data: { ...data, hasProfileImage: !!profileImagePath },
          }),
      );
    });
  }

  private syncPartnersV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.PartnerDeleteV1;
      yield* forEachRow(
        this.fx.sync.partner.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.PartnerV1;
      yield* forEachRow(
        this.fx.sync.partner.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncAssetsV1(): SyncEffect {
    return Effect.fail(
      new BadRequestException('SyncRequestType.AssetsV1 is deprecated, use SyncRequestType.AssetsV2 instead'),
    );
  }

  private syncAssetsV2(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AssetDeleteV1;
      yield* forEachRow(
        this.fx.sync.asset.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.AssetV2;
      yield* forEachRow(
        this.fx.sync.asset.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data: mapSyncAssetV2(data) }),
      );
    });
  }

  private syncPartnerAssetsV1(): SyncEffect {
    return Effect.fail(
      new BadRequestException(
        'SyncRequestType.PartnerAssetsV1 is deprecated, use SyncRequestType.PartnerAssetsV2 instead',
      ),
    );
  }

  private syncPartnerAssetsV2(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.PartnerAssetDeleteV1;
      yield* forEachRow(
        this.fx.sync.partnerAsset.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const backfillType = SyncEntityType.PartnerAssetBackfillV2;
      const backfillCheckpoint = checkpointMap[backfillType];
      const partners = yield* this.fx.sync.partner.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });
      const upsertType = SyncEntityType.PartnerAssetV2;
      const upsertCheckpoint = checkpointMap[upsertType];
      if (upsertCheckpoint) {
        const endId = upsertCheckpoint.updateId;

        for (const partner of partners) {
          const createId = partner.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.partnerAsset.getBackfill(
              { ...options, afterUpdateId: startId, beforeUpdateId: endId },
              partner.sharedById,
            ),
            ({ updateId, ...data }) =>
              sendFx(response, {
                type: backfillType,
                ids: [createId, updateId],
                data: mapSyncAssetV2(data),
              }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, createId);
        }
      } else if (partners.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: partners.at(-1)!.createId,
        });
      }

      yield* forEachRow(
        this.fx.sync.partnerAsset.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data: mapSyncAssetV2(data) }),
      );
    });
  }

  private syncAssetExifsV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    const upsertType = SyncEntityType.AssetExifV1;
    return forEachRow(
      this.fx.sync.assetExif.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
      ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
    );
  }

  private syncAssetEditsV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AssetEditDeleteV1;
      yield* forEachRow(
        this.fx.sync.assetEdit.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.AssetEditV1;
      yield* forEachRow(
        this.fx.sync.assetEdit.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncPartnerAssetExifsV1(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const backfillType = SyncEntityType.PartnerAssetExifBackfillV1;
      const backfillCheckpoint = checkpointMap[backfillType];
      const partners = yield* this.fx.sync.partner.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });

      const upsertType = SyncEntityType.PartnerAssetExifV1;
      const upsertCheckpoint = checkpointMap[upsertType];
      if (upsertCheckpoint) {
        const endId = upsertCheckpoint.updateId;

        for (const partner of partners) {
          const createId = partner.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.partnerAssetExif.getBackfill(
              { ...options, afterUpdateId: startId, beforeUpdateId: endId },
              partner.sharedById,
            ),
            ({ updateId, ...data }) =>
              sendFx(response, { type: backfillType, ids: [partner.createId, updateId], data }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, partner.createId);
        }
      } else if (partners.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: partners.at(-1)!.createId,
        });
      }

      yield* forEachRow(
        this.fx.sync.partnerAssetExif.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncAlbumsV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AlbumDeleteV1;
      yield* forEachRow(
        this.fx.sync.album.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.AlbumV1;
      yield* forEachRow(
        this.fx.sync.album.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) =>
          Effect.gen({ self: this }, function* () {
            const albumUsers = yield* this.fx.sync.album.getAlbumUsers(data.id);
            yield* sendFx(response, {
              type: upsertType,
              ids: [updateId],
              // TODO: return null instead of '' in v4
              data: syncAlbumV2ToV1({ ...data, description: data.description ?? '' }, albumUsers),
            });
          }),
      );
    });
  }

  private syncAlbumsV2(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AlbumDeleteV1;
      yield* forEachRow(
        this.fx.sync.album.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.AlbumV2;
      yield* forEachRow(
        this.fx.sync.album.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) =>
          // TODO: return null instead of '' in v4
          sendFx(response, {
            type: upsertType,
            ids: [updateId],
            data: { ...data, description: data.description ?? '' },
          }),
      );
    });
  }

  private syncAlbumUsersV1(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AlbumUserDeleteV1;
      yield* forEachRow(
        this.fx.sync.albumUser.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const backfillType = SyncEntityType.AlbumUserBackfillV1;
      const backfillCheckpoint = checkpointMap[backfillType];
      const albums = yield* this.fx.sync.album.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });
      const upsertType = SyncEntityType.AlbumUserV1;
      const upsertCheckpoint = checkpointMap[upsertType];
      if (upsertCheckpoint) {
        const endId = upsertCheckpoint.updateId;

        for (const album of albums) {
          const createId = album.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.albumUser.getBackfill({ ...options, afterUpdateId: startId, beforeUpdateId: endId }, album.id),
            ({ updateId, ...data }) => sendFx(response, { type: backfillType, ids: [createId, updateId], data }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, createId);
        }
      } else if (albums.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: albums.at(-1)!.createId,
        });
      }

      yield* forEachRow(
        this.fx.sync.albumUser.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncAlbumAssetsV1(): SyncEffect {
    return Effect.fail(
      new BadRequestException('SyncRequestType.AlbumAssetsV1 is deprecated, use SyncRequestType.AlbumAssetsV2 instead'),
    );
  }

  private syncAlbumAssetsV2(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const backfillType = SyncEntityType.AlbumAssetBackfillV2;
      const backfillCheckpoint = checkpointMap[backfillType];
      const albums = yield* this.fx.sync.album.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });
      const updateType = SyncEntityType.AlbumAssetUpdateV2;
      const createType = SyncEntityType.AlbumAssetCreateV2;
      const updateCheckpoint = checkpointMap[updateType];
      const createCheckpoint = checkpointMap[createType];
      if (createCheckpoint) {
        const endId = createCheckpoint.updateId;

        for (const album of albums) {
          const createId = album.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.albumAsset.getBackfill(
              { ...options, afterUpdateId: startId, beforeUpdateId: endId },
              album.id,
              options.userId,
            ),
            ({ updateId, ...data }) =>
              sendFx(response, { type: backfillType, ids: [createId, updateId], data: mapSyncAssetV2(data) }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, createId);
        }
      } else if (albums.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: albums.at(-1)!.createId,
        });
      }

      if (createCheckpoint) {
        yield* forEachRow(
          this.fx.sync.albumAsset.getUpdates({ ...options, ack: updateCheckpoint }, createCheckpoint),
          ({ updateId, ...data }) =>
            sendFx(response, { type: updateType, ids: [updateId], data: mapSyncAssetV2(data) }),
        );
      }

      let isFirst = true;
      yield* forEachRow(
        this.fx.sync.albumAsset.getCreates({ ...options, ack: createCheckpoint }),
        ({ updateId, ...data }) =>
          Effect.gen(function* () {
            if (isFirst) {
              yield* sendFx(response, {
                type: SyncEntityType.SyncAckV1,
                data: {},
                ackType: SyncEntityType.AlbumAssetUpdateV2,
                ids: [options.nowId],
              });
              isFirst = false;
            }
            yield* sendFx(response, { type: createType, ids: [updateId], data: mapSyncAssetV2(data) });
          }),
      );
    });
  }

  private syncAlbumAssetExifsV1(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const backfillType = SyncEntityType.AlbumAssetExifBackfillV1;
      const backfillCheckpoint = checkpointMap[backfillType];
      const albums = yield* this.fx.sync.album.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });
      const updateType = SyncEntityType.AlbumAssetExifUpdateV1;
      const createType = SyncEntityType.AlbumAssetExifCreateV1;
      const upsertCheckpoint = checkpointMap[updateType];
      const createCheckpoint = checkpointMap[createType];
      if (createCheckpoint) {
        const endId = createCheckpoint.updateId;

        for (const album of albums) {
          const createId = album.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.albumAssetExif.getBackfill(
              { ...options, afterUpdateId: startId, beforeUpdateId: endId },
              album.id,
            ),
            ({ updateId, ...data }) => sendFx(response, { type: backfillType, ids: [createId, updateId], data }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, createId);
        }
      } else if (albums.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: albums.at(-1)!.createId,
        });
      }

      if (createCheckpoint) {
        yield* forEachRow(
          this.fx.sync.albumAssetExif.getUpdates({ ...options, ack: upsertCheckpoint }, createCheckpoint),
          ({ updateId, ...data }) => sendFx(response, { type: updateType, ids: [updateId], data }),
        );
      }

      let isFirst = true;
      yield* forEachRow(
        this.fx.sync.albumAssetExif.getCreates({ ...options, ack: createCheckpoint }),
        ({ updateId, ...data }) =>
          Effect.gen(function* () {
            if (isFirst) {
              yield* sendFx(response, {
                type: SyncEntityType.SyncAckV1,
                data: {},
                ackType: SyncEntityType.AlbumAssetExifUpdateV1,
                ids: [options.nowId],
              });
              isFirst = false;
            }
            yield* sendFx(response, { type: createType, ids: [updateId], data });
          }),
      );
    });
  }

  private syncAlbumToAssetsV1(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AlbumToAssetDeleteV1;
      yield* forEachRow(
        this.fx.sync.albumToAsset.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const backfillType = SyncEntityType.AlbumToAssetBackfillV1;
      const backfillCheckpoint = checkpointMap[backfillType];
      const albums = yield* this.fx.sync.album.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });
      const upsertType = SyncEntityType.AlbumToAssetV1;
      const upsertCheckpoint = checkpointMap[upsertType];
      if (upsertCheckpoint) {
        const endId = upsertCheckpoint.updateId;

        for (const album of albums) {
          const createId = album.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.albumToAsset.getBackfill(
              { ...options, afterUpdateId: startId, beforeUpdateId: endId },
              album.id,
            ),
            ({ updateId, ...data }) => sendFx(response, { type: backfillType, ids: [createId, updateId], data }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, createId);
        }
      } else if (albums.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: albums.at(-1)!.createId,
        });
      }

      yield* forEachRow(
        this.fx.sync.albumToAsset.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncMemoriesV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.MemoryDeleteV1;
      yield* forEachRow(
        this.fx.sync.memory.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.MemoryV1;
      yield* forEachRow(
        this.fx.sync.memory.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) =>
          data.type === MemoryType.OnThisDay
            ? sendFx(response, { type: upsertType, ids: [updateId], data: { ...data, type: data.type } })
            : Effect.void,
      );
    });
  }

  private syncMemoriesV2(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.MemoryDeleteV1;
      yield* forEachRow(
        this.fx.sync.memory.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.MemoryV2;
      yield* forEachRow(
        this.fx.sync.memory.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncMemoryAssetsV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.MemoryToAssetDeleteV1;
      yield* forEachRow(
        this.fx.sync.memoryToAsset.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.MemoryToAssetV1;
      yield* forEachRow(
        this.fx.sync.memoryToAsset.getUpsertsV1({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncMemoryAssetsV2(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.MemoryToAssetDeleteV1;
      yield* forEachRow(
        this.fx.sync.memoryToAsset.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.MemoryToAssetV2;
      yield* forEachRow(
        this.fx.sync.memoryToAsset.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncStackV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.StackDeleteV1;
      yield* forEachRow(
        this.fx.sync.stack.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.StackV1;
      yield* forEachRow(
        this.fx.sync.stack.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncPartnerStackV1(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    sessionId: string,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.PartnerStackDeleteV1;
      yield* forEachRow(
        this.fx.sync.partnerStack.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const backfillType = SyncEntityType.PartnerStackBackfillV1;
      const backfillCheckpoint = checkpointMap[backfillType];
      const partners = yield* this.fx.sync.partner.getCreatedAfter({
        ...options,
        afterCreateId: backfillCheckpoint?.updateId,
      });
      const upsertType = SyncEntityType.PartnerStackV1;
      const upsertCheckpoint = checkpointMap[upsertType];
      if (upsertCheckpoint) {
        const endId = upsertCheckpoint.updateId;

        for (const partner of partners) {
          const createId = partner.createId;
          if (isEntityBackfillComplete(createId, backfillCheckpoint)) {
            continue;
          }

          const startId = getStartId(createId, backfillCheckpoint);
          yield* forEachRow(
            this.fx.sync.partnerStack.getBackfill(
              { ...options, afterUpdateId: startId, beforeUpdateId: endId },
              partner.sharedById,
            ),
            ({ updateId, ...data }) =>
              sendFx(response, {
                type: backfillType,
                ids: [createId, updateId],
                data,
              }),
          );

          yield* sendEntityBackfillCompleteAck(response, backfillType, createId);
        }
      } else if (partners.length > 0) {
        yield* this.upsertBackfillCheckpoint({
          type: backfillType,
          sessionId,
          createId: partners.at(-1)!.createId,
        });
      }

      yield* forEachRow(
        this.fx.sync.partnerStack.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncUserMetadataV1(options: SyncQueryOptions, response: Writable, checkpointMap: CheckpointMap): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.UserMetadataDeleteV1;
      yield* forEachRow(
        this.fx.sync.userMetadata.getDeletes({ ...options, ack: checkpointMap[deleteType] }),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.UserMetadataV1;
      yield* forEachRow(
        this.fx.sync.userMetadata.getUpserts({ ...options, ack: checkpointMap[upsertType] }),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private syncAssetMetadataV1(
    options: SyncQueryOptions,
    response: Writable,
    checkpointMap: CheckpointMap,
    auth: AuthDto,
  ): SyncEffect {
    return Effect.gen({ self: this }, function* () {
      const deleteType = SyncEntityType.AssetMetadataDeleteV1;
      yield* forEachRow(
        this.fx.sync.assetMetadata.getDeletes({ ...options, ack: checkpointMap[deleteType] }, auth.user.id),
        ({ id, ...data }) => sendFx(response, { type: deleteType, ids: [id], data }),
      );

      const upsertType = SyncEntityType.AssetMetadataV1;
      yield* forEachRow(
        this.fx.sync.assetMetadata.getUpserts({ ...options, ack: checkpointMap[upsertType] }, auth.user.id),
        ({ updateId, ...data }) => sendFx(response, { type: upsertType, ids: [updateId], data }),
      );
    });
  }

  private upsertBackfillCheckpoint(item: { type: SyncEntityType; sessionId: string; createId: string }) {
    const { type, sessionId, createId } = item;
    return this.fx.syncCheckpoint.upsertAll([
      {
        type,
        sessionId,
        ack: toAck({
          type,
          updateId: createId,
          extraId: COMPLETE_ID,
        }),
      },
    ]);
  }
}
