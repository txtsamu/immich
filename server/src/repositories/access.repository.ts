import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Kysely, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { ChunkedSet, DummyValue, GenerateSql } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumUserRole, AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { asUuid } from 'src/utils/database.js';

const activity = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.activity.checkOwnerAccess')(function* (userId: string, activityIds: Set<string>) {
    if (activityIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('activity')
        .select('activity.id')
        .where('activity.id', 'in', [...activityIds])
        .where('activity.userId', '=', userId)
        .execute()
        .then((activities) => new Set(activities.map((activity) => activity.id))),
    );
  }),

  checkAlbumOwnerAccess: Effect.fn('Access.activity.checkAlbumOwnerAccess')(function* (
    userId: string,
    activityIds: Set<string>,
  ) {
    if (activityIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('activity')
        .select('activity.id')
        .innerJoin('album', (join) => join.onRef('activity.albumId', '=', 'album.id').on('album.deletedAt', 'is', null))
        .innerJoin('album_user', (join) =>
          join
            .onRef('album.id', '=', 'album_user.albumId')
            .on('album_user.role', '=', sql.lit(AlbumUserRole.Owner))
            .on('album_user.userId', '=', asUuid(userId)),
        )
        .where('activity.id', 'in', [...activityIds])
        .execute()
        .then((activities) => new Set(activities.map((activity) => activity.id))),
    );
  }),

  checkCreateAccess: Effect.fn('Access.activity.checkCreateAccess')(function* (userId: string, albumIds: Set<string>) {
    if (albumIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('album')
        .select('album.id')
        .innerJoin('album_user as albumUsers', 'albumUsers.albumId', 'album.id')
        .innerJoin('user', (join) => join.onRef('user.id', '=', 'albumUsers.userId').on('user.deletedAt', 'is', null))
        .where('album.id', 'in', [...albumIds])
        .where('album.isActivityEnabled', '=', true)
        .where((eb) => eb('user.id', '=', userId))
        .where('album.deletedAt', 'is', null)
        .execute()
        .then((albums) => new Set(albums.map((album) => album.id))),
    );
  }),
});

const album = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.album.checkOwnerAccess')(function* (userId: string, albumIds: Set<string>) {
    if (albumIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('album')
        .select('album.id')
        .where('album.id', 'in', [...albumIds])
        .innerJoin('album_user', (join) =>
          join
            .onRef('album.id', '=', 'album_user.albumId')
            .on('album_user.role', '=', sql.lit(AlbumUserRole.Owner))
            .on('album_user.userId', '=', userId),
        )
        .where('album.deletedAt', 'is', null)
        .execute()
        .then((albums) => new Set(albums.map((album) => album.id))),
    );
  }),

  checkSharedAlbumAccess: Effect.fn('Access.album.checkSharedAlbumAccess')(function* (
    userId: string,
    albumIds: Set<string>,
    access: AlbumUserRole,
  ) {
    if (albumIds.size === 0) {
      return new Set<string>();
    }

    const accessRole =
      access === AlbumUserRole.Editor ? [AlbumUserRole.Editor] : [AlbumUserRole.Editor, AlbumUserRole.Viewer];

    return yield* query(db, (db) =>
      db
        .selectFrom('album')
        .select('album.id')
        .innerJoin('album_user', 'album_user.albumId', 'album.id')
        .innerJoin('user', (join) => join.onRef('user.id', '=', 'album_user.userId').on('user.deletedAt', 'is', null))
        .where('album.id', 'in', [...albumIds])
        .where('album.deletedAt', 'is', null)
        .where('user.id', '=', userId)
        .where('album_user.role', 'in', [...accessRole])
        .execute()
        .then((albums) => new Set(albums.map((album) => album.id))),
    );
  }),

  checkSharedLinkAccess: Effect.fn('Access.album.checkSharedLinkAccess')(function* (
    sharedLinkId: string,
    albumIds: Set<string>,
  ) {
    if (albumIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('shared_link')
        .select('shared_link.albumId')
        .where('shared_link.id', '=', sharedLinkId)
        .where('shared_link.albumId', 'in', [...albumIds])
        .execute()
        .then(
          (sharedLinks) =>
            new Set(
              sharedLinks.filter((sharedLink) => sharedLink.albumId).map((sharedLink) => sharedLink.albumId),
            ) as Set<string>,
        ),
    );
  }),
});

const asset = (db: Kysely<DB>) => ({
  checkAlbumAccess: Effect.fn('Access.asset.checkAlbumAccess')(function* (userId: string, assetIds: Set<string>) {
    if (assetIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .with('target', (qb) => qb.selectNoFrom(sql`array[${sql.join([...assetIds])}]::uuid[]`.as('ids')))
        .selectFrom('album')
        .innerJoin('album_asset as albumAssets', 'album.id', 'albumAssets.albumId')
        .innerJoin('asset', (join) =>
          join.onRef('asset.id', '=', 'albumAssets.assetId').on('asset.deletedAt', 'is', null),
        )
        .leftJoin('album_user as albumUsers', 'albumUsers.albumId', 'album.id')
        .leftJoin('user', (join) => join.onRef('user.id', '=', 'albumUsers.userId').on('user.deletedAt', 'is', null))
        .crossJoin('target')
        .select(['asset.id', 'asset.livePhotoVideoId'])
        .where((eb) =>
          eb.or([
            eb('asset.id', '=', sql<string>`any(target.ids)`),
            eb('asset.livePhotoVideoId', '=', sql<string>`any(target.ids)`),
          ]),
        )
        .where('user.id', '=', userId)
        .where('album.deletedAt', 'is', null)
        .execute()
        .then((assets) => {
          const allowedIds = new Set<string>();
          for (const asset of assets) {
            if (asset.id && assetIds.has(asset.id)) {
              allowedIds.add(asset.id);
            }
            if (asset.livePhotoVideoId && assetIds.has(asset.livePhotoVideoId)) {
              allowedIds.add(asset.livePhotoVideoId);
            }
          }
          return allowedIds;
        }),
    );
  }),

  checkOwnerAccess: Effect.fn('Access.asset.checkOwnerAccess')(function* (
    userId: string,
    assetIds: Set<string>,
    hasElevatedPermission: boolean | undefined,
  ) {
    if (assetIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('asset')
        .select('asset.id')
        .where('asset.id', 'in', [...assetIds])
        .where('asset.ownerId', '=', userId)
        .$if(!hasElevatedPermission, (eb) => eb.where('asset.visibility', '!=', AssetVisibility.Locked))
        .execute()
        .then((assets) => new Set(assets.map((asset) => asset.id))),
    );
  }),

  checkPartnerAccess: Effect.fn('Access.asset.checkPartnerAccess')(function* (userId: string, assetIds: Set<string>) {
    if (assetIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('partner')
        .innerJoin('user as sharedBy', (join) =>
          join.onRef('sharedBy.id', '=', 'partner.sharedById').on('sharedBy.deletedAt', 'is', null),
        )
        .innerJoin('asset', (join) => join.onRef('asset.ownerId', '=', 'sharedBy.id').on('asset.deletedAt', 'is', null))
        .select('asset.id')
        .where('partner.sharedWithId', '=', userId)
        .where((eb) =>
          eb.or([
            eb('asset.visibility', '=', sql.lit(AssetVisibility.Timeline)),
            eb('asset.visibility', '=', sql.lit(AssetVisibility.Hidden)),
          ]),
        )

        .where('asset.id', 'in', [...assetIds])
        .execute()
        .then((assets) => new Set(assets.map((asset) => asset.id))),
    );
  }),

  checkSharedLinkAccess: Effect.fn('Access.asset.checkSharedLinkAccess')(function* (
    sharedLinkId: string,
    assetIds: Set<string>,
  ) {
    if (assetIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .with('target', (qb) => qb.selectNoFrom(sql<string[]>`array[${sql.join([...assetIds])}]::uuid[]`.as('ids')))
        .with('link', (qb) =>
          qb
            .selectFrom('shared_link')
            .select(['shared_link.id', 'shared_link.albumId'])
            .where('shared_link.id', '=', sharedLinkId),
        )
        .selectFrom('asset')
        .crossJoin('target')
        .innerJoinLateral(
          (eb) =>
            eb
              .selectFrom('link')
              .innerJoin('shared_link_asset', 'shared_link_asset.sharedLinkId', 'link.id')
              .select(sql.lit(1).as('match'))
              .whereRef('shared_link_asset.assetId', '=', 'asset.id')
              .unionAll(
                eb
                  .selectFrom('link')
                  .innerJoin('album', (join) =>
                    join.onRef('album.id', '=', 'link.albumId').on('album.deletedAt', 'is', null),
                  )
                  .innerJoin('album_asset', 'album_asset.albumId', 'album.id')
                  .select(sql.lit(1).as('match'))
                  .whereRef('album_asset.assetId', '=', 'asset.id'),
              )
              .limit(1)
              .as('membership'),
          (join) => join.onTrue(),
        )
        .select(['asset.id', 'asset.livePhotoVideoId'])
        .where('asset.deletedAt', 'is', null)
        .where((eb) =>
          eb.or([
            eb('asset.id', '=', eb.fn.any('target.ids')),
            eb('asset.livePhotoVideoId', '=', eb.fn.any('target.ids')),
          ]),
        )
        .execute()
        .then((rows) => {
          const allowedIds = new Set<string>();
          for (const row of rows) {
            if (assetIds.has(row.id)) {
              allowedIds.add(row.id);
            }
            if (row.livePhotoVideoId && assetIds.has(row.livePhotoVideoId)) {
              allowedIds.add(row.livePhotoVideoId);
            }
          }
          return allowedIds;
        }),
    );
  }),
});

const assetFile = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.assetFile.checkOwnerAccess')(function* (
    userId: string,
    fileIds: Set<string>,
    hasElevatedPermission: boolean | undefined,
  ) {
    if (fileIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('asset_file')
        .select('asset_file.id')
        .innerJoin('asset', 'asset.id', 'asset_file.assetId')
        .$if(!hasElevatedPermission, (eb) => eb.where('asset.visibility', '!=', AssetVisibility.Locked))
        .where('asset.ownerId', '=', userId)
        .where('asset_file.id', 'in', [...fileIds])
        .execute()
        .then((files) => new Set(files.map(({ id }) => id))),
    );
  }),
});

const authDevice = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.authDevice.checkOwnerAccess')(function* (userId: string, deviceIds: Set<string>) {
    if (deviceIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('session')
        .select('session.id')
        .where('session.userId', '=', userId)
        .where('session.id', 'in', [...deviceIds])
        .execute()
        .then((tokens) => new Set(tokens.map((token) => token.id))),
    );
  }),
});

const notification = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.notification.checkOwnerAccess')(function* (
    userId: string,
    notificationIds: Set<string>,
  ) {
    if (notificationIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('notification')
        .select('notification.id')
        .where('notification.id', 'in', [...notificationIds])
        .where('notification.userId', '=', userId)
        .execute()
        .then((stacks) => new Set(stacks.map((stack) => stack.id))),
    );
  }),
});

const session = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.session.checkOwnerAccess')(function* (userId: string, sessionIds: Set<string>) {
    if (sessionIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('session')
        .select('session.id')
        .where('session.id', 'in', [...sessionIds])
        .where('session.userId', '=', userId)
        .execute()
        .then((sessions) => new Set(sessions.map((session) => session.id))),
    );
  }),
});

const stack = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.stack.checkOwnerAccess')(function* (userId: string, stackIds: Set<string>) {
    if (stackIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('stack')
        .select('stack.id')
        .where('stack.id', 'in', [...stackIds])
        .where('stack.ownerId', '=', userId)
        .execute()
        .then((stacks) => new Set(stacks.map((stack) => stack.id))),
    );
  }),
});

const timeline = (db: Kysely<DB>) => ({
  checkPartnerAccess: Effect.fn('Access.timeline.checkPartnerAccess')(function* (
    userId: string,
    partnerIds: Set<string>,
  ) {
    if (partnerIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('partner')
        .select('partner.sharedById')
        .where('partner.sharedById', 'in', [...partnerIds])
        .where('partner.sharedWithId', '=', userId)
        .execute()
        .then((partners) => new Set(partners.map((partner) => partner.sharedById))),
    );
  }),
});

const memory = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.memory.checkOwnerAccess')(function* (userId: string, memoryIds: Set<string>) {
    if (memoryIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('memory')
        .select('memory.id')
        .where('memory.id', 'in', [...memoryIds])
        .where('memory.ownerId', '=', userId)
        .where('memory.deletedAt', 'is', null)
        .execute()
        .then((memories) => new Set(memories.map((memory) => memory.id))),
    );
  }),
});

const passkey = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.passkey.checkOwnerAccess')(function* (userId: string, passkeyIds: Set<string>) {
    if (passkeyIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('passkey')
        .select('passkey.id')
        .where('passkey.id', 'in', [...passkeyIds])
        .where('passkey.userId', '=', userId)
        .execute()
        .then((passkeys) => new Set(passkeys.map((passkey) => passkey.id))),
    );
  }),
});

const partner = (db: Kysely<DB>) => ({
  checkUpdateAccess: Effect.fn('Access.partner.checkUpdateAccess')(function* (userId: string, partnerIds: Set<string>) {
    if (partnerIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('partner')
        .select('partner.sharedById')
        .where('partner.sharedById', 'in', [...partnerIds])
        .where('partner.sharedWithId', '=', userId)
        .execute()
        .then((partners) => new Set(partners.map((partner) => partner.sharedById))),
    );
  }),
});

const tag = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.tag.checkOwnerAccess')(function* (userId: string, tagIds: Set<string>) {
    if (tagIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('tag')
        .select('tag.id')
        .where('tag.id', 'in', [...tagIds])
        .where('tag.userId', '=', userId)
        .execute()
        .then((tags) => new Set(tags.map((tag) => tag.id))),
    );
  }),
});

const workflow = (db: Kysely<DB>) => ({
  checkOwnerAccess: Effect.fn('Access.workflow.checkOwnerAccess')(function* (userId: string, workflowIds: Set<string>) {
    if (workflowIds.size === 0) {
      return new Set<string>();
    }

    return yield* query(db, (db) =>
      db
        .selectFrom('workflow')
        .select('workflow.id')
        .where('workflow.id', 'in', [...workflowIds])
        .where('workflow.ownerId', '=', userId)
        .execute()
        .then((workflows) => new Set(workflows.map((workflow) => workflow.id))),
    );
  }),
});

export class AccessRepo extends Context.Service<
  AccessRepo,
  {
    activity: ReturnType<typeof activity>;
    album: ReturnType<typeof album>;
    asset: ReturnType<typeof asset>;
    assetFile: ReturnType<typeof assetFile>;
    authDevice: ReturnType<typeof authDevice>;
    notification: ReturnType<typeof notification>;
    session: ReturnType<typeof session>;
    stack: ReturnType<typeof stack>;
    timeline: ReturnType<typeof timeline>;
    memory: ReturnType<typeof memory>;
    passkey: ReturnType<typeof passkey>;
    partner: ReturnType<typeof partner>;
    tag: ReturnType<typeof tag>;
    workflow: ReturnType<typeof workflow>;
  }
>()('immich/repositories/AccessRepo') {
  static readonly make = (db: Kysely<DB>) =>
    AccessRepo.of({
      activity: activity(db),
      album: album(db),
      asset: asset(db),
      assetFile: assetFile(db),
      authDevice: authDevice(db),
      notification: notification(db),
      session: session(db),
      stack: stack(db),
      timeline: timeline(db),
      memory: memory(db),
      passkey: passkey(db),
      partner: partner(db),
      tag: tag(db),
      workflow: workflow(db),
    });

  static readonly layer = Layer.effect(
    AccessRepo,
    Effect.gen(function* () {
      return AccessRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapters for the Nest services. Delegate to `AccessRepo`. */
class ActivityAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['activity']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, activityIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, activityIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkAlbumOwnerAccess(userId: string, activityIds: Set<string>) {
    return runPromise(this.effect.checkAlbumOwnerAccess(userId, activityIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkCreateAccess(userId: string, albumIds: Set<string>) {
    return runPromise(this.effect.checkCreateAccess(userId, albumIds));
  }
}

class AlbumAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['album']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, albumIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, albumIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkSharedAlbumAccess(userId: string, albumIds: Set<string>, access: AlbumUserRole) {
    return runPromise(this.effect.checkSharedAlbumAccess(userId, albumIds, access));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkSharedLinkAccess(sharedLinkId: string, albumIds: Set<string>) {
    return runPromise(this.effect.checkSharedLinkAccess(sharedLinkId, albumIds));
  }
}

class AssetAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['asset']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkAlbumAccess(userId: string, assetIds: Set<string>) {
    return runPromise(this.effect.checkAlbumAccess(userId, assetIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, assetIds: Set<string>, hasElevatedPermission: boolean | undefined) {
    return runPromise(this.effect.checkOwnerAccess(userId, assetIds, hasElevatedPermission));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkPartnerAccess(userId: string, assetIds: Set<string>) {
    return runPromise(this.effect.checkPartnerAccess(userId, assetIds));
  }

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkSharedLinkAccess(sharedLinkId: string, assetIds: Set<string>) {
    return runPromise(this.effect.checkSharedLinkAccess(sharedLinkId, assetIds));
  }
}

class AssetFileAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['assetFile']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, fileIds: Set<string>, hasElevatedPermission: boolean | undefined) {
    return runPromise(this.effect.checkOwnerAccess(userId, fileIds, hasElevatedPermission));
  }
}

class AuthDeviceAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['authDevice']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, deviceIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, deviceIds));
  }
}

class NotificationAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['notification']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, notificationIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, notificationIds));
  }
}

class SessionAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['session']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, sessionIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, sessionIds));
  }
}

class StackAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['stack']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, stackIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, stackIds));
  }
}

class TimelineAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['timeline']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkPartnerAccess(userId: string, partnerIds: Set<string>) {
    return runPromise(this.effect.checkPartnerAccess(userId, partnerIds));
  }
}

class MemoryAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['memory']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, memoryIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, memoryIds));
  }
}

class PasskeyAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['passkey']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, passkeyIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, passkeyIds));
  }
}

class PartnerAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['partner']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkUpdateAccess(userId: string, partnerIds: Set<string>) {
    return runPromise(this.effect.checkUpdateAccess(userId, partnerIds));
  }
}

class TagAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['tag']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, tagIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, tagIds));
  }
}

class WorkflowAccess {
  constructor(private effect: Context.Service.Shape<typeof AccessRepo>['workflow']) {}

  @GenerateSql({ params: [DummyValue.UUID, DummyValue.UUID_SET] })
  @ChunkedSet({ paramIndex: 1 })
  checkOwnerAccess(userId: string, workflowIds: Set<string>) {
    return runPromise(this.effect.checkOwnerAccess(userId, workflowIds));
  }
}

@Injectable()
export class AccessRepository {
  activity: ActivityAccess;
  album: AlbumAccess;
  asset: AssetAccess;
  assetFile: AssetFileAccess;
  authDevice: AuthDeviceAccess;
  memory: MemoryAccess;
  notification: NotificationAccess;
  partner: PartnerAccess;
  passkey: PasskeyAccess;
  session: SessionAccess;
  stack: StackAccess;
  tag: TagAccess;
  timeline: TimelineAccess;
  workflow: WorkflowAccess;

  constructor(@InjectKysely() db: Kysely<DB>) {
    const effect = AccessRepo.make(db);
    this.activity = new ActivityAccess(effect.activity);
    this.album = new AlbumAccess(effect.album);
    this.asset = new AssetAccess(effect.asset);
    this.assetFile = new AssetFileAccess(effect.assetFile);
    this.authDevice = new AuthDeviceAccess(effect.authDevice);
    this.memory = new MemoryAccess(effect.memory);
    this.notification = new NotificationAccess(effect.notification);
    this.partner = new PartnerAccess(effect.partner);
    this.passkey = new PasskeyAccess(effect.passkey);
    this.session = new SessionAccess(effect.session);
    this.stack = new StackAccess(effect.stack);
    this.tag = new TagAccess(effect.tag);
    this.timeline = new TimelineAccess(effect.timeline);
    this.workflow = new WorkflowAccess(effect.workflow);
    registerEffect(this, effect);
  }
}
