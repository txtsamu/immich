import { BadRequestException, UnauthorizedException } from '@nestjs/common';
import { Context, Effect } from 'effect';
import { AuthSharedLink } from 'src/database.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { effectOf } from 'src/effect/bridge.js';
import { LiftedError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumUserRole, Permission } from 'src/enum.js';
import { AccessRepo, AccessRepository } from 'src/repositories/access.repository.js';

/** The Effect shape of the access repository (what `checkAccessFx` / `requireAccessFx` operate on). */
export type AccessFx = Context.Service.Shape<typeof AccessRepo>;

export type GrantedRequest = {
  requested: Permission[];
  current: Permission[];
};

export const isGranted = ({ requested, current }: GrantedRequest) => {
  if (current.includes(Permission.All)) {
    return true;
  }

  return new Set(current).isSupersetOf(new Set(requested));
};

export const areSetsEqual = <T>(setA: Set<T>, setB: Set<T>): boolean => {
  return setA.size === setB.size && setA.isSupersetOf(setB);
};

export type AccessRequest = {
  auth: AuthDto;
  permission: Permission;
  ids: Set<string> | string[];
};

type SharedLinkAccessRequest = { sharedLink: AuthSharedLink; permission: Permission; ids: Set<string> };
type OtherAccessRequest = { auth: AuthDto; permission: Permission; ids: Set<string> };

export const requireUploadAccess = (auth: AuthDto | null): AuthDto => {
  if (!auth || (auth.sharedLink && !auth.sharedLink.allowUpload)) {
    throw new UnauthorizedException();
  }
  return auth;
};

/**
 * `effectOf` only lifts the top level of a Promise-based repository, so the nested access groups
 * (`access.asset`, ...) of a mock still return Promises. Real Effect services pass through unchanged.
 */
const asEffect = <A, E>(value: Effect.Effect<A, E>): Effect.Effect<A, E> =>
  Effect.isEffect(value)
    ? value
    : (Effect.tryPromise({
        try: async () => await (value as unknown as PromiseLike<A>),
        catch: (cause) => new LiftedError({ cause }),
      }) as unknown as Effect.Effect<A, E>);

export const requireAccessFx = Effect.fn('Access.requireAccess')(function* (access: AccessFx, request: AccessRequest) {
  const allowedIds = yield* checkAccessFx(access, request);
  if (!areSetsEqual(new Set(request.ids), allowedIds)) {
    return yield* Effect.fail(new BadRequestException(`Not found or no ${request.permission} access`));
  }
});

export const checkAccessFx = (access: AccessFx, { ids, auth, permission }: AccessRequest) =>
  Effect.gen(function* () {
    const idSet = Array.isArray(ids) ? new Set(ids) : ids;
    if (idSet.size === 0) {
      return new Set<string>();
    }

    return yield* auth.sharedLink
      ? checkSharedLinkAccessFx(access, { sharedLink: auth.sharedLink, permission, ids: idSet })
      : checkOtherAccessFx(access, { auth, permission, ids: idSet });
  });

export const requireAccess = (access: AccessRepository, request: AccessRequest): Promise<void> =>
  runPromise(requireAccessFx(effectOf(AccessRepo, access), request));

export const checkAccess = (access: AccessRepository, request: AccessRequest): Promise<Set<string>> =>
  runPromise(checkAccessFx(effectOf(AccessRepo, access), request));

const checkSharedLinkAccessFx = (access: AccessFx, request: SharedLinkAccessRequest) =>
  Effect.gen(function* () {
    const { sharedLink, permission, ids } = request;
    const sharedLinkId = sharedLink.id;

    switch (permission) {
      case Permission.AssetRead: {
        return yield* asEffect(access.asset.checkSharedLinkAccess(sharedLinkId, ids));
      }

      case Permission.AssetView: {
        return yield* asEffect(access.asset.checkSharedLinkAccess(sharedLinkId, ids));
      }

      case Permission.AssetDownload: {
        return sharedLink.allowDownload
          ? yield* asEffect(access.asset.checkSharedLinkAccess(sharedLinkId, ids))
          : new Set<string>();
      }

      case Permission.AssetUpload: {
        return sharedLink.allowUpload ? ids : new Set<string>();
      }

      case Permission.AlbumRead: {
        return yield* asEffect(access.album.checkSharedLinkAccess(sharedLinkId, ids));
      }

      case Permission.AlbumDownload: {
        return sharedLink.allowDownload
          ? yield* asEffect(access.album.checkSharedLinkAccess(sharedLinkId, ids))
          : new Set<string>();
      }

      case Permission.AlbumAssetCreate: {
        return sharedLink.allowUpload
          ? yield* asEffect(access.album.checkSharedLinkAccess(sharedLinkId, ids))
          : new Set<string>();
      }

      default: {
        return new Set<string>();
      }
    }
  });

const checkOtherAccessFx = (access: AccessFx, request: OtherAccessRequest) =>
  Effect.gen(function* () {
    const { auth, permission, ids } = request;
    const userId = auth.user.id;
    const elevated = auth.session?.hasElevatedPermission;

    // owner, then album, then partner access to assets
    const assetAccess = Effect.gen(function* () {
      const isOwner = yield* asEffect(access.asset.checkOwnerAccess(userId, ids, elevated));
      const isAlbum = yield* asEffect(access.asset.checkAlbumAccess(userId, ids.difference(isOwner)));
      const isPartner = yield* asEffect(
        access.asset.checkPartnerAccess(userId, ids.difference(isOwner).difference(isAlbum)),
      );
      return isOwner.union(isAlbum).union(isPartner);
    });

    // owner access, then shared album access with at least the given role
    const albumAccess = (role: AlbumUserRole) =>
      Effect.gen(function* () {
        const isOwner = yield* asEffect(access.album.checkOwnerAccess(userId, ids));
        const isShared = yield* asEffect(access.album.checkSharedAlbumAccess(userId, ids.difference(isOwner), role));
        return isOwner.union(isShared);
      });

    switch (permission) {
      // uses album id
      case Permission.ActivityCreate: {
        return yield* asEffect(access.activity.checkCreateAccess(userId, ids));
      }

      // uses activity id
      case Permission.ActivityDelete: {
        const isOwner = yield* asEffect(access.activity.checkOwnerAccess(userId, ids));
        const isAlbumOwner = yield* asEffect(access.activity.checkAlbumOwnerAccess(userId, ids.difference(isOwner)));
        return isOwner.union(isAlbumOwner);
      }

      case Permission.AssetRead: {
        return yield* assetAccess;
      }

      case Permission.AssetShare: {
        const isOwner = yield* asEffect(access.asset.checkOwnerAccess(userId, ids, false));
        const isPartner = yield* asEffect(access.asset.checkPartnerAccess(userId, ids.difference(isOwner)));
        return isOwner.union(isPartner);
      }

      case Permission.AssetFileDownload: {
        return yield* asEffect(access.assetFile.checkOwnerAccess(userId, ids, elevated));
      }

      case Permission.AssetView: {
        return yield* assetAccess;
      }

      case Permission.AssetDownload: {
        return yield* assetAccess;
      }

      case Permission.AssetUpdate:
      case Permission.AssetDelete:
      case Permission.AssetCopy:
      case Permission.AssetEditGet:
      case Permission.AssetEditCreate:
      case Permission.AssetEditDelete: {
        return yield* asEffect(access.asset.checkOwnerAccess(userId, ids, elevated));
      }

      case Permission.AssetFileRead:
      case Permission.AssetFileDelete: {
        return yield* asEffect(access.assetFile.checkOwnerAccess(userId, ids, elevated));
      }

      case Permission.AlbumRead: {
        return yield* albumAccess(AlbumUserRole.Viewer);
      }

      case Permission.AlbumAssetCreate: {
        return yield* albumAccess(AlbumUserRole.Editor);
      }

      case Permission.AlbumUpdate: {
        return yield* albumAccess(AlbumUserRole.Editor);
      }

      case Permission.AlbumDelete: {
        return yield* asEffect(access.album.checkOwnerAccess(userId, ids));
      }

      case Permission.AlbumShare: {
        return yield* albumAccess(AlbumUserRole.Editor);
      }

      case Permission.AlbumDownload: {
        return yield* albumAccess(AlbumUserRole.Viewer);
      }

      case Permission.AlbumAssetDelete: {
        return yield* albumAccess(AlbumUserRole.Editor);
      }

      case Permission.AssetUpload: {
        return ids.has(userId) ? new Set([userId]) : new Set<string>();
      }

      case Permission.ArchiveRead: {
        return ids.has(userId) ? new Set([userId]) : new Set<string>();
      }

      case Permission.AuthDeviceDelete: {
        return yield* asEffect(access.authDevice.checkOwnerAccess(userId, ids));
      }

      case Permission.NotificationRead:
      case Permission.NotificationUpdate:
      case Permission.NotificationDelete: {
        return yield* asEffect(access.notification.checkOwnerAccess(userId, ids));
      }

      case Permission.TagAsset:
      case Permission.TagRead:
      case Permission.TagUpdate:
      case Permission.TagDelete: {
        return yield* asEffect(access.tag.checkOwnerAccess(userId, ids));
      }

      case Permission.TimelineRead: {
        const isOwner = ids.has(userId) ? new Set([userId]) : new Set<string>();
        const isPartner = yield* asEffect(access.timeline.checkPartnerAccess(userId, ids.difference(isOwner)));
        return isOwner.union(isPartner);
      }

      case Permission.TimelineDownload: {
        return ids.has(userId) ? new Set([userId]) : new Set<string>();
      }

      case Permission.MemoryRead:
      case Permission.MemoryUpdate:
      case Permission.MemoryDelete: {
        return yield* asEffect(access.memory.checkOwnerAccess(userId, ids));
      }

      case Permission.PartnerUpdate: {
        return yield* asEffect(access.partner.checkUpdateAccess(userId, ids));
      }

      case Permission.PasskeyRead:
      case Permission.PasskeyUpdate:
      case Permission.PasskeyDelete: {
        return yield* asEffect(access.passkey.checkOwnerAccess(userId, ids));
      }

      case Permission.SessionRead:
      case Permission.SessionUpdate:
      case Permission.SessionDelete:
      case Permission.SessionLock: {
        return yield* asEffect(access.session.checkOwnerAccess(userId, ids));
      }

      case Permission.StackRead:
      case Permission.StackUpdate:
      case Permission.StackDelete: {
        return yield* asEffect(access.stack.checkOwnerAccess(userId, ids));
      }

      case Permission.WorkflowRead:
      case Permission.WorkflowUpdate:
      case Permission.WorkflowDelete:
      case Permission.WorkflowLogs: {
        return yield* asEffect(access.workflow.checkOwnerAccess(userId, ids));
      }

      default: {
        return new Set<string>();
      }
    }
  });

export const requireElevatedPermission = (auth: AuthDto) => {
  if (!auth.session?.hasElevatedPermission) {
    throw new UnauthorizedException('Elevated permission is required');
  }
};
