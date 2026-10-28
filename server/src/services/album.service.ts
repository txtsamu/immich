import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import {
  AddUsersDto,
  AlbumResponseDto,
  AlbumStatisticsResponseDto,
  AlbumsAddAssetsDto,
  AlbumsAddAssetsResponseDto,
  CreateAlbumDto,
  GetAlbumsDto,
  MapAlbumDto,
  UpdateAlbumDto,
  UpdateAlbumUserDto,
  mapAlbum,
} from 'src/dtos/album.dto.js';
import { BulkIdErrorReason, BulkIdResponseDto, BulkIdsDto } from 'src/dtos/asset-ids.response.dto.js';
import { AuthDto, isMetadataHidden } from 'src/dtos/auth.dto.js';
import { MapMarkerResponseDto } from 'src/dtos/map.dto.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumUserRole, Permission } from 'src/enum.js';
import { AlbumAssetCount } from 'src/repositories/album.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { addAssetsFx, removeAssetsFx } from 'src/utils/asset.util.js';
import { asDateTimeString } from 'src/utils/date.js';
import { getPreferences } from 'src/utils/preferences.js';

const mapAlbumMetadata = (metadata?: AlbumAssetCount) => ({
  startDate: asDateTimeString(metadata?.startDate ?? undefined),
  endDate: asDateTimeString(metadata?.endDate ?? undefined),
  assetCount: metadata?.assetCount ?? 0,
  // lastModifiedAssetTimestamp is only used in mobile app, please remove if not need
  lastModifiedAssetTimestamp: asDateTimeString(metadata?.lastModifiedAssetTimestamp ?? undefined),
});

@Injectable()
export class AlbumService extends BaseService {
  async getStatistics(auth: AuthDto): Promise<AlbumStatisticsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const [owned, shared, notShared] = yield* Effect.all(
          [
            this.fx.album.getAll(auth.user.id, { isOwned: true }),
            this.fx.album.getAll(auth.user.id, { isShared: true }),
            this.fx.album.getAll(auth.user.id, { isOwned: true, isShared: false }),
          ],
          { concurrency: 'unbounded' },
        );

        return {
          owned: owned.length,
          shared: shared.length,
          notShared: notShared.length,
        };
      }),
    );
  }

  async getAll({ user: { id: ownerId } }: AuthDto, { assetId, ...rest }: GetAlbumsDto): Promise<AlbumResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.album.updateThumbnails();

        const albums = assetId
          ? yield* this.fx.album.getByAssetId(ownerId, assetId)
          : yield* this.fx.album.getAll(ownerId, rest);

        if (albums.length === 0) {
          return [];
        }

        // Get asset count for each album. Then map the result to an object:
        // { [albumId]: assetCount }
        const results = yield* this.fx.album.getMetadataForIds(albums.map((album) => album.id));
        const albumMetadata: Record<string, AlbumAssetCount> = {};
        for (const metadata of results) {
          albumMetadata[metadata.albumId] = metadata;
        }

        return albums.map((album) => ({
          ...mapAlbum(album),
          sharedLinks: undefined,
          ...mapAlbumMetadata(albumMetadata[album.id]),
        }));
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<AlbumResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumRead, ids: [id] });
        yield* this.fx.album.updateThumbnails();
        const album = yield* this.findOrFail(id, auth.user.id);

        const hasSharedUsers = album.albumUsers && album.albumUsers.length > 1;
        const hasSharedLink = album.sharedLinks && album.sharedLinks.length > 0;
        const isShared = hasSharedUsers || hasSharedLink;

        const mapped = yield* this.mapWithMetadata(album, auth);
        return {
          ...mapped,
          contributorCounts:
            isShared && !auth.sharedLink ? yield* this.fx.album.getContributorCounts(album.id) : undefined,
        };
      }),
    );
  }

  async getMapMarkers(auth: AuthDto, id: string): Promise<MapMarkerResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumRead, ids: [id] });

        if (isMetadataHidden(auth)) {
          return [];
        }

        return yield* this.fx.map.getAlbumMapMarkers(id);
      }),
    );
  }

  async create(auth: AuthDto, dto: CreateAlbumDto): Promise<AlbumResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const albumUsers = (dto.albumUsers || []).filter(({ userId }) => userId !== auth.user.id);

        for (const { userId } of albumUsers) {
          const exists = yield* this.fx.user.get(userId, {});
          if (!exists) {
            this.logger.debug('Album creation failed: user not found');
            return yield* Effect.fail(new BadRequestException('Invalid user'));
          }
        }

        const allowedAssetIdsSet = yield* this.checkAccessFx({
          auth,
          permission: Permission.AssetShare,
          ids: dto.assetIds || [],
        });
        const assetIds = [...allowedAssetIdsSet].map((id) => id);

        const userMetadata = yield* this.fx.user.getMetadata(auth.user.id);

        const album = yield* this.fx.album.create(
          {
            albumName: dto.albumName,
            description: dto.description,
            albumThumbnailAssetId: assetIds[0] || null,
            order: getPreferences(userMetadata).albums.defaultAssetOrder,
          },
          assetIds,
          [{ userId: auth.user.id, role: AlbumUserRole.Owner }, ...albumUsers],
          auth.user.id,
        );

        for (const { userId } of albumUsers) {
          yield* this.fx.event.emit('AlbumInvite', { id: album.id, userId, senderName: auth.user.name });
        }

        return yield* this.mapWithMetadata(album);
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: UpdateAlbumDto): Promise<AlbumResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumUpdate, ids: [id] });

        const album = yield* this.findOrFail(id, auth.user.id);

        if (dto.albumThumbnailAssetId) {
          const results = yield* this.fx.album.getAssetIds(id, [dto.albumThumbnailAssetId]);
          if (results.size === 0) {
            return yield* Effect.fail(new BadRequestException('Invalid album thumbnail'));
          }
        }
        const updatedAlbum = yield* this.fx.album.update(
          album.id,
          {
            id: album.id,
            albumName: dto.albumName,
            description: dto.description,
            albumThumbnailAssetId: dto.albumThumbnailAssetId,
            isActivityEnabled: dto.isActivityEnabled,
            order: dto.order,
          },
          auth.user.id,
        );

        return yield* this.mapWithMetadata(updatedAlbum);
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumDelete, ids: [id] });
        yield* this.fx.album.delete(id);
      }),
    );
  }

  async addAssets(auth: AuthDto, id: string, dto: BulkIdsDto): Promise<BulkIdResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const album = yield* this.findOrFail(id, auth.user.id);
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumAssetCreate, ids: [id] });

        const results = yield* addAssetsFx(
          auth,
          { access: this.fx.access, bulk: this.fx.album },
          { parentId: id, assetIds: dto.ids, permission: Permission.AssetShare },
        );

        const { id: firstNewAssetId } = results.find(({ success }) => success) || {};
        if (firstNewAssetId) {
          yield* this.fx.album.update(
            id,
            {
              id,
              updatedAt: new Date(),
              albumThumbnailAssetId: album.albumThumbnailAssetId ?? firstNewAssetId,
            },
            auth.user.id,
          );

          const userIds = album.albumUsers.map(({ user }) => user.id);
          const recipientIds = userIds.filter((userId) => userId !== auth.user.id);
          yield* this.fx.event.emit('AlbumUpdate', { id, userIds, recipientIds });
        }

        return results;
      }),
    );
  }

  async addAssetsToAlbums(auth: AuthDto, dto: AlbumsAddAssetsDto): Promise<AlbumsAddAssetsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const results: AlbumsAddAssetsResponseDto = {
          success: false,
          error: BulkIdErrorReason.DUPLICATE,
        };

        const allowedAlbumIds = yield* this.checkAccessFx({
          auth,
          permission: Permission.AlbumAssetCreate,
          ids: dto.albumIds,
        });
        if (allowedAlbumIds.size === 0) {
          results.error = BulkIdErrorReason.NO_PERMISSION;
          return results;
        }

        const allowedAssetIds = yield* this.checkAccessFx({
          auth,
          permission: Permission.AssetShare,
          ids: dto.assetIds,
        });
        if (allowedAssetIds.size === 0) {
          results.error = BulkIdErrorReason.NO_PERMISSION;
          return results;
        }

        const albumAssetValues: { albumId: string; assetId: string }[] = [];
        const events: { id: string; userIds: string[]; recipientIds: string[] }[] = [];
        for (const albumId of allowedAlbumIds) {
          const existingAssetIds = yield* this.fx.album.getAssetIds(albumId, [...allowedAssetIds]);
          const notPresentAssetIds = [...allowedAssetIds.difference(existingAssetIds)];
          if (notPresentAssetIds.length === 0) {
            continue;
          }
          const album = yield* this.findOrFail(albumId, auth.user.id);
          results.error = undefined;
          results.success = true;

          for (const assetId of notPresentAssetIds) {
            albumAssetValues.push({ albumId, assetId });
          }
          yield* this.fx.album.update(
            albumId,
            {
              id: albumId,
              updatedAt: new Date(),
              albumThumbnailAssetId: album.albumThumbnailAssetId ?? notPresentAssetIds[0],
            },
            auth.user.id,
          );
          const userIds = album.albumUsers.map(({ user }) => user.id);
          const recipientIds = userIds.filter((userId) => userId !== auth.user.id);
          events.push({ id: albumId, userIds, recipientIds });
        }

        yield* this.fx.album.addAssetIdsToAlbums(albumAssetValues);
        for (const event of events) {
          yield* this.fx.event.emit('AlbumUpdate', event);
        }

        return results;
      }),
    );
  }

  async removeAssets(auth: AuthDto, id: string, dto: BulkIdsDto): Promise<BulkIdResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumAssetDelete, ids: [id] });

        const album = yield* this.findOrFail(id, auth.user.id);
        const results = yield* removeAssetsFx(
          auth,
          { access: this.fx.access, bulk: this.fx.album },
          { parentId: id, assetIds: dto.ids, canAlwaysRemove: Permission.AlbumDelete },
        );

        const removedIds = results.filter(({ success }) => success).map(({ id }) => id);
        if (removedIds.length > 0) {
          if (album.albumThumbnailAssetId && removedIds.includes(album.albumThumbnailAssetId)) {
            yield* this.fx.album.updateThumbnails();
          }

          yield* this.fx.event.emit('AlbumUpdate', {
            id,
            userIds: album.albumUsers.map(({ user }) => user.id),
            recipientIds: [],
          });
        }

        return results;
      }),
    );
  }

  async addUsers(auth: AuthDto, id: string, { albumUsers }: AddUsersDto): Promise<AlbumResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumShare, ids: [id] });

        const album = yield* this.findOrFail(id, auth.user.id);

        for (const { userId, role } of albumUsers) {
          if (role === AlbumUserRole.Owner) {
            return yield* Effect.fail(new BadRequestException('Cannot add another owner'));
          }

          const exists = album.albumUsers.some(({ user: { id } }) => id === userId);
          if (exists) {
            continue;
          }

          const user = yield* this.fx.user.get(userId, {});
          if (!user) {
            this.logger.debug('Adding user to album failed: user not found');
            return yield* Effect.fail(new BadRequestException('Invalid user'));
          }

          yield* this.fx.albumUser.create({ userId, albumId: id, role });
          yield* this.fx.event.emit('AlbumInvite', { id, userId, senderName: auth.user.name });
        }

        return yield* this.mapWithMetadata(yield* this.findOrFail(id, auth.user.id));
      }),
    );
  }

  async removeUser(auth: AuthDto, id: string, userId: string | 'me'): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (userId === 'me') {
          userId = auth.user.id;
        }

        const album = yield* this.findOrFail(id, auth.user.id);

        const exists = album.albumUsers.find(({ user: { id } }) => id === userId);
        if (!exists) {
          return yield* Effect.fail(new BadRequestException('Album not shared with user'));
        }

        if (
          exists.role === AlbumUserRole.Owner &&
          album.albumUsers.filter(({ role }) => role === AlbumUserRole.Owner).length === 1
        ) {
          return yield* Effect.fail(new BadRequestException('Cannot remove the last album owner'));
        }

        // non-admin can remove themselves
        if (auth.user.id !== userId) {
          yield* this.requireAccessFx({ auth, permission: Permission.AlbumShare, ids: [id] });
        }

        yield* this.fx.albumUser.delete({ albumId: id, userId });
      }),
    );
  }

  async updateUser(auth: AuthDto, id: string, userId: string, dto: UpdateAlbumUserDto): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AlbumShare, ids: [id] });

        const album = yield* this.findOrFail(id, userId);
        const owner = album.albumUsers[0];

        if (owner.user.id === userId) {
          return yield* Effect.fail(new BadRequestException('User is owner'));
        }

        yield* this.fx.albumUser.update({ albumId: id, userId }, { role: dto.role });
      }),
    );
  }

  private findOrFail = Effect.fn('AlbumService.findOrFail')(function* (
    this: AlbumService,
    id: string,
    authUserId: string,
  ) {
    const value = yield* this.fx.album.getById(id, authUserId);
    if (!value) {
      return yield* Effect.fail(new BadRequestException('Album not found'));
    }

    return value;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private mapWithMetadata = Effect.fn('AlbumService.mapWithMetadata')(function* (
    this: AlbumService,
    album: MapAlbumDto,
    auth?: AuthDto,
  ) {
    const [metadata] = yield* this.fx.album.getMetadataForIds([album.id]);
    return { ...mapAlbum(album, { auth }), ...mapAlbumMetadata(metadata) } as AlbumResponseDto;
  });
}
