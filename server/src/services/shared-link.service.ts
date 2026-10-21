import { BadRequestException, ForbiddenException, Injectable, UnauthorizedException } from '@nestjs/common';
import { Effect } from 'effect';
import { PostgresError } from 'postgres';
import { SharedLink } from 'src/database.js';
import { AssetIdErrorReason, AssetIdsResponseDto } from 'src/dtos/asset-ids.response.dto.js';
import { AssetIdsDto } from 'src/dtos/asset.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  SharedLinkCreateDto,
  SharedLinkEditDto,
  SharedLinkLoginDto,
  SharedLinkResponseDto,
  SharedLinkSearchDto,
  mapSharedLink,
} from 'src/dtos/shared-link.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission, SharedLinkType } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { asDateTimeString } from 'src/utils/date.js';
import { OpenGraphTags, getExternalDomain } from 'src/utils/misc.js';

/** The original thrown value behind a wrapped repository error (see `runPromise`). */
const unwrapError = (error: unknown) =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;

@Injectable()
export class SharedLinkService extends BaseService {
  async getAll(auth: AuthDto, { id, albumId }: SharedLinkSearchDto): Promise<SharedLinkResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const links = yield* this.fx.sharedLink.getAll({ userId: auth.user.id, id, albumId });
        return links.map((link) => mapSharedLink(link, { auth }));
      }),
    );
  }

  async login(auth: AuthDto, dto: SharedLinkLoginDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!auth.sharedLink) {
          return yield* Effect.fail(new ForbiddenException());
        }

        const sharedLink = yield* this.findOrFail(auth.user.id, auth.sharedLink.id);
        const { id, password } = sharedLink;

        if (!password) {
          return yield* Effect.fail(new BadRequestException('Shared link is not password protected'));
        }

        if (password !== dto.password) {
          return yield* Effect.fail(new UnauthorizedException('Invalid password'));
        }

        return {
          sharedLink: yield* this.mapWithAlbumMetadata(sharedLink, { auth }),
          token: yield* this.asToken({ id, password }),
        };
      }),
    );
  }

  async getMine(auth: AuthDto, authTokens: string[]) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!auth.sharedLink) {
          return yield* Effect.fail(new ForbiddenException());
        }

        const sharedLink = yield* this.findOrFail(auth.user.id, auth.sharedLink.id);
        const { id, password } = sharedLink;

        if (password && !authTokens.includes(yield* this.asToken({ id, password }))) {
          return yield* Effect.fail(new UnauthorizedException('Password required'));
        }

        return yield* this.mapWithAlbumMetadata(sharedLink, { auth });
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<SharedLinkResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sharedLink = yield* this.findOrFail(auth.user.id, id);
        return yield* this.mapWithAlbumMetadata(sharedLink, { auth });
      }),
    );
  }

  async create(auth: AuthDto, dto: SharedLinkCreateDto): Promise<SharedLinkResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        switch (dto.type) {
          case SharedLinkType.Album: {
            if (!dto.albumId) {
              return yield* Effect.fail(new BadRequestException('Invalid albumId'));
            }
            yield* this.requireAccessFx({ auth, permission: Permission.AlbumShare, ids: [dto.albumId] });
            break;
          }

          case SharedLinkType.Individual: {
            if (!dto.assetIds || dto.assetIds.length === 0) {
              return yield* Effect.fail(new BadRequestException('Invalid assetIds'));
            }

            yield* this.requireAccessFx({ auth, permission: Permission.AssetShare, ids: dto.assetIds });

            break;
          }
        }

        return yield* Effect.gen({ self: this }, function* () {
          const sharedLink = yield* this.fx.sharedLink.create({
            key: yield* this.fx.crypto.randomBytes(50),
            userId: auth.user.id,
            type: dto.type,
            albumId: dto.albumId || null,
            assetIds: dto.assetIds,
            description: dto.description || null,
            password: dto.password,
            expiresAt: dto.expiresAt || null,
            allowUpload: dto.allowUpload ?? true,
            allowDownload: dto.showMetadata !== false && (dto.allowDownload ?? true),
            showExif: dto.showMetadata ?? true,
            slug: dto.slug || null,
          });

          return mapSharedLink(sharedLink, { auth });
        }).pipe(Effect.catch((error) => this.handleError(error)));
      }),
    );
  }

  private handleError(error: unknown) {
    const original = unwrapError(error);
    if ((original as PostgresError).constraint_name === 'shared_link_slug_uq') {
      this.logger.debug('Shared link with this slug already exists');
      return Effect.fail(new BadRequestException('Failed to save shared link'));
    }
    return Effect.fail(original);
  }

  async update(auth: AuthDto, id: string, dto: SharedLinkEditDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.findOrFail(auth.user.id, id);
        return yield* Effect.gen({ self: this }, function* () {
          const sharedLink = yield* this.fx.sharedLink.update({
            id,
            userId: auth.user.id,
            description: dto.description,
            password: dto.password,
            expiresAt: dto.expiresAt,
            allowUpload: dto.allowUpload,
            allowDownload: dto.allowDownload,
            showExif: dto.showMetadata,
            slug: dto.slug || null,
          });
          return mapSharedLink(sharedLink, { auth });
        }).pipe(Effect.catch((error) => this.handleError(error)));
      }),
    );
  }

  async remove(auth: AuthDto, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sharedLink = yield* this.findOrFail(auth.user.id, id);
        yield* this.fx.sharedLink.remove(sharedLink.id);
      }),
    );
  }

  // TODO: replace `userId` with permissions and access control checks
  private readonly findOrFail = Effect.fn('SharedLink.findOrFail')(function* (
    this: SharedLinkService,
    userId: string,
    id: string,
  ) {
    const value = yield* this.fx.sharedLink.get(userId, id);
    if (!value) {
      return yield* Effect.fail(new BadRequestException('Shared link not found'));
    }

    return value;
  });

  async addAssets(auth: AuthDto, id: string, dto: AssetIdsDto): Promise<AssetIdsResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sharedLink = yield* this.findOrFail(auth.user.id, id);
        if (sharedLink.type !== SharedLinkType.Individual) {
          return yield* Effect.fail(new BadRequestException('Invalid shared link type'));
        }

        const existingAssetIds = new Set(sharedLink.assets.map((asset) => asset.id));
        const notPresentAssetIds = dto.assetIds.filter((assetId) => !existingAssetIds.has(assetId));
        const allowedAssetIds = yield* this.checkAccessFx({
          auth,
          permission: Permission.AssetShare,
          ids: notPresentAssetIds,
        });

        const results: AssetIdsResponseDto[] = [];
        for (const assetId of dto.assetIds) {
          const hasAsset = existingAssetIds.has(assetId);
          if (hasAsset) {
            results.push({ assetId, success: false, error: AssetIdErrorReason.DUPLICATE });
            continue;
          }

          const hasAccess = allowedAssetIds.has(assetId);
          if (!hasAccess) {
            results.push({ assetId, success: false, error: AssetIdErrorReason.NO_PERMISSION });
            continue;
          }

          results.push({ assetId, success: true });
        }

        yield* this.fx.sharedLink.update({
          ...sharedLink,
          assetIds: results.filter(({ success }) => success).map(({ assetId }) => assetId),
        });

        return results;
      }),
    );
  }

  async removeAssets(auth: AuthDto, id: string, dto: AssetIdsDto): Promise<AssetIdsResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const sharedLink = yield* this.findOrFail(auth.user.id, id);

        if (sharedLink.type !== SharedLinkType.Individual) {
          return yield* Effect.fail(new BadRequestException('Invalid shared link type'));
        }

        const removedAssetIds = yield* this.fx.sharedLinkAsset.remove(id, dto.assetIds);

        const results: AssetIdsResponseDto[] = [];
        for (const assetId of dto.assetIds) {
          const wasRemoved = removedAssetIds.includes(assetId);
          if (!wasRemoved) {
            results.push({ assetId, success: false, error: AssetIdErrorReason.NOT_FOUND });
            continue;
          }

          results.push({ assetId, success: true });
          sharedLink.assets = sharedLink.assets.filter((asset) => asset.id !== assetId);
        }

        yield* this.fx.sharedLink.update(sharedLink);

        return results;
      }),
    );
  }

  async getMetadataTags(auth: AuthDto, defaultDomain?: string): Promise<null | OpenGraphTags> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (!auth.sharedLink || auth.sharedLink.password) {
          return null;
        }

        const config = yield* this.getConfigFx({ withCache: true });
        const sharedLink = yield* this.findOrFail(auth.sharedLink.userId, auth.sharedLink.id);
        const assetId = sharedLink.album?.albumThumbnailAssetId || sharedLink.assets[0]?.id;
        const [albumMetadata] = sharedLink.album ? yield* this.fx.album.getMetadataForIds([sharedLink.album.id]) : [];
        const assetCount = sharedLink.assets.length > 0 ? sharedLink.assets.length : albumMetadata?.assetCount || 0;
        const imagePath = assetId
          ? `/api/assets/${assetId}/thumbnail?key=${sharedLink.key.toString('base64url')}`
          : '/feature-panel.png';

        return {
          title: sharedLink.album ? sharedLink.album.albumName : 'Public Share',
          description: sharedLink.description || `${assetCount} shared photos & videos`,
          imageUrl: new URL(imagePath, getExternalDomain(config.server, defaultDomain)).href,
        };
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly mapWithAlbumMetadata = Effect.fn('SharedLink.mapWithAlbumMetadata')(function* (
    this: SharedLinkService,
    sharedLink: SharedLink,
    { auth }: { auth: AuthDto },
  ) {
    const response = mapSharedLink(sharedLink, { auth });
    if (response.album) {
      const [metadata] = yield* this.fx.album.getMetadataForIds([response.album.id]);
      response.album.startDate = asDateTimeString(metadata?.startDate ?? undefined);
      response.album.endDate = asDateTimeString(metadata?.endDate ?? undefined);
      response.album.assetCount = metadata?.assetCount ?? 0;
    }

    return response;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly asToken = Effect.fn('SharedLink.asToken')(function* (
    this: SharedLinkService,
    sharedLink: { id: string; password: string },
  ) {
    const hashed = yield* this.fx.crypto.hashSha256(`${sharedLink.id}-${sharedLink.password}`);
    return hashed.toString('base64');
  });
}
