import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AssetMapOptions, AssetResponseDto, MapAsset, mapAsset } from 'src/dtos/asset-response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  LargeAssetSearchDto,
  MetadataSearchDto,
  PlacesResponseDto,
  RandomSearchDto,
  SearchFilter,
  SearchPlacesDto,
  SearchResponseDto,
  SearchStatisticsResponseDto,
  SearchSuggestionRequestDto,
  SearchSuggestionType,
  StatisticsSearchDto,
  isFullyAlbumConfined,
  isNewShapeRequest,
  mapPlaces,
} from 'src/dtos/search.dto.js';
import { runPromise } from 'src/effect/run.js';
import { AssetOrder, AssetVisibility, Permission } from 'src/enum.js';
import { AssetSearchScope } from 'src/repositories/search.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { requireElevatedPermission } from 'src/utils/access.js';
import { getMyPartnerIds } from 'src/utils/asset.util.js';
import { decodeSearchCursor, encodeSearchCursor } from 'src/utils/search-cursor.js';
import { applyLockedVisibilityPolicy, collectFilterIds } from 'src/utils/search-filter.js';

@Injectable()
export class SearchService extends BaseService {
  async searchPlaces(dto: SearchPlacesDto): Promise<PlacesResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const places = yield* this.fx.search.searchPlaces(dto.name);
        return places.map((place) => mapPlaces(place));
      }),
    );
  }

  async getExploreData(auth: AuthDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const options = { maxFields: 12, minAssetsPerField: 5 };

        const cities = yield* this.fx.asset.getAssetIdByCity(auth.user.id, options);
        const cityAssets = yield* this.fx.asset.getByIdsWithAllRelationsButStacks(cities.items.map(({ data }) => data));
        const cityItems = cityAssets.map((asset) => ({
          value: asset.exifInfo!.city!,
          data: mapAsset(asset, { auth }),
        }));

        const recents = yield* this.fx.asset.getRecentlyCreatedAssetIds(auth.user.id, options.maxFields);
        const recentAssets = yield* this.fx.asset.getByIdsWithAllRelationsButStacks(
          recents.items.map((item) => item.data),
        );
        const recentItems = recentAssets.map((asset) => ({
          value: asset.createdAt.toISOString(),
          data: mapAsset(asset, { auth }),
        }));

        return [
          { fieldName: cities.fieldName, items: cityItems },
          { fieldName: recents.fieldName, items: recentItems },
        ];
      }),
    );
  }

  async searchMetadata(auth: AuthDto, dto: MetadataSearchDto): Promise<SearchResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (isNewShapeRequest(dto)) {
          return yield* this.searchMetadataV3(auth, dto);
        }

        if (dto.visibility === AssetVisibility.Locked) {
          requireElevatedPermission(auth);
        }

        let checksum: Buffer | undefined;
        if (dto.checksum) {
          const encoding = dto.checksum.length === 28 ? 'base64' : 'hex';
          checksum = Buffer.from(dto.checksum, encoding);
        }

        let userIds: string[] | undefined;

        if (dto.albumIds && dto.albumIds.length > 0) {
          yield* this.requireAccessFx({ auth, ids: dto.albumIds, permission: Permission.AlbumRead });
        } else if (auth.sharedLink) {
          return yield* Effect.fail(
            new BadRequestException('Shared link access is only allowed in combination with an albumIds filter'),
          );
        } else {
          userIds = yield* this.getUserIdsToSearch(auth, dto.visibility);
        }

        const page = dto.page ?? 1;
        const size = dto.size;
        const { hasNextPage, items } = yield* this.fx.search.searchMetadata(
          { page, size },
          {
            ...dto,
            checksum,
            visibility: dto.visibility ?? (auth.session?.hasElevatedPermission ? undefined : 'not-locked'),
            userIds,
            orderDirection: dto.order ?? AssetOrder.Desc,
          },
        );

        return this.mapResponse(items, { auth }, { nextPage: hasNextPage ? (page + 1).toString() : null });
      }),
    );
  }

  async searchStatistics(auth: AuthDto, dto: StatisticsSearchDto): Promise<SearchStatisticsResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (isNewShapeRequest(dto)) {
          return yield* this.searchStatisticsV3(auth, dto);
        }

        const userIds = yield* this.getUserIdsToSearch(auth, dto.visibility);
        if (dto.visibility === AssetVisibility.Locked) {
          requireElevatedPermission(auth);
        }

        return yield* this.fx.search.searchStatistics({
          ...dto,
          visibility: dto.visibility ?? (auth.session?.hasElevatedPermission ? undefined : 'not-locked'),
          userIds,
        });
      }),
    );
  }

  async searchRandom(auth: AuthDto, dto: RandomSearchDto): Promise<AssetResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (isNewShapeRequest(dto)) {
          return yield* this.searchRandomV3(auth, dto);
        }

        if (dto.visibility === AssetVisibility.Locked) {
          requireElevatedPermission(auth);
        }

        const userIds = yield* this.getUserIdsToSearch(auth, dto.visibility);
        const items = yield* this.fx.search.searchRandom(dto.size, {
          ...dto,
          visibility: dto.visibility ?? (auth.session?.hasElevatedPermission ? undefined : 'not-locked'),
          userIds,
        });
        return items.map((item) => mapAsset(item, { auth }));
      }),
    );
  }

  async searchLargeAssets(auth: AuthDto, dto: LargeAssetSearchDto): Promise<AssetResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (dto.visibility === AssetVisibility.Locked) {
          requireElevatedPermission(auth);
        }

        const userIds = yield* this.getUserIdsToSearch(auth, dto.visibility);
        const items = yield* this.fx.search.searchLargeAssets(dto.size, {
          ...dto,
          visibility: dto.visibility ?? (auth.session?.hasElevatedPermission ? undefined : 'not-locked'),
          userIds,
        });
        return items.map((item) => mapAsset(item, { auth }));
      }),
    );
  }

  async getAssetsByCity(auth: AuthDto): Promise<AssetResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const userIds = yield* this.getUserIdsToSearch(auth);
        const assets = yield* this.fx.search.getAssetsByCity(userIds);
        return assets.map((asset) => mapAsset(asset));
      }),
    );
  }

  async getSearchSuggestions(auth: AuthDto, dto: SearchSuggestionRequestDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const userIds = yield* this.getUserIdsToSearch(auth);
        const suggestions = yield* this.getSuggestions(userIds, dto);
        if (dto.includeNull) {
          suggestions.push(null);
        }
        return suggestions;
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly getSuggestions = Effect.fn('Search.getSuggestions')(function* (
    this: SearchService,
    userIds: string[],
    dto: SearchSuggestionRequestDto,
  ) {
    let suggestions: Array<string | null>;
    switch (dto.type) {
      case SearchSuggestionType.COUNTRY: {
        suggestions = yield* this.fx.search.getCountries(userIds);
        break;
      }
      case SearchSuggestionType.STATE: {
        suggestions = yield* this.fx.search.getStates(userIds, dto);
        break;
      }
      case SearchSuggestionType.CITY: {
        suggestions = yield* this.fx.search.getCities(userIds, dto);
        break;
      }
      case SearchSuggestionType.CAMERA_MAKE: {
        suggestions = yield* this.fx.search.getCameraMakes(userIds, dto);
        break;
      }
      case SearchSuggestionType.CAMERA_MODEL: {
        suggestions = yield* this.fx.search.getCameraModels(userIds, dto);
        break;
      }
      case SearchSuggestionType.CAMERA_LENS_MODEL: {
        suggestions = yield* this.fx.search.getCameraLensModels(userIds, dto);
        break;
      }
      default: {
        suggestions = [];
      }
    }
    return suggestions;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly searchMetadataV3 = Effect.fn('Search.searchMetadataV3')(function* (
    this: SearchService,
    auth: AuthDto,
    dto: MetadataSearchDto,
  ) {
    const { filter, scope } = yield* this.resolveSearchScopeV3(auth, dto);

    const { offset } = decodeSearchCursor(dto.cursor);
    const size = dto.size;
    const { hasNextPage, items } = yield* this.fx.search.searchMetadataV3(
      { take: size, skip: offset },
      {
        filter,
        withExif: dto.withExif,
        withStacked: dto.withStacked,
        order: dto.orderBy,
      },
      scope,
    );

    return this.mapResponse(items, { auth }, { nextCursor: hasNextPage ? encodeSearchCursor(offset + size) : null });
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly searchStatisticsV3 = Effect.fn('Search.searchStatisticsV3')(function* (
    this: SearchService,
    auth: AuthDto,
    dto: StatisticsSearchDto,
  ) {
    const { filter, scope } = yield* this.resolveSearchScopeV3(auth, dto);
    return yield* this.fx.search.searchStatisticsV3({ filter }, scope);
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly searchRandomV3 = Effect.fn('Search.searchRandomV3')(function* (
    this: SearchService,
    auth: AuthDto,
    dto: RandomSearchDto,
  ) {
    const { filter, scope } = yield* this.resolveSearchScopeV3(auth, dto);
    const items = yield* this.fx.search.searchRandomV3(
      dto.size,
      {
        filter,
        withExif: dto.withExif,
        withStacked: dto.withStacked,
      },
      scope,
    );
    return items.map((item) => mapAsset(item, { auth }));
  });

  private readonly resolveSearchScopeV3 = Effect.fn('Search.resolveSearchScopeV3')(function* (
    this: SearchService,
    auth: AuthDto,
    dto: { filter?: SearchFilter },
  ) {
    const filter = dto.filter ?? {};
    const effectiveFilter = applyLockedVisibilityPolicy(auth, filter);

    const fullyConfined = isFullyAlbumConfined(filter);
    // a shared link visitor does not have a universe, so there every branch must be confined
    if (auth.sharedLink && !fullyConfined) {
      return yield* Effect.fail(
        new BadRequestException('Shared link access is only allowed in combination with an albumIds filter'),
      );
    }

    const albumIds = collectFilterIds(filter, 'albumIds');
    const [userIds] = yield* Effect.all(
      [
        // a fully confined filter searches albums only, so the unused universe can skip the partner lookup
        fullyConfined ? Effect.succeed([auth.user.id]) : this.getUserIdsToSearch(auth),
        albumIds.length > 0
          ? this.requireAccessFx({ auth, ids: albumIds, permission: Permission.AlbumRead })
          : Effect.void,
      ],
      { concurrency: 'unbounded' },
    );

    return { filter: effectiveFilter, scope: { userIds, lockedOwnerId: auth.user.id } satisfies AssetSearchScope };
  });

  private readonly getUserIdsToSearch = Effect.fn('Search.getUserIdsToSearch')(function* (
    this: SearchService,
    auth: AuthDto,
    visibility?: AssetVisibility,
  ) {
    // Locked assets are personal. Never include partner IDs, regardless of A's elevated session.
    if (visibility === AssetVisibility.Locked) {
      return [auth.user.id];
    }
    const partnerIds = yield* Effect.tryPromise({
      try: () =>
        getMyPartnerIds({
          userId: auth.user.id,
          repository: this.partnerRepository,
          timelineEnabled: true,
        }),
      catch: (cause) => cause,
    });
    return [auth.user.id, ...partnerIds];
  });

  private mapResponse(
    assets: MapAsset[],
    options: AssetMapOptions,
    page: { nextPage?: string | null; nextCursor?: string | null } = {},
  ): SearchResponseDto {
    return {
      albums: { total: 0, count: 0, items: [], facets: [] },
      assets: {
        total: assets.length,
        count: assets.length,
        items: assets.map((asset) => mapAsset(asset, options)),
        facets: [],
        nextPage: page.nextPage ?? null,
        nextCursor: page.nextCursor ?? null,
      },
    };
  }
}
