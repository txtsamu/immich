import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { type Kysely, type OrderByDirection, type Selectable, type ShallowDehydrateObject, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { MapAsset } from 'src/dtos/asset-response.dto.js';
import { SearchFilter, SearchOrder } from 'src/dtos/search.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { AssetStatus, AssetType, AssetVisibility } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { AssetExifTable } from 'src/schema/tables/asset-exif.table.js';
import {
  anyUuid,
  searchAssetBuilder,
  searchAssetBuilderLegacy,
  searchMetadataV3Examples,
  searchRandomV3Examples,
  searchStatisticsV3Examples,
  withExifInner,
  withSearchOrder,
} from 'src/utils/database.js';
import { type PaginationOptions, paginationHelper } from 'src/utils/pagination.js';

export interface SearchAssetIdOptions {
  checksum?: Buffer;
  id?: string;
}

export interface SearchUserIdOptions {
  libraryId?: string | null;
  userIds?: string[];
}

export type SearchIdOptions = SearchAssetIdOptions & SearchUserIdOptions;

export interface SearchStatusOptions {
  isEncoded?: boolean;
  isFavorite?: boolean;
  isMotion?: boolean;
  isOffline?: boolean;
  isNotInAlbum?: boolean;
  type?: AssetType;
  status?: AssetStatus;
  withArchived?: boolean;
  withDeleted?: boolean;
  visibility?: AssetVisibility;
}

export interface SearchOneToOneRelationOptions {
  withExif?: boolean;
  withStacked?: boolean;
}

export interface SearchDateOptions {
  createdBefore?: Date;
  createdAfter?: Date;
  takenBefore?: Date;
  takenAfter?: Date;
  trashedBefore?: Date;
  trashedAfter?: Date;
  updatedBefore?: Date;
  updatedAfter?: Date;
}

export interface SearchPathOptions {
  encodedVideoPath?: string;
  originalFileName?: string;
  originalPath?: string;
  previewPath?: string;
  thumbnailPath?: string;
}

export interface SearchExifOptions {
  city?: string | null;
  country?: string | null;
  lensModel?: string | null;
  make?: string | null;
  model?: string | null;
  state?: string | null;
  description?: string | null;
  rating?: number | null;
}

export interface SearchTagOptions {
  tagIds?: string[] | null;
}

export interface SearchAlbumOptions {
  albumIds?: string[];
}

export interface SearchOrderOptions {
  orderDirection?: 'asc' | 'desc';
}

export interface SearchPaginationOptions {
  page: number;
  size: number;
}

type BaseAssetSearchOptions = SearchDateOptions &
  SearchIdOptions &
  SearchExifOptions &
  SearchOrderOptions &
  SearchPathOptions &
  SearchStatusOptions &
  SearchUserIdOptions &
  SearchTagOptions &
  SearchAlbumOptions;

export type AssetSearchOptions = Omit<BaseAssetSearchOptions, 'visibility'> &
  SearchOneToOneRelationOptions & { visibility?: AssetVisibility | 'not-locked' };

export type AssetSearchBuilderOptions = Omit<AssetSearchOptions, 'orderDirection'>;

export interface AssetSearchScope {
  userIds: string[];
  lockedOwnerId: string;
}

export interface AssetSearchBuilderV3Options {
  filter?: SearchFilter;
  withExif?: boolean;
  withStacked?: boolean;
  order?: SearchOrder;
}

export type LargeAssetSearchOptions = AssetSearchOptions & { minFileSize?: number };

export interface GetStatesOptions {
  country?: string;
}

export interface GetCitiesOptions extends GetStatesOptions {
  state?: string;
}

export interface GetCameraModelsOptions {
  make?: string;
  lensModel?: string;
}

export interface GetCameraMakesOptions {
  model?: string;
  lensModel?: string;
}

export interface GetCameraLensModelsOptions {
  make?: string;
  model?: string;
}

const makeSearchService = (db: Kysely<DB>) => {
  const getExifField = (field: 'city' | 'state' | 'country' | 'make' | 'model' | 'lensModel', userIds: string[]) =>
    db
      .selectFrom('asset_exif')
      .select(field)
      .distinctOn(field)
      .innerJoin('asset', 'asset.id', 'asset_exif.assetId')
      .where('ownerId', '=', anyUuid(userIds))
      .where('visibility', '=', AssetVisibility.Timeline)
      .where('deletedAt', 'is', null)
      .where(field, 'is not', null)
      .where(field, '!=', '');

  return {
    searchMetadata: Effect.fn('Search.searchMetadata')(function* (
      pagination: SearchPaginationOptions,
      options: AssetSearchOptions,
    ) {
      const orderDirection = (options.orderDirection?.toLowerCase() || 'desc') as OrderByDirection;
      const items = yield* query(db, (db) =>
        searchAssetBuilderLegacy(db, options)
          .select(columns.searchAsset)
          .orderBy('asset.fileCreatedAt', orderDirection)
          .orderBy('asset.id', orderDirection)
          .limit(pagination.size + 1)
          .offset((pagination.page - 1) * pagination.size)
          .execute(),
      );

      return paginationHelper(items, pagination.size);
    }),

    searchStatistics: Effect.fn('Search.searchStatistics')(function* (options: AssetSearchOptions) {
      return yield* query(db, (db) =>
        searchAssetBuilderLegacy(db, options)
          .select((qb) => qb.fn.countAll<number>().as('total'))
          .executeTakeFirstOrThrow(),
      );
    }),

    searchRandom: Effect.fn('Search.searchRandom')(function* (size: number, options: AssetSearchOptions) {
      return yield* query(db, (db) =>
        searchAssetBuilderLegacy(db, options)
          .select(columns.searchAsset)
          .orderBy(sql`random()`)
          .limit(size)
          .execute(),
      );
    }),

    searchLargeAssets: Effect.fn('Search.searchLargeAssets')(function* (
      size: number,
      options: LargeAssetSearchOptions,
    ) {
      const orderDirection = (options.orderDirection?.toLowerCase() || 'desc') as OrderByDirection;
      return yield* query(db, (db) =>
        searchAssetBuilderLegacy(db, options)
          .select(columns.searchAsset)
          .$call(withExifInner)
          .$if(options.visibility !== AssetVisibility.Hidden, (qb) =>
            qb.where('asset.visibility', '!=', sql.lit(AssetVisibility.Hidden)),
          )
          .where('asset_exif.fileSizeInByte', '>', options.minFileSize || 0)
          .orderBy('asset_exif.fileSizeInByte', orderDirection)
          .limit(size)
          .execute(),
      );
    }),

    searchPlaces: Effect.fn('Search.searchPlaces')(function* (placeName: string) {
      return yield* query(db, (db) =>
        db
          .selectFrom('geodata_places')
          .selectAll()
          .where(
            () =>
              // kysely doesn't support trigram %>> or <->>> operators
              sql`
            f_unaccent(name) %>> f_unaccent(${placeName}) or
            f_unaccent("admin2Name") %>> f_unaccent(${placeName}) or
            f_unaccent("admin1Name") %>> f_unaccent(${placeName}) or
            f_unaccent("alternateNames") %>> f_unaccent(${placeName})
          `,
          )
          .orderBy(
            sql`
          coalesce(f_unaccent(name) <->>> f_unaccent(${placeName}), 0.1) +
          coalesce(f_unaccent("admin2Name") <->>> f_unaccent(${placeName}), 0.1) +
          coalesce(f_unaccent("admin1Name") <->>> f_unaccent(${placeName}), 0.1) +
          coalesce(f_unaccent("alternateNames") <->>> f_unaccent(${placeName}), 0.1)
        `,
          )
          .limit(20)
          .execute(),
      );
    }),

    getAssetsByCity: Effect.fn('Search.getAssetsByCity')(function* (userIds: string[]) {
      return yield* query(db, (db) =>
        db
          .withRecursive('cte', (qb) => {
            const base = qb
              .selectFrom('asset_exif')
              .select(['city', 'assetId'])
              .innerJoin('asset', 'asset.id', 'asset_exif.assetId')
              .where('asset.ownerId', '=', anyUuid(userIds))
              .where('asset.visibility', '=', AssetVisibility.Timeline)
              .where('asset.type', '=', AssetType.Image)
              .where('asset.deletedAt', 'is', null)
              .orderBy('city')
              .limit(1);

            const recursive = qb
              .selectFrom('cte')
              .select(['l.city', 'l.assetId'])
              .innerJoinLateral(
                (qb) =>
                  qb
                    .selectFrom('asset_exif')
                    .select(['city', 'assetId'])
                    .innerJoin('asset', 'asset.id', 'asset_exif.assetId')
                    .where('asset.ownerId', '=', anyUuid(userIds))
                    .where('asset.visibility', '=', AssetVisibility.Timeline)
                    .where('asset.type', '=', AssetType.Image)
                    .where('asset.deletedAt', 'is', null)
                    .whereRef('asset_exif.city', '>', 'cte.city')
                    .orderBy('city')
                    .limit(1)
                    .as('l'),
                (join) => join.onTrue(),
              );

            return sql<{ city: string; assetId: string }>`(${base} union all ${recursive})`;
          })
          .selectFrom('asset')
          .innerJoin('asset_exif', 'asset.id', 'asset_exif.assetId')
          .innerJoin('cte', 'asset.id', 'cte.assetId')
          .select(columns.searchAsset)
          .select((eb) =>
            eb
              .fn('to_jsonb', [eb.table('asset_exif')])
              .$castTo<ShallowDehydrateObject<Selectable<AssetExifTable>>>()
              .as('exifInfo'),
          )
          .orderBy('asset_exif.city')
          .execute(),
      );
    }),

    getCountries: Effect.fn('Search.getCountries')(function* (userIds: string[]) {
      const res = yield* query(db, () => getExifField('country', userIds).execute());
      return res.map((row) => row.country!) as string[];
    }),

    getStates: Effect.fn('Search.getStates')(function* (userIds: string[], { country }: GetStatesOptions) {
      const res = yield* query(db, () =>
        getExifField('state', userIds)
          .$if(!!country, (qb) => qb.where('country', '=', country!))
          .execute(),
      );

      return res.map((row) => row.state!) as string[];
    }),

    getCities: Effect.fn('Search.getCities')(function* (userIds: string[], { country, state }: GetCitiesOptions) {
      const res = yield* query(db, () =>
        getExifField('city', userIds)
          .$if(!!country, (qb) => qb.where('country', '=', country!))
          .$if(!!state, (qb) => qb.where('state', '=', state!))
          .execute(),
      );

      return res.map((row) => row.city!) as string[];
    }),

    getCameraMakes: Effect.fn('Search.getCameraMakes')(function* (
      userIds: string[],
      { model, lensModel }: GetCameraMakesOptions,
    ) {
      const res = yield* query(db, () =>
        getExifField('make', userIds)
          .$if(!!model, (qb) => qb.where('model', '=', model!))
          .$if(!!lensModel, (qb) => qb.where('lensModel', '=', lensModel!))
          .execute(),
      );

      return res.map((row) => row.make!) as string[];
    }),

    getCameraModels: Effect.fn('Search.getCameraModels')(function* (
      userIds: string[],
      { make, lensModel }: GetCameraModelsOptions,
    ) {
      const res = yield* query(db, () =>
        getExifField('model', userIds)
          .$if(!!make, (qb) => qb.where('make', '=', make!))
          .$if(!!lensModel, (qb) => qb.where('lensModel', '=', lensModel!))
          .execute(),
      );

      return res.map((row) => row.model!) as string[];
    }),

    getCameraLensModels: Effect.fn('Search.getCameraLensModels')(function* (
      userIds: string[],
      { make, model }: GetCameraLensModelsOptions,
    ) {
      const res = yield* query(db, () =>
        getExifField('lensModel', userIds)
          .$if(!!make, (qb) => qb.where('make', '=', make!))
          .$if(!!model, (qb) => qb.where('model', '=', model!))
          .execute(),
      );

      return res.map((row) => row.lensModel!) as string[];
    }),

    searchMetadataV3: Effect.fn('Search.searchMetadataV3')(function* (
      pagination: PaginationOptions,
      options: AssetSearchBuilderV3Options,
      scope: AssetSearchScope,
    ) {
      const items = yield* query(db, (db) =>
        withSearchOrder(searchAssetBuilder(db, options, scope), options.order)
          .select(columns.searchAsset)
          .limit(pagination.take + 1)
          .offset(pagination.skip ?? 0)
          .execute(),
      );
      return paginationHelper(items, pagination.take);
    }),

    searchRandomV3: Effect.fn('Search.searchRandomV3')(function* (
      size: number,
      options: Omit<AssetSearchBuilderV3Options, 'order'>,
      scope: AssetSearchScope,
    ) {
      return yield* query(db, (db): Promise<MapAsset[]> =>
        searchAssetBuilder(db, options, scope)
          .select(columns.searchAsset)
          .orderBy(sql`random()`)
          .limit(size)
          .execute(),
      );
    }),

    searchStatisticsV3: Effect.fn('Search.searchStatisticsV3')(function* (
      options: AssetSearchBuilderV3Options,
      scope: AssetSearchScope,
    ) {
      return yield* query(db, (db) =>
        searchAssetBuilder(db, options, scope)
          .select((qb) => qb.fn.countAll<number>().as('total'))
          .executeTakeFirstOrThrow(),
      );
    }),
  };
};

export class SearchRepo extends Context.Service<SearchRepo, ReturnType<typeof makeSearchService>>()(
  'immich/repositories/SearchRepo',
) {
  static readonly make = (db: Kysely<DB>) => SearchRepo.of(makeSearchService(db));

  static readonly layer = Layer.effect(
    SearchRepo,
    Effect.gen(function* () {
      return SearchRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SearchRepo`. */
@Injectable()
export class SearchRepository {
  private readonly effect: Context.Service.Shape<typeof SearchRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = SearchRepo.make(db);
    registerEffect(this, this.effect);
  }

  // TODO(v4): remove with the deprecated flat-field search API
  @GenerateSql({
    params: [
      { page: 1, size: 100 },
      {
        takenAfter: DummyValue.DATE,
        lensModel: DummyValue.STRING,
        withStacked: true,
        isFavorite: true,
        userIds: [DummyValue.UUID],
      },
    ],
  })
  searchMetadata(pagination: SearchPaginationOptions, options: AssetSearchOptions) {
    return runPromise(this.effect.searchMetadata(pagination, options));
  }

  // TODO(v4): remove with the deprecated flat-field search API
  @GenerateSql({
    params: [
      {
        takenAfter: DummyValue.DATE,
        lensModel: DummyValue.STRING,
        isFavorite: true,
        userIds: [DummyValue.UUID],
      },
    ],
  })
  searchStatistics(options: AssetSearchOptions) {
    return runPromise(this.effect.searchStatistics(options));
  }

  // TODO(v4): remove with the deprecated flat-field search API
  @GenerateSql({
    params: [
      100,
      {
        takenAfter: DummyValue.DATE,
        lensModel: DummyValue.STRING,
        withStacked: true,
        isFavorite: true,
        userIds: [DummyValue.UUID],
      },
    ],
  })
  searchRandom(size: number, options: AssetSearchOptions) {
    return runPromise(this.effect.searchRandom(size, options));
  }

  // TODO(v4): remove with the deprecated flat-field search API
  @GenerateSql({
    params: [
      100,
      {
        takenAfter: DummyValue.DATE,
        lensModel: DummyValue.STRING,
        withStacked: true,
        isFavorite: true,
        userIds: [DummyValue.UUID],
      },
    ],
  })
  searchLargeAssets(size: number, options: LargeAssetSearchOptions) {
    return runPromise(this.effect.searchLargeAssets(size, options));
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  searchPlaces(placeName: string) {
    return runPromise(this.effect.searchPlaces(placeName));
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  getAssetsByCity(userIds: string[]) {
    return runPromise(this.effect.getAssetsByCity(userIds));
  }

  getCountries(userIds: string[]): Promise<string[]> {
    return runPromise(this.effect.getCountries(userIds));
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING] })
  getStates(userIds: string[], options: GetStatesOptions): Promise<string[]> {
    return runPromise(this.effect.getStates(userIds, options));
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING, DummyValue.STRING] })
  getCities(userIds: string[], options: GetCitiesOptions): Promise<string[]> {
    return runPromise(this.effect.getCities(userIds, options));
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING, DummyValue.STRING] })
  getCameraMakes(userIds: string[], options: GetCameraMakesOptions): Promise<string[]> {
    return runPromise(this.effect.getCameraMakes(userIds, options));
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING, DummyValue.STRING] })
  getCameraModels(userIds: string[], options: GetCameraModelsOptions): Promise<string[]> {
    return runPromise(this.effect.getCameraModels(userIds, options));
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING] })
  getCameraLensModels(userIds: string[], options: GetCameraLensModelsOptions): Promise<string[]> {
    return runPromise(this.effect.getCameraLensModels(userIds, options));
  }

  // TODO(v4): drop the V3 suffix once the legacy methods are removed
  @GenerateSql(...searchMetadataV3Examples)
  searchMetadataV3(pagination: PaginationOptions, options: AssetSearchBuilderV3Options, scope: AssetSearchScope) {
    return runPromise(this.effect.searchMetadataV3(pagination, options, scope));
  }

  // TODO(v4): drop the V3 suffix once the legacy methods are removed
  @GenerateSql(...searchRandomV3Examples)
  searchRandomV3(
    size: number,
    options: Omit<AssetSearchBuilderV3Options, 'order'>,
    scope: AssetSearchScope,
  ): Promise<MapAsset[]> {
    return runPromise(this.effect.searchRandomV3(size, options, scope));
  }

  // TODO(v4): drop the V3 suffix once the legacy methods are removed
  @GenerateSql(...searchStatisticsV3Examples)
  searchStatisticsV3(options: AssetSearchBuilderV3Options, scope: AssetSearchScope) {
    return runPromise(this.effect.searchStatisticsV3(options, scope));
  }
}
