import { Injectable } from '@nestjs/common';
import { type Kysely, type OrderByDirection, type Selectable, type ShallowDehydrateObject, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { MapAsset } from 'src/dtos/asset-response.dto.js';
import { SearchFilter, SearchOrder } from 'src/dtos/search.dto.js';
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

@Injectable()
export class SearchRepository {
  constructor(@InjectKysely() private db: Kysely<DB>) {}

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
  async searchMetadata(pagination: SearchPaginationOptions, options: AssetSearchOptions) {
    const orderDirection = (options.orderDirection?.toLowerCase() || 'desc') as OrderByDirection;
    const items = await searchAssetBuilderLegacy(this.db, options)
      .select(columns.searchAsset)
      .orderBy('asset.fileCreatedAt', orderDirection)
      .orderBy('asset.id', orderDirection)
      .limit(pagination.size + 1)
      .offset((pagination.page - 1) * pagination.size)
      .execute();

    return paginationHelper(items, pagination.size);
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
    return searchAssetBuilderLegacy(this.db, options)
      .select((qb) => qb.fn.countAll<number>().as('total'))
      .executeTakeFirstOrThrow();
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
  async searchRandom(size: number, options: AssetSearchOptions) {
    return searchAssetBuilderLegacy(this.db, options)
      .select(columns.searchAsset)
      .orderBy(sql`random()`)
      .limit(size)
      .execute();
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
    const orderDirection = (options.orderDirection?.toLowerCase() || 'desc') as OrderByDirection;
    return searchAssetBuilderLegacy(this.db, options)
      .select(columns.searchAsset)
      .$call(withExifInner)
      .$if(options.visibility !== AssetVisibility.Hidden, (qb) =>
        qb.where('asset.visibility', '!=', sql.lit(AssetVisibility.Hidden)),
      )
      .where('asset_exif.fileSizeInByte', '>', options.minFileSize || 0)
      .orderBy('asset_exif.fileSizeInByte', orderDirection)
      .limit(size)
      .execute();
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  searchPlaces(placeName: string) {
    return this.db
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
      .execute();
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  getAssetsByCity(userIds: string[]) {
    return this.db
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
      .execute();
  }

  async getCountries(userIds: string[]): Promise<string[]> {
    const res = await this.getExifField('country', userIds).execute();
    return res.map((row) => row.country!);
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING] })
  async getStates(userIds: string[], { country }: GetStatesOptions): Promise<string[]> {
    const res = await this.getExifField('state', userIds)
      .$if(!!country, (qb) => qb.where('country', '=', country!))
      .execute();

    return res.map((row) => row.state!);
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING, DummyValue.STRING] })
  async getCities(userIds: string[], { country, state }: GetCitiesOptions): Promise<string[]> {
    const res = await this.getExifField('city', userIds)
      .$if(!!country, (qb) => qb.where('country', '=', country!))
      .$if(!!state, (qb) => qb.where('state', '=', state!))
      .execute();

    return res.map((row) => row.city!);
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING, DummyValue.STRING] })
  async getCameraMakes(userIds: string[], { model, lensModel }: GetCameraMakesOptions): Promise<string[]> {
    const res = await this.getExifField('make', userIds)
      .$if(!!model, (qb) => qb.where('model', '=', model!))
      .$if(!!lensModel, (qb) => qb.where('lensModel', '=', lensModel!))
      .execute();

    return res.map((row) => row.make!);
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING, DummyValue.STRING] })
  async getCameraModels(userIds: string[], { make, lensModel }: GetCameraModelsOptions): Promise<string[]> {
    const res = await this.getExifField('model', userIds)
      .$if(!!make, (qb) => qb.where('make', '=', make!))
      .$if(!!lensModel, (qb) => qb.where('lensModel', '=', lensModel!))
      .execute();

    return res.map((row) => row.model!);
  }

  @GenerateSql({ params: [[DummyValue.UUID], DummyValue.STRING] })
  async getCameraLensModels(userIds: string[], { make, model }: GetCameraLensModelsOptions): Promise<string[]> {
    const res = await this.getExifField('lensModel', userIds)
      .$if(!!make, (qb) => qb.where('make', '=', make!))
      .$if(!!model, (qb) => qb.where('model', '=', model!))
      .execute();

    return res.map((row) => row.lensModel!);
  }

  // TODO(v4): drop the V3 suffix once the legacy methods are removed
  @GenerateSql(...searchMetadataV3Examples)
  async searchMetadataV3(pagination: PaginationOptions, options: AssetSearchBuilderV3Options, scope: AssetSearchScope) {
    const items = await withSearchOrder(searchAssetBuilder(this.db, options, scope), options.order)
      .select(columns.searchAsset)
      .limit(pagination.take + 1)
      .offset(pagination.skip ?? 0)
      .execute();
    return paginationHelper(items, pagination.take);
  }

  // TODO(v4): drop the V3 suffix once the legacy methods are removed
  @GenerateSql(...searchRandomV3Examples)
  searchRandomV3(
    size: number,
    options: Omit<AssetSearchBuilderV3Options, 'order'>,
    scope: AssetSearchScope,
  ): Promise<MapAsset[]> {
    return searchAssetBuilder(this.db, options, scope)
      .select(columns.searchAsset)
      .orderBy(sql`random()`)
      .limit(size)
      .execute();
  }

  // TODO(v4): drop the V3 suffix once the legacy methods are removed
  @GenerateSql(...searchStatisticsV3Examples)
  searchStatisticsV3(options: AssetSearchBuilderV3Options, scope: AssetSearchScope) {
    return searchAssetBuilder(this.db, options, scope)
      .select((qb) => qb.fn.countAll<number>().as('total'))
      .executeTakeFirstOrThrow();
  }

  private getExifField(field: 'city' | 'state' | 'country' | 'make' | 'model' | 'lensModel', userIds: string[]) {
    return this.db
      .selectFrom('asset_exif')
      .select(field)
      .distinctOn(field)
      .innerJoin('asset', 'asset.id', 'asset_exif.assetId')
      .where('ownerId', '=', anyUuid(userIds))
      .where('visibility', '=', AssetVisibility.Timeline)
      .where('deletedAt', 'is', null)
      .where(field, 'is not', null)
      .where(field, '!=', '');
  }
}
