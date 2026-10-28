import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { Expression, Insertable, Kysely, NotNull, SqlBool, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { createReadStream, existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import readLine from 'node:readline';
import { citiesFile, reverseGeocodeMaxDistance } from 'src/constants.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { effectOf, registerEffect } from 'src/effect/bridge.js';
import { Db, query } from 'src/effect/db.js';
import { ConfigError, DatabaseError, FileSystemError, LibraryError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { AssetVisibility, SystemMetadataKey } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { SystemMetadataRepo, SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { DB } from 'src/schema/index.js';
import { GeodataPlacesTable } from 'src/schema/tables/geodata-places.table.js';
import { NaturalEarthCountriesTable } from 'src/schema/tables/natural-earth-countries.table.js';

export interface MapMarkerSearchOptions {
  isArchived?: boolean;
  isFavorite?: boolean;
  fileCreatedBefore?: Date;
  fileCreatedAfter?: Date;
}

export interface GeoPoint {
  latitude: number;
  longitude: number;
}

export interface ReverseGeocodeResult {
  country: string | null;
  state: string | null;
  city: string | null;
}

interface MapDB extends DB {
  geodata_places_tmp: GeodataPlacesTable;
  naturalearth_countries_tmp: NaturalEarthCountriesTable;
}

interface CountryNames {
  byAlpha2: Map<string, string>;
  byAlpha3: Map<string, string>;
}

/** Dependencies of the geodata import and reverse geocoding that are not the database. */
export interface MapDeps {
  configRepository: ConfigRepository;
  metadata: Context.Service.Shape<typeof SystemMetadataRepo>;
  logger: LoggingRepository;
}

const readCountryNames = async (filePath: string, logger: LoggingRepository): Promise<CountryNames> => {
  const byAlpha2 = new Map<string, string>();
  const byAlpha3 = new Map<string, string>();

  if (!existsSync(filePath)) {
    throw new Error(`Geodata file ${filePath} not found`);
  }

  const lineReader = readLine.createInterface({ input: createReadStream(filePath) });
  for await (const line of lineReader) {
    if (line.startsWith('#')) {
      continue;
    }
    // Columns: ISO alpha-2, ISO alpha-3, ISO numeric, fips, Country, ...
    const fields = line.split('\t', 5);
    const alpha2 = fields[0];
    const alpha3 = fields[1];
    const name = fields[4]?.trim();
    if (!name) {
      continue;
    }
    if (alpha2) {
      byAlpha2.set(alpha2, name);
    }
    if (alpha3) {
      byAlpha3.set(alpha3, name);
    }
  }

  logger.log(`Loaded ${byAlpha2.size} country names from ${filePath}`);
  return { byAlpha2, byAlpha3 };
};

const loadAdmin = async (filePath: string, logger: LoggingRepository) => {
  if (!existsSync(filePath)) {
    logger.error(`Geodata file ${filePath} not found`);
    throw new Error(`Geodata file ${filePath} not found`);
  }

  const input = createReadStream(filePath, { highWaterMark: 512 * 1024 * 1024 });
  const lineReader = readLine.createInterface({ input });

  const adminMap = new Map<string, string>();
  for await (const line of lineReader) {
    const lineSplit = line.split('\t');
    adminMap.set(lineSplit[0], lineSplit[1]);
  }

  return adminMap;
};

const makeMapService = (db: Kysely<MapDB>, { configRepository, metadata, logger }: MapDeps) => {
  // init() skips the geodata import when it is up to date, so the names are loaded (once) on demand
  let countryNames: Promise<CountryNames> | undefined;

  const getEnv = Effect.try({ try: () => configRepository.getEnv(), catch: (cause) => new ConfigError({ cause }) });

  const loadCountryNames = Effect.fn('Map.loadCountryNames')(function* () {
    return yield* Effect.tryPromise({
      try: async () => {
        countryNames ??= readCountryNames(configRepository.getEnv().resourcePaths.geodata.countryInfo, logger);
        try {
          return await countryNames;
        } catch (error) {
          countryNames = undefined;
          throw error;
        }
      },
      catch: (cause) => new FileSystemError({ cause }),
    });
  });

  const mapMarkersQuery = () =>
    db
      .selectFrom('asset')
      .innerJoin('asset_exif', (builder) =>
        builder
          .onRef('asset.id', '=', 'asset_exif.assetId')
          .on('asset_exif.latitude', 'is not', null)
          .on('asset_exif.longitude', 'is not', null),
      )
      .where('asset.deletedAt', 'is', null)
      .orderBy('fileCreatedAt', 'desc')
      .select([
        'id',
        'asset_exif.latitude as lat',
        'asset_exif.longitude as lon',
        'asset_exif.city',
        'asset_exif.state',
        'asset_exif.country',
      ])
      .$narrowType<{ lat: NotNull; lon: NotNull }>();

  /** Nearest populated place within `reverseGeocodeMaxDistance` of the point, if any. */
  const findNearestPlace = Effect.fn('Map.findNearestPlace')(function* (point: GeoPoint) {
    return yield* query(db, (db) =>
      db
        .selectFrom('geodata_places')
        .selectAll()
        .where(
          sql`earth_box(ll_to_earth_public(${point.latitude}, ${point.longitude}), ${reverseGeocodeMaxDistance})`,
          '@>',
          sql`ll_to_earth_public(latitude, longitude)`,
        )
        .orderBy(
          sql`(earth_distance(ll_to_earth_public(${point.latitude}, ${point.longitude}), ll_to_earth_public(latitude, longitude)))`,
        )
        .limit(1)
        .executeTakeFirst(),
    );
  });

  /** Natural Earth country polygon containing the point, if any. */
  const findCountryAtPoint = Effect.fn('Map.findCountryAtPoint')(function* (point: GeoPoint) {
    return yield* query(db, (db) =>
      db
        .selectFrom('naturalearth_countries')
        .selectAll()
        .where('coordinates', '@>', sql<string>`point(${point.longitude}, ${point.latitude})`)
        .limit(1)
        .executeTakeFirst(),
    );
  });

  /** City (falling back to the country polygon) for a point; country names come from the geonames country info file. */
  const reverseGeocode = Effect.fn('Map.reverseGeocode')(function* (point: GeoPoint) {
    logger.debug(`Request: ${point.latitude},${point.longitude}`);

    const names = yield* loadCountryNames();

    const response = yield* findNearestPlace(point);

    if (response) {
      logger.verboseFn(() => `Raw: ${JSON.stringify(response, null, 2)}`);

      const { countryCode, name: city, admin1Name } = response;
      const country = names.byAlpha2.get(countryCode) ?? null;
      const state = admin1Name;

      return { country, state, city } satisfies ReverseGeocodeResult;
    }

    logger.log(
      `Empty response from database for city reverse geocoding lat: ${point.latitude}, lon: ${point.longitude}. Likely cause: no nearby large populated place (500+ within ${reverseGeocodeMaxDistance / 1000}km). Falling back to country boundaries.`,
    );

    const ne_response = yield* findCountryAtPoint(point);

    if (!ne_response) {
      logger.log(
        `Empty response from database for natural earth country reverse geocoding lat: ${point.latitude}, lon: ${point.longitude}`,
      );

      return { country: null, state: null, city: null } satisfies ReverseGeocodeResult;
    }

    logger.verboseFn(() => `Raw: ${JSON.stringify(ne_response, ['id', 'admin', 'admin_a3', 'type'], 2)}`);

    const { admin_a3 } = ne_response;
    const country = names.byAlpha3.get(admin_a3) ?? null;
    const state = null;
    const city = null;

    return { country, state, city } satisfies ReverseGeocodeResult;
  });

  const importNaturalEarthCountries = Effect.fn('Map.importNaturalEarthCountries')(function* () {
    const { resourcePaths } = yield* getEnv;
    const geoJSONData = yield* Effect.tryPromise({
      try: async () => JSON.parse(await readFile(resourcePaths.geodata.naturalEarthCountriesPath, 'utf8')),
      catch: (cause) => new FileSystemError({ cause }),
    });
    if (geoJSONData.type !== 'FeatureCollection' || !Array.isArray(geoJSONData.features)) {
      logger.fatal('Invalid GeoJSON FeatureCollection');
      return;
    }

    const entities = yield* Effect.try({
      try: () => {
        const entities: Insertable<NaturalEarthCountriesTable>[] = [];
        for (const feature of geoJSONData.features) {
          for (const entry of feature.geometry.coordinates) {
            const coordinates: number[][][] = feature.geometry.type === 'MultiPolygon' ? entry[0] : entry;
            const featureRecord: Insertable<NaturalEarthCountriesTable> = {
              admin: feature.properties.ADMIN,
              admin_a3: feature.properties.ADM0_A3,
              type: feature.properties.TYPE,
              coordinates: `(${coordinates.map((point) => `(${point[0]},${point[1]})`).join(', ')})`,
            };
            entities.push(featureRecord);
            if (feature.geometry.type === 'Polygon') {
              break;
            }
          }
        }
        return entities;
      },
      catch: (cause) => new LibraryError({ cause }),
    });

    yield* query(db, (db) =>
      db.transaction().execute(async (manager) => {
        await sql`CREATE TABLE naturalearth_countries_tmp
                (
                  LIKE naturalearth_countries INCLUDING ALL EXCLUDING INDEXES
                )`.execute(manager);
        await manager.schema.dropTable('naturalearth_countries').execute();
        await manager.schema.alterTable('naturalearth_countries_tmp').renameTo('naturalearth_countries').execute();
      }),
    );

    yield* query(db, (db) => db.insertInto('naturalearth_countries').values(entities).execute());
    yield* query(db, (db) =>
      sql`ALTER TABLE naturalearth_countries ADD PRIMARY KEY (id) WITH (FILLFACTOR = 100)`.execute(db),
    );
  });

  const loadAdminFile = (filePath: string) =>
    Effect.tryPromise({ try: () => loadAdmin(filePath, logger), catch: (cause) => new FileSystemError({ cause }) });

  /** Streams the cities500 file line by line (unbuffered) and inserts it in batches of 5000 over up to 9 connections. */
  const loadCities500 = Effect.fn('Map.loadCities500')(function* (
    admin1Map: Map<string, string>,
    admin2Map: Map<string, string>,
  ) {
    const { resourcePaths } = yield* getEnv;
    const cities500 = resourcePaths.geodata.cities500;
    if (!existsSync(cities500)) {
      return yield* Effect.fail(new Error(`Geodata file ${cities500} not found`));
    }

    logger.log(`Starting geodata import`);
    const startTime = performance.now();

    const count = yield* Effect.tryPromise({
      try: async () => {
        const input = createReadStream(cities500, { highWaterMark: 512 * 1024 * 1024 });
        let bufferGeodata = [];
        const lineReader = readLine.createInterface({ input });
        let count = 0;

        let futures = [];
        for await (const line of lineReader) {
          const lineSplit = line.split('\t');
          if ((lineSplit[7] === 'PPLX' && lineSplit[8] !== 'AU') || lineSplit[7] === 'PPLH') {
            continue;
          }

          const geoData = {
            id: Number.parseInt(lineSplit[0]),
            name: lineSplit[1],
            alternateNames: lineSplit[3],
            latitude: Number(lineSplit[4]),
            longitude: Number(lineSplit[5]),
            countryCode: lineSplit[8],
            admin1Code: lineSplit[10],
            admin2Code: lineSplit[11],
            modificationDate: lineSplit[18],
            admin1Name: admin1Map.get(`${lineSplit[8]}.${lineSplit[10]}`) ?? null,
            admin2Name: admin2Map.get(`${lineSplit[8]}.${lineSplit[10]}.${lineSplit[11]}`) ?? null,
          };
          bufferGeodata.push(geoData);
          // eslint-disable-next-line unicorn/prefer-continue
          if (bufferGeodata.length >= 5000) {
            const curLength = bufferGeodata.length;
            futures.push(
              db
                .insertInto('geodata_places')
                .values(bufferGeodata)
                .execute()

                .then(() => {
                  count += curLength;
                  if (count % 10_000 === 0) {
                    logger.log(`${count} geodata records imported`);
                  }
                }),
            );
            bufferGeodata = [];
            // leave spare connection for other queries
            if (futures.length >= 9) {
              await Promise.all(futures);
              futures = [];
            }
          }
        }

        if (bufferGeodata.length > 0) {
          await db.insertInto('geodata_places').values(bufferGeodata).execute();
          count += bufferGeodata.length;
        }

        await Promise.all(futures);
        return count;
      },
      catch: (cause) => new DatabaseError({ cause }),
    });

    const duration = performance.now() - startTime;
    const seconds = duration / 1000;
    const recordsPerSecond = Math.round(count / seconds);

    logger.log(
      `Successfully imported ${count} geodata records in ${seconds.toFixed(2)}s (${recordsPerSecond} records/second)`,
    );
  });

  const createGeodataIndices = Effect.fn('Map.createGeodataIndices')(function* () {
    return yield* Effect.all(
      [
        query(db, (db) => sql`ALTER TABLE geodata_places ADD PRIMARY KEY (id) WITH (FILLFACTOR = 100)`.execute(db)),
        query(db, (db) =>
          db.schema
            .createIndex(`idx_geodata_places_alternate_names`)
            .on('geodata_places')
            .using('gin (f_unaccent("alternateNames") gin_trgm_ops)')
            .execute(),
        ),
        query(db, (db) =>
          db.schema
            .createIndex(`idx_geodata_places_name`)
            .on('geodata_places')
            .using('gin (f_unaccent(name) gin_trgm_ops)')
            .execute(),
        ),
        query(db, (db) =>
          db.schema
            .createIndex(`idx_geodata_places_admin1_name`)
            .on('geodata_places')
            .using('gin (f_unaccent("admin1Name") gin_trgm_ops)')
            .execute(),
        ),
        query(db, (db) =>
          db.schema
            .createIndex(`idx_geodata_places_admin2_name`)
            .on('geodata_places')
            .using('gin (f_unaccent("admin2Name") gin_trgm_ops)')
            .execute(),
        ),
      ],
      { concurrency: 'unbounded' },
    );
  });

  const importGeodata = Effect.fn('Map.importGeodata')(function* () {
    const { resourcePaths } = yield* getEnv;
    const [admin1, admin2] = yield* Effect.all(
      [loadAdminFile(resourcePaths.geodata.admin1), loadAdminFile(resourcePaths.geodata.admin2)],
      { concurrency: 'unbounded' },
    );

    yield* query(db, (db) => db.schema.dropTable('geodata_places_tmp').ifExists().execute());
    yield* query(db, (db) =>
      db.transaction().execute(async (manager) => {
        await sql`CREATE TABLE geodata_places_tmp
                (
                  LIKE geodata_places INCLUDING ALL EXCLUDING INDEXES
                )`.execute(manager);
        await manager.schema.dropTable('geodata_places').execute();
        await manager.schema.alterTable('geodata_places_tmp').renameTo('geodata_places').execute();
      }),
    );
    yield* query(db, (db) =>
      db.schema
        .createIndex('IDX_geodata_gist_earthcoord')
        .on('geodata_places')
        .using('gist')
        .expression(sql`ll_to_earth_public(latitude, longitude)`)
        .execute(),
    );
    yield* loadCities500(admin1, admin2);
    yield* createGeodataIndices();
  });

  /** Imports the bundled geodata and Natural Earth countries unless the stored import is already up to date. */
  const init = Effect.fn('Map.init')(function* () {
    logger.log('Initializing metadata repository');
    const { resourcePaths } = yield* getEnv;
    const geodataDate = yield* Effect.tryPromise({
      try: () => readFile(resourcePaths.geodata.dateFile, 'utf8'),
      catch: (cause) => new FileSystemError({ cause }),
    });

    // TODO move to service init
    const geocodingMetadata = yield* metadata.get(SystemMetadataKey.ReverseGeocodingState);
    if (geocodingMetadata?.lastUpdate === geodataDate) {
      return;
    }

    yield* Effect.all([importGeodata(), importNaturalEarthCountries()], { concurrency: 'unbounded' });

    yield* metadata.set(SystemMetadataKey.ReverseGeocodingState, {
      lastUpdate: geodataDate,
      lastImportFileName: citiesFile,
    });

    logger.log('Geodata import completed');
  });

  return {
    getAlbumMapMarkers: Effect.fn('Map.getAlbumMapMarkers')(function* (albumId: string) {
      return yield* query(db, () =>
        mapMarkersQuery()
          .innerJoin('album_asset', 'asset.id', 'album_asset.assetId')
          .where('album_asset.albumId', '=', albumId)
          .execute(),
      );
    }),

    getMapMarkers: Effect.fn('Map.getMapMarkers')(function* (
      authUserId: string,
      ownerIds: string[],
      albumIds: string[],
      { isArchived, isFavorite, fileCreatedAfter, fileCreatedBefore }: MapMarkerSearchOptions = {},
    ) {
      return yield* query(db, () =>
        mapMarkersQuery()
          .$if(isArchived === true, (qb) =>
            qb.where((eb) =>
              eb.or([
                eb('asset.visibility', '=', AssetVisibility.Timeline),
                eb.and([eb('asset.ownerId', '=', authUserId), eb('asset.visibility', '=', AssetVisibility.Archive)]),
              ]),
            ),
          )
          .$if(isArchived === false || isArchived === undefined, (qb) =>
            qb.where('asset.visibility', '=', AssetVisibility.Timeline),
          )
          .$if(isFavorite !== undefined, (q) => q.where('isFavorite', '=', isFavorite!))
          .$if(fileCreatedAfter !== undefined, (q) => q.where('fileCreatedAt', '>=', fileCreatedAfter!))
          .$if(fileCreatedBefore !== undefined, (q) => q.where('fileCreatedAt', '<=', fileCreatedBefore!))
          .where((eb) => {
            const expression: Expression<SqlBool>[] = [];

            if (ownerIds.length > 0) {
              expression.push(eb('ownerId', 'in', ownerIds));
            }

            if (albumIds.length > 0) {
              expression.push(
                eb.exists((eb) =>
                  eb
                    .selectFrom('album_asset')
                    .whereRef('asset.id', '=', 'album_asset.assetId')
                    .where('album_asset.albumId', 'in', albumIds),
                ),
              );
            }

            return eb.or(expression);
          })
          .execute(),
      );
    }),

    findNearestPlace,
    findCountryAtPoint,

    reverseGeocode,

    init,
  };
};

export class MapRepo extends Context.Service<MapRepo, ReturnType<typeof makeMapService>>()(
  'immich/repositories/MapRepo',
) {
  static readonly make = (db: Kysely<MapDB>, deps: MapDeps) => MapRepo.of(makeMapService(db, deps));

  /** Unlike the database-only services the config, logger and system metadata are supplied by the caller. */
  static readonly layer = (deps: MapDeps) =>
    Layer.effect(
      MapRepo,
      Effect.gen(function* () {
        return MapRepo.make((yield* Db) as Kysely<MapDB>, deps);
      }),
    );
}

/** Promise-facing adapter for the Nest services. Delegates to `MapRepo`. */
@Injectable()
export class MapRepository {
  private readonly effect: Context.Service.Shape<typeof MapRepo>;

  constructor(
    private configRepository: ConfigRepository,
    private metadataRepository: SystemMetadataRepository,
    private logger: LoggingRepository,
    @InjectKysely() private db: Kysely<MapDB>,
  ) {
    this.logger.setContext(MapRepository.name);
    this.effect = MapRepo.make(this.db, {
      configRepository: this.configRepository,
      metadata: effectOf(SystemMetadataRepo, this.metadataRepository),
      logger: this.logger,
    });
    registerEffect(this, this.effect);
  }

  async init(): Promise<void> {
    return runPromise(this.effect.init());
  }

  @GenerateSql({ params: [DummyValue.UUID] })
  getAlbumMapMarkers(albumId: string) {
    return runPromise(this.effect.getAlbumMapMarkers(albumId));
  }

  @GenerateSql({ params: [DummyValue.UUID, [DummyValue.UUID], [DummyValue.UUID]] })
  getMapMarkers(authUserId: string, ownerIds: string[], albumIds: string[], options: MapMarkerSearchOptions = {}) {
    return runPromise(this.effect.getMapMarkers(authUserId, ownerIds, albumIds, options));
  }

  async reverseGeocode(point: GeoPoint): Promise<ReverseGeocodeResult> {
    return runPromise(this.effect.reverseGeocode(point));
  }
}
