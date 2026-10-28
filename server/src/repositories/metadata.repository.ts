import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { BinaryField, DefaultReadTaskOptions, ExifTool, ReadTaskOptions, Tags } from 'exiftool-vendored';
import geotz from 'geo-tz';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { mimeTypes } from 'src/utils/mime-types.js';

interface ExifDuration {
  Value: number;
  Scale?: number;
}

type StringOrNumber = string | number;

type TagsWithWrongTypes =
  | 'FocalLength'
  | 'Duration'
  | 'Description'
  | 'ImageDescription'
  | 'RegionInfo'
  | 'TagsList'
  | 'Keywords'
  | 'HierarchicalSubject'
  | 'ISO'
  | 'LensModel';

export interface ImmichTags extends Omit<Tags, TagsWithWrongTypes> {
  ContentIdentifier?: string;
  MotionPhoto?: number;
  MotionPhotoVersion?: number;
  MotionPhotoPresentationTimestampUs?: number;
  MediaGroupUUID?: string;
  ImagePixelDepth?: string;
  FocalLength?: number;
  Duration?: number | string | ExifDuration;
  EmbeddedVideoType?: string;
  EmbeddedVideoFile?: BinaryField;
  MotionPhotoVideo?: BinaryField;
  TagsList?: StringOrNumber[];
  HierarchicalSubject?: StringOrNumber[];
  Keywords?: StringOrNumber | StringOrNumber[];
  ISO?: number | number[];

  // Type is wrong, can also be number.
  Description?: StringOrNumber;
  ImageDescription?: StringOrNumber;

  // Apparently LensModel can also be a float: https://github.com/immich-app/immich/issues/30492
  LensModel?: StringOrNumber;

  // Extended properties for image regions, such as faces
  RegionInfo?: {
    AppliedToDimensions: {
      W: number;
      H: number;
      Unit: string;
    };
    RegionList: {
      Area: {
        // (X,Y) // center of the rectangle
        X: number | string;
        Y: number | string;
        W: number | string;
        H: number | string;
        Unit: string;
      };
      Rotation?: number;
      Type?: string;
      Name?: string;
    }[];
  };

  Device?: {
    Manufacturer?: string;
    ModelName?: string;
  };

  AndroidMake?: string;
  AndroidModel?: string;
  DeviceManufacturer?: string;
  DeviceModelName?: string;

  // Samsung specific tags
  Author?: string;
  SamsungModel?: string;
}

export class MetadataRepo extends Context.Service<
  MetadataRepo,
  {
    setMaxConcurrency(concurrency: number): Effect.Effect<void, LibraryError>;
    teardown(): Effect.Effect<void, LibraryError>;
    /** Never fails: a read error is logged and an empty tag set is returned. */
    readTags(path: string): Effect.Effect<ImmichTags>;
    extractBinaryTag(path: string, tagName: string): Effect.Effect<Buffer, LibraryError>;
    /** Never fails: a write error is logged and swallowed. */
    writeTags(path: string, tags: Partial<Tags>): Effect.Effect<void>;
  }
>()('immich/repositories/MetadataRepo') {
  static readonly make = (logger: LoggingRepository) => {
    const exiftool = new ExifTool({
      defaultVideosToUTC: true,
      backfillTimezones: true,
      inferTimezoneFromDatestamps: true,
      inferTimezoneFromTimeStamp: true,
      useMWG: true,
      numericTags: [...DefaultReadTaskOptions.numericTags, 'FocalLength', 'FileSize', 'Rotation'],
      /* eslint unicorn/no-array-callback-reference: off, unicorn/no-array-method-this-argument: off */
      // eslint-disable-next-line import-x/no-named-as-default-member
      geoTz: (lat, lon) => geotz.find(lat, lon)[0],
      geolocation: true,
      readArgs: [
        // Enable exiftool LFS to parse metadata for files larger than 2GB.
        '-api',
        'largefilesupport=1',
        '--ICC_Profile:DeviceManufacturer',
        '--ICC_Profile:DeviceModelName',
        // Ignore embedded thumbnail dimensions/orientation for the main asset.
        '--IFD1:Orientation',
        '--MWG:Orientation',
        '--IFD1:ImageWidth',
        '--IFD1:ImageHeight',
        '--Samsung:Rotation',
      ],
      writeArgs: ['-api', 'largefilesupport=1', '-overwrite_original'],
      taskTimeoutMillis: 2 * 60 * 1000,
    });

    return MetadataRepo.of({
      setMaxConcurrency: Effect.fn('Metadata.setMaxConcurrency')((concurrency: number) =>
        Effect.try({
          try: () => exiftool.batchCluster.setMaxProcs(concurrency),
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),

      teardown: Effect.fn('Metadata.teardown')(() =>
        Effect.tryPromise({
          try: async () => {
            await exiftool.end();
          },
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),

      readTags: Effect.fn('Metadata.readTags')((path: string) => {
        const options: ReadTaskOptions | undefined = mimeTypes.isVideo(path) ? { readArgs: ['-ee'] } : undefined;

        return Effect.tryPromise({
          try: () => exiftool.read(path, options),
          catch: (cause) => new LibraryError({ cause }),
        }).pipe(
          Effect.catch(({ cause: error }) =>
            Effect.sync(() => {
              logger.warn(`Error reading exif data (${path}): ${error}\n${(error as Error)?.stack}`);
              return {};
            }),
          ),
          Effect.map((tags) => tags as ImmichTags),
        );
      }),

      extractBinaryTag: Effect.fn('Metadata.extractBinaryTag')((path: string, tagName: string) =>
        Effect.tryPromise({
          try: () => exiftool.extractBinaryTagToBuffer(tagName, path),
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),

      writeTags: Effect.fn('Metadata.writeTags')((path: string, tags: Partial<Tags>) => {
        // If exiftool assigns a field with ^= instead of =, empty values will be written too.
        // Since exiftool-vendored doesn't support an option for this, we append the ^ to the name of the tag instead.
        // https://exiftool.org/exiftool_pod.html#:~:text=is%20used%20to%20write%20an%20empty%20string
        const tagsToWrite = Object.fromEntries(Object.entries(tags).map(([key, value]) => [`${key}^`, value]));
        return Effect.tryPromise({
          try: async () => {
            await exiftool.write(path, tagsToWrite);
          },
          catch: (cause) => new LibraryError({ cause }),
        }).pipe(
          Effect.catch(({ cause: error }) =>
            Effect.sync(() => {
              logger.warn(`Error writing exif data (${path}): ${error}`);
            }),
          ),
        );
      }),
    });
  };

  static readonly layer = (logger: LoggingRepository) => Layer.sync(MetadataRepo, () => MetadataRepo.make(logger));
}

/** Promise-facing adapter for the Nest services. Delegates to `MetadataRepo`. */
@Injectable()
export class MetadataRepository {
  private readonly effect: Context.Service.Shape<typeof MetadataRepo>;

  constructor(private logger: LoggingRepository) {
    this.logger.setContext(MetadataRepository.name);
    this.effect = MetadataRepo.make(this.logger);
    registerEffect(this, this.effect);
  }

  setMaxConcurrency(concurrency: number) {
    runSync(this.effect.setMaxConcurrency(concurrency));
  }

  async teardown() {
    await runPromise(this.effect.teardown());
  }

  readTags(path: string): Promise<ImmichTags> {
    return runPromise(this.effect.readTags(path));
  }

  extractBinaryTag(path: string, tagName: string): Promise<Buffer> {
    return runPromise(this.effect.extractBinaryTag(path, tagName));
  }

  async writeTags(path: string, tags: Partial<Tags>): Promise<void> {
    await runPromise(this.effect.writeTags(path, tags));
  }
}
