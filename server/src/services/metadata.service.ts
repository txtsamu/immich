import { Injectable } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import { ContainerDirectoryItem, ExifDateTime, Tags } from 'exiftool-vendored';
import { Insertable } from 'kysely';
import { isUndefined, omitBy, pick } from 'lodash-es';
import { DateTime, Duration } from 'luxon';
import { Stats } from 'node:fs';
import { constants } from 'node:fs/promises';
import { join, parse } from 'node:path';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { JobOf } from 'src/types.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { Asset, AssetFile } from 'src/database.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { runPromise } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetType,
  AssetVisibility,
  ChecksumAlgorithm,
  DatabaseLock,
  ExifOrientation,
  ImmichWorker,
  JobName,
  JobStatus,
  QueueName,
} from 'src/enum.js';
import { ReverseGeocodeResult } from 'src/repositories/map.repository.js';
import { ImmichTags } from 'src/repositories/metadata.repository.js';
import { AssetExifTable } from 'src/schema/tables/asset-exif.table.js';
import { BaseService } from 'src/services/base.service.js';
import { getAssetFiles } from 'src/utils/asset.util.js';
import { isAssetChecksumConstraint } from 'src/utils/database.js';
import { mergeTimeZone } from 'src/utils/date.js';
import { mimeTypes } from 'src/utils/mime-types.js';
import { batched } from 'src/utils/misc.js';
import { upsertTags } from 'src/utils/tag.js';

const POSTGRES_INT_MAX = 2_147_483_647;
const POSTGRES_INT_MIN = -2_147_483_648;

/** look for a date from these tags (in order) */
const EXIF_DATE_TAGS: Array<keyof ImmichTags> = [
  'SubSecDateTimeOriginal',
  'SubSecCreateDate',
  'DateTimeOriginal',
  'CreationDate',
  'CreateDate',
  'MediaCreateDate',
  'DateTimeCreated',
  'GPSDateTime',
  'DateTimeUTC',
  'SonyDateTime2',
  // Undocumented, non-standard tag from insta360 in xmp.GPano namespace
  'SourceImageCreateTime' as keyof ImmichTags,
];

export function firstDateTime(tags: ImmichTags) {
  for (const tag of EXIF_DATE_TAGS) {
    const tagValue = tags?.[tag];

    if (tagValue instanceof ExifDateTime) {
      return {
        tag,
        dateTime: tagValue,
      };
    }

    if (typeof tagValue !== 'string') {
      continue;
    }

    const exifDateTime = ExifDateTime.fromEXIF(tagValue);
    if (exifDateTime) {
      return {
        tag,
        dateTime: exifDateTime,
      };
    }
  }
}

const validate = <T>(value: T): NonNullable<T> | null => {
  // handle lists of numbers
  if (Array.isArray(value)) {
    value = value[0];
  }

  if (typeof value === 'string') {
    // string means a failure to parse a number, throw out result
    return null;
  }

  if (
    typeof value === 'number' &&
    (Number.isNaN(value) || !Number.isFinite(value) || value < POSTGRES_INT_MIN || value > POSTGRES_INT_MAX)
  ) {
    return null;
  }

  return value ?? null;
};

const validateRange = (value: number | undefined, min: number, max: number): NonNullable<number> | null => {
  // reutilizes the validate function
  const val = validate(value);

  // check if the value is within the range
  if (val === null || val < min || val > max) {
    return null;
  }

  return Math.round(val);
};

const getLensModel = (exifTags: ImmichTags): string | null => {
  const lensModel = String(
    exifTags.LensID ?? exifTags.LensType ?? exifTags.LensSpec ?? exifTags.LensModel ?? '',
  ).trim();
  if (lensModel === '----') {
    return null;
  }
  if (lensModel.startsWith('Unknown')) {
    return null;
  }
  return lensModel || null;
};

/** The original thrown value behind a failure, unwrapping the tagged errors that carry it in `cause`. */
const originalError = (cause: Cause.Cause<unknown>): any => {
  const error = Cause.squash(cause);
  return typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;
};

type Dates = {
  dateTimeOriginal: Date;
  localDateTime: Date;
};

@Injectable()
export class MetadataService extends BaseService {
  @OnEvent({ name: 'AppBootstrap', workers: [ImmichWorker.Microservices] })
  async onBootstrap() {
    this.logger.log('Bootstrapping metadata service');
    await this.init();
  }

  @OnEvent({ name: 'AppShutdown' })
  async onShutdown() {
    await runPromise(this.fx.metadata.teardown());
  }

  @OnEvent({ name: 'ConfigInit', workers: [ImmichWorker.Microservices] })
  onConfigInit({ newConfig }: ArgOf<'ConfigInit'>) {
    this.metadataRepository.setMaxConcurrency(newConfig.job.metadataExtraction.concurrency);
  }

  @OnEvent({ name: 'ConfigUpdate', workers: [ImmichWorker.Microservices], server: true })
  onConfigUpdate({ newConfig }: ArgOf<'ConfigUpdate'>) {
    this.metadataRepository.setMaxConcurrency(newConfig.job.metadataExtraction.concurrency);
  }

  private async init() {
    this.logger.log('Initializing metadata service');

    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.job.pause(QueueName.MetadataExtraction);
        yield* this.fx.database.withLock(DatabaseLock.GeodataImport, () => this.mapRepository.init());
        yield* this.fx.job.resume(QueueName.MetadataExtraction);

        this.logger.log(`Initialized local reverse geocoder`);
      }).pipe(
        Effect.catchCause((cause) => {
          const error = originalError(cause);
          this.logger.error(`Unable to initialize reverse geocoding: ${error}`, error?.stack);
          return Effect.fail(new Error('Metadata service init failed', { cause: error }));
        }),
      ),
    );
  }

  private linkLivePhotos = Effect.fn('MetadataService.linkLivePhotos')(function* (
    this: MetadataService,
    asset: { id: string; type: AssetType; ownerId: string; libraryId: string | null },
    exifInfo: Insertable<AssetExifTable>,
  ) {
    if (!exifInfo.livePhotoCID) {
      return;
    }

    const otherType = asset.type === AssetType.Video ? AssetType.Image : AssetType.Video;
    const match = yield* this.fx.asset.findLivePhotoMatch({
      livePhotoCID: exifInfo.livePhotoCID,
      ownerId: asset.ownerId,
      libraryId: asset.libraryId,
      otherAssetId: asset.id,
      type: otherType,
    });

    if (!match) {
      return;
    }

    const [photoAsset, motionAsset] = asset.type === AssetType.Image ? [asset, match] : [match, asset];
    yield* Effect.all(
      [
        this.fx.asset.update({ id: photoAsset.id, livePhotoVideoId: motionAsset.id }),
        this.fx.asset.update({ id: motionAsset.id, visibility: AssetVisibility.Hidden }),
        this.fx.album.removeAssetsFromAll([motionAsset.id]),
      ],
      { concurrency: 'unbounded' },
    );

    yield* this.fx.event.emit('AssetHide', { assetId: motionAsset.id, userId: motionAsset.ownerId });
  });

  private isOrientationSidewards(orientation: ExifOrientation | number): boolean {
    return [
      ExifOrientation.MirrorHorizontalRotate270CW,
      ExifOrientation.Rotate90CW,
      ExifOrientation.MirrorHorizontalRotate90CW,
      ExifOrientation.Rotate270CW,
    ].includes(orientation);
  }

  @OnJob({ name: JobName.AssetExtractMetadataQueueAll, queue: QueueName.MetadataExtraction })
  async handleQueueMetadataExtraction(job: JobOf<JobName.AssetExtractMetadataQueueAll>): Promise<JobStatus> {
    const { force } = job;

    for await (const assets of batched(this.assetJobRepository.streamForMetadataExtraction(force))) {
      await runPromise(
        this.fx.job.queueAll(assets.map((asset) => ({ name: JobName.AssetExtractMetadata, data: { id: asset.id } }))),
      );
    }

    return JobStatus.Success;
  }

  @OnJob({ name: JobName.AssetExtractMetadata, queue: QueueName.MetadataExtraction })
  async handleMetadataExtraction(data: JobOf<JobName.AssetExtractMetadata>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const [{ reverseGeocoding }, asset] = yield* Effect.all(
          [this.getConfigFx({ withCache: true }), this.fx.assetJob.getForMetadataExtraction(data.id)],
          { concurrency: 'unbounded' },
        );

        if (!asset) {
          return;
        }

        const [exifResult, stats] = yield* Effect.all(
          [this.getExifTags(asset), this.fx.storage.stat(asset.originalPath)],
          { concurrency: 'unbounded' },
        );
        const { tags: exifTags, audio, video, packets, format } = exifResult;
        this.logger.verbose('Exif Tags', exifTags);

        const dates = this.getDates(asset, exifTags, stats);

        const { width, height } = this.getImageDimensions(exifTags);
        let geo: ReverseGeocodeResult = { country: null, state: null, city: null },
          latitude: number | null = null,
          longitude: number | null = null;
        if (this.hasGeo(exifTags)) {
          const point = { latitude: Number(exifTags.GPSLatitude), longitude: Number(exifTags.GPSLongitude) };
          latitude = point.latitude;
          longitude = point.longitude;
          if (reverseGeocoding.enabled) {
            geo = yield* Effect.tryPromise({
              try: () => this.mapRepository.reverseGeocode(point),
              catch: (cause) => cause,
            });
          }
        }

        const tags = this.getTagList(exifTags);

        const exifData: Insertable<AssetExifTable> = {
          assetId: asset.id,

          // dates
          dateTimeOriginal: dates.dateTimeOriginal,
          modifyDate: stats.mtime,
          timeZone: dates.timeZone,

          // gps
          latitude,
          longitude,
          country: geo.country,
          state: geo.state,
          city: geo.city,

          // image/file
          fileSizeInByte: stats.size,
          exifImageHeight: validate(height),
          exifImageWidth: validate(width),
          orientation: validate(exifTags.Orientation)?.toString() ?? null,
          projectionType: exifTags.ProjectionType ? exifTags.ProjectionType.toUpperCase() : null,
          bitsPerSample: this.getBitsPerSample(exifTags),
          colorspace: exifTags.ColorSpace === undefined ? null : String(exifTags.ColorSpace),

          // camera
          make:
            exifTags.Make ??
            exifTags.Device?.Manufacturer ??
            exifTags.AndroidMake ??
            exifTags.DeviceManufacturer ??
            (exifTags.SamsungModel ? 'Samsung' : null),
          model:
            exifTags.Model ??
            exifTags.Device?.ModelName ??
            exifTags.AndroidModel ??
            exifTags.DeviceModelName ??
            exifTags.Author ??
            null,
          fps: video?.frameRate ?? validate(Number(exifTags.VideoFrameRate!)),
          iso: validate(
            exifTags.RecommendedExposureIndex ?? exifTags.StandardOutputSensitivity ?? exifTags.ISO,
          ) as number,
          exposureTime: exifTags.ExposureTime ?? null,
          lensModel: getLensModel(exifTags),
          fNumber: validate(exifTags.FNumber),
          focalLength: validate(exifTags.FocalLength),

          // comments
          description: String(exifTags.ImageDescription || exifTags.Description || '').trim(),
          profileDescription: exifTags.ProfileDescription || null,
          rating: exifTags.Rating === 0 ? null : validateRange(exifTags.Rating, 1, 5),

          // grouping
          livePhotoCID: (exifTags.ContentIdentifier || exifTags.MediaGroupUUID) ?? null,
          autoStackId: this.getAutoStackId(exifTags),

          tags: tags.length > 0 ? tags : null,
        };

        const audioData =
          format && audio?.codecName
            ? {
                assetId: asset.id,
                bitrate: audio.bitrate,
                index: audio.index,
                profile: audio.profile,
                codecName: audio.codecName,
              }
            : undefined;

        const videoData =
          format?.formatName && format.formatLongName && video?.codecName && video?.timeBase
            ? {
                assetId: asset.id,
                bitrate: video.bitrate,
                frameCount: video.frameCount,
                timeBase: video.timeBase,
                index: video.index,
                profile: video.profile,
                level: video.level,
                colorPrimaries: video.colorPrimaries,
                colorTransfer: video.colorTransfer,
                colorMatrix: video.colorMatrix,
                dvProfile: video.dvProfile,
                dvLevel: video.dvLevel,
                dvBlSignalCompatibilityId: video.dvBlSignalCompatibilityId,
                codecName: video.codecName,
                formatName: format.formatName,
                formatLongName: format.formatLongName,
                pixelFormat: video.pixelFormat,
              }
            : undefined;

        const keyframeData =
          packets && packets.keyframePts.length > 0
            ? {
                assetId: asset.id,
                totalDuration: packets.totalDuration,
                packetCount: packets.packetCount,
                outputFrames: packets.outputFrames,
                pts: packets.keyframePts,
                accDuration: packets.keyframeAccDuration,
                ownDuration: packets.keyframeOwnDuration,
              }
            : undefined;

        const isSidewards = exifTags.Orientation && this.isOrientationSidewards(exifTags.Orientation);
        const assetWidth = validate(isSidewards ? height : width);
        const assetHeight = validate(isSidewards ? width : height);

        const tasks: Effect.Effect<unknown, unknown>[] = [
          this.fx.asset.update({
            id: asset.id,
            duration: this.getDuration(exifTags),
            localDateTime: dates.localDateTime,
            fileCreatedAt: dates.dateTimeOriginal ?? undefined,
            fileModifiedAt: stats.mtime,

            // Keep unedited assets in sync with the file on disk, but don't overwrite edited dimensions.
            width: !asset.isEdited || asset.width === null ? assetWidth : undefined,
            height: !asset.isEdited || asset.height === null ? assetHeight : undefined,
          }),
          Effect.gen({ self: this }, function* () {
            yield* this.fx.asset.upsertExif({
              exif: exifData,
              audio: audioData,
              video: videoData,
              keyframes: keyframeData,
              lockedPropertiesBehavior: 'skip',
            });
            yield* this.applyTagList(asset);
          }),
        ];

        if (this.isMotionPhoto(asset, exifTags)) {
          tasks.push(this.applyMotionPhotos(asset, exifTags, dates, stats));
        }

        yield* Effect.all(tasks, { concurrency: 'unbounded' });

        if (exifData.livePhotoCID) {
          yield* this.linkLivePhotos(asset, exifData);
        }

        yield* this.fx.asset.upsertJobStatus({ assetId: asset.id, metadataExtractedAt: new Date() });

        yield* this.fx.event.emit('AssetMetadataExtracted', {
          assetId: asset.id,
          userId: asset.ownerId,
          source: data.source,
        });
      }),
    );
  }

  @OnJob({ name: JobName.SidecarQueueAll, queue: QueueName.Sidecar })
  async handleQueueSidecar({ force }: JobOf<JobName.SidecarQueueAll>): Promise<JobStatus> {
    for await (const assets of batched(this.assetJobRepository.streamForSidecar(force))) {
      await runPromise(
        this.fx.job.queueAll(assets.map((asset) => ({ name: JobName.SidecarCheck, data: { id: asset.id } }))),
      );
    }

    return JobStatus.Success;
  }

  @OnJob({ name: JobName.SidecarCheck, queue: QueueName.Sidecar })
  async handleSidecarCheck({ id }: JobOf<JobName.SidecarCheck>): Promise<JobStatus | undefined> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const asset = yield* this.fx.assetJob.getForSidecarCheckJob(id);
        if (!asset) {
          return;
        }

        let sidecarPath = null;
        for (const candidate of this.getSidecarCandidates(asset)) {
          const isExists = yield* this.fx.storage.checkFileExists(candidate, constants.R_OK);
          if (!isExists) {
            continue;
          }

          sidecarPath = candidate;
          break;
        }

        const { sidecarFile } = getAssetFiles(asset.files);

        const isChanged = sidecarPath !== (sidecarFile?.path ?? null);

        if (sidecarFile?.path || sidecarPath) {
          this.logger.debug(
            `Sidecar check found old=${sidecarFile?.path}, new=${sidecarPath} will ${isChanged ? 'update' : 'do nothing for'} asset ${asset.id}: ${asset.originalPath}`,
          );
        } else {
          this.logger.verbose(`No sidecars found for asset ${asset.id}: ${asset.originalPath}`);
        }

        if (!isChanged) {
          return JobStatus.Skipped;
        }

        yield* sidecarPath === null
          ? this.fx.asset.deleteFile({ assetId: asset.id, type: AssetFileType.Sidecar })
          : this.fx.asset.upsertFile({ assetId: asset.id, type: AssetFileType.Sidecar, path: sidecarPath });

        return JobStatus.Success;
      }),
    );
  }

  @OnEvent({ name: 'AssetTag' })
  async handleTagAsset({ assetId }: ArgOf<'AssetTag'>) {
    await runPromise(this.fx.job.queue({ name: JobName.SidecarWrite, data: { id: assetId } }));
  }

  @OnEvent({ name: 'AssetUntag' })
  async handleUntagAsset({ assetId }: ArgOf<'AssetUntag'>) {
    await runPromise(this.fx.job.queue({ name: JobName.SidecarWrite, data: { id: assetId } }));
  }

  @OnJob({ name: JobName.SidecarWrite, queue: QueueName.Sidecar })
  async handleSidecarWrite(job: JobOf<JobName.SidecarWrite>): Promise<JobStatus> {
    const { id } = job;
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const asset = yield* this.fx.assetJob.getForSidecarWriteJob(id);
        if (!asset) {
          return JobStatus.Failed;
        }

        const lockedProperties = yield* this.fx.assetJob.getLockedPropertiesForMetadataExtraction(id);

        const { sidecarFile } = getAssetFiles(asset.files);
        const sidecarPath = sidecarFile?.path || `${asset.originalPath}.xmp`;

        const { description, dateTimeOriginal, latitude, longitude, rating, tags, timeZone } = pick(
          {
            description: asset.exifInfo.description,
            dateTimeOriginal: asset.exifInfo.dateTimeOriginal,
            latitude: asset.exifInfo.latitude,
            longitude: asset.exifInfo.longitude,
            rating: asset.exifInfo.rating ?? 0,
            tags: asset.exifInfo.tags,
            timeZone: asset.exifInfo.timeZone,
          },
          lockedProperties,
        );

        const exif = omitBy(
          <Tags>{
            Description: description,
            ImageDescription: description,
            DateTimeOriginal: mergeTimeZone(dateTimeOriginal, timeZone)?.toISO(),
            GPSLatitude: latitude,
            GPSLongitude: longitude,
            Rating: rating,
            TagsList: tags,
          },
          isUndefined,
        );

        if (Object.keys(exif).length === 0) {
          return JobStatus.Skipped;
        }

        yield* this.fx.metadata.writeTags(sidecarPath, exif);

        if (asset.files.length === 0) {
          yield* this.fx.asset.upsertFile({ assetId: id, type: AssetFileType.Sidecar, path: sidecarPath });
        }

        yield* this.fx.asset.unlockProperties(asset.id, lockedProperties);

        return JobStatus.Success;
      }),
    );
  }

  private getSidecarCandidates({ files, originalPath }: { files: AssetFile[]; originalPath: string }) {
    const candidates: string[] = [];

    const { sidecarFile } = getAssetFiles(files);
    if (sidecarFile?.path) {
      candidates.push(sidecarFile.path);
    }

    const assetPath = parse(originalPath);

    candidates.push(
      // IMG_123.jpg.xmp
      `${originalPath}.xmp`,
      // IMG_123.xmp
      `${join(assetPath.dir, assetPath.name)}.xmp`,
    );

    return candidates;
  }

  private getImageDimensions(exifTags: ImmichTags): { width?: number; height?: number } {
    /*
     * The "true" values for width and height are a bit hidden, depending on the camera model and file format.
     * For RAW images in the CR2 or RAF format, the "ImageSize" value seems to be correct,
     * but ImageWidth and ImageHeight are not correct (they contain the dimensions of the preview image).
     */
    let [width, height] =
      exifTags.ImageSize?.toString()
        ?.split('x')
        ?.map((dim) => Number.parseInt(dim) || undefined) ?? [];
    if (!width || !height) {
      [width, height] = [exifTags.ImageWidth, exifTags.ImageHeight];
    }
    return { width, height };
  }

  private getExifTags = Effect.fn('MetadataService.getExifTags')(function* (
    this: MetadataService,
    asset: { originalPath: string; files: AssetFile[]; type: AssetType },
  ) {
    const { sidecarFile } = getAssetFiles(asset.files);
    const shouldProbe = asset.type === AssetType.Video || asset.originalPath.toLowerCase().endsWith('.gif');

    const [mediaTags, sidecarTags, videoResult] = yield* Effect.all(
      [
        this.fx.metadata.readTags(asset.originalPath),
        sidecarFile ? this.fx.metadata.readTags(sidecarFile.path) : Effect.succeed(null),
        shouldProbe ? this.getVideoTags(asset.originalPath) : Effect.succeed(null),
      ],
      { concurrency: 'unbounded' },
    );

    // prefer dates from sidecar tags
    if (sidecarTags) {
      const result = firstDateTime(sidecarTags);
      const sidecarDate = result?.dateTime;
      if (sidecarDate) {
        for (const tag of EXIF_DATE_TAGS) {
          delete mediaTags[tag];
        }

        // exiftool-vendored derives tz information from the date.
        // if the sidecar file has date information, we also assume the tz information come from there.
        //
        // this is especially important in the case of UTC+0 where exiftool-vendored does not return tz/zone fields
        // and as such the tags aren't overwritten when returning all tags.
        for (const tag of ['zone', 'tz', 'tzSource'] as const) {
          delete mediaTags[tag];
        }
      }
    }

    // prefer duration from video tags
    // don't save duration if asset is definitely not an animated image (see e.g. CR3 with Duration: 1s)
    if (videoResult || !mimeTypes.isPossiblyAnimatedImage(asset.originalPath)) {
      delete mediaTags.Duration;
    }

    // never use duration from sidecar
    delete sidecarTags?.Duration;

    // don't use Exif Orientation for HEIF based images, it's usually missing or invalid.
    // prefer irot (ExifTool QuickTime:Rotation) mapped to ExifOrientation.
    if (mimeTypes.isHeifImage(asset.originalPath)) {
      const orientation = this.getHeifOrientation(mediaTags);
      if (orientation === null) {
        delete mediaTags.Orientation;
      } else {
        mediaTags.Orientation = orientation;
      }
    }

    return {
      tags: { ...mediaTags, ...videoResult?.tags, ...sidecarTags },
      audio: videoResult?.audio,
      video: videoResult?.video,
      packets: videoResult?.packets,
      format: videoResult?.format ?? null,
    };
  });

  private getTagList(exifTags: ImmichTags): string[] {
    let tags: string[];
    if (exifTags.TagsList) {
      tags = exifTags.TagsList.map(String);
    } else if (exifTags.HierarchicalSubject) {
      tags = exifTags.HierarchicalSubject.map((tag) =>
        // convert | to /
        typeof tag === 'number'
          ? String(tag)
          : tag
              .split('|')
              .map((tag) => tag.replaceAll('/', '|'))
              .join('/'),
      );
    } else if (exifTags.Keywords) {
      let keywords = exifTags.Keywords;
      if (!Array.isArray(keywords)) {
        keywords = [keywords];
      }
      tags = keywords.map(String);
    } else {
      tags = [];
    }
    return tags;
  }

  private applyTagList = Effect.fn('MetadataService.applyTagList')(function* (
    this: MetadataService,
    { id, ownerId }: { id: string; ownerId: string },
  ) {
    const asset = yield* this.fx.asset.getForMetadataExtractionTags(id);
    // `upsertTags` is a Promise-based util that takes the Promise adapter
    const results = yield* Effect.tryPromise({
      try: () => upsertTags(this.tagRepository, { userId: ownerId, tags: asset?.tags ?? [] }),
      catch: (cause) => cause,
    });
    yield* this.fx.tag.replaceAssetTags(
      id,
      results.map((tag) => tag.id),
    );
  });

  private isMotionPhoto(asset: { type: AssetType }, tags: ImmichTags): boolean {
    return asset.type === AssetType.Image && !!(tags.MotionPhoto || tags.MicroVideo);
  }

  private applyMotionPhotos = Effect.fn('MetadataService.applyMotionPhotos')(function* (
    this: MetadataService,
    asset: Asset,
    tags: ImmichTags,
    dates: Dates,
    stats: Stats,
  ) {
    const isMotionPhoto = tags.MotionPhoto;
    const isMicroVideo = tags.MicroVideo;
    const videoOffset = tags.MicroVideoOffset;
    const hasMotionPhotoVideo = tags.MotionPhotoVideo;
    const hasEmbeddedVideoFile = tags.EmbeddedVideoType === 'MotionPhoto_Data' && tags.EmbeddedVideoFile;
    const directory = Array.isArray(tags.ContainerDirectory)
      ? (tags.ContainerDirectory as ContainerDirectoryItem[])
      : null;

    let length = 0;
    let padding = 0;

    if (isMotionPhoto && directory) {
      for (const entry of directory) {
        if (entry?.Item?.Semantic === 'MotionPhoto') {
          length = entry.Item.Length ?? 0;
          padding = entry.Item.Padding ?? 0;
          break;
        }
      }
    }

    if (isMicroVideo && typeof videoOffset === 'number') {
      length = videoOffset;
    }

    if (!length && !hasEmbeddedVideoFile && !hasMotionPhotoVideo) {
      return;
    }

    this.logger.debug(`Starting motion photo video extraction for asset ${asset.id}: ${asset.originalPath}`);

    yield* Effect.gen({ self: this }, function* () {
      const position = stats.size - length - padding;
      let video: Buffer;
      // Samsung MotionPhoto video extraction
      //     HEIC-encoded
      if (hasMotionPhotoVideo) {
        video = yield* this.fx.metadata.extractBinaryTag(asset.originalPath, 'MotionPhotoVideo');
      }
      //     JPEG-encoded; HEIC also contains these tags, so this conditional must come second
      else if (hasEmbeddedVideoFile) {
        video = yield* this.fx.metadata.extractBinaryTag(asset.originalPath, 'EmbeddedVideoFile');
      }
      // Default video extraction
      else {
        video = yield* this.fx.storage.readFile(asset.originalPath, {
          buffer: Buffer.alloc(length),
          position,
          length,
        });
      }
      const checksum = yield* this.fx.crypto.hashSha1(video);
      const checksumQuery = { ownerId: asset.ownerId, libraryId: asset.libraryId ?? undefined, checksum };

      let motionAsset = yield* this.fx.asset.getByChecksum(checksumQuery);
      let isNewMotionAsset = false;

      if (!motionAsset) {
        let created = false;
        const result = yield* Effect.gen({ self: this }, function* () {
          const motionAssetId = yield* this.fx.crypto.randomUUID();
          const createdAsset = yield* this.fx.asset.create({
            id: motionAssetId,
            libraryId: asset.libraryId,
            type: AssetType.Video,
            fileCreatedAt: dates.dateTimeOriginal,
            fileModifiedAt: stats.mtime,
            localDateTime: dates.localDateTime,
            checksum,
            checksumAlgorithm: ChecksumAlgorithm.sha1File,
            ownerId: asset.ownerId,
            originalPath: StorageCore.getAndroidMotionPath(asset, motionAssetId),
            originalFileName: `${parse(asset.originalFileName).name}.mp4`,
            visibility: AssetVisibility.Hidden,
          });

          created = true;

          if (!asset.isExternal) {
            yield* this.fx.user.updateUsage(asset.ownerId, video.byteLength);
          }

          return { motionAsset: createdAsset, isNew: true };
        }).pipe(
          Effect.catchCause((cause) => {
            if (!isAssetChecksumConstraint(originalError(cause))) {
              return Effect.failCause(cause);
            }

            return Effect.gen({ self: this }, function* () {
              const existing = yield* this.fx.asset.getByChecksum(checksumQuery);
              return existing ? { motionAsset: existing, isNew: created } : undefined;
            });
          }),
        );

        if (!result) {
          this.logger.warn(`Unable to find existing motion video asset for ${asset.id}: ${asset.originalPath}`);
          return;
        }

        motionAsset = result.motionAsset;
        isNewMotionAsset = result.isNew;
      }

      if (!isNewMotionAsset) {
        this.logger.debugFn(() => {
          const base64Checksum = checksum.toString('base64');
          return `Motion asset with checksum ${base64Checksum} already exists for asset ${asset.id}: ${asset.originalPath}`;
        });
      }

      // Hide the motion photo video asset if it's not already hidden to prepare for linking
      if (motionAsset.visibility === AssetVisibility.Timeline) {
        yield* this.fx.asset.update({
          id: motionAsset.id,
          visibility: AssetVisibility.Hidden,
        });
        this.logger.log(`Hid unlinked motion photo video asset (${motionAsset.id})`);
      }

      if (asset.livePhotoVideoId !== motionAsset.id) {
        yield* this.fx.asset.update({ id: asset.id, livePhotoVideoId: motionAsset.id });

        // If the asset already had an associated livePhotoVideo, delete it, because
        // its checksum doesn't match the checksum of the motionAsset we just extracted
        // (if it did, getByChecksum() would've returned a motionAsset with the same ID as livePhotoVideoId)
        // note asset.livePhotoVideoId is not motionAsset.id yet
        if (asset.livePhotoVideoId) {
          yield* this.fx.job.queue({
            name: JobName.AssetDelete,
            data: { id: asset.livePhotoVideoId, deleteOnDisk: true },
          });
          this.logger.log(`Removed old motion photo video asset (${asset.livePhotoVideoId})`);
        }
      }

      // write extracted motion video to disk, especially if the encoded-video folder has been deleted
      const isExistsOnDisk = yield* this.fx.storage.checkFileExists(motionAsset.originalPath);
      if (!isExistsOnDisk) {
        this.storageCore.ensureFolders(motionAsset.originalPath);
        yield* this.fx.storage.createFile(motionAsset.originalPath, video);
        this.logger.log(`Wrote motion photo video to ${motionAsset.originalPath}`);

        // other service-level method, still Promise based
        const motionAssetId = motionAsset.id;
        yield* Effect.tryPromise({
          try: () => this.handleMetadataExtraction({ id: motionAssetId }),
          catch: (cause) => cause,
        });
        yield* this.fx.job.queue({ name: JobName.AssetEncodeVideo, data: { id: motionAsset.id } });
      }

      this.logger.debug(`Finished motion photo video extraction for asset ${asset.id}: ${asset.originalPath}`);
    }).pipe(
      Effect.catchCause((cause) => {
        const error = originalError(cause);
        this.logger.error(
          `Failed to extract motion video for ${asset.id}: ${asset.originalPath}: ${error}`,
          error?.stack,
        );
        return Effect.void;
      }),
    );
  });

  private getDates(
    asset: { id: string; originalPath: string; fileCreatedAt: Date },
    exifTags: ImmichTags,
    stats: Stats,
  ) {
    const result = firstDateTime(exifTags);
    const tag = result?.tag;
    const dateTime = result?.dateTime;
    if (dateTime) {
      this.logger.verbose(
        `Date and time is ${dateTime} using exifTag ${tag} for asset ${asset.id}: ${asset.originalPath}`,
      );
    } else {
      this.logger.verbose(`No exif date time information found for asset ${asset.id}: ${asset.originalPath}`);
    }

    // timezone
    let timeZone = exifTags.zone ?? null;
    if (timeZone === null && (dateTime?.rawValue?.endsWith('Z') || dateTime?.rawValue?.endsWith('+00:00'))) {
      // exiftool-vendored returns "no timezone" information even though "+00:00" might be set explicitly
      // https://github.com/photostructure/exiftool-vendored.js/issues/203
      timeZone = 'UTC+0';
    }

    if (timeZone) {
      this.logger.verbose(
        `Found timezone ${timeZone} via ${exifTags.zoneSource} for asset ${asset.id}: ${asset.originalPath}`,
      );
    } else {
      this.logger.debug(`No timezone information found for asset ${asset.id}: ${asset.originalPath}`);
    }

    let dateTimeOriginal = dateTime?.toDateTime();

    // do not let JavaScript use local timezone
    if (dateTimeOriginal && !dateTime?.hasZone) {
      dateTimeOriginal = dateTimeOriginal.setZone('UTC', { keepLocalTime: true });
    }

    // align with whatever timeZone we chose
    dateTimeOriginal = dateTimeOriginal?.setZone(timeZone ?? 'UTC');

    // store as "local time"
    let localDateTime = dateTimeOriginal?.setZone('UTC', { keepLocalTime: true });

    if (!localDateTime || !dateTimeOriginal) {
      // FileCreateDate is not available on linux, likely because exiftool hasn't integrated the statx syscall yet
      // birthtime is not available in Docker on macOS, so it appears as 0
      const earliestDate = DateTime.fromMillis(
        Math.min(
          asset.fileCreatedAt.getTime(),
          stats.birthtimeMs ? Math.min(stats.mtimeMs, stats.birthtimeMs) : stats.mtime.getTime(),
        ),
      );
      this.logger.debug(
        `No exif date time found, falling back on ${earliestDate.toISO()}, earliest of file creation and modification for asset ${asset.id}: ${asset.originalPath}`,
      );
      dateTimeOriginal = localDateTime = earliestDate;
    }

    this.logger.verbose(`Found local date time ${localDateTime.toISO()} for asset ${asset.id}: ${asset.originalPath}`);

    return {
      timeZone,
      localDateTime: localDateTime.toJSDate(),
      dateTimeOriginal: dateTimeOriginal.toJSDate(),
    };
  }

  private hasGeo(tags: ImmichTags) {
    const lat = Number(tags.GPSLatitude);
    const lng = Number(tags.GPSLongitude);
    return !Number.isNaN(lat) && !Number.isNaN(lng) && (lat !== 0 || lng !== 0);
  }

  private getAutoStackId(tags: ImmichTags | null): string | null {
    if (!tags) {
      return null;
    }
    return tags.BurstID ?? tags.BurstUUID ?? tags.CameraBurstID ?? tags.MediaUniqueID ?? null;
  }

  private getBitsPerSample(tags: ImmichTags): number | null {
    const bitDepthTags = [
      tags.BitsPerSample,
      tags.ComponentBitDepth,
      tags.ImagePixelDepth,
      tags.BitDepth,
      tags.ColorBitDepth,
      // `numericTags` doesn't parse values like '12 12 12'
    ].map((tag) => (typeof tag === 'string' ? Number.parseInt(tag) : tag));

    let bitsPerSample = bitDepthTags.find((tag) => typeof tag === 'number' && !Number.isNaN(tag)) ?? null;
    if (bitsPerSample && bitsPerSample >= 24 && bitsPerSample % 3 === 0) {
      bitsPerSample /= 3; // converts per-pixel bit depth to per-channel
    }

    return bitsPerSample;
  }

  private getDuration(tags: ImmichTags): number | null {
    const duration = tags.Duration;
    // eslint-disable-next-line unicorn/prefer-number-coercion
    const seconds = typeof duration === 'number' ? duration : Number.parseFloat(duration as string);
    return Number.isFinite(seconds) ? Math.round(Duration.fromObject({ seconds }).toMillis()) : null;
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private getVideoTags = Effect.fn('MetadataService.getVideoTags')(function* (
    this: MetadataService,
    originalPath: string,
  ) {
    const { videoStreams, audioStreams, format } = yield* this.fx.media.probe(originalPath);
    const video = videoStreams[0];
    const audio = audioStreams[0];
    const packets = video?.timeBase ? yield* this.fx.media.probePackets(originalPath, video.index) : null;

    const tags: Pick<ImmichTags, 'Duration' | 'Orientation' | 'ImageWidth' | 'ImageHeight'> = {};

    if (video) {
      if (video.width) {
        tags.ImageWidth = video.width;
      }
      if (video.height) {
        tags.ImageHeight = video.height;
      }

      switch (video.rotation) {
        case -90: {
          tags.Orientation = ExifOrientation.Rotate90CW;
          break;
        }
        case 0: {
          tags.Orientation = ExifOrientation.Horizontal;
          break;
        }
        case 90: {
          tags.Orientation = ExifOrientation.Rotate270CW;
          break;
        }
        case 180: {
          tags.Orientation = ExifOrientation.Rotate180;
          break;
        }
      }
    }

    if (format.duration) {
      tags.Duration = format.duration;
    }

    return { tags, audio, video, packets, format };
  });

  private getHeifOrientation(exifTags: ImmichTags): ExifOrientation | null {
    // https://exiftool.org/TagNames/QuickTime.html#ItemPropCont
    const rotation = typeof exifTags.Rotation === 'number' ? exifTags.Rotation : undefined;
    switch (rotation) {
      case 0: {
        return ExifOrientation.Horizontal;
      }
      case 1: {
        return ExifOrientation.Rotate270CW;
      }
      case 2: {
        return ExifOrientation.Rotate180;
      }
      case 3: {
        return ExifOrientation.Rotate90CW;
      }
      default: {
        return null;
      }
    }
  }
}
