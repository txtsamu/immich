import { Injectable } from '@nestjs/common';
import { Cause, Effect } from 'effect';
import type {
  AudioStreamInfo,
  DecodeToBufferOptions,
  JobItem,
  JobOf,
  VideoFormat,
  VideoInterfaces,
  VideoStreamInfo,
} from 'src/types.js';
import { ImagePathOptions, StorageCore, ThumbnailPathEntity } from 'src/cores/storage.core.js';
import { AssetFile } from 'src/database.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { ConfigFFmpegDto, SystemConfig } from 'src/dtos/config.dto.js';
import { runPromise } from 'src/effect/run.js';
import {
  AssetFileType,
  AssetType,
  AssetVisibility,
  AudioCodec,
  Colorspace,
  ImageFormat,
  ImmichWorker,
  JobName,
  JobStatus,
  QueueName,
  RawExtractedFormat,
  StorageFolder,
  TranscodeHardwareAcceleration,
  TranscodePolicy,
  TranscodeTarget,
  VideoCodec,
  VideoContainer,
} from 'src/enum.js';
import { AssetJobRepository } from 'src/repositories/asset-job.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { getAssetFile, getDimensions } from 'src/utils/asset.util.js';
import { BaseConfig, ThumbnailConfig } from 'src/utils/media.js';
import { mimeTypes } from 'src/utils/mime-types.js';
import { batched } from 'src/utils/misc.js';
import { getOutputDimensions } from 'src/utils/transform.js';

interface UpsertFileOptions {
  assetId: string;
  type: AssetFileType;
  path: string;
  isEdited: boolean;
  isProgressive: boolean;
  isTransparent: boolean;
}

type ThumbnailAsset = NonNullable<Awaited<ReturnType<AssetJobRepository['getForGenerateThumbnailJob']>>>;

/** Runs a Promise-based collaborator (storage core, ...); the error stays the original thrown value. */
const attempt = <A>(run: () => A | PromiseLike<A>) =>
  Effect.tryPromise({ try: async () => await run(), catch: (cause) => cause });

/** Effect errors wrap the original thrown value in `cause`; the transcoding fallbacks inspect the original. */
const unwrapCause = (error: unknown) =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;

/** Mirrors `for await (const items of batches)`: the iterator is closed when the loop exits early or fails. */
const forEachBatch = <T, E>(batches: AsyncGenerator<T[]>, handle: (items: T[]) => Effect.Effect<unknown, E>) =>
  Effect.ensuring(
    Effect.gen(function* () {
      while (true) {
        const next = yield* attempt(() => batches.next());
        if (next.done) {
          break;
        }

        yield* handle(next.value);
      }
    }),
    Effect.promise(() => batches.return(undefined).catch(() => {})),
  );

@Injectable()
export class MediaService extends BaseService {
  videoInterfaces: VideoInterfaces = { dri: [], mali: false };

  @OnEvent({ name: 'AppBootstrap', workers: [ImmichWorker.Microservices] })
  async onBootstrap() {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.videoInterfaces = yield* attempt(() => this.storageCore.getVideoInterfaces());
      }),
    );
  }

  @OnJob({ name: JobName.AssetGenerateThumbnailsQueueAll, queue: QueueName.ThumbnailGeneration })
  async handleQueueGenerateThumbnails({ force }: JobOf<JobName.AssetGenerateThumbnailsQueueAll>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.getConfigFx({ withCache: true });

        const isFullsizeEnabled = config.image.fullsize.enabled;
        const batches = batched(
          yield* this.fx.assetJob.streamForThumbnailJob({ force, fullsizeEnabled: isFullsizeEnabled }),
        );
        yield* forEachBatch(batches, (assets) => {
          const jobs: JobItem[] = [];
          for (const asset of assets) {
            if (force || !asset.isEdited) {
              jobs.push({ name: JobName.AssetGenerateThumbnails, data: { id: asset.id } });
            }

            if (asset.isEdited) {
              jobs.push({ name: JobName.AssetEditThumbnailGeneration, data: { id: asset.id } });
            }
          }

          return this.fx.job.queueAll(jobs);
        });

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.FileMigrationQueueAll, queue: QueueName.Migration })
  async handleQueueMigration(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { active, waiting } = yield* this.fx.job.getJobCounts(QueueName.Migration);
        if (active === 1 && waiting === 0) {
          yield* attempt(() => this.storageCore.removeEmptyDirs(StorageFolder.Thumbnails));
          yield* attempt(() => this.storageCore.removeEmptyDirs(StorageFolder.EncodedVideo));
        }

        const batches = batched(yield* this.fx.assetJob.streamForMigrationJob());
        yield* forEachBatch(batches, (assets) =>
          this.fx.job.queueAll(assets.map((asset) => ({ name: JobName.AssetFileMigration, data: { id: asset.id } }))),
        );

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.AssetFileMigration, queue: QueueName.Migration })
  async handleAssetMigration({ id }: JobOf<JobName.AssetFileMigration>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { image } = yield* this.getConfigFx({ withCache: true });
        const asset = yield* this.fx.assetJob.getForMigrationJob(id);
        if (!asset) {
          return JobStatus.Failed;
        }

        yield* attempt(() => this.storageCore.moveAssetImage(asset, AssetFileType.FullSize, image.fullsize.format));
        yield* attempt(() => this.storageCore.moveAssetImage(asset, AssetFileType.Preview, image.preview.format));
        yield* attempt(() => this.storageCore.moveAssetImage(asset, AssetFileType.Thumbnail, image.thumbnail.format));
        yield* attempt(() => this.storageCore.moveAssetVideo(asset));

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.AssetEditThumbnailGeneration, queue: QueueName.Editor })
  async handleAssetEditThumbnailGeneration({ id }: JobOf<JobName.AssetEditThumbnailGeneration>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const asset = yield* this.fx.assetJob.getForGenerateThumbnailJob(id);
        const config = yield* this.getConfigFx({ withCache: true });

        if (!asset) {
          this.logger.warn(`Thumbnail generation failed for asset ${id}: not found in database or missing metadata`);
          return JobStatus.Failed;
        }

        const generated = yield* this.generateEditedThumbnailsFx(asset, config);
        yield* this.syncFilesFx(
          asset.files.filter((file) => file.isEdited),
          generated?.files ?? [],
        );

        let thumbhash: Buffer | undefined = generated?.thumbhash;
        if (!thumbhash) {
          const extractedImage = yield* this.extractOriginalImageFx(asset, config.image);
          const { info, data, colorspace } = extractedImage;

          thumbhash = yield* this.fx.media.generateThumbhash(
            { data, info },
            { colorspace, processInvalidImages: false, edits: [] },
          );
        }

        if (!asset.thumbhash || Buffer.compare(asset.thumbhash, thumbhash) !== 0) {
          yield* this.fx.asset.update({ id: asset.id, thumbhash });
        }

        const fullsizeDimensions = generated?.fullsizeDimensions ?? getDimensions(asset.exifInfo!);
        yield* this.fx.asset.update({ id: asset.id, ...fullsizeDimensions });

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.AssetGenerateThumbnails, queue: QueueName.ThumbnailGeneration })
  async handleGenerateThumbnails({ id }: JobOf<JobName.AssetGenerateThumbnails>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const asset = yield* this.fx.assetJob.getForGenerateThumbnailJob(id);
        const config = yield* this.getConfigFx({ withCache: true });

        if (!asset) {
          this.logger.warn(`Thumbnail generation failed for asset ${id}: not found in database or missing metadata`);
          return JobStatus.Failed;
        }

        if (asset.visibility === AssetVisibility.Hidden) {
          this.logger.verbose(`Thumbnail generation skipped for asset ${id}: not visible`);
          return JobStatus.Skipped;
        }

        let generated: Effect.Success<ReturnType<MediaService['generateImageThumbnailsFx']>>;
        if (asset.type === AssetType.Video || asset.originalFileName.toLowerCase().endsWith('.gif')) {
          this.logger.verbose(`Thumbnail generation for video ${id} ${asset.originalPath}`);
          generated = yield* this.generateVideoThumbnailsFx(asset, config);
        } else if (asset.type === AssetType.Image) {
          this.logger.verbose(`Thumbnail generation for image ${id} ${asset.originalPath}`);
          generated = yield* this.generateImageThumbnailsFx(asset, config);
        } else {
          this.logger.warn(`Skipping thumbnail generation for asset ${id}: ${asset.type} is not an image or video`);
          return JobStatus.Skipped;
        }

        const editedGenerated = yield* this.generateEditedThumbnailsFx(asset, config);
        if (editedGenerated) {
          generated.files.push(...editedGenerated.files);
        }

        yield* this.syncFilesFx(asset.files, generated.files);
        const thumbhash = editedGenerated?.thumbhash || generated.thumbhash;

        if (!asset.thumbhash || Buffer.compare(asset.thumbhash, thumbhash) !== 0) {
          yield* this.fx.asset.update({ id: asset.id, thumbhash });
        }

        return JobStatus.Success;
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly extractImageFx = Effect.fn('MediaService.extractImage')(function* (
    this: MediaService,
    originalPath: string,
    minSize: number,
  ) {
    let extracted = yield* this.fx.media.extract(originalPath);
    if (extracted && !(yield* this.shouldUseExtractedImageFx(extracted.buffer, minSize))) {
      extracted = null;
    }

    return extracted;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly decodeImageFx = Effect.fn('MediaService.decodeImage')(function* (
    this: MediaService,
    thumbSource: string | Buffer,
    exifInfo: ThumbnailAsset['exifInfo'],
    targetSize?: number,
  ) {
    const { image } = yield* this.getConfigFx({ withCache: true });
    const colorspace = this.isSRGB(exifInfo) ? Colorspace.Srgb : image.colorspace;
    const decodeOptions: DecodeToBufferOptions = {
      colorspace,
      processInvalidImages: process.env.IMMICH_PROCESS_INVALID_IMAGES === 'true',
      size: targetSize,
      orientation: exifInfo.orientation ? Number(exifInfo.orientation) : undefined,
    };

    const { info, data } = yield* this.fx.media.decodeImage(thumbSource, decodeOptions);
    return { info, data, colorspace };
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly extractOriginalImageFx = Effect.fn('MediaService.extractOriginalImage')(function* (
    this: MediaService,
    asset: ThumbnailAsset,
    image: SystemConfig['image'],
    useEdits = false,
  ) {
    const isExtractEmbedded = image.extractEmbedded && mimeTypes.isRaw(asset.originalFileName);
    const extracted = isExtractEmbedded ? yield* this.extractImageFx(asset.originalPath, image.preview.size) : null;
    const isGenerateFullsize =
      ((image.fullsize.enabled || asset.exifInfo.projectionType === 'EQUIRECTANGULAR') &&
        !mimeTypes.isWebSupportedImage(asset.originalPath)) ||
      useEdits;
    const isConvertFullsize =
      isGenerateFullsize && (!extracted || !mimeTypes.isWebSupportedImage(` .${extracted.format}`));

    const thumbSource = extracted ? extracted.buffer : asset.originalPath;
    const { data, info, colorspace } = yield* this.decodeImageFx(
      thumbSource,
      // only specify orientation to extracted images which don't have EXIF orientation data
      // or it can double rotate the image
      extracted ? asset.exifInfo : { ...asset.exifInfo, orientation: null },
      isConvertFullsize ? undefined : image.preview.size,
    );

    let isTransparent = false;
    if (!extracted && mimeTypes.canBeTransparent(asset.originalPath)) {
      ({ isTransparent } = yield* this.fx.media.getImageMetadata(asset.originalPath));
    }

    return {
      extracted,
      data,
      info,
      colorspace,
      convertFullsize: isConvertFullsize,
      generateFullsize: isGenerateFullsize,
      isTransparent,
    };
  });

  private readonly generateImageThumbnailsFx = Effect.fn('MediaService.generateImageThumbnails')(function* (
    this: MediaService,
    asset: ThumbnailAsset,
    { image }: SystemConfig,
    useEdits: boolean = false,
  ) {
    // Handle embedded preview extraction for RAW files
    const extractedImage = yield* this.extractOriginalImageFx(asset, image, useEdits);
    const { info, data, colorspace, generateFullsize, convertFullsize, extracted, isTransparent } = extractedImage;

    const previewFormat = image.preview.format;
    this.warnOnTransparencyLoss(isTransparent, previewFormat, asset.id);

    const thumbnailFormat = image.thumbnail.format;
    this.warnOnTransparencyLoss(isTransparent, thumbnailFormat, asset.id);

    const previewFile = this.getImageFile(asset, {
      fileType: AssetFileType.Preview,
      format: previewFormat,
      isEdited: useEdits,
      isProgressive: !!image.preview.progressive && previewFormat !== ImageFormat.Webp,
      isTransparent,
    });
    const thumbnailFile = this.getImageFile(asset, {
      fileType: AssetFileType.Thumbnail,
      format: thumbnailFormat,
      isEdited: useEdits,
      isProgressive: !!image.thumbnail.progressive && thumbnailFormat !== ImageFormat.Webp,
      isTransparent,
    });
    this.storageCore.ensureFolders(previewFile.path);

    // generate final images
    const decoded = { data, info };
    const baseOptions = { colorspace, processInvalidImages: false, edits: useEdits ? asset.edits : [] };
    const thumbnailOptions = { ...image.thumbnail, ...baseOptions, format: thumbnailFormat };
    const previewOptions = { ...image.preview, ...baseOptions, format: previewFormat };
    // the effects run together below (like the original already-started promises), the raw fullsize write included
    const promises: Effect.Effect<unknown, unknown>[] = [
      this.fx.media.generateThumbhash(decoded, baseOptions),
      this.fx.media.generateThumbnail(decoded, thumbnailOptions, thumbnailFile.path),
      this.fx.media.generateThumbnail(decoded, previewOptions, previewFile.path),
    ];

    let fullsizeFile: UpsertFileOptions | undefined;
    if (convertFullsize) {
      const fullsizeFormat = image.fullsize.format;
      this.warnOnTransparencyLoss(isTransparent, fullsizeFormat, asset.id);
      // convert a new fullsize image from the same source as the thumbnail
      fullsizeFile = this.getImageFile(asset, {
        fileType: AssetFileType.FullSize,
        format: fullsizeFormat,
        isEdited: useEdits,
        isProgressive: !!image.fullsize.progressive && fullsizeFormat !== ImageFormat.Webp,
        isTransparent,
      });
      const fullsizeOptions = {
        ...baseOptions,
        format: fullsizeFormat,
        quality: image.fullsize.quality,
        progressive: image.fullsize.progressive,
      };
      promises.push(this.fx.media.generateThumbnail(decoded, fullsizeOptions, fullsizeFile.path));
    } else if (generateFullsize && extracted && extracted.format === RawExtractedFormat.Jpeg) {
      fullsizeFile = this.getImageFile(asset, {
        fileType: AssetFileType.FullSize,
        format: extracted.format,
        isEdited: false,
        isProgressive: !!image.fullsize.progressive && image.fullsize.format !== ImageFormat.Webp,
        isTransparent,
      });
      this.storageCore.ensureFolders(fullsizeFile.path);

      // Write the buffer to disk with essential EXIF data
      const rawFullsizePath = fullsizeFile.path;
      promises.push(
        Effect.gen({ self: this }, function* () {
          yield* this.fx.storage.createOrOverwriteFile(rawFullsizePath, extracted.buffer);
          yield* this.fx.media.writeExif(
            {
              orientation: asset.exifInfo.orientation,
              colorspace: asset.exifInfo.colorspace,
            },
            rawFullsizePath,
          );
        }),
      );
    }

    const outputs = yield* Effect.all(promises, { concurrency: 'unbounded' });

    if (asset.exifInfo.projectionType === 'EQUIRECTANGULAR') {
      const promises = [
        this.fx.media.copyTagGroup('XMP-GPano', asset.originalPath, previewFile.path),
        fullsizeFile ? this.fx.media.copyTagGroup('XMP-GPano', asset.originalPath, fullsizeFile.path) : Effect.void,
      ];
      yield* Effect.all(promises, { concurrency: 'unbounded' });
    }

    const decodedDimensions = { width: info.width, height: info.height };
    const fullsizeDimensions = useEdits ? getOutputDimensions(asset.edits, decodedDimensions) : decodedDimensions;

    return {
      files: fullsizeFile ? [previewFile, thumbnailFile, fullsizeFile] : [previewFile, thumbnailFile],
      thumbhash: outputs[0] as Buffer,
      fullsizeDimensions,
    };
  });

  private readonly generateVideoThumbnailsFx = Effect.fn('MediaService.generateVideoThumbnails')(function* (
    this: MediaService,
    asset: ThumbnailAsset,
    { ffmpeg, image }: SystemConfig,
  ) {
    const previewFile = this.getImageFile(asset, {
      fileType: AssetFileType.Preview,
      format: image.preview.format,
      isEdited: false,
      isProgressive: false,
      isTransparent: false,
    });
    const thumbnailFile = this.getImageFile(asset, {
      fileType: AssetFileType.Thumbnail,
      format: image.thumbnail.format,
      isEdited: false,
      isProgressive: false,
      isTransparent: false,
    });
    this.storageCore.ensureFolders(previewFile.path);

    const { videoStream, format } = asset;
    if (!videoStream || !format) {
      return yield* Effect.fail(new Error(`Missing video metadata for asset ${asset.id}`));
    }

    const previewConfig = ThumbnailConfig.create({ ...ffmpeg, targetResolution: image.preview.size.toString() });
    const thumbConfig = ThumbnailConfig.create({ ...ffmpeg, targetResolution: image.thumbnail.size.toString() });
    const previewOptions = previewConfig.getCommand(TranscodeTarget.Video, videoStream, undefined, format ?? undefined);
    const thumbnailOptions = thumbConfig.getCommand(TranscodeTarget.Video, videoStream, undefined, format ?? undefined);

    yield* this.fx.media.transcode(asset.originalPath, previewFile.path, previewOptions);
    yield* this.fx.media.transcode(asset.originalPath, thumbnailFile.path, thumbnailOptions);

    const processInvalidImages = process.env.IMMICH_PROCESS_INVALID_IMAGES === 'true';
    const preview = yield* this.fx.media.decodeImage(previewFile.path, {
      colorspace: image.colorspace,
      processInvalidImages,
    });
    const thumbhash = yield* this.fx.media.generateThumbhash(preview, {
      colorspace: image.colorspace,
      processInvalidImages,
    });

    return {
      files: [previewFile, thumbnailFile],
      thumbhash,
      fullsizeDimensions: { width: videoStream.width, height: videoStream.height },
    };
  });

  @OnJob({ name: JobName.AssetEncodeVideoQueueAll, queue: QueueName.VideoConversion })
  async handleQueueVideoConversion(job: JobOf<JobName.AssetEncodeVideoQueueAll>): Promise<JobStatus> {
    const { force } = job;

    return runPromise(
      Effect.gen({ self: this }, function* () {
        const batches = batched(yield* this.fx.assetJob.streamForVideoConversion(force));
        yield* forEachBatch(batches, (assets) =>
          this.fx.job.queueAll(assets.map((asset) => ({ name: JobName.AssetEncodeVideo, data: { id: asset.id } }))),
        );

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.AssetEncodeVideo, queue: QueueName.VideoConversion })
  async handleVideoConversion({ id }: JobOf<JobName.AssetEncodeVideo>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const asset = yield* this.fx.assetJob.getForVideoConversion(id);
        if (!asset) {
          return JobStatus.Failed;
        }

        const input = asset.originalPath;
        const output = StorageCore.getEncodedVideoPath(asset);
        this.storageCore.ensureFolders(output);

        const { videoStream, format } = asset;
        const audioStream = asset.audioStream ?? undefined;
        if (!videoStream || !format) {
          this.logger.warn(`Skipped transcoding for asset ${asset.id}: missing metadata; re-run extraction first`);
          return JobStatus.Failed;
        }
        if (!videoStream.height || !videoStream.width) {
          this.logger.warn(`Skipped transcoding for asset ${asset.id}: no video dimensions`);
          return JobStatus.Failed;
        }

        let { ffmpeg } = yield* this.getConfigFx({ withCache: true });
        const target = this.getTranscodeTarget(ffmpeg, videoStream, audioStream);
        if (target === TranscodeTarget.None && !this.isRemuxRequired(ffmpeg, format)) {
          const encodedVideo = getAssetFile(asset.files, AssetFileType.EncodedVideo, { isEdited: false });
          if (encodedVideo) {
            this.logger.log(`Transcoded video exists for asset ${asset.id}, but is no longer required. Deleting...`);
            yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: [encodedVideo.path] } });
            yield* this.fx.asset.deleteFiles([encodedVideo]);
          } else {
            this.logger.verbose(`Asset ${asset.id} does not require transcoding based on current policy, skipping`);
          }

          return JobStatus.Skipped;
        }

        const command = BaseConfig.create(ffmpeg, this.videoInterfaces).getCommand(target, videoStream, audioStream);
        if (ffmpeg.accel === TranscodeHardwareAcceleration.Disabled) {
          this.logger.log(`Transcoding video ${asset.id} without hardware acceleration`);
        } else {
          this.logger.log(
            `Transcoding video ${asset.id} with ${ffmpeg.accel.toUpperCase()}-accelerated encoding and${ffmpeg.accelDecode ? '' : ' software'} decoding`,
          );
        }

        const isFailed = yield* this.fx.media.transcode(input, output, command).pipe(
          Effect.as(false),
          Effect.catch((error_) =>
            Effect.gen({ self: this }, function* () {
              const error: any = unwrapCause(error_);
              this.logger.error(`Error occurred during transcoding: ${error.message}`);
              if (ffmpeg.accel === TranscodeHardwareAcceleration.Disabled) {
                return true;
              }

              let isPartialFallbackSuccess = false;
              if (ffmpeg.accelDecode) {
                yield* Effect.gen({ self: this }, function* () {
                  this.logger.error(
                    `Retrying with ${ffmpeg.accel.toUpperCase()}-accelerated encoding and software decoding`,
                  );
                  ffmpeg = { ...ffmpeg, accelDecode: false };
                  const command = BaseConfig.create(ffmpeg, this.videoInterfaces).getCommand(
                    target,
                    videoStream,
                    audioStream,
                  );
                  yield* this.fx.media.transcode(input, output, command);
                  isPartialFallbackSuccess = true;
                }).pipe(
                  Effect.catchCause((cause) =>
                    Effect.sync(() => {
                      const error: any = unwrapCause(Cause.squash(cause));
                      this.logger.error(`Error occurred during transcoding: ${error.message}`);
                    }),
                  ),
                );
              }

              if (!isPartialFallbackSuccess) {
                this.logger.error(`Retrying with ${ffmpeg.accel.toUpperCase()} acceleration disabled`);
                ffmpeg = { ...ffmpeg, accel: TranscodeHardwareAcceleration.Disabled };
                const command = BaseConfig.create(ffmpeg, this.videoInterfaces).getCommand(
                  target,
                  videoStream,
                  audioStream,
                );
                yield* this.fx.media.transcode(input, output, command);
              }

              return false;
            }),
          ),
        );
        if (isFailed) {
          return JobStatus.Failed;
        }

        this.logger.log(`Successfully encoded ${asset.id}`);

        yield* this.fx.asset.upsertFile({
          assetId: asset.id,
          type: AssetFileType.EncodedVideo,
          path: output,
          isEdited: false,
        });

        return JobStatus.Success;
      }),
    );
  }

  private getTranscodeTarget(
    config: ConfigFFmpegDto,
    videoStream: VideoStreamInfo,
    audioStream?: AudioStreamInfo,
  ): TranscodeTarget {
    const isAudioTranscodeRequired = this.isAudioTranscodeRequired(config, audioStream);
    const isVideoTranscodeRequired = this.isVideoTranscodeRequired(config, videoStream);

    if (isAudioTranscodeRequired && isVideoTranscodeRequired) {
      return TranscodeTarget.All;
    }

    if (isAudioTranscodeRequired) {
      return TranscodeTarget.Audio;
    }

    if (isVideoTranscodeRequired) {
      return TranscodeTarget.Video;
    }

    return TranscodeTarget.None;
  }

  private isAudioTranscodeRequired(ffmpegConfig: ConfigFFmpegDto, stream?: AudioStreamInfo): boolean {
    if (!stream) {
      return false;
    }

    switch (ffmpegConfig.transcode) {
      case TranscodePolicy.Disabled: {
        return false;
      }
      case TranscodePolicy.All: {
        return true;
      }
      case TranscodePolicy.Required:
      case TranscodePolicy.Optimal:
      case TranscodePolicy.Bitrate: {
        return !ffmpegConfig.acceptedAudioCodecs.includes(stream.codecName as AudioCodec);
      }
      default: {
        throw new Error(`Unsupported transcode policy: ${ffmpegConfig.transcode}`);
      }
    }
  }

  private isVideoTranscodeRequired(ffmpegConfig: ConfigFFmpegDto, stream: VideoStreamInfo): boolean {
    const isScalingEnabled = ffmpegConfig.targetResolution !== 'original';
    const targetRes = Number.parseInt(ffmpegConfig.targetResolution);
    const isLargerThanTargetRes = isScalingEnabled && Math.min(stream.height, stream.width) > targetRes;
    const maxBitrate = this.parseBitrateToBps(ffmpegConfig.maxBitrate);
    const isLargerThanTargetBitrate = maxBitrate > 0 && stream.bitrate > maxBitrate;

    const isTargetVideoCodec = ffmpegConfig.acceptedVideoCodecs.includes(stream.codecName as VideoCodec);
    const isRequired = !isTargetVideoCodec || !stream.pixelFormat.endsWith('420p');

    switch (ffmpegConfig.transcode) {
      case TranscodePolicy.Disabled: {
        return false;
      }
      case TranscodePolicy.All: {
        return true;
      }
      case TranscodePolicy.Required: {
        return isRequired;
      }
      case TranscodePolicy.Optimal: {
        return isRequired || isLargerThanTargetRes;
      }
      case TranscodePolicy.Bitrate: {
        return isRequired || isLargerThanTargetBitrate;
      }
      default: {
        throw new Error(`Unsupported transcode policy: ${ffmpegConfig.transcode}`);
      }
    }
  }

  private isRemuxRequired(ffmpegConfig: ConfigFFmpegDto, { formatName, formatLongName }: VideoFormat): boolean {
    if (ffmpegConfig.transcode === TranscodePolicy.Disabled) {
      return false;
    }

    const formatLongNameMapping: Record<string, VideoContainer> = {
      'QuickTime / MOV': VideoContainer.Mov,
      'Matroska / WebM': VideoContainer.Webm,
    };

    const name = (formatLongName ? formatLongNameMapping[formatLongName] : undefined) ?? (formatName as VideoContainer);

    return name !== VideoContainer.Mp4 && !ffmpegConfig.acceptedContainers.includes(name);
  }

  isSRGB({
    colorspace,
    profileDescription,
    bitsPerSample,
  }: {
    colorspace: string | null;
    profileDescription: string | null;
    bitsPerSample: number | null;
  }): boolean {
    if (colorspace || profileDescription) {
      return [colorspace, profileDescription].some((s) => s?.toLowerCase().includes('srgb'));
    }
    if (bitsPerSample) {
      // assume sRGB for 8-bit images with no color profile or colorspace metadata
      return bitsPerSample === 8;
    }
    // assume sRGB for images with no relevant metadata
    return true;
  }

  private parseBitrateToBps(bitrateString: string) {
    const bitrateValue = Number.parseInt(bitrateString);

    if (Number.isNaN(bitrateValue)) {
      this.logger.log(`Maximum bitrate '${bitrateString} is not a number and will be ignored.`);
      return 0;
    }

    if (bitrateString.toLowerCase().endsWith('k')) {
      return bitrateValue * 1000; // Kilobits per second to bits per second
    }
    if (bitrateString.toLowerCase().endsWith('m')) {
      return bitrateValue * 1_000_000; // Megabits per second to bits per second
    }
    return bitrateValue;
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly shouldUseExtractedImageFx = Effect.fn('MediaService.shouldUseExtractedImage')(function* (
    this: MediaService,
    extractedPathOrBuffer: string | Buffer,
    targetSize: number,
  ) {
    const { width, height } = yield* this.fx.media.getImageMetadata(extractedPathOrBuffer);
    const extractedSize = Math.min(width, height);
    return extractedSize >= targetSize;
  });

  // kept as a Promise method: the spec calls it directly
  private async syncFiles(
    oldFiles: (AssetFile & { isProgressive: boolean; isTransparent: boolean })[],
    newFiles: UpsertFileOptions[],
  ) {
    return runPromise(this.syncFilesFx(oldFiles, newFiles));
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly syncFilesFx = Effect.fn('MediaService.syncFiles')(function* (
    this: MediaService,
    oldFiles: (AssetFile & { isProgressive: boolean; isTransparent: boolean })[],
    newFiles: UpsertFileOptions[],
  ) {
    const toUpsert: UpsertFileOptions[] = [];
    const pathsToDelete: string[] = [];
    const toDelete = new Set(oldFiles);

    for (const newFile of newFiles) {
      const existingFile = oldFiles.find((file) => file.type === newFile.type && file.isEdited === newFile.isEdited);
      if (existingFile) {
        toDelete.delete(existingFile);
      }

      // upsert new file path
      // eslint-disable-next-line unicorn/prefer-continue
      if (
        existingFile?.path !== newFile.path ||
        existingFile.isProgressive !== newFile.isProgressive ||
        existingFile.isTransparent !== newFile.isTransparent
      ) {
        toUpsert.push(newFile);

        // delete old file from disk
        if (existingFile && existingFile.path !== newFile.path) {
          this.logger.debug(
            `Deleting old ${newFile.type} image for asset ${newFile.assetId} in favor of a replacement`,
          );
          pathsToDelete.push(existingFile.path);
        }
      }
    }

    if (toUpsert.length > 0) {
      yield* this.fx.asset.upsertFiles(toUpsert);
    }

    if (toDelete.size > 0) {
      const toDeleteArray = [...toDelete];
      for (const file of toDeleteArray) {
        pathsToDelete.push(file.path);
      }
      yield* this.fx.asset.deleteFiles(toDeleteArray);
    }

    if (pathsToDelete.length > 0) {
      yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: pathsToDelete } });
    }
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private readonly generateEditedThumbnailsFx = Effect.fn('MediaService.generateEditedThumbnails')(function* (
    this: MediaService,
    asset: ThumbnailAsset,
    config: SystemConfig,
  ) {
    if (asset.type !== AssetType.Image || (asset.files.length === 0 && asset.edits.length === 0)) {
      return;
    }

    const generated = asset.edits.length > 0 ? yield* this.generateImageThumbnailsFx(asset, config, true) : undefined;

    return generated;
  });

  private warnOnTransparencyLoss(isTransparent: boolean, format: ImageFormat, assetId: string) {
    if (isTransparent && format === ImageFormat.Jpeg) {
      this.logger.warn(
        `Asset ${assetId} has transparency but the configured format is ${format} which does not support it, consider using a format that does, such as ${ImageFormat.Webp}`,
      );
    }
  }

  private getImageFile(
    asset: ThumbnailPathEntity,
    options: ImagePathOptions & { isProgressive: boolean; isTransparent: boolean },
  ) {
    const path = StorageCore.getImagePath(asset, options);
    return {
      assetId: asset.id,
      type: options.fileType,
      path,
      isEdited: options.isEdited,
      isProgressive: options.isProgressive,
      isTransparent: options.isTransparent,
    };
  }
}
