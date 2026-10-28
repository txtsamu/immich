import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer, Result } from 'effect';
import { ExifDateTime, WriteTags, exiftool } from 'exiftool-vendored';
import ffmpeg, { FfprobeData, FfprobeStream } from 'fluent-ffmpeg';
import { camelCase, upperFirst } from 'lodash-es';
import { Duration } from 'luxon';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import { Writable } from 'node:stream';
import sharp, { Sharp } from 'sharp';
import type {
  Bitmap,
  DecodeToBufferOptions,
  GenerateThumbhashOptions,
  GenerateThumbnailOptions,
  ImageDimensions,
  ProbeOptions,
  TranscodeCommand,
  TransformOptions,
  VideoInfo,
  VideoPacketInfo,
} from 'src/types.js';
import { ORIENTATION_TO_SHARP_ROTATION } from 'src/constants.js';
import { Exif } from 'src/database.js';
import { AssetEditActionItem } from 'src/dtos/editing.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import {
  AacProfile,
  Av1Profile,
  ColorMatrix,
  ColorPrimaries,
  ColorTransfer,
  Colorspace,
  DvProfile,
  DvSignalCompatibility,
  H264Profile,
  HevcProfile,
  LogLevel,
  RawExtractedFormat,
} from 'src/enum.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { handlePromiseError } from 'src/utils/misc.js';
import { createAffineMatrix } from 'src/utils/transform.js';

const probe = (input: string, options: string[]): Promise<FfprobeData> =>
  new Promise((resolve, reject) =>
    // eslint-disable-next-line import-x/no-named-as-default-member
    ffmpeg.ffprobe(input, options, (error, data) => (error ? reject(error) : resolve(data))),
  );

const pascalCase = (str: string) => upperFirst(camelCase(str.toLowerCase()));

type ProgressEvent = {
  frames: number;
  currentFps: number;
  currentKbps: number;
  targetSize: number;
  timemark: string;
  percent?: number;
};

export type ExtractResult = {
  buffer: Buffer;
  format: RawExtractedFormat;
};

const libPromise = <A>(run: () => PromiseLike<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new LibraryError({ cause }) });

function edit(pipeline: Sharp, edits: AssetEditActionItem[]): Sharp {
  const crop = edits.find((edit) => edit.action === 'crop');
  if (crop) {
    pipeline = pipeline.extract({
      left: Math.round(crop.parameters.x),
      top: Math.round(crop.parameters.y),
      width: Math.round(crop.parameters.width),
      height: Math.round(crop.parameters.height),
    });
  }

  const affineEditOperations = edits.filter((edit) => edit.action !== 'crop');
  if (affineEditOperations.length > 0) {
    const { a, b, c, d } = createAffineMatrix(affineEditOperations);
    pipeline = pipeline.affine([
      [a, b],
      [c, d],
    ]);
  }

  return pipeline;
}

function getImageDecodingPipeline(input: string | Buffer, options: DecodeToBufferOptions) {
  // some invalid images can still be processed by sharp, but we want to fail on them by default to avoid crashes
  let pipeline = encoded(input, options.processInvalidImages ? 'none' : 'error')
    .pipelineColorspace(getPipelineColorspace(options.colorspace))
    .withIccProfile(options.colorspace);

  const { angle, flip, flop } = options.orientation ? ORIENTATION_TO_SHARP_ROTATION[options.orientation] : {};
  pipeline = pipeline.rotate(angle);
  if (flip) {
    pipeline = pipeline.flip();
  }

  if (flop) {
    pipeline = pipeline.flop();
  }

  return pipeline;
}

function getPipelineColorspace(colorspace: string) {
  return colorspace === Colorspace.Srgb ? 'srgb' : 'rgb16';
}

/* Resamples in linear light; averaging gamma-encoded values darkens the result and loses detail. `colorspace`
 * always has a sRGB transfer function and scRGB applies no primaries matrix, so linearising as sRGB is exact. */
function transform(image: Bitmap, { size, fit = 'outside', edits = [] }: TransformOptions): Promise<Bitmap> {
  if (!size && edits.length === 0) {
    return Promise.resolve(image);
  }

  return edit(raw(image).pipelineColorspace('scrgb'), edits)
    .resize(size, size, { fit, withoutEnlargement: true })
    .raw()
    .toBuffer({ resolveWithObject: true });
}

function raw({ data, info: raw }: Bitmap) {
  return sharp(data, { raw, limitInputChannels: false, limitInputPixels: false, unlimited: true });
}

function encoded(image: string | Buffer, failOn: 'none' | 'error') {
  return sharp(image, { failOn, limitInputChannels: false, limitInputPixels: false, unlimited: true });
}

/** Re-attaches the profile, converting nothing: the pixels are already in that colourspace. */
function tag(image: Bitmap, colorspace: string): Sharp {
  return raw(image).pipelineColorspace(getPipelineColorspace(colorspace)).withIccProfile(colorspace);
}

function parseInteger(value: string | number | undefined): number {
  return Number.parseInt(value as string) || 0;
}

function parseFloating(value: string | number | undefined): number {
  // eslint-disable-next-line unicorn/prefer-number-coercion
  return Number.parseFloat(value as string) || 0;
}

function parseOptionalInt(value: string | number | undefined): number | null {
  const parsed = Number.parseInt(value as string);
  return Number.isNaN(parsed) ? null : parsed;
}

function parseEnum<E extends Record<string, number | string>>(enumObj: E, value?: string) {
  return value && typeof value === 'string'
    ? ((enumObj[pascalCase(value)] as Extract<E[keyof E], number> | undefined) ?? null)
    : null;
}

/** Parse a rational like "60000/1001" or "1/600" into `{ num, den }`. */
function parseRational(value: string | undefined): { num: number; den: number } | null {
  if (value) {
    const [num, den = 1] = value.split('/').map(Number);
    if (num && den) {
      return { num, den };
    }
  }
  return null;
}

function parseFrameRate(value: string | undefined): number | null {
  const r = parseRational(value);
  return r ? r.num / r.den : null;
}

function getDar(dar: string | undefined): number {
  if (dar) {
    const [darW, darH] = dar.split(':').map(Number);
    if (darW && darH) {
      return darW / darH;
    }
  }

  return 0;
}

function parseVideoProfile(codec?: string, profile?: string) {
  switch (codec) {
    case 'h264': {
      return parseEnum(H264Profile, profile);
    }
    case 'h265':
    case 'hevc': {
      return parseEnum(HevcProfile, profile);
    }
    case 'av1': {
      return parseEnum(Av1Profile, profile);
    }
    default: {
      return null;
    }
  }
}

function compareStreams(a: FfprobeStream, b: FfprobeStream): number {
  const d = (b.disposition?.default ?? 0) - (a.disposition?.default ?? 0);
  if (d !== 0) {
    return d;
  }
  return parseInteger(b.bit_rate) - parseInteger(a.bit_rate);
}

/* Ported from https://code.ffmpeg.org/FFmpeg/FFmpeg/src/commit/5c44245878e235ae64fe87fb9877644856d33d1d/fftools/ffmpeg_filter.c
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright (c) FFmpeg authors and contributors — https://ffmpeg.org/
 * Modifications: TS port operating on probe-derived packet metadata rather than decoded AVFrames. */
function cfrOutputFrames(packets: { pts: number; duration: number }[], slotsPerTick: number) {
  packets.sort((a, b) => a.pts - b.pts);
  const firstPts = packets[0].pts;
  let outputFrames = 0;
  let nextPts = 0;
  const history = [0, 0, 0];
  for (const pkt of packets) {
    const syncIpts = (pkt.pts - firstPts) * slotsPerTick;
    const duration = pkt.duration * slotsPerTick;
    let delta0 = syncIpts - nextPts;
    const delta = delta0 + duration;

    if (delta0 < 0 && delta > 0) {
      delta0 = 0;
    }

    let nb = 1;
    let nbPrev = 0;
    if (delta < -1.1) {
      nb = 0;
    } else if (delta > 1.1) {
      nb = Math.round(delta);
      if (delta0 > 1.1) {
        nbPrev = Math.round(delta0 - 0.6);
      }
    }
    outputFrames += nb;
    nextPts += nb;
    history[2] = history[1];
    history[1] = history[0];
    history[0] = nbPrev;
  }
  const median = history.sort((a, b) => a - b)[1];
  return outputFrames + median;
}

function configureFfmpegCall(
  logger: LoggingRepository,
  input: string,
  output: string | Writable,
  options: TranscodeCommand,
) {
  const ffmpegCall = ffmpeg(input, { niceness: 10 })
    .inputOptions(options.inputOptions)
    .outputOptions(options.outputOptions)
    .output(output)
    .on('start', (command: string) => logger.debug(command))
    .on('error', (error, _, stderr) => logger.error(stderr || error));

  const { frameCount, percentInterval } = options.progress;
  const frameInterval = Math.ceil(frameCount / (100 / percentInterval));
  if (logger.isLevelEnabled(LogLevel.Debug) && frameCount && frameInterval) {
    let lastProgressFrame: number = 0;
    ffmpegCall.on('progress', (progress: ProgressEvent) => {
      if (progress.frames - lastProgressFrame < frameInterval) {
        return;
      }

      lastProgressFrame = progress.frames;
      const percent = ((progress.frames / frameCount) * 100).toFixed(2);
      const ms = progress.currentFps ? Math.floor((frameCount - progress.frames) / progress.currentFps) * 1000 : 0;
      const duration = ms ? Duration.fromMillis(ms).rescale().toHuman({ unitDisplay: 'narrow' }) : '';
      const outputText = output instanceof Writable ? 'stream' : output.split('/').pop();
      logger.debug(
        `Transcoding ${percent}% done${duration ? `, estimated ${duration} remaining` : ''} for output ${outputText}`,
      );
    });
  }

  return ffmpegCall;
}

const toVideoInfo = (results: FfprobeData, options?: ProbeOptions): VideoInfo => {
  return {
    format: {
      formatName: results.format.format_name,
      formatLongName: results.format.format_long_name,
      duration: parseFloating(results.format.duration),
      bitrate: parseInteger(results.format.bit_rate),
    },
    videoStreams: results.streams
      .filter((stream) => stream.codec_type === 'video' && !stream.disposition?.attached_pic)
      .sort((a, b) => compareStreams(a, b))
      .map((stream) => {
        const height = parseInteger(stream.height);
        const dar = getDar(stream.display_aspect_ratio);
        return {
          index: stream.index,
          height,
          width: dar ? Math.round(height * dar) : parseInteger(stream.width),
          codecName: stream.codec_name === 'h265' ? 'hevc' : (stream.codec_name ?? null),
          profile: parseVideoProfile(stream.codec_name, stream.profile as string | undefined) ?? null,
          level: parseOptionalInt(stream.level),
          frameCount: parseInteger(options?.countFrames ? stream.nb_read_packets : stream.nb_frames),
          frameRate: parseFrameRate(stream.avg_frame_rate ?? stream.r_frame_rate),
          timeBase: parseRational(stream.time_base)?.den ?? null,
          rotation: parseInteger(stream.rotation),
          bitrate: parseInteger(stream.bit_rate),
          pixelFormat: stream.pix_fmt || 'yuv420p',
          colorPrimaries: parseEnum(ColorPrimaries, stream.color_primaries) ?? ColorPrimaries.Unknown,
          colorMatrix: parseEnum(ColorMatrix, stream.color_space) ?? ColorMatrix.Unknown,
          colorTransfer: parseEnum(ColorTransfer, stream.color_transfer) ?? ColorTransfer.Unknown,
          dvProfile: parseOptionalInt(stream.dv_profile) as DvProfile | null,
          dvLevel: parseOptionalInt(stream.dv_level),
          dvBlSignalCompatibilityId: parseOptionalInt(
            stream.dv_bl_signal_compatibility_id,
          ) as DvSignalCompatibility | null,
        };
      }),
    audioStreams: results.streams
      .filter((stream) => stream.codec_type === 'audio' && stream.codec_name !== 'unknown')
      .sort((a, b) => compareStreams(a, b))
      .map((stream) => ({
        index: stream.index,
        codecName: stream.codec_name ?? null,
        profile: stream.codec_name === 'aac' ? parseEnum(AacProfile, stream.profile as string | undefined) : null,
        bitrate: parseInteger(stream.bit_rate),
      })),
  };
};

export class MediaRepo extends Context.Service<
  MediaRepo,
  {
    /** @param input file path to the input image; resolves to `null` if no embedded preview could be extracted */
    extract(input: string): Effect.Effect<ExtractResult | null>;
    /** Never fails: a write error is logged and reported as `false`. */
    writeExif(tags: Partial<Exif>, output: string): Effect.Effect<boolean>;
    /** Never fails: a copy error is logged and reported as `false`. */
    copyTagGroup(tagGroup: string, source: string, target: string): Effect.Effect<boolean>;
    decodeImage(input: string | Buffer, options: DecodeToBufferOptions): Effect.Effect<Bitmap, LibraryError>;
    generateThumbnail(
      image: Bitmap,
      options: GenerateThumbnailOptions,
      output: string,
    ): Effect.Effect<void, LibraryError>;
    generateThumbhash(image: Bitmap, options: GenerateThumbhashOptions): Effect.Effect<Buffer, LibraryError>;
    probe(input: string, options?: ProbeOptions): Effect.Effect<VideoInfo, LibraryError>;
    probePackets(input: string, streamIndex: number): Effect.Effect<VideoPacketInfo | null, LibraryError>;
    transcode(input: string, output: string | Writable, options: TranscodeCommand): Effect.Effect<void, LibraryError>;
    getImageMetadata(input: string | Buffer): Effect.Effect<ImageDimensions & { isTransparent: boolean }, LibraryError>;
  }
>()('immich/repositories/MediaRepo') {
  static readonly make = (logger: LoggingRepository) => {
    // eslint-disable-next-line import-x/no-named-as-default-member
    sharp.concurrency(0);
    // eslint-disable-next-line import-x/no-named-as-default-member
    sharp.cache({ files: 0 });

    return MediaRepo.of({
      extract: Effect.fn('Media.extract')(function* (input: string) {
        for (const { tag, format } of [
          { tag: 'JpgFromRaw2', format: RawExtractedFormat.Jpeg },
          { tag: 'JpgFromRaw', format: RawExtractedFormat.Jpeg },
          { tag: 'PreviewJXL', format: RawExtractedFormat.Jxl },
          { tag: 'PreviewImage', format: RawExtractedFormat.Jpeg },
        ]) {
          const result = yield* Effect.result(libPromise(() => exiftool.extractBinaryTagToBuffer(tag, input)));
          if (Result.isSuccess(result)) {
            logger.debug(`Successfully extracted ${tag} buffer from image`);
            return { buffer: result.success, format };
          }
          logger.debug(`Could not extract ${tag} buffer from image: ${result.failure.cause}`);
        }
        return null;
      }),

      writeExif: Effect.fn('Media.writeExif')((tags: Partial<Exif>, output: string) =>
        libPromise(async () => {
          const tagsToWrite: WriteTags = {
            ExifImageWidth: tags.exifImageWidth,
            ExifImageHeight: tags.exifImageHeight,
            DateTimeOriginal: tags.dateTimeOriginal && ExifDateTime.fromMillis(tags.dateTimeOriginal.getTime()),
            ModifyDate: tags.modifyDate && ExifDateTime.fromMillis(tags.modifyDate.getTime()),
            TimeZone: tags.timeZone,
            GPSLatitude: tags.latitude,
            GPSLongitude: tags.longitude,
            ProjectionType: tags.projectionType,
            City: tags.city,
            Country: tags.country,
            Make: tags.make,
            Model: tags.model,
            LensModel: tags.lensModel,
            Fnumber: tags.fNumber?.toFixed(1),
            FocalLength: tags.focalLength?.toFixed(1),
            ISO: tags.iso,
            ExposureTime: tags.exposureTime,
            ProfileDescription: tags.profileDescription,
            ColorSpace: tags.colorspace,
            Rating: tags.rating === null ? 0 : tags.rating,
            // specially convert Orientation to numeric Orientation# for exiftool
            'Orientation#': tags.orientation ? Number(tags.orientation) : undefined,
          };

          await exiftool.write(output, tagsToWrite, {
            ignoreMinorErrors: true,
            writeArgs: ['-overwrite_original'],
          });
          return true;
        }).pipe(
          Effect.catch(({ cause }) =>
            Effect.sync(() => {
              logger.warn(`Could not write exif data to image: ${(cause as Error).message}`);
              return false;
            }),
          ),
        ),
      ),

      copyTagGroup: Effect.fn('Media.copyTagGroup')((tagGroup: string, source: string, target: string) =>
        libPromise(async () => {
          await exiftool.write(
            target,
            {},
            {
              ignoreMinorErrors: true,
              writeArgs: ['-TagsFromFile', source, `-${tagGroup}:all>${tagGroup}:all`, '-overwrite_original'],
            },
          );
          return true;
        }).pipe(
          Effect.catch(({ cause }) =>
            Effect.sync(() => {
              logger.warn(`Could not copy tag data to image: ${(cause as Error).message}`);
              return false;
            }),
          ),
        ),
      ),

      decodeImage: Effect.fn('Media.decodeImage')((input: string | Buffer, options: DecodeToBufferOptions) =>
        libPromise(async () => {
          const decoded = await getImageDecodingPipeline(input, options).raw().toBuffer({ resolveWithObject: true });
          return await transform(decoded, options);
        }),
      ),

      generateThumbnail: Effect.fn('Media.generateThumbnail')(
        (image: Bitmap, options: GenerateThumbnailOptions, output: string) =>
          libPromise(async () => {
            const transformed = await transform(image, options);
            await tag(transformed, options.colorspace)
              .toFormat(options.format, {
                quality: options.quality,
                // this is default in libvips (except the threshold is 90), but we need to set it manually in sharp
                chromaSubsampling: options.quality >= 80 ? '4:4:4' : '4:2:0',
                progressive: options.progressive,
              })
              .toFile(output);
          }),
      ),

      generateThumbhash: Effect.fn('Media.generateThumbhash')((image: Bitmap, options: GenerateThumbhashOptions) =>
        libPromise(async () => {
          const { rgbaToThumbHash } = await import('thumbhash');

          const transformed = await transform(image, { edits: options.edits, fit: 'inside', size: 100 });

          const { data, info } = await sharp(transformed.data, { raw: transformed.info })
            .ensureAlpha()
            .raw()
            .toBuffer({ resolveWithObject: true });

          return Buffer.from(rgbaToThumbHash(info.width, info.height, data));
        }),
      ),

      probe: Effect.fn('Media.probe')((input: string, options?: ProbeOptions) =>
        libPromise(async () => {
          const results = await probe(input, options?.countFrames ? ['-count_packets'] : []); // gets frame count quickly: https://stackoverflow.com/a/28376817
          return toVideoInfo(results, options);
        }),
      ),

      /**
       * Needed for accurate segments, especially when remuxing, seeking and/or VFR is involved.
       * Scanning packets for keyframes in JS is much faster than -skip_frame nokey since it avoids decoding the video.
       */
      probePackets: Effect.fn('Media.probePackets')((input: string, streamIndex: number) =>
        libPromise(() => {
          const ffprobe = spawn(
            'ffprobe',
            [
              '-v',
              'error',
              '-select_streams',
              String(streamIndex),
              '-show_entries',
              'packet=pts,duration,flags',
              '-of',
              'csv=p=0',
              input,
            ],
            { stdio: ['ignore', 'pipe', 'pipe'] },
          );

          let totalDuration = 0;
          const keyframePts: number[] = [];
          const keyframeAccDuration: number[] = [];
          const keyframeOwnDuration: number[] = [];
          const postDiscard: { pts: number; duration: number }[] = [];
          const parseLine = (line: string) => {
            if (!line) {
              return;
            }
            const [ptsStr, durationStr, flags] = line.split(',', 3);
            const pts = Number.parseInt(ptsStr);
            const duration = Number.parseInt(durationStr);
            if (Number.isNaN(pts) || Number.isNaN(duration) || !flags) {
              return;
            }
            // Discarded packets don't contribute to packet count, but still contribute to video duration
            totalDuration += duration;
            if (flags[1] !== 'D') {
              postDiscard.push({ pts, duration });
            }

            if (flags[0] !== 'K') {
              return;
            }

            keyframePts.push(pts);
            keyframeAccDuration.push(totalDuration);
            // VFR content can have variable duration keyframes,
            // so we need to track their duration separately for accurate segment boundaries.
            // Non-keyframes are accounted for in totalDuration.
            keyframeOwnDuration.push(duration);
          };

          let stderr = '';
          let remainder = '';
          ffprobe.stderr.setEncoding('utf8');
          ffprobe.stderr.on('data', (chunk: string) => (stderr += chunk));
          ffprobe.stdout.setEncoding('utf8');
          ffprobe.stdout.on('data', (chunk: string) => {
            const lines = chunk.split('\n');
            lines[0] = remainder + lines[0];
            remainder = lines.pop() as string;
            for (const line of lines) {
              parseLine(line);
            }
          });

          return new Promise<VideoPacketInfo | null>((resolve, reject) => {
            ffprobe.on('error', reject);
            ffprobe.on('close', (code) => {
              if (code !== 0) {
                return reject(new Error(`ffprobe exited with code ${code}: ${stderr.trim()}`));
              }
              parseLine(remainder);
              if (postDiscard.length === 0) {
                return resolve(null);
              }

              resolve({
                totalDuration,
                packetCount: postDiscard.length,
                outputFrames: cfrOutputFrames(postDiscard, postDiscard.length / totalDuration),
                keyframePts,
                keyframeAccDuration,
                keyframeOwnDuration,
              });
            });
          });
        }),
      ),

      transcode: Effect.fn('Media.transcode')((input: string, output: string | Writable, options: TranscodeCommand) =>
        libPromise(() => {
          if (!options.twoPass) {
            return new Promise<void>((resolve, reject) => {
              configureFfmpegCall(logger, input, output, options)
                .on('error', reject)
                .on('end', () => resolve())
                .run();
            });
          }

          if (typeof output !== 'string') {
            throw new TypeError('Two-pass transcoding does not support writing to a stream');
          }

          // two-pass allows for precise control of bitrate at the cost of running twice
          // recommended for vp9 for better quality and compression
          return new Promise<void>((resolve, reject) => {
            // first pass output is not saved as only the .log file is needed
            configureFfmpegCall(logger, input, '/dev/null', options)
              .addOptions('-pass', '1')
              .addOptions('-passlogfile', output)
              .addOptions('-f null')
              .on('error', reject)
              .on('end', () => {
                // second pass
                configureFfmpegCall(logger, input, output, options)
                  .addOptions('-pass', '2')
                  .addOptions('-passlogfile', output)
                  .on('error', reject)
                  .on('end', () => handlePromiseError(fs.unlink(`${output}-0.log`), logger))
                  .on('end', () => handlePromiseError(fs.rm(`${output}-0.log.mbtree`, { force: true }), logger))
                  .on('end', () => resolve())
                  .run();
              })
              .run();
          });
        }),
      ),

      getImageMetadata: Effect.fn('Media.getImageMetadata')((input: string | Buffer) =>
        libPromise(async () => {
          const { width = 0, height = 0, hasAlpha = false } = await sharp(input, { unlimited: true }).metadata();
          return { width, height, isTransparent: hasAlpha };
        }),
      ),
    });
  };

  static readonly layer = (logger: LoggingRepository) => Layer.sync(MediaRepo, () => MediaRepo.make(logger));
}

/** Promise-facing adapter for the Nest services. Delegates to `MediaRepo`. */
@Injectable()
export class MediaRepository {
  private readonly effect: Context.Service.Shape<typeof MediaRepo>;

  constructor(private logger: LoggingRepository) {
    this.logger.setContext(MediaRepository.name);
    this.effect = MediaRepo.make(this.logger);
    registerEffect(this, this.effect);
  }

  /**
   *
   * @param input file path to the input image
   * @returns ExtractResult if succeeded, or null if failed
   */
  extract(input: string): Promise<ExtractResult | null> {
    return runPromise(this.effect.extract(input));
  }

  writeExif(tags: Partial<Exif>, output: string): Promise<boolean> {
    return runPromise(this.effect.writeExif(tags, output));
  }

  copyTagGroup(tagGroup: string, source: string, target: string): Promise<boolean> {
    return runPromise(this.effect.copyTagGroup(tagGroup, source, target));
  }

  decodeImage(input: string | Buffer, options: DecodeToBufferOptions): Promise<Bitmap> {
    return runPromise(this.effect.decodeImage(input, options));
  }

  /** Pure sharp pipeline helper; kept on the adapter because `media.repository.spec.ts` exercises it directly. */
  private edit(pipeline: Sharp, edits: AssetEditActionItem[]): Sharp {
    return edit(pipeline, edits);
  }

  generateThumbnail(image: Bitmap, options: GenerateThumbnailOptions, output: string): Promise<void> {
    return runPromise(this.effect.generateThumbnail(image, options, output));
  }

  generateThumbhash(image: Bitmap, options: GenerateThumbhashOptions): Promise<Buffer> {
    return runPromise(this.effect.generateThumbhash(image, options));
  }

  probe(input: string, options?: ProbeOptions): Promise<VideoInfo> {
    return runPromise(this.effect.probe(input, options));
  }

  probePackets(input: string, streamIndex: number): Promise<VideoPacketInfo | null> {
    return runPromise(this.effect.probePackets(input, streamIndex));
  }

  transcode(input: string, output: string | Writable, options: TranscodeCommand): Promise<void> {
    return runPromise(this.effect.transcode(input, output, options));
  }

  getImageMetadata(input: string | Buffer): Promise<ImageDimensions & { isTransparent: boolean }> {
    return runPromise(this.effect.getImageMetadata(input));
  }
}
