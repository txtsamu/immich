import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { exiftool } from 'exiftool-vendored';
import { exec as execCallback } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import sharp from 'sharp';
import { FileSystemError, LibraryError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { ReleaseChannel } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';

export interface VersionResponse {
  version: string;
  published_at: string;
}

export interface ServerBuildVersions {
  nodejs: string;
  ffmpeg: string;
  libvips: string;
  exiftool: string;
  imagemagick: string;
}

const exec = promisify(execCallback);
const maybeFirstLine = async (command: string): Promise<string> => {
  try {
    const { stdout } = await exec(command);
    return stdout.trim().split('\n', 1)[0] || '';
  } catch {
    return '';
  }
};

type BuildLockfile = {
  sources: Array<{ name: string; version: string }>;
  packages: Array<{ name: string; version: string }>;
};

const getLockfileVersion = (name: string, lockfile?: BuildLockfile) => {
  if (!lockfile) {
    return;
  }

  const items = [...(lockfile.sources || []), ...(lockfile?.packages || [])];
  const item = items.find((item) => item.name === name);
  return item?.version;
};

/** Holder for the memoized build versions; the Nest adapter exposes it as its public `buildVersions` field. */
type BuildVersionsCache = { buildVersions?: ServerBuildVersions };

export class ServerInfoRepo extends Context.Service<
  ServerInfoRepo,
  {
    getLatestRelease(channel: ReleaseChannel): Effect.Effect<VersionResponse, Error | LibraryError>;
    getBuildVersions(): Effect.Effect<ServerBuildVersions, LibraryError>;
  }
>()('immich/repositories/ServerInfoRepo') {
  static readonly make = (configRepository: ConfigRepository, logger: LoggingRepository, cache: BuildVersionsCache) => {
    const retrieveVersionFallback = (
      command: string,
      commandTransform?: (output: string) => string,
      version?: string,
    ) =>
      Effect.promise(async () => {
        if (!version) {
          const output = await maybeFirstLine(command);
          version = commandTransform ? commandTransform(output) : output;
        }
        return version;
      });

    return ServerInfoRepo.of({
      getLatestRelease: Effect.fn('ServerInfo.getLatestRelease')(function* (channel: ReleaseChannel) {
        const response = yield* Effect.tryPromise({
          try: async () => {
            const { versionCheck } = configRepository.getEnv();
            const url = new URL(versionCheck.url);
            switch (channel) {
              case ReleaseChannel.Stable: {
                url.searchParams.append('channel', 'stable');
                break;
              }
              case ReleaseChannel.ReleaseCandidate: {
                url.searchParams.append('channel', 'rc');
                break;
              }
            }
            const response = await fetch(url);

            if (!response.ok) {
              throw new Error(`Version check request failed with status ${response.status}: ${await response.text()}`);
            }

            return response;
          },
          catch: (error) => new Error('Failed to fetch latest release', { cause: error }),
        });

        // the original returned the json() promise un-awaited inside its try, so a body parse failure was not wrapped
        return yield* Effect.tryPromise({
          try: (): Promise<VersionResponse> => response.json(),
          catch: (cause) => new LibraryError({ cause }),
        });
      }),

      getBuildVersions: Effect.fn('ServerInfo.getBuildVersions')(function* () {
        if (!cache.buildVersions) {
          const { nodeVersion, resourcePaths } = configRepository.getEnv();

          const lockfile = yield* Effect.tryPromise({
            try: () =>
              readFile(resourcePaths.lockFile).then((buffer) => JSON.parse(buffer.toString()) as BuildLockfile),
            catch: (cause) => new FileSystemError({ cause }),
          }).pipe(
            Effect.catch(() =>
              Effect.sync((): BuildLockfile | undefined => {
                logger.warn(`Failed to read ${resourcePaths.lockFile}`);
                return undefined;
              }),
            ),
          );

          const [nodejsVersion, ffmpegVersion, magickVersion, exiftoolVersion] = yield* Effect.all(
            [
              retrieveVersionFallback('node --version', undefined, nodeVersion),
              retrieveVersionFallback(
                'ffmpeg -version',
                (output) => output.replaceAll('ffmpeg version ', ''),
                getLockfileVersion('ffmpeg', lockfile),
              ),
              retrieveVersionFallback(
                'magick --version',
                (output) => output.replaceAll('Version: ImageMagick ', ''),
                getLockfileVersion('imagemagick', lockfile),
              ),
              Effect.tryPromise({ try: () => exiftool.version(), catch: (cause) => new LibraryError({ cause }) }),
            ],
            { concurrency: 'unbounded' },
          );

          // eslint-disable-next-line import-x/no-named-as-default-member
          const libvipsVersion = getLockfileVersion('libvips', lockfile) || sharp.versions.vips;

          cache.buildVersions = {
            nodejs: nodejsVersion,
            exiftool: exiftoolVersion,
            ffmpeg: ffmpegVersion,
            libvips: libvipsVersion,
            imagemagick: magickVersion,
          };
        }

        return cache.buildVersions;
      }),
    });
  };

  /** Unlike the database-backed services there is no shared `Db`; the deps are supplied by the caller. */
  static readonly layer = (configRepository: ConfigRepository, logger: LoggingRepository) =>
    Layer.succeed(ServerInfoRepo, ServerInfoRepo.make(configRepository, logger, {}));
}

/** Promise-facing adapter for the Nest services. Delegates to `ServerInfoRepo`. */
@Injectable()
export class ServerInfoRepository {
  private readonly effect: Context.Service.Shape<typeof ServerInfoRepo>;

  buildVersions?: ServerBuildVersions;

  constructor(
    private configRepository: ConfigRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(ServerInfoRepository.name);
    this.effect = ServerInfoRepo.make(this.configRepository, this.logger, this);
  }

  getLatestRelease(channel: ReleaseChannel): Promise<VersionResponse> {
    return runPromise(this.effect.getLatestRelease(channel));
  }

  getBuildVersions(): Promise<ServerBuildVersions> {
    return runPromise(this.effect.getBuildVersions());
  }
}
