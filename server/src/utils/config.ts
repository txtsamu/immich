import AsyncLock from 'async-lock';
import { Context, Effect, Exit } from 'effect';
import { load as loadYaml } from 'js-yaml';
import { cloneDeep, get, isEmpty, isEqual, set } from 'lodash-es';
import type { DeepPartial } from 'src/types.js';
import { AdminConfigDto, SystemConfig, defaults } from 'src/dtos/config.dto.js';
import { effectOf } from 'src/effect/bridge.js';
import { runPromise } from 'src/effect/run.js';
import { DatabaseLock, SystemMetadataKey } from 'src/enum.js';
import { ConfigRepo, ConfigRepository } from 'src/repositories/config.repository.js';
import { LoggingRepo, LoggingRepository } from 'src/repositories/logging.repository.js';
import { SystemMetadataRepo, SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { getKeysDeep, unsetDeep } from 'src/utils/misc.js';

type RepoDeps = {
  configRepo: ConfigRepository;
  metadataRepo: SystemMetadataRepository;
  logger: LoggingRepository;
};

/** The Effect services behind `RepoDeps`. */
export type RepoFxDeps = {
  configRepo: Context.Service.Shape<typeof ConfigRepo>;
  metadataRepo: Context.Service.Shape<typeof SystemMetadataRepo>;
  logger: Context.Service.Shape<typeof LoggingRepo>;
};

const toFx = (repos: RepoDeps): RepoFxDeps => ({
  configRepo: effectOf(ConfigRepo, repos.configRepo),
  metadataRepo: effectOf(SystemMetadataRepo, repos.metadataRepo),
  logger: effectOf(LoggingRepo, repos.logger),
});

/** Errors that wrap the original thrown value in `cause` (see `src/effect/errors.ts`). */
const unwrapCause = (error: unknown) =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error ? error.cause : error;

const asyncLock = new AsyncLock();
let config: SystemConfig | null = null;
let lastUpdated: number | null = null;

export const clearConfigCache = () => {
  config = null;
  lastUpdated = null;
};

export const getConfigFx = (repos: RepoFxDeps, { withCache }: { withCache: boolean }) =>
  Effect.gen(function* () {
    if (!withCache || !config) {
      const timestamp = lastUpdated;
      // the lock serializes rebuilds across every caller (Promise or Effect); the build runs inside it
      const failure = yield* Effect.promise(() =>
        asyncLock.acquire(DatabaseLock[DatabaseLock.GetSystemConfig], async () => {
          if (timestamp !== lastUpdated) {
            return;
          }

          const exit = await Effect.runPromiseExit(buildConfig(repos));
          if (Exit.isFailure(exit)) {
            return exit.cause;
          }

          config = exit.value;
          lastUpdated = Date.now();
        }),
      );
      if (failure) {
        return yield* Effect.failCause(failure);
      }
    }

    return config!;
  });

export const updateConfigFx = (repos: RepoFxDeps, newConfig: SystemConfig) =>
  Effect.gen(function* () {
    const { metadataRepo } = repos;
    // get the difference between the new config and the default config
    const partialConfig: DeepPartial<SystemConfig> = {};
    for (const property of getKeysDeep(defaults)) {
      const newValue = get(newConfig, property);
      const isEmpty = [undefined, null, ''].includes(newValue);
      const defaultValue = get(defaults, property);
      const equal = newValue === defaultValue || isEqual(newValue, defaultValue);

      if (isEmpty || equal) {
        continue;
      }

      set(partialConfig, property, newValue);
    }

    yield* metadataRepo.set(SystemMetadataKey.SystemConfig, partialConfig);

    return yield* getConfigFx(repos, { withCache: false });
  });

export const getConfig = (repos: RepoDeps, options: { withCache: boolean }): Promise<SystemConfig> =>
  runPromise(getConfigFx(toFx(repos), options));

export const updateConfig = (repos: RepoDeps, newConfig: SystemConfig): Promise<SystemConfig> =>
  runPromise(updateConfigFx(toFx(repos), newConfig));

const loadFromFile = ({ metadataRepo, logger }: RepoFxDeps, filepath: string) =>
  Effect.gen(function* () {
    const file = yield* metadataRepo.readFile(filepath);
    return yield* Effect.try({ try: () => loadYaml(file) as unknown, catch: (error) => error });
  }).pipe(
    Effect.tapError((error) =>
      Effect.gen(function* () {
        yield* logger.error(`Unable to load configuration file: ${filepath}`);
        yield* logger.error(unwrapCause(error) as Error);
      }),
    ),
  );

const buildConfig = (repos: RepoFxDeps) =>
  Effect.gen(function* () {
    const { configRepo, metadataRepo, logger } = repos;
    const { configFile } = yield* configRepo.getEnv();

    // load partial
    const partial = configFile
      ? yield* loadFromFile(repos, configFile)
      : yield* metadataRepo.get(SystemMetadataKey.SystemConfig);

    // merge with defaults
    const rawConfig = cloneDeep(defaults);
    for (const property of getKeysDeep(partial)) {
      set(rawConfig, property, get(partial, property));
    }

    // check for extra properties
    const unknownKeys = cloneDeep(rawConfig);
    for (const property of getKeysDeep(defaults)) {
      unsetDeep(unknownKeys, property);
    }

    if (!isEmpty(unknownKeys)) {
      yield* logger.warn(`Unknown keys found: ${JSON.stringify(unknownKeys, null, 2)}`);
    }

    // validate with Zod schema
    const result = AdminConfigDto.schema.safeParse(rawConfig);
    if (!result.success) {
      const messages = ['Invalid system config: '];
      for (const issue of result.error.issues) {
        const path = issue.path.join('.');
        messages.push(`  - [${path}] ${issue.message}`);
      }
      if (configFile) {
        return yield* Effect.fail(new Error(messages.join('\n')));
      }
      yield* logger.error('Validation error', messages);
    }

    const config = (result.success ? result.data : rawConfig) as SystemConfig;

    if (config.server.externalDomain.length > 0) {
      const domain = new URL(config.server.externalDomain);

      const externalDomain =
        domain.password && domain.username
          ? `${domain.protocol}//${domain.username}:${domain.password}@${domain.host}`
          : domain.origin;

      config.server.externalDomain = externalDomain;
    }

    if (!config.ffmpeg.acceptedVideoCodecs.includes(config.ffmpeg.targetVideoCodec)) {
      config.ffmpeg.acceptedVideoCodecs.push(config.ffmpeg.targetVideoCodec);
    }

    if (!config.ffmpeg.acceptedAudioCodecs.includes(config.ffmpeg.targetAudioCodec)) {
      config.ffmpeg.acceptedAudioCodecs.push(config.ffmpeg.targetAudioCodec);
    }

    return config;
  });
