import { Injectable } from '@nestjs/common';
import archiver from 'archiver';
import { ChokidarOptions, watch as chokidarWatch } from 'chokidar';
import { Context, Effect, Layer, Result } from 'effect';
import fastGlob from 'fast-glob';
import {
  Dirent,
  ReadOptionsWithBuffer,
  type Stats,
  constants,
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  watch,
} from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import { PassThrough, Readable, Writable } from 'node:stream';
import { createGunzip, createGzip } from 'node:zlib';
import picomatch from 'picomatch';
import { CrawlOptionsDto, WalkOptionsDto } from 'src/dtos/library.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { FileSystemError, LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { mimeTypes } from 'src/utils/mime-types.js';

export interface WatchEvents {
  onReady(): void;
  onAdd(path: string): void;
  onChange(path: string): void;
  onUnlink(path: string): void;
  onError(error: Error): void;
}

export type WatchOptions = Omit<ChokidarOptions, 'ignored'> & { ignored?: string[] };

export interface ImmichReadStream {
  stream: Readable;
  type?: string;
  disposition?: string | string[];
  length?: number;
}

export interface ImmichZipStream extends ImmichReadStream {
  addFile: (inputPath: string, filename: string) => void;
  finalize: () => Promise<void>;
}

export interface DiskUsage {
  available: number;
  free: number;
  total: number;
}

const fsTry = <A>(run: () => A) => Effect.try({ try: run, catch: (cause) => new FileSystemError({ cause }) });

const fsPromise = <A>(run: () => PromiseLike<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new FileSystemError({ cause }) });

const libTry = <A>(run: () => A) => Effect.try({ try: run, catch: (cause) => new LibraryError({ cause }) });

const libPromise = <A>(run: () => PromiseLike<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new LibraryError({ cause }) });

const asGlob = (pathToCrawl: string): string => {
  // eslint-disable-next-line import-x/no-named-as-default-member
  const escapedPath = fastGlob
    .escapePath(pathToCrawl)
    .replaceAll('"', '["]')
    .replaceAll("'", "[']")
    .replaceAll('`', '[`]');
  const extensions = `*{${mimeTypes.getSupportedFileExtensions().join(',')}}`;
  return `${escapedPath}/**/${extensions}`;
};

/** Lazy and unbuffered: batches are produced while the caller iterates. */
async function* walkGenerator(walkOptions: WalkOptionsDto): AsyncGenerator<string[]> {
  const { pathsToCrawl, exclusionPatterns, includeHidden } = walkOptions;
  if (pathsToCrawl.length === 0) {
    async function* emptyGenerator() {}
    return emptyGenerator();
  }

  const globbedPaths = pathsToCrawl.map((path) => asGlob(path));

  // eslint-disable-next-line import-x/no-named-as-default-member
  const stream = fastGlob.globStream(globbedPaths, {
    absolute: true,
    caseSensitiveMatch: false,
    onlyFiles: true,
    dot: includeHidden,
    ignore: exclusionPatterns,
  });

  let batch: string[] = [];
  for await (const value of stream) {
    batch.push(value.toString());
    if (batch.length !== walkOptions.take) {
      continue;
    }

    yield batch;
    batch = [];
  }

  if (batch.length > 0) {
    yield batch;
  }
}

export class StorageRepo extends Context.Service<
  StorageRepo,
  {
    realpath(filepath: string): Effect.Effect<string, FileSystemError>;
    readdir(folder: string): Effect.Effect<string[], FileSystemError>;
    readdirWithTypes(folder: string): Effect.Effect<Dirent[], FileSystemError>;
    copyFile(source: string, target: string): Effect.Effect<void, FileSystemError>;
    stat(filepath: string): Effect.Effect<Stats, FileSystemError>;
    createFile(filepath: string, buffer: Buffer): Effect.Effect<void, FileSystemError>;
    createWriteStream(filepath: string): Effect.Effect<Writable, FileSystemError>;
    createOrOverwriteFile(filepath: string, buffer: Buffer): Effect.Effect<void, FileSystemError>;
    overwriteFile(filepath: string, buffer: Buffer): Effect.Effect<void, FileSystemError>;
    rename(source: string, target: string): Effect.Effect<void, FileSystemError>;
    utimes(filepath: string, atime: Date, mtime: Date): Effect.Effect<void, FileSystemError>;
    createZipStream(): Effect.Effect<ImmichZipStream, LibraryError>;
    createGzip(): Effect.Effect<PassThrough, LibraryError>;
    createGunzip(): Effect.Effect<PassThrough, LibraryError>;
    createPlainReadStream(filepath: string): Effect.Effect<Readable, FileSystemError>;
    createReadStream(filepath: string, mimeType?: string | null): Effect.Effect<ImmichReadStream, FileSystemError>;
    readFile(filepath: string, options?: ReadOptionsWithBuffer<Buffer>): Effect.Effect<Buffer, FileSystemError>;
    readJsonFile<T>(filepath: string): Effect.Effect<T, FileSystemError | LibraryError>;
    checkFileExists(filepath: string, mode?: number): Effect.Effect<boolean>;
    unlink(file: string): Effect.Effect<void, FileSystemError>;
    unlinkDir(folder: string, options: { recursive?: boolean; force?: boolean }): Effect.Effect<void, FileSystemError>;
    removeEmptyDirs(directory: string, self?: boolean): Effect.Effect<void, FileSystemError>;
    mkdirSync(filepath: string): Effect.Effect<void, FileSystemError>;
    existsSync(filepath: string): Effect.Effect<boolean, FileSystemError>;
    checkDiskUsage(folder: string): Effect.Effect<DiskUsage, FileSystemError>;
    crawl(crawlOptions: CrawlOptionsDto): Effect.Effect<string[], LibraryError>;
    /** The generator is returned lazily; errors raised while iterating it surface to the iterating caller. */
    walk(walkOptions: WalkOptionsDto): Effect.Effect<AsyncGenerator<string[]>>;
    watch(
      paths: string[],
      options: WatchOptions,
      events: Partial<WatchEvents>,
    ): Effect.Effect<() => Promise<void>, LibraryError>;
  }
>()('immich/repositories/StorageRepo') {
  static readonly make = (logger: LoggingRepository) => {
    const removeEmptyDirs = (directory: string, self: boolean = false): Effect.Effect<void, FileSystemError> =>
      Effect.gen(function* () {
        // lstat does not follow symlinks (in contrast to stat)
        const stats = yield* fsPromise(() => fs.lstat(directory));
        if (!stats.isDirectory()) {
          return;
        }

        const files = yield* fsPromise(() => fs.readdir(directory));
        // wait for every child to settle (like the original Promise.all, siblings are not cancelled), then fail with the first error
        const results = yield* Effect.all(
          files.map((file) => Effect.result(removeEmptyDirs(path.join(directory, file), true))),
          { concurrency: 'unbounded' },
        );
        for (const result of results) {
          if (Result.isFailure(result)) {
            return yield* Effect.fail(result.failure);
          }
        }

        if (!self) {
          return;
        }

        const updated = yield* fsPromise(() => fs.readdir(directory));
        if (updated.length === 0) {
          yield* fsPromise(() => fs.rmdir(directory)).pipe(
            Effect.catch(({ cause }) =>
              Effect.sync(() => {
                const error: Error | any = cause;
                if (error.code !== 'ENOTEMPTY') {
                  logger.warn(`Attempted to remove directory, but failed: ${error}`);
                }
              }),
            ),
          );
        }
      });

    return StorageRepo.of({
      realpath: Effect.fn('Storage.realpath')((filepath: string) => fsPromise(() => fs.realpath(filepath))),

      readdir: Effect.fn('Storage.readdir')((folder: string) => fsPromise(() => fs.readdir(folder))),

      readdirWithTypes: Effect.fn('Storage.readdirWithTypes')((folder: string) =>
        fsPromise(() => fs.readdir(folder, { withFileTypes: true })),
      ),

      copyFile: Effect.fn('Storage.copyFile')((source: string, target: string) =>
        fsPromise(() => fs.copyFile(source, target)),
      ),

      stat: Effect.fn('Storage.stat')((filepath: string) => fsPromise(() => fs.stat(filepath))),

      createFile: Effect.fn('Storage.createFile')((filepath: string, buffer: Buffer) =>
        fsPromise(() => fs.writeFile(filepath, buffer, { flag: 'wx' })),
      ),

      createWriteStream: Effect.fn('Storage.createWriteStream')((filepath: string) =>
        fsTry((): Writable => createWriteStream(filepath, { flags: 'w', flush: true })),
      ),

      createOrOverwriteFile: Effect.fn('Storage.createOrOverwriteFile')((filepath: string, buffer: Buffer) =>
        fsPromise(() => fs.writeFile(filepath, buffer, { flag: 'w' })),
      ),

      overwriteFile: Effect.fn('Storage.overwriteFile')((filepath: string, buffer: Buffer) =>
        fsPromise(() => fs.writeFile(filepath, buffer, { flag: 'r+' })),
      ),

      rename: Effect.fn('Storage.rename')((source: string, target: string) =>
        fsPromise(() => fs.rename(source, target)),
      ),

      utimes: Effect.fn('Storage.utimes')((filepath: string, atime: Date, mtime: Date) =>
        fsPromise(() => fs.utimes(filepath, atime, mtime)),
      ),

      createZipStream: Effect.fn('Storage.createZipStream')(() =>
        libTry((): ImmichZipStream => {
          const archive = archiver('zip', { store: true });

          const addFile = (input: string, filename: string) => {
            archive.file(input, { name: filename, mode: 0o644 });
          };

          const finalize = () => archive.finalize();

          return { stream: archive, addFile, finalize };
        }),
      ),

      createGzip: Effect.fn('Storage.createGzip')(() => libTry((): PassThrough => createGzip())),

      createGunzip: Effect.fn('Storage.createGunzip')(() => libTry((): PassThrough => createGunzip())),

      createPlainReadStream: Effect.fn('Storage.createPlainReadStream')((filepath: string) =>
        fsTry((): Readable => createReadStream(filepath)),
      ),

      createReadStream: Effect.fn('Storage.createReadStream')((filepath: string, mimeType?: string | null) =>
        fsPromise(async (): Promise<ImmichReadStream> => {
          const { size } = await fs.stat(filepath);
          await fs.access(filepath, constants.R_OK);
          return {
            stream: createReadStream(filepath),
            length: size,
            type: mimeType || undefined,
          };
        }),
      ),

      readFile: Effect.fn('Storage.readFile')((filepath: string, options?: ReadOptionsWithBuffer<Buffer>) => {
        // read everything
        if (!options) {
          return fsPromise(() => fs.readFile(filepath));
        }

        // read a slice
        return Effect.acquireUseRelease(
          fsPromise(() => fs.open(filepath)),
          (file) =>
            fsPromise(async () => {
              const { buffer } = await file.read(options);
              return buffer as Buffer;
            }),
          (file) => fsPromise(() => file.close()).pipe(Effect.orDie),
        );
      }),

      // Effect.fn does not preserve generics, so the span is attached manually
      readJsonFile: <T>(filepath: string) =>
        Effect.gen(function* () {
          const file = yield* fsPromise(() => fs.readFile(filepath, 'utf8'));
          return yield* libTry(() => JSON.parse(file) as T);
        }).pipe(Effect.withSpan('Storage.readJsonFile')),

      checkFileExists: Effect.fn('Storage.checkFileExists')((filepath: string, mode = constants.F_OK) =>
        fsPromise(() => fs.access(filepath, mode)).pipe(
          Effect.map(() => true),
          Effect.orElseSucceed(() => false),
        ),
      ),

      unlink: Effect.fn('Storage.unlink')((file: string) =>
        fsPromise(() => fs.unlink(file)).pipe(
          Effect.catch((error) =>
            (error.cause as NodeJS.ErrnoException)?.code === 'ENOENT'
              ? Effect.sync(() => logger.warn(`File ${file} does not exist.`))
              : Effect.fail(error),
          ),
        ),
      ),

      unlinkDir: Effect.fn('Storage.unlinkDir')((folder: string, options: { recursive?: boolean; force?: boolean }) =>
        fsPromise(() => fs.rm(folder, { ...options, maxRetries: 5, retryDelay: 100 })),
      ),

      removeEmptyDirs: Effect.fn('Storage.removeEmptyDirs')((directory: string, self: boolean = false) =>
        removeEmptyDirs(directory, self),
      ),

      mkdirSync: Effect.fn('Storage.mkdirSync')((filepath: string) =>
        fsTry(() => {
          if (!existsSync(filepath)) {
            mkdirSync(filepath, { recursive: true });
          }
        }),
      ),

      existsSync: Effect.fn('Storage.existsSync')((filepath: string) => fsTry(() => existsSync(filepath))),

      checkDiskUsage: Effect.fn('Storage.checkDiskUsage')((folder: string) =>
        fsPromise(async (): Promise<DiskUsage> => {
          const stats = await fs.statfs(folder);
          return {
            available: stats.bavail * stats.frsize,
            free: stats.bfree * stats.frsize,
            total: stats.blocks * stats.frsize,
          };
        }),
      ),

      crawl: Effect.fn('Storage.crawl')((crawlOptions: CrawlOptionsDto) => {
        const { pathsToCrawl, exclusionPatterns, includeHidden } = crawlOptions;
        if (pathsToCrawl.length === 0) {
          return Effect.succeed<string[]>([]);
        }

        return libPromise(() => {
          const globbedPaths = pathsToCrawl.map((path) => asGlob(path));

          // eslint-disable-next-line import-x/no-named-as-default-member
          return fastGlob.glob(globbedPaths, {
            absolute: true,
            caseSensitiveMatch: false,
            onlyFiles: true,
            dot: includeHidden,
            ignore: exclusionPatterns,
          });
        });
      }),

      walk: Effect.fn('Storage.walk')((walkOptions: WalkOptionsDto) => Effect.sync(() => walkGenerator(walkOptions))),

      watch: Effect.fn('Storage.watch')((paths: string[], options: WatchOptions, events: Partial<WatchEvents>) =>
        libTry(() => {
          const matchesIgnoredPath = picomatch(options.ignored ?? [], {
            dot: true, // Match the behavior of fast-glob's micromatch by using these settings
            nocase: true,
            posix: true,
            strictSlashes: false,
          });

          const watcher = chokidarWatch(paths, { ...options, ignored: (path) => matchesIgnoredPath(path) });

          watcher.on('ready', () => events.onReady?.());
          watcher.on('add', (path) => events.onAdd?.(path));
          watcher.on('change', (path) => events.onChange?.(path));
          watcher.on('unlink', (path) => events.onUnlink?.(path));
          watcher.on('error', (error) => events.onError?.(error as Error));

          return () => watcher.close();
        }),
      ),
    });
  };

  static readonly layer = (logger: LoggingRepository) => Layer.sync(StorageRepo, () => StorageRepo.make(logger));
}

/** Promise-facing adapter for the Nest services. Delegates to `StorageRepo`. */
@Injectable()
export class StorageRepository {
  private readonly effect: Context.Service.Shape<typeof StorageRepo>;

  constructor(private logger: LoggingRepository) {
    this.logger.setContext(StorageRepository.name);
    this.effect = StorageRepo.make(this.logger);
    registerEffect(this, this.effect);
  }

  realpath(filepath: string) {
    return runPromise(this.effect.realpath(filepath));
  }

  readdir(folder: string): Promise<string[]> {
    return runPromise(this.effect.readdir(folder));
  }

  readdirWithTypes(folder: string): Promise<Dirent[]> {
    return runPromise(this.effect.readdirWithTypes(folder));
  }

  copyFile(source: string, target: string) {
    return runPromise(this.effect.copyFile(source, target));
  }

  stat(filepath: string) {
    return runPromise(this.effect.stat(filepath));
  }

  createFile(filepath: string, buffer: Buffer) {
    return runPromise(this.effect.createFile(filepath, buffer));
  }

  createWriteStream(filepath: string): Writable {
    return runSync(this.effect.createWriteStream(filepath));
  }

  createOrOverwriteFile(filepath: string, buffer: Buffer) {
    return runPromise(this.effect.createOrOverwriteFile(filepath, buffer));
  }

  overwriteFile(filepath: string, buffer: Buffer) {
    return runPromise(this.effect.overwriteFile(filepath, buffer));
  }

  rename(source: string, target: string) {
    return runPromise(this.effect.rename(source, target));
  }

  utimes(filepath: string, atime: Date, mtime: Date) {
    return runPromise(this.effect.utimes(filepath, atime, mtime));
  }

  createZipStream(): ImmichZipStream {
    return runSync(this.effect.createZipStream());
  }

  createGzip(): PassThrough {
    return runSync(this.effect.createGzip());
  }

  createGunzip(): PassThrough {
    return runSync(this.effect.createGunzip());
  }

  createPlainReadStream(filepath: string): Readable {
    return runSync(this.effect.createPlainReadStream(filepath));
  }

  createReadStream(filepath: string, mimeType?: string | null): Promise<ImmichReadStream> {
    return runPromise(this.effect.createReadStream(filepath, mimeType));
  }

  readFile(filepath: string, options?: ReadOptionsWithBuffer<Buffer>): Promise<Buffer> {
    return runPromise(this.effect.readFile(filepath, options));
  }

  readJsonFile<T>(filepath: string): Promise<T> {
    return runPromise(this.effect.readJsonFile<T>(filepath));
  }

  checkFileExists(filepath: string, mode = constants.F_OK): Promise<boolean> {
    return runPromise(this.effect.checkFileExists(filepath, mode));
  }

  unlink(file: string) {
    return runPromise(this.effect.unlink(file));
  }

  unlinkDir(folder: string, options: { recursive?: boolean; force?: boolean }) {
    return runPromise(this.effect.unlinkDir(folder, options));
  }

  removeEmptyDirs(directory: string, self: boolean = false) {
    return runPromise(this.effect.removeEmptyDirs(directory, self));
  }

  mkdirSync(filepath: string): void {
    runSync(this.effect.mkdirSync(filepath));
  }

  existsSync(filepath: string) {
    return runSync(this.effect.existsSync(filepath));
  }

  checkDiskUsage(folder: string): Promise<DiskUsage> {
    return runPromise(this.effect.checkDiskUsage(folder));
  }

  crawl(crawlOptions: CrawlOptionsDto): Promise<string[]> {
    return runPromise(this.effect.crawl(crawlOptions));
  }

  walk(walkOptions: WalkOptionsDto): AsyncGenerator<string[]> {
    return runSync(this.effect.walk(walkOptions));
  }

  watch(paths: string[], options: WatchOptions, events: Partial<WatchEvents>) {
    return runSync(this.effect.watch(paths, options, events));
  }

  watchDir = watch; // Native fs.watch without chokidar overhead; left as a direct passthrough (overloaded, callback-driven)
}
