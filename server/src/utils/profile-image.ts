import { Context, Effect } from 'effect';
import { join } from 'node:path';
import { StorageCore } from 'src/cores/storage.core.js';
import { SystemConfig } from 'src/dtos/config.dto.js';
import { effectOf } from 'src/effect/bridge.js';
import { FileSystemError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { StorageFolder } from 'src/enum.js';
import { CryptoRepo, CryptoRepository } from 'src/repositories/crypto.repository.js';
import { MediaRepo, MediaRepository } from 'src/repositories/media.repository.js';

type Repos = {
  media: MediaRepository;
  crypto: CryptoRepository;
  storageCore: StorageCore;
};

/** The Effect shapes of `Repos` (`storageCore` is a plain class and is called directly). */
type ReposFx = {
  media: Context.Service.Shape<typeof MediaRepo>;
  crypto: Context.Service.Shape<typeof CryptoRepo>;
  storageCore: StorageCore;
};

export const generateProfileImageFx = (
  { media, crypto, storageCore }: ReposFx,
  { image }: SystemConfig,
  userId: string,
  input: string | Buffer,
) =>
  Effect.gen(function* () {
    const uuid = yield* crypto.randomUUID();
    const outputPath = join(
      StorageCore.getFolderLocation(StorageFolder.Profile, userId),
      `${uuid}.${image.thumbnail.format}`,
    );
    yield* Effect.try({
      try: () => storageCore.ensureFolders(outputPath),
      catch: (cause) => new FileSystemError({ cause }),
    });

    const decoded = yield* media.decodeImage(input, { colorspace: image.colorspace, processInvalidImages: false });
    yield* media.generateThumbnail(
      decoded,
      {
        colorspace: image.colorspace,
        format: image.thumbnail.format,
        quality: image.thumbnail.quality,
        progressive: image.thumbnail.progressive,
        size: image.thumbnail.size,
        processInvalidImages: false,
      },
      outputPath,
    );

    return outputPath;
  });

export const generateProfileImage = (
  { media, crypto, storageCore }: Repos,
  config: SystemConfig,
  userId: string,
  input: string | Buffer,
): Promise<string> =>
  runPromise(
    generateProfileImageFx(
      { media: effectOf(MediaRepo, media), crypto: effectOf(CryptoRepo, crypto), storageCore },
      config,
      userId,
      input,
    ),
  );
