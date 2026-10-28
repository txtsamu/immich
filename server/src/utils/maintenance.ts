import { Context, Effect } from 'effect';
import { SignJWT } from 'jose';
import { randomBytes } from 'node:crypto';
import { join } from 'node:path';
import { StorageCore } from 'src/cores/storage.core.js';
import { MaintenanceAuthDto, MaintenanceDetectInstallResponseDto } from 'src/dtos/maintenance.dto.js';
import { effectOf } from 'src/effect/bridge.js';
import { runPromise } from 'src/effect/run.js';
import { StorageFolder } from 'src/enum.js';
import { StorageRepo, StorageRepository } from 'src/repositories/storage.repository.js';

export async function createMaintenanceLoginUrl(
  baseUrl: string,
  auth: MaintenanceAuthDto,
  secret: string,
): Promise<string> {
  return `${baseUrl}/maintenance?token=${await signMaintenanceJwt(secret, auth)}`;
}

export async function signMaintenanceJwt(secret: string, data: MaintenanceAuthDto): Promise<string> {
  const alg = 'HS256';

  return await new SignJWT({ ...data })
    .setProtectedHeader({ alg })
    .setIssuedAt()
    .setExpirationTime('4h')
    .sign(new TextEncoder().encode(secret));
}

export function generateMaintenanceSecret(): string {
  return randomBytes(64).toString('hex');
}

export const detectPriorInstallFx = (storageRepository: Context.Service.Shape<typeof StorageRepo>) =>
  Effect.gen(function* () {
    const storage = yield* Effect.all(
      Object.values(StorageFolder).map((folder) =>
        Effect.gen(function* () {
          const path = StorageCore.getBaseFolder(folder);
          const files = yield* storageRepository.readdir(path);
          const filename = join(StorageCore.getBaseFolder(folder), '.immich');

          let isReadable = false,
            isWritable = false;

          yield* Effect.gen(function* () {
            yield* storageRepository.readFile(filename);
            isReadable = true;

            yield* storageRepository.overwriteFile(filename, Buffer.from(Date.now().toString()));
            isWritable = true;
          }).pipe(Effect.catch(() => Effect.void)); // no-op

          return {
            folder,
            readable: isReadable,
            writable: isWritable,
            files: files.filter((fn) => fn !== '.immich').length,
          };
        }),
      ),
      { concurrency: 'unbounded' },
    );

    return { storage } satisfies MaintenanceDetectInstallResponseDto;
  });

export async function detectPriorInstall(
  storageRepository: StorageRepository,
): Promise<MaintenanceDetectInstallResponseDto> {
  return runPromise(detectPriorInstallFx(effectOf(StorageRepo, storageRepository)));
}
