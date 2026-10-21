import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AssetFileResponseDto, AssetFileSearchDto, mapAssetFile } from 'src/dtos/asset-file.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { runPromise } from 'src/effect/run.js';
import { AssetFileType, CacheControl, JobName, Permission } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';
import { ImmichFileResponse, getFileNameWithoutExtension, getFilenameExtension } from 'src/utils/file.js';
import { mimeTypes } from 'src/utils/mime-types.js';

@Injectable()
export class AssetFileService extends BaseService {
  async search(auth: AuthDto, dto: AssetFileSearchDto): Promise<AssetFileResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetRead, ids: [dto.assetId] });
        const files = yield* this.fx.assetFile.search(dto);
        return files.map((file) => mapAssetFile(file));
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<AssetFileResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetFileRead, ids: [id] });
        const file = yield* this.fx.assetFile.get(id);
        if (!file) {
          return yield* Effect.fail(new BadRequestException('Asset file not found'));
        }
        return mapAssetFile(file);
      }),
    );
  }

  async download(auth: AuthDto, id: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetFileDownload, ids: [id] });
        const file = yield* this.fx.assetFile.get(id);
        if (!file) {
          return yield* Effect.fail(new BadRequestException('Asset file not found'));
        }

        return new ImmichFileResponse({
          path: file.path,
          fileName: getFileNameWithoutExtension(file.path) + getFilenameExtension(file.path),
          contentType: mimeTypes.lookup(file.path),
          cacheControl: CacheControl.PrivateWithCache,
        });
      }),
    );
  }

  async delete(auth: AuthDto, id: string) {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.AssetFileDelete, ids: [id] });

        const file = yield* this.fx.assetFile.get(id);
        if (!file) {
          return yield* Effect.fail(new BadRequestException('Asset file not found'));
        }
        // TODO consider implications of allowing sidecar files to be deleted
        if (file.type === AssetFileType.Sidecar) {
          return yield* Effect.fail(new BadRequestException('Sidecar files cannot be deleted'));
        }

        yield* this.fx.assetFile.delete(id);
        yield* this.fx.job.queue({ name: JobName.FileDelete, data: { files: [file.path] } });
      }),
    );
  }
}
