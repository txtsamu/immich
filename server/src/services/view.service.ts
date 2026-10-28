import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AssetResponseDto, mapAsset } from 'src/dtos/asset-response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { runPromise } from 'src/effect/run.js';
import { BaseService } from 'src/services/base.service.js';

@Injectable()
export class ViewService extends BaseService {
  getUniqueOriginalPaths(auth: AuthDto): Promise<string[]> {
    return runPromise(this.fx.view.getUniqueOriginalPaths(auth.user.id));
  }

  async getAssetsByOriginalPath(auth: AuthDto, path: string): Promise<AssetResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const assets = yield* this.fx.view.getAssetsByOriginalPath(auth.user.id, path);
        return assets.map((asset) => mapAsset(asset, { auth }));
      }),
    );
  }
}
