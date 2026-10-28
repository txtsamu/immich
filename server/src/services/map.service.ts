import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { MapMarkerDto, MapMarkerResponseDto, MapReverseGeocodeDto } from 'src/dtos/map.dto.js';
import { runPromise } from 'src/effect/run.js';
import { BaseService } from 'src/services/base.service.js';
import { getMyPartnerIdsFx } from 'src/utils/asset.util.js';

@Injectable()
export class MapService extends BaseService {
  async getMapMarkers(auth: AuthDto, options: MapMarkerDto): Promise<MapMarkerResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const userIds = [auth.user.id];
        if (options.withPartners) {
          const partnerIds = yield* getMyPartnerIdsFx({ userId: auth.user.id, repository: this.fx.partner });
          userIds.push(...partnerIds);
        }

        const albumIds = options.withSharedAlbums ? yield* this.fx.album.getAllIds(auth.user.id) : [];

        return yield* this.fx.map.getMapMarkers(auth.user.id, userIds, albumIds, options);
      }),
    );
  }

  async reverseGeocode(dto: MapReverseGeocodeDto) {
    const { lat: latitude, lon: longitude } = dto;
    // eventually this should probably return an array of results
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const result = yield* this.fx.map.reverseGeocode({ latitude, longitude });
        return result ? [result] : [];
      }),
    );
  }
}
