import { Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { runPromise } from 'src/effect/run.js';
import { BaseService } from 'src/services/base.service.js';

@Injectable()
export class AuthAdminService extends BaseService {
  async unlinkAll(_auth: AuthDto) {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.fx.user.updateAll({ oauthId: null });
      }),
    );
  }
}
