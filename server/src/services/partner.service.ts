import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { Partner } from 'src/database.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { PartnerCreateDto, PartnerResponseDto, PartnerSearchDto, PartnerUpdateDto } from 'src/dtos/partner.dto.js';
import { mapUser } from 'src/dtos/user.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission, SharingDirection } from 'src/enum.js';
import { PartnerIds } from 'src/repositories/partner.repository.js';
import { BaseService } from 'src/services/base.service.js';

@Injectable()
export class PartnerService extends BaseService {
  async create(auth: AuthDto, { sharedWithId }: PartnerCreateDto): Promise<PartnerResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const partnerId: PartnerIds = { sharedById: auth.user.id, sharedWithId };
        const exists = yield* this.fx.partner.get(partnerId);
        if (exists) {
          return yield* Effect.fail(new BadRequestException(`Partner already exists`));
        }

        const user = yield* this.fx.user.get(sharedWithId, {});
        if (!user) {
          this.logger.debug('Partner creation failed: user not found');
          return yield* Effect.fail(new BadRequestException('Invalid user'));
        }

        const partner = yield* this.fx.partner.create(partnerId);
        return this.mapPartner(partner, SharingDirection.SharedBy);
      }),
    );
  }

  async remove(auth: AuthDto, sharedWithId: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        const partnerId: PartnerIds = { sharedById: auth.user.id, sharedWithId };
        const partner = yield* this.fx.partner.get(partnerId);
        if (!partner) {
          return yield* Effect.fail(new BadRequestException('Partner not found'));
        }

        yield* this.fx.partner.remove(partnerId);
      }),
    );
  }

  async search(auth: AuthDto, { direction }: PartnerSearchDto): Promise<PartnerResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const partners = yield* this.fx.partner.getAll(auth.user.id);
        const key = direction === SharingDirection.SharedBy ? 'sharedById' : 'sharedWithId';
        return partners
          .filter((partner): partner is Partner => !!(partner.sharedBy && partner.sharedWith)) // Filter out soft deleted users
          .filter((partner) => partner[key] === auth.user.id)
          .map((partner) => this.mapPartner(partner, direction));
      }),
    );
  }

  async update(auth: AuthDto, sharedById: string, dto: PartnerUpdateDto): Promise<PartnerResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.PartnerUpdate, ids: [sharedById] });
        const partnerId: PartnerIds = { sharedById, sharedWithId: auth.user.id };

        const entity = yield* this.fx.partner.update(partnerId, { inTimeline: dto.inTimeline });
        return this.mapPartner(entity, SharingDirection.SharedWith);
      }),
    );
  }

  private mapPartner(partner: Partner, direction: SharingDirection): PartnerResponseDto {
    // this is opposite to return the non-me user of the "partner"
    const sharedUser = direction === SharingDirection.SharedBy ? partner.sharedWith : partner.sharedBy;
    const user = mapUser(sharedUser);

    return { ...user, inTimeline: partner.inTimeline };
  }
}
