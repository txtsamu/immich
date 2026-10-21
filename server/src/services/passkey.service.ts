import { BadRequestException, Injectable, NotFoundException, UnauthorizedException } from '@nestjs/common';
import aaguids from 'aaguids';
import { Effect } from 'effect';
import { DateTime, Duration } from 'luxon';
import { IncomingHttpHeaders } from 'node:http';
import type {
  AuthenticationResponseJSON,
  RegistrationResponseJSON,
  VerifiedAuthenticationResponse,
  VerifiedRegistrationResponse,
} from '@simplewebauthn/server';
import type { LoginDetails } from 'src/services/auth.service.js';
import { OnJob } from 'src/decorators.js';
import { AuthDto, LoginResponseDto } from 'src/dtos/auth.dto.js';
import { SystemConfig } from 'src/dtos/config.dto.js';
import {
  PasskeyAuthenticationFinishDto,
  PasskeyAuthenticationOptionsDto,
  PasskeyRegistrationFinishDto,
  PasskeyRegistrationOptionsDto,
  PasskeyResponseDto,
  PasskeySearchDto,
  PasskeyUpdateDto,
  mapPasskey,
} from 'src/dtos/passkey.dto.js';
import { runPromise } from 'src/effect/run.js';
import { AuthChallengeType, JobName, JobStatus, Permission, QueueName } from 'src/enum.js';
import { BaseService } from 'src/services/base.service.js';

const RP_NAME = 'Immich';

const CHALLENGE_TTL = Duration.fromObject({ minutes: 5 });

const MOBILE_RP_ID = 'my.immich.app';

// keep in sync with https://my.immich.app/.well-known/assetlinks.json
const ANDROID_CERTIFICATE_FINGERPRINTS = [
  '86:C5:C4:55:DF:AF:49:85:92:3A:8F:35:AD:B3:1D:0C:9E:0B:95:7D:7F:94:C2:D2:AF:6A:24:38:AA:96:00:20',
  '5A:22:C1:83:47:54:05:F5:49:C4:EB:9F:B2:6C:2E:93:A3:EF:9C:57:66:15:0A:7A:F3:8C:8D:3F:E5:15:CC:D6',
];

const ANDROID_ORIGINS = ANDROID_CERTIFICATE_FINGERPRINTS.map(
  (fingerprint) => `android:apk-key-hash:${Buffer.from(fingerprint.replaceAll(':', ''), 'hex').toString('base64url')}`,
);

@Injectable()
export class PasskeyService extends BaseService {
  async startRegistration(auth: AuthDto, headers: IncomingHttpHeaders): Promise<PasskeyRegistrationOptionsDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.requireSession(auth);
        const config = yield* this.requireEnabled();

        const { rpId } = this.getRelyingParty(config, headers);
        const passkeys = yield* this.fx.passkey.search({ userId: auth.user.id });

        const options = yield* this.fx.webAuthn.generateRegistrationOptions({
          rpName: RP_NAME,
          rpID: rpId,
          userID: new TextEncoder().encode(auth.user.id),
          userName: auth.user.email,
          userDisplayName: auth.user.name,
          attestationType: 'none',
          excludeCredentials: passkeys.map(({ credentialId, transports }) => ({ id: credentialId, transports })),
          authenticatorSelection: { residentKey: 'required', userVerification: 'required' },
        });

        yield* this.fx.authChallenge.create({
          type: AuthChallengeType.PasskeyRegistration,
          challenge: options.challenge,
          userId: auth.user.id,
          expiresAt: DateTime.now().plus(CHALLENGE_TTL).toJSDate(),
        });

        return options;
      }),
    );
  }

  async finishRegistration(
    auth: AuthDto,
    dto: PasskeyRegistrationFinishDto,
    headers: IncomingHttpHeaders,
    loginDetails: LoginDetails,
  ): Promise<PasskeyResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        this.requireSession(auth);
        const config = yield* this.requireEnabled();

        const { rpId, origin } = this.getRelyingParty(config, headers);

        const result: VerifiedRegistrationResponse = yield* this.fx.webAuthn
          .verifyRegistrationResponse({
            response: dto.response as RegistrationResponseJSON,
            expectedChallenge: async (challenge) => {
              const record = await runPromise(
                this.fx.authChallenge.consume({
                  type: AuthChallengeType.PasskeyRegistration,
                  challenge,
                }),
              );
              return record?.userId === auth.user.id;
            },
            expectedOrigin: origin,
            expectedRPID: rpId,
          })
          .pipe(
            Effect.catch((error_) => {
              const error = error_.cause as any;
              this.logger.warn(`Passkey registration failed for user ${auth.user.id}: ${error?.message}`);
              return Effect.fail(new BadRequestException('Passkey registration failed'));
            }),
          );

        if (!result.verified) {
          return yield* Effect.fail(new BadRequestException('Passkey registration failed'));
        }

        const { aaguid, credential, credentialBackedUp, credentialDeviceType } = result.registrationInfo;

        const duplicate = yield* this.fx.passkey.getByCredentialId(credential.id);
        if (duplicate) {
          return yield* Effect.fail(new BadRequestException('Passkey is already registered'));
        }
        const passkey = yield* this.fx.passkey.create({
          userId: auth.user.id,
          name:
            dto.name ||
            (aaguids as unknown as Record<string, string>)[aaguid] ||
            [loginDetails.deviceType, loginDetails.deviceOS].filter(Boolean).join(' on ') ||
            'Passkey',
          credentialId: credential.id,
          publicKey: Buffer.from(credential.publicKey),
          counter: credential.counter,
          transports: credential.transports ?? [],
          backedUp: credentialBackedUp,
          deviceType: credentialDeviceType,
        });

        return mapPasskey(passkey);
      }),
    );
  }

  async startAuthentication(headers: IncomingHttpHeaders): Promise<PasskeyAuthenticationOptionsDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.requireEnabled();

        const { rpId } = this.getRelyingParty(config, headers);

        const options = yield* this.fx.webAuthn.generateAuthenticationOptions({
          rpID: rpId,
          userVerification: 'required',
        });

        yield* this.fx.authChallenge.create({
          type: AuthChallengeType.PasskeyAuthentication,
          challenge: options.challenge,
          userId: null,
          expiresAt: DateTime.now().plus(CHALLENGE_TTL).toJSDate(),
        });

        return options;
      }),
    );
  }

  async finishAuthentication(
    dto: PasskeyAuthenticationFinishDto,
    headers: IncomingHttpHeaders,
    loginDetails: LoginDetails,
  ): Promise<LoginResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* this.requireEnabled();

        const { rpId, origin } = this.getRelyingParty(config, headers);

        const passkey = yield* this.fx.passkey.getByCredentialId(dto.response.id);
        const userHandle = dto.response.response.userHandle;
        const userId = userHandle ? Buffer.from(userHandle, 'base64url').toString() : undefined;
        if (!passkey || (userId && userId !== passkey.userId)) {
          this.logger.warn(`Failed passkey login attempt from ip address ${loginDetails.clientIp}`);
          return yield* Effect.fail(new UnauthorizedException('Invalid passkey'));
        }

        const result: VerifiedAuthenticationResponse = yield* this.fx.webAuthn
          .verifyAuthenticationResponse({
            response: dto.response as unknown as AuthenticationResponseJSON,
            expectedChallenge: async (challenge) => {
              const record = await runPromise(
                this.fx.authChallenge.consume({
                  type: AuthChallengeType.PasskeyAuthentication,
                  challenge,
                }),
              );
              return !!record;
            },
            expectedOrigin: origin,
            expectedRPID: rpId,
            credential: {
              id: passkey.credentialId,
              publicKey: new Uint8Array(passkey.publicKey),
              counter: passkey.counter,
              transports: passkey.transports,
            },
          })
          .pipe(
            Effect.catch((error_) => {
              const error = error_.cause as any;
              this.logger.warn(
                `Failed passkey login attempt from ip address ${loginDetails.clientIp}: ${error?.message}`,
              );
              return Effect.fail(new UnauthorizedException('Invalid passkey'));
            }),
          );

        if (!result.verified) {
          return yield* Effect.fail(new UnauthorizedException('Invalid passkey'));
        }

        const user = yield* this.fx.user.get(passkey.userId, {});
        if (!user) {
          return yield* Effect.fail(new UnauthorizedException('Invalid passkey'));
        }

        yield* this.fx.passkey.update(passkey.id, {
          counter: result.authenticationInfo.newCounter,
          usedAt: new Date(),
        });

        return yield* this.createLoginResponseFx(user, loginDetails);
      }),
    );
  }

  async search(auth: AuthDto, dto: PasskeySearchDto): Promise<PasskeyResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const passkeys = yield* this.fx.passkey.search({ id: dto.id, userId: auth.user.id });
        return passkeys.map((passkey) => mapPasskey(passkey));
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<PasskeyResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.PasskeyRead, ids: [id] });
        const passkey = yield* this.fx.passkey.get(id);
        if (!passkey) {
          return yield* Effect.fail(new BadRequestException('Passkey not found'));
        }
        return mapPasskey(passkey);
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: PasskeyUpdateDto): Promise<PasskeyResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.PasskeyUpdate, ids: [id] });
        const passkey = yield* this.fx.passkey.update(id, { name: dto.name });
        return mapPasskey(passkey);
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.PasskeyDelete, ids: [id] });
        yield* this.fx.passkey.delete(id);
      }),
    );
  }

  private requireSession(auth: AuthDto) {
    if (!auth.session) {
      throw new BadRequestException('This endpoint can only be used with a session token');
    }
  }

  async getWellKnown(): Promise<{ origins: string[] }> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { passkey } = yield* this.getConfigFx({ withCache: true });
        if (!passkey.enabled || !passkey.domain) {
          return yield* Effect.fail(new NotFoundException());
        }

        return {
          origins: [passkey.domain, ...passkey.additionalDomains].map((value) => new URL(value).origin),
        };
      }),
    );
  }

  @OnJob({ name: JobName.AuthChallengeCleanup, queue: QueueName.BackgroundTask })
  async handleCleanup(): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const count = yield* this.fx.authChallenge.deleteExpired();
        if (count > 0) {
          this.logger.log(`Deleted ${count} expired auth challenges`);
        }
        return JobStatus.Success;
      }),
    );
  }

  private requireEnabled = Effect.fn('PasskeyService.requireEnabled')(function* (this: PasskeyService) {
    const { passkey } = yield* this.getConfigFx({ withCache: false });
    if (!passkey.enabled) {
      return yield* Effect.fail(new BadRequestException('Passkeys are disabled'));
    }

    return passkey;
  });

  private getRelyingParty(
    config: SystemConfig['passkey'],
    headers: IncomingHttpHeaders,
  ): { origin: string | string[]; rpId: string } {
    const origin = headers.origin;
    // no origin implies a mobile app
    if (!origin) {
      return { origin: ANDROID_ORIGINS, rpId: MOBILE_RP_ID };
    }

    let hostname: string;
    try {
      hostname = new URL(origin).hostname;
    } catch {
      throw new BadRequestException('Origin header is invalid');
    }

    if (!config.domain) {
      return { origin, rpId: hostname };
    }

    const rpId = new URL(config.domain).hostname;
    if (hostname !== rpId && config.additionalDomains.every((value) => new URL(value).origin !== origin)) {
      throw new BadRequestException('Origin is not trusted');
    }

    return { origin, rpId };
  }
}
