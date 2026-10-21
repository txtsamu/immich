import { Injectable, InternalServerErrorException } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { JWTVerifyGetKey, createRemoteJWKSet, jwtVerify } from 'jose';
import {
  ClientSecretBasic,
  ClientSecretPost,
  None,
  type UserInfoResponse,
  allowInsecureRequests as allowInsecureRequestsExecute,
  authorizationCodeGrant,
  buildAuthorizationUrl,
  calculatePKCECodeChallenge,
  discovery,
  fetchUserInfo,
  randomPKCECodeVerifier,
  randomState,
  skipSubjectCheck,
} from 'openid-client';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { OAuthTokenEndpointAuthMethod } from 'src/enum.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';

export type OAuthConfig = {
  clientId: string;
  clientSecret?: string;
  issuerUrl: string;
  accountManagementUrl: string;
  endSessionEndpoint: string;
  mobileOverrideEnabled: boolean;
  mobileRedirectUri: string;
  profileSigningAlgorithm: string;
  prompt: string;
  scope: string;
  signingAlgorithm: string;
  tokenEndpointAuthMethod: OAuthTokenEndpointAuthMethod;
  timeout: number;
  allowInsecureRequests: boolean;
};
export type OAuthProfile = UserInfoResponse;

const getTokenAuthMethod = (tokenEndpointAuthMethod: OAuthTokenEndpointAuthMethod, clientSecret?: string) => {
  if (!clientSecret) {
    return None();
  }

  switch (tokenEndpointAuthMethod) {
    case OAuthTokenEndpointAuthMethod.ClientSecretPost: {
      return ClientSecretPost(clientSecret);
    }

    case OAuthTokenEndpointAuthMethod.ClientSecretBasic: {
      return ClientSecretBasic(clientSecret);
    }

    default: {
      return None();
    }
  }
};

const libPromise = <A>(run: () => PromiseLike<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new LibraryError({ cause }) });

export class OAuthRepo extends Context.Service<
  OAuthRepo,
  {
    authorize(
      config: OAuthConfig,
      redirectUrl: string,
      state?: string,
      codeChallenge?: string,
    ): Effect.Effect<{ url: string; state: string; codeVerifier: string | null }, LibraryError>;
    getLogoutEndpoint(config: OAuthConfig): Effect.Effect<string | undefined, LibraryError>;
    getProfileAndOAuthSid(
      config: OAuthConfig,
      url: string,
      expectedState: string,
      codeVerifier: string,
    ): Effect.Effect<{ profile: OAuthProfile; sid?: string; idToken?: string }, LibraryError>;
    getProfilePicture(url: string): Effect.Effect<ArrayBuffer, LibraryError>;
    validateLogoutToken(
      config: OAuthConfig,
      logoutToken: string,
    ): Effect.Effect<{ sub?: string; sid?: string } | null, LibraryError>;
  }
>()('immich/repositories/OAuthRepo') {
  static readonly make = (logger: LoggingRepository) => {
    // useful for caching and performance
    const jwksClients: Map<string, JWTVerifyGetKey> = new Map();

    const getClient = Effect.fn('OAuth.getClient')(
      ({
        issuerUrl,
        clientId,
        clientSecret,
        profileSigningAlgorithm,
        signingAlgorithm,
        tokenEndpointAuthMethod,
        timeout,
        allowInsecureRequests,
      }: OAuthConfig) =>
        libPromise(() =>
          discovery(
            new URL(issuerUrl),
            clientId,
            {
              client_secret: clientSecret,
              response_types: ['code'],
              userinfo_signed_response_alg: profileSigningAlgorithm === 'none' ? undefined : profileSigningAlgorithm,
              id_token_signed_response_alg: signingAlgorithm,
            },
            getTokenAuthMethod(tokenEndpointAuthMethod, clientSecret),
            {
              execute: allowInsecureRequests ? [allowInsecureRequestsExecute] : [],
              timeout,
            },
          ),
        ).pipe(
          Effect.tapError(({ cause: error }) =>
            Effect.sync(() => logger.error('Error in OAuth discovery', error as any)),
          ),
          Effect.mapError(
            ({ cause: error }) =>
              new LibraryError({
                cause: new InternalServerErrorException(`Error in OAuth discovery: ${error}`, { cause: error }),
              }),
          ),
        ),
    );

    return OAuthRepo.of({
      authorize: Effect.fn('OAuth.authorize')(function* (
        config: OAuthConfig,
        redirectUrl: string,
        state?: string,
        codeChallenge?: string,
      ) {
        const client = yield* getClient(config);
        return yield* libPromise(async () => {
          state ??= randomState();

          let codeVerifier: string | null;
          if (codeChallenge) {
            codeVerifier = null;
          } else {
            codeVerifier = randomPKCECodeVerifier();
            codeChallenge = await calculatePKCECodeChallenge(codeVerifier);
          }

          const params: Record<string, string> = {
            redirect_uri: redirectUrl,
            scope: config.scope,
            state,
          };

          if (config.prompt) {
            params.prompt = config.prompt;
          }

          if (client.serverMetadata().supportsPKCE()) {
            params.code_challenge = codeChallenge;
            params.code_challenge_method = 'S256';
          }

          const url = buildAuthorizationUrl(client, params).href;

          return { url, state, codeVerifier };
        });
      }),

      getLogoutEndpoint: Effect.fn('OAuth.getLogoutEndpoint')(function* (config: OAuthConfig) {
        const client = yield* getClient(config);
        return client.serverMetadata().end_session_endpoint;
      }),

      getProfileAndOAuthSid: Effect.fn('OAuth.getProfileAndOAuthSid')(function* (
        config: OAuthConfig,
        url: string,
        expectedState: string,
        codeVerifier: string,
      ) {
        const client = yield* getClient(config);
        const pkceCodeVerifier = client.serverMetadata().supportsPKCE() ? codeVerifier : undefined;

        return yield* libPromise(async (): Promise<{ profile: OAuthProfile; sid?: string; idToken?: string }> => {
          const tokens = await authorizationCodeGrant(client, new URL(url), { expectedState, pkceCodeVerifier });

          let profile: OAuthProfile;
          const tokenClaims = tokens.claims();
          if (tokenClaims && 'email' in tokenClaims) {
            logger.debug('Using ID token claims instead of userinfo endpoint');
            profile = tokenClaims as OAuthProfile;
          } else {
            profile = await fetchUserInfo(client, tokens.access_token, skipSubjectCheck);
          }

          if (!profile.sub) {
            throw new Error('Unexpected profile response, no `sub`');
          }

          let sid: string | undefined;
          if (tokens.id_token) {
            const claims = tokens.claims();
            if (typeof claims?.sid === 'string') {
              sid = claims.sid;
            }
          }

          return { profile, sid, idToken: tokens.id_token };
        }).pipe(
          Effect.tapError(({ cause }) =>
            Effect.sync(() => {
              const error: Error | any = cause;
              if (error.message.includes('unexpected JWT alg received')) {
                logger.warn(
                  [
                    'Algorithm mismatch. Make sure the signing algorithm is set correctly in the OAuth settings.',
                    'Or, that you have specified a signing key in your OAuth provider.',
                  ].join(' '),
                );
              }

              logger.error('OAuth login failed', error);
            }),
          ),
          Effect.mapError(({ cause }) => new LibraryError({ cause: new Error('OAuth login failed', { cause }) })),
        );
      }),

      getProfilePicture: Effect.fn('OAuth.getProfilePicture')((url: string) =>
        libPromise(async () => {
          const response = await fetch(url);
          if (!response.ok) {
            throw new Error(`Failed to fetch picture: ${response.statusText}`);
          }

          return response.arrayBuffer();
        }),
      ),

      validateLogoutToken: Effect.fn('OAuth.validateLogoutToken')(function* (config: OAuthConfig, logoutToken: string) {
        const client = yield* getClient(config);
        const algorithm = client.clientMetadata().id_token_signed_response_alg ?? 'RS256';
        let keyOrGetter: Uint8Array | JWTVerifyGetKey;

        return yield* libPromise(async () => {
          if (algorithm.startsWith('HS')) {
            keyOrGetter = new TextEncoder().encode(config.clientSecret);
          } else {
            const jwksUri = client.serverMetadata().jwks_uri;
            if (!jwksUri) {
              throw new Error('Unable to get JWKS URI');
            }

            if (!jwksClients.has(jwksUri)) {
              jwksClients.set(jwksUri, createRemoteJWKSet(new URL(jwksUri)));
            }
            keyOrGetter = jwksClients.get(jwksUri) as JWTVerifyGetKey;
          }

          const { payload } = await jwtVerify(logoutToken, keyOrGetter as any, {
            issuer: client.serverMetadata().issuer,
            audience: config.clientId,
            algorithms: [algorithm],
            maxTokenAge: '2m',
            clockTolerance: '5s',
          });

          // Validate specific Logout Token claims (RFC 8963):
          // "events" claim must exist and contain the backchannel-logout event
          const events = payload.events as Record<string, any> | undefined;
          // eslint-disable-next-line unicorn/prefer-https
          if (!events || !events['http://schemas.openid.net/event/backchannel-logout']) {
            throw new Error('Missing backchannel-logout event claim');
          }

          // "nonce" must not be present
          if (payload.nonce) {
            throw new Error('Logout token must not contain a nonce');
          }

          return {
            sub: payload.sub,
            sid: payload.sid as string | undefined,
          };
        }).pipe(
          Effect.tapError(({ cause }) =>
            Effect.sync(() => {
              const error: Error | any = cause;
              logger.error(`Error validating JWT logout token: ${error.message}`);
              logger.error(error);
            }),
          ),
          Effect.mapError(
            ({ cause }) => new LibraryError({ cause: new Error('Error validating JWT logout token', { cause }) }),
          ),
        );
      }),
    });
  };

  static readonly layer = (logger: LoggingRepository) => Layer.sync(OAuthRepo, () => OAuthRepo.make(logger));
}

/** Promise-facing adapter for the Nest services. Delegates to `OAuthRepo`. */
@Injectable()
export class OAuthRepository {
  private readonly effect: Context.Service.Shape<typeof OAuthRepo>;

  constructor(private logger: LoggingRepository) {
    this.logger.setContext(OAuthRepository.name);
    this.effect = OAuthRepo.make(this.logger);
    registerEffect(this, this.effect);
  }

  authorize(config: OAuthConfig, redirectUrl: string, state?: string, codeChallenge?: string) {
    return runPromise(this.effect.authorize(config, redirectUrl, state, codeChallenge));
  }

  getLogoutEndpoint(config: OAuthConfig) {
    return runPromise(this.effect.getLogoutEndpoint(config));
  }

  getProfileAndOAuthSid(
    config: OAuthConfig,
    url: string,
    expectedState: string,
    codeVerifier: string,
  ): Promise<{ profile: OAuthProfile; sid?: string; idToken?: string }> {
    return runPromise(this.effect.getProfileAndOAuthSid(config, url, expectedState, codeVerifier));
  }

  getProfilePicture(url: string) {
    return runPromise(this.effect.getProfilePicture(url));
  }

  validateLogoutToken(config: OAuthConfig, logoutToken: string): Promise<{ sub?: string; sid?: string } | null> {
    return runPromise(this.effect.validateLogoutToken(config, logoutToken));
  }
}
