import { Injectable } from '@nestjs/common';
import {
  type GenerateAuthenticationOptionsOpts,
  type GenerateRegistrationOptionsOpts,
  type VerifyAuthenticationResponseOpts,
  type VerifyRegistrationResponseOpts,
  generateAuthenticationOptions,
  generateRegistrationOptions,
  verifyAuthenticationResponse,
  verifyRegistrationResponse,
} from '@simplewebauthn/server';
import { Context, Effect, Layer } from 'effect';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';

const methods = () => ({
  generateRegistrationOptions: Effect.fn('WebAuthn.generateRegistrationOptions')(
    (options: GenerateRegistrationOptionsOpts) =>
      Effect.tryPromise({
        try: () => generateRegistrationOptions(options),
        catch: (cause) => new LibraryError({ cause }),
      }),
  ),

  verifyRegistrationResponse: Effect.fn('WebAuthn.verifyRegistrationResponse')(
    (options: VerifyRegistrationResponseOpts) =>
      Effect.tryPromise({
        try: () => verifyRegistrationResponse(options),
        catch: (cause) => new LibraryError({ cause }),
      }),
  ),

  generateAuthenticationOptions: Effect.fn('WebAuthn.generateAuthenticationOptions')(
    (options: GenerateAuthenticationOptionsOpts) =>
      Effect.tryPromise({
        try: () => generateAuthenticationOptions(options),
        catch: (cause) => new LibraryError({ cause }),
      }),
  ),

  verifyAuthenticationResponse: Effect.fn('WebAuthn.verifyAuthenticationResponse')(
    (options: VerifyAuthenticationResponseOpts) =>
      Effect.tryPromise({
        try: () => verifyAuthenticationResponse(options),
        catch: (cause) => new LibraryError({ cause }),
      }),
  ),
});

export class WebAuthnRepo extends Context.Service<WebAuthnRepo, ReturnType<typeof methods>>()(
  'immich/repositories/WebAuthnRepo',
) {
  static readonly make = () => WebAuthnRepo.of(methods());

  /** No database dependency: this service only wraps `@simplewebauthn/server`. */
  static readonly layer = Layer.effect(
    WebAuthnRepo,
    Effect.sync(() => WebAuthnRepo.make()),
  );
}

/** Stateless, so one module-level instance is shared (no instance fields keeps `Mocked<...>` assignable to the class). */
const effect = WebAuthnRepo.make();

/** Promise-facing adapter for the Nest services. Delegates to `WebAuthnRepo`. */
@Injectable()
export class WebAuthnRepository {
  constructor() {
    registerEffect(this, effect);
  }

  generateRegistrationOptions(options: GenerateRegistrationOptionsOpts) {
    return runPromise(effect.generateRegistrationOptions(options));
  }

  verifyRegistrationResponse(options: VerifyRegistrationResponseOpts) {
    return runPromise(effect.verifyRegistrationResponse(options));
  }

  generateAuthenticationOptions(options: GenerateAuthenticationOptionsOpts) {
    return runPromise(effect.generateAuthenticationOptions(options));
  }

  verifyAuthenticationResponse(options: VerifyAuthenticationResponseOpts) {
    return runPromise(effect.verifyAuthenticationResponse(options));
  }
}
