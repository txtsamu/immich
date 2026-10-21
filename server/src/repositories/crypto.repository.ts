import { Injectable } from '@nestjs/common';
import { compareSync, hash } from 'bcrypt';
import { Context, Effect, Layer } from 'effect';
import jwt from 'jsonwebtoken';
import { createHash, createPublicKey, createVerify, randomBytes, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';

const lib = <A>(run: () => A) => Effect.try({ try: run, catch: (cause) => new LibraryError({ cause }) });

const libPromise = <A>(run: () => PromiseLike<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new LibraryError({ cause }) });

export class CryptoRepo extends Context.Service<
  CryptoRepo,
  {
    randomUUID(): Effect.Effect<string, LibraryError>;
    randomBytes(size: number): Effect.Effect<Buffer, LibraryError>;
    hashBcrypt(data: string | Buffer, saltOrRounds: string | number): Effect.Effect<string, LibraryError>;
    compareBcrypt(data: string | Buffer, encrypted: string): Effect.Effect<boolean, LibraryError>;
    hashSha256(value: string): Effect.Effect<Buffer, LibraryError>;
    verifySha256(value: string, encryptedValue: string, publicKey: string): Effect.Effect<boolean, LibraryError>;
    hashSha1(value: string | Buffer): Effect.Effect<Buffer, LibraryError>;
    hashFile(filepath: string | Buffer): Effect.Effect<Buffer, LibraryError>;
    randomBytesAsText(bytes: number): Effect.Effect<string, LibraryError>;
    signJwt(
      payload: string | object | Buffer,
      secret: string,
      options?: jwt.SignOptions,
    ): Effect.Effect<string, LibraryError>;
    verifyJwt<T = any>(token: string, secret: string): Effect.Effect<T, LibraryError>;
  }
>()('immich/repositories/CryptoRepo') {
  static readonly make = () =>
    CryptoRepo.of({
      randomUUID: Effect.fn('Crypto.randomUUID')(() => lib(() => randomUUID())),

      randomBytes: Effect.fn('Crypto.randomBytes')((size: number) => lib(() => randomBytes(size))),

      hashBcrypt: Effect.fn('Crypto.hashBcrypt')((data: string | Buffer, saltOrRounds: string | number) =>
        libPromise(() => hash(data, saltOrRounds)),
      ),

      compareBcrypt: Effect.fn('Crypto.compareBcrypt')((data: string | Buffer, encrypted: string) =>
        lib(() => compareSync(data, encrypted)),
      ),

      hashSha256: Effect.fn('Crypto.hashSha256')((value: string) =>
        lib(() => createHash('sha256').update(value).digest()),
      ),

      verifySha256: Effect.fn('Crypto.verifySha256')((value: string, encryptedValue: string, publicKey: string) =>
        lib(() => {
          const publicKeyBuffer = Buffer.from(publicKey, 'base64');
          const cryptoPublicKey = createPublicKey({
            key: publicKeyBuffer,
            type: 'spki',
            format: 'pem',
          });

          const verifier = createVerify('SHA256');
          verifier.update(value);
          verifier.end();
          const encryptedValueBuffer = Buffer.from(encryptedValue, 'base64');
          return verifier.verify(cryptoPublicKey, encryptedValueBuffer);
        }),
      ),

      hashSha1: Effect.fn('Crypto.hashSha1')((value: string | Buffer) =>
        lib(() => createHash('sha1').update(value).digest()),
      ),

      hashFile: Effect.fn('Crypto.hashFile')((filepath: string | Buffer) =>
        libPromise(
          () =>
            new Promise<Buffer>((resolve, reject) => {
              const hash = createHash('sha1');
              const stream = createReadStream(filepath);
              stream.on('error', (error) => reject(error));
              stream.on('data', (chunk) => hash.update(chunk));
              stream.on('end', () => resolve(hash.digest()));
            }),
        ),
      ),

      randomBytesAsText: Effect.fn('Crypto.randomBytesAsText')((bytes: number) =>
        lib(() => randomBytes(bytes).toString('base64').replaceAll(/\W/g, '')),
      ),

      signJwt: Effect.fn('Crypto.signJwt')(
        (payload: string | object | Buffer, secret: string, options?: jwt.SignOptions) =>
          // eslint-disable-next-line import-x/no-named-as-default-member
          lib(() => jwt.sign(payload, secret, { algorithm: 'HS256', ...options })),
      ),

      verifyJwt: Effect.fn('Crypto.verifyJwt')(function* <T = any>(token: string, secret: string) {
        // eslint-disable-next-line import-x/no-named-as-default-member
        return yield* lib(() => jwt.verify(token, secret, { algorithms: ['HS256'] }) as T);
      }),
    });

  static readonly layer = Layer.sync(CryptoRepo, () => CryptoRepo.make());
}

/** Stateless, so shared by all adapter instances (the adapter class must not gain private fields, see specs). */
const service = CryptoRepo.make();

/** Promise-facing adapter for the Nest services. Delegates to `CryptoRepo`. */
@Injectable()
export class CryptoRepository {
  randomUUID(): string {
    return runSync(service.randomUUID());
  }

  randomBytes(size: number) {
    return runSync(service.randomBytes(size));
  }

  hashBcrypt(data: string | Buffer, saltOrRounds: string | number) {
    return runPromise(service.hashBcrypt(data, saltOrRounds));
  }

  compareBcrypt(data: string | Buffer, encrypted: string) {
    return runSync(service.compareBcrypt(data, encrypted));
  }

  hashSha256(value: string) {
    return runSync(service.hashSha256(value));
  }

  verifySha256(value: string, encryptedValue: string, publicKey: string) {
    return runSync(service.verifySha256(value, encryptedValue, publicKey));
  }

  hashSha1(value: string | Buffer): Buffer {
    return runSync(service.hashSha1(value));
  }

  hashFile(filepath: string | Buffer): Promise<Buffer> {
    return runPromise(service.hashFile(filepath));
  }

  randomBytesAsText(bytes: number) {
    return runSync(service.randomBytesAsText(bytes));
  }

  signJwt(payload: string | object | Buffer, secret: string, options?: jwt.SignOptions): string {
    return runSync(service.signJwt(payload, secret, options));
  }

  verifyJwt<T = any>(token: string, secret: string): T {
    return runSync(service.verifyJwt<T>(token, secret));
  }
}
