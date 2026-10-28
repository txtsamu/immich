# Converting a Nest service to Effect (stage 2 pattern)

Stage 1 gave every repository an Effect service (`<Name>Repo`) plus a Promise adapter (`<Name>Repository`), see `effect-repositories.md`. Stage 2 converts the Nest services in `server/src/services/` (and `utils/` helpers they call) so their logic runs as Effects over those `*Repo` services.

## How services reach the repositories

`BaseService` has `protected get fx(): Fx` (`server/src/effect/fx.ts`, generated from the BaseService constructor). `this.fx.asset` is the **Effect** service for `this.assetRepository`:

- In production the adapters register their Effect service (`registerEffect`, `src/effect/bridge.ts`), so `this.fx.asset.getById(id)` calls the Effect implementation directly.
- In specs the repositories are vitest mocks with Promise methods; `effectOf` lifts them (`lift` in `bridge.ts`), so `this.fx.asset.getById(id)` calls `mocks.asset.getById(id)` and wraps the result. **All existing specs and mocks keep working untouched.** Do not edit specs to make a conversion pass; if a spec fails, the conversion changed behavior.

Key names are the constructor property names minus `Repository`: `access, activity, album, albumUser, apiKey, app, asset, assetEdit, assetFile, assetJob, authChallenge, config, cron, crypto, database, download, email, event, integrity, job, library, map, media, memory, metadata, move, notification, oauth, partner, passkey, plugin, process, search, serverInfo, session, sharedLink, sharedLinkAsset, stack, storage, sync, syncCheckpoint, systemMetadata, tag, telemetry, trash, user, version, videoStream, view, webAuthn, websocket, workflow` plus `logger`. See `fx.ts`.

## Shape of a converted method

The public method keeps its **name, parameters, return type and decorators** (`@OnEvent`, `@OnJob`, `@Injectable`, ...). Controllers, other services, and specs call it as before. The body becomes an Effect run through `runPromise`:

```ts
import { Effect } from 'effect';
import { runPromise } from 'src/effect/run.js';

async getStatistics(auth: AuthDto, dto: ActivityDto): Promise<ActivityStatisticsResponseDto> {
  return runPromise(
    Effect.gen({ self: this }, function* () {
      yield* this.requireAccessFx({ auth, permission: Permission.AlbumRead, ids: [dto.albumId] });
      return yield* this.fx.activity.getStatistics({ albumId: dto.albumId, assetId: dto.assetId });
    }),
  );
}
```

- `Effect.gen({ self: this }, function* () { ... })` gives `this` inside the generator (v4 syntax; v3's `Effect.gen(this, ...)` does not compile). Use `Effect.fn('Name.method')(function* (this: Self, ...) {...})` only for private reusable helpers.
- `this` inside the generator is fine in `src/services/**` (eslint `unicorn/no-this-outside-of-class` is off there). `lift` in `bridge.ts` also lifts nested plain-object groups (e.g. `fx.access.asset.*`), and `utils/access.ts` additionally guards with `asEffect`.
- Expected failures that were `throw new BadRequestException(...)` etc. become `return yield* Effect.fail(new BadRequestException(...))` (Nest exception classes are used as the error type `E`). `runPromise` rethrows the same instance, so the HTTP response and specs are identical. A plain `throw` inside the generator also works (defect, rethrown unchanged) but loses the typed error; prefer `Effect.fail` for domain errors, keep `throw` for programmer errors.
- Promise-returning non-repository calls (`fetch`, library calls, `this.someOtherService.x()`) use `Effect.tryPromise({ try: ..., catch: (cause) => cause })` or `Effect.promise` only when it cannot reject, never raw `await` inside the generator. Calls into other **services** (`this.jobRepository.queue(...)` is a repository; `this.otherService.x()` is a service) stay Promise calls wrapped with `Effect.tryPromise` until that service is converted.
- Sequential `for` loops and `await Promise.all(...)` map to `for ... of` with `yield*` and `Effect.all([...], { concurrency: 'unbounded' })` (use bounded concurrency only if the original was bounded). Preserve ordering and short-circuit semantics exactly.
- Thrown values must stay the same classes with the same messages. Logging calls (`this.logger.*`) stay as plain calls; they are synchronous.
- Preserve sync/async behavior of non-async methods (getters, validators, decorators-registered handlers): keep them plain functions unless they call repositories.
- Do not change behavior, query parameters, ordering, or logging order. This is a mechanical re-expression.

## Helpers (added by the foundation step)

`BaseService` gains `requireAccessFx`, `checkAccessFx`, `getConfigFx`, `updateConfigFx`, `isSetupAvailableFx`, `requireSetupAvailableFx`, `createUserFx`, `createLoginResponseFx`; the Promise versions delegate to them. `src/utils/access.ts` and `src/utils/config.ts` export `requireAccessFx`/`checkAccessFx`/`getConfigFx`/`updateConfigFx` taking the Effect repo shapes. If you need an Effect version of another helper, add `xFx` next to the original and make the original delegate; mention it in your report.

## Do not touch

Other agents' services, `base.service.ts`, `src/effect/*`, `test/**`, controllers, `src/repositories/**`, registries. If a service needs a change in one of those, report it instead.

## Verify (from `server/`)

```bash
pnpm exec tsc --noEmit 2>&1 | grep -E '<your files>'       # then also grep the whole output for your class names
pnpm exec eslint <your files> --max-warnings 0
pnpm exec prettier --write <your files>
pnpm exec vitest run --config test/vitest.config.mjs src/services/<name>.service.spec.ts   # rtk hides vitest output: read server/.vitest/json/output.json
```
