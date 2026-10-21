# Converting a repository to Effect (stage 1 pattern)

Reference implementation: `server/src/repositories/system-metadata.repository.ts`. Helpers live in `server/src/effect/` (`db.ts`, `errors.ts`, `run.ts`). Effect version is `effect@4.0.5` (installed API wins over `repos/effect` main; check `server/node_modules/effect/dist/*.d.ts` when in doubt). Read `repos/effect/LLMS.md` first.

## Shape: Effect service + thin Promise adapter

Every repository file exports two things:

1. **`<Name>Repo`** (e.g. `AssetRepo`; not `<Name>Repo`, which collides with the Nest services in `src/services/`): a `Context.Service` whose methods return `Effect.Effect<A, E>` with typed errors, a `static make(deps)` that builds the implementation with `Effect.fn('<Name>.<method>')`, and a `static layer` (`Layer.effect(...)`, reading `Db` via `yield* Db`).
2. **`<Name>Repository`**: the existing `@Injectable()` class. It keeps its **exact** constructor signature, public method names, parameter types, return types, decorators (`@GenerateSql`, `@Chunked`, ...) and JSDoc. Its constructor does `this.effect = <Name>Service.make(db, ...)` and each method is `return runPromise(this.effect.method(...))`.

Why the adapter: Nest services, `test/medium.factory.ts` (`new key(db)`), the mocks in `test/repositories/*.mock.ts` and all 2k unit tests depend on the Promise-based class. They must keep compiling and passing untouched. Later stages migrate services to consume `<Name>Repo` directly and then the adapters are deleted.

## Rules

- Kysely stays the query builder (the `.sql` docs are generated from it). Wrap execution with `query(db, (db) => <kysely expression>)` from `src/effect/db.ts`; it maps thrown values to `DatabaseError`.
- Typed errors are `Data.TaggedError` classes in `src/effect/errors.ts` (`DatabaseError`, `FileSystemError`, `LibraryError`), each with `cause: unknown`. Add a new tagged error there only if none fits.
- `runPromise` / `runSync` (`src/effect/run.ts`) rethrow the **original** thrown value (`cause`) so Postgres error-code checks and the global exception filter behave identically. Never change what a method throws or resolves with.
- Use `Effect.fn('Name.method')(function* (...) { ... })` for methods with multiple steps. Use `Effect.try` / `Effect.tryPromise` around throwing library calls (bcrypt, jwt, fs, ffmpeg, ...).
- Synchronous methods in the adapter stay synchronous (use `runSync`). Methods returning streams, async generators, `ReadStream`s, or other non-Promise values keep the same return type: build the value inside the Effect service and return it from the adapter without changing its type. Do not buffer a stream into memory.
- Methods that take a transaction or `Kysely` argument keep it (pass it through to `query`).
- Do not change SQL, query shape, ordering, or logic. This stage is a mechanical re-expression, not a refactor.
- Use `yield*` of the effect inside `Effect.gen`; do not mix `async`/`await` inside the service (only inside `Effect.tryPromise`).

## Do not touch

`index.ts` registries, `base.service.ts`, `app.module.ts`, `test/**` mocks and factories, any service, controller, or spec. Only edit your assigned `*.repository.ts` files (and add an `*.effect.spec.ts` next to one only if it is cheap and needs no database).

## Verify (run from `server/`)

```bash
pnpm exec tsc --noEmit 2>&1 | grep -E '<your files>'      # other agents are editing other files; ignore their errors
pnpm exec eslint <your files> --max-warnings 0
pnpm exec prettier --write <your files>
pnpm exec vitest run --config test/vitest.config.mjs <related spec files>   # unit specs that mock your repo
```

## Conventions learned in stage 1

- **Service type is inferred.** Define `const makeX = (db) => ({ method: Effect.fn('X.method')(function* (...) {...}), ... })`, declare `class XRepo extends Context.Service<XRepo, ReturnType<typeof makeX>>()('immich/repositories/XRepo')`, and `static make = (db) => XRepo.of(makeX(db))`. Hand-writing Kysely result types is impractical.
- **No new instance fields on classes without private members.** Adding `private effect` to a class that had none breaks specs that assign `Mocked<RepositoryInterface<X>>` to `X`. Use a module-level singleton (`const service = XRepo.make()`) or a lazily created optional field instead. Classes that already had `private db` / `private logger` just swap/add the field.
- **Keep sync members sync** (`runSync`) and lazy streams/generators unbuffered. Keep stateful framework callbacks (gateway hooks, extism pools, bulk DDL imports) as plain code and say so in the file.
- **Dependencies that are not the DB** (logger, config, cache) are passed as arguments to `make` / `layer(deps)`.
- **Known behavior shifts**: methods that used to throw synchronously before returning a Promise now reject (e.g. `JobRepository.pause/resume/empty/clear` with a missing queue token, `MediaRepository.transcode` twoPass+stream, email methods).
- **Left partially converted** (stage 2 candidates): `plugin.load/callMethod`, `map.init` + geodata import helpers, `websocket` gateway hooks, `telemetry` prototype wrapping, `database.withLock` internals.
