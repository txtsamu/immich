# Agent instructions

## Fork context

This is a fork of Immich on the `strip-ai` branch. All AI / machine-learning features (smart search, faces/people, OCR, duplicate detection, the `machine-learning` service) are removed and must stay removed. Do not reintroduce them, their tables, DTOs, permissions, job names or UI.

The server (`server/`) is being incrementally rewritten from plain NestJS services/repositories to Effect v4 (`effect@4`). Constraints:

- The generated OpenAPI spec (`open-api/immich-openapi-specs.json`) must stay identical after each stage so the web app, SDK and mobile client keep working.
- `pnpm exec tsc --noEmit`, `pnpm run lint` and the unit tests in `server/` must stay green after each stage.

## Vendored repositories (`repos/`)

`repos/` holds squashed `git subtree` copies of upstream libraries so you can read their real source, tests and module structure.

- `repos/effect/` is `Effect-TS/effect` (Effect v4). Before writing any Effect code, read `repos/effect/LLMS.md`. Also see `repos/effect/migration/` and `repos/effect/ai-docs/`.
- Treat everything under `repos/` as read-only reference material.
- Prefer patterns found there over generated guesses or web search results.
- Do not edit files under `repos/` unless explicitly asked.
- Do not import from `repos/`. Application code imports from normal package dependencies (`effect`).
- The vendored `main` may be ahead of the installed `effect` version in `server/package.json`; if an API from `repos/` is missing from `node_modules/effect`, check the installed package before using it.

Update a vendored repo with:

```bash
git subtree pull --prefix=repos/effect https://github.com/Effect-TS/effect.git main --squash
```

## Distilled patterns

Project-local notes on how Effect is used in this codebase live in `agent-patterns/` (e.g. `agent-patterns/effect-schema.md`). Read the relevant file before writing code that touches that module, and add a new one when you study a module in `repos/` for the first time.
