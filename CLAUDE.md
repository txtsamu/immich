@AGENTS.md

## Working style

Use subagents whenever possible: fan independent work out in parallel (one subagent per repository/service/spec group, plus read-only Explore agents for research) instead of doing it all inline. Give each subagent a disjoint set of files, tell it not to touch shared registries (`index.ts`, `base.service.ts`, `app.module.ts`), and review and integrate its result yourself.
