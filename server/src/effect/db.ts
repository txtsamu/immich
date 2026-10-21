import { Context, Effect } from 'effect';
import type { Kysely } from 'kysely';
import { DatabaseError } from 'src/effect/errors.js';
import { DB } from 'src/schema/index.js';

/** The shared Kysely instance. Kysely stays the query builder; Effect wraps execution and typed errors. */
export class Db extends Context.Service<Db, Kysely<DB>>()('immich/effect/Db') {}

/** Run a Kysely query as an Effect, mapping any thrown value to a `DatabaseError`. */
export const query = <A>(db: Kysely<DB>, run: (db: Kysely<DB>) => PromiseLike<A>) =>
  Effect.tryPromise({ try: () => run(db), catch: (cause) => new DatabaseError({ cause }) });
