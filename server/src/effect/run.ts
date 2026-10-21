import { Cause, Effect, Exit } from 'effect';

/** Errors wrapping the original thrown value in `cause` (see `errors.ts`). */
type Wrapped = { _tag: string; cause: unknown };

const isWrapped = (error: unknown): error is Wrapped =>
  typeof error === 'object' && error !== null && '_tag' in error && 'cause' in error;

const unwrap = (error: unknown) => (isWrapped(error) ? error.cause : error);

/**
 * Bridge from Effect to the Promise-based Nest services.
 * Rejects with the original thrown value (not a FiberFailure) so existing error handling,
 * e.g. Postgres error code checks and the global exception filter, behaves identically.
 */
export const runPromise = async <A, E>(effect: Effect.Effect<A, E>): Promise<A> => {
  const exit = await Effect.runPromiseExit(effect);
  if (Exit.isSuccess(exit)) {
    return exit.value;
  }
  throw unwrap(Cause.squash(exit.cause));
};

/** Synchronous variant of `runPromise`. */
export const runSync = <A, E>(effect: Effect.Effect<A, E>): A => {
  const exit = Effect.runSyncExit(effect);
  if (Exit.isSuccess(exit)) {
    return exit.value;
  }
  throw unwrap(Cause.squash(exit.cause));
};
