import { Context, Effect } from 'effect';
import { LiftedError } from 'src/effect/errors.js';

type AnyService = Context.Service<any, any>;
type Shape<S extends AnyService> = Context.Service.Shape<S>;

/**
 * Maps a Promise-facing repository adapter to the Effect service it delegates to.
 * Adapters register themselves in their constructor, so services running in production call the
 * Effect implementation directly instead of going through the Promise facade.
 */
const registry = new WeakMap<object, unknown>();

export const registerEffect = <S extends AnyService>(adapter: object, service: Shape<S>) => {
  registry.set(adapter, service);
};

const liftCache = new WeakMap<object, unknown>();

/**
 * Wraps every function on a Promise-based object (typically a vitest mock) so it returns an Effect.
 * Used when no Effect service is registered for the object, which keeps the existing mock-based specs working.
 */
const lift = (target: object): unknown => {
  const cached = liftCache.get(target);
  if (cached) {
    return cached;
  }

  const lifted = new Proxy(
    {},
    {
      get: (_, property) => {
        const value = Reflect.get(target, property);
        // nested groups, e.g. the access repository's `asset`, `album`, ... sub-objects
        if (typeof value === 'object' && value !== null && Object.getPrototypeOf(value) === Object.prototype) {
          return lift(value);
        }
        if (typeof value !== 'function') {
          return value;
        }
        return (...args: unknown[]) =>
          Effect.tryPromise({
            try: async () => await Reflect.apply(value, target, args),
            catch: (cause) => new LiftedError({ cause }),
          });
      },
    },
  );
  liftCache.set(target, lifted);
  return lifted;
};

/** The Effect service behind a repository adapter; falls back to lifting the adapter's Promise methods. */
export const effectOf = <S extends AnyService>(_tag: S, adapter: object): Shape<S> =>
  (registry.get(adapter) ?? lift(adapter)) as Shape<S>;
