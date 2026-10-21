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

const liftFunction =
  (target: object, fn: (...args: unknown[]) => unknown) =>
  (...args: unknown[]) =>
    Effect.tryPromise({
      try: async () => await Reflect.apply(fn, target, args),
      catch: (cause) => new LiftedError({ cause }),
    });

const isPlainObject = (value: unknown): value is object =>
  typeof value === 'object' && value !== null && Object.getPrototypeOf(value) === Object.prototype;

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
        if (isPlainObject(value)) {
          return lift(value);
        }
        if (typeof value !== 'function') {
          return value;
        }
        return liftFunction(target, value);
      },
    },
  );
  liftCache.set(target, lifted);
  return lifted;
};

const guardCache = new WeakMap<object, unknown>();

/**
 * Serves the registered Effect service, except for members the adapter has overridden on the instance
 * (a `vi.spyOn(adapter, 'method')` or an own function property) and members only the adapter has:
 * those are lifted from the adapter, so replacing an adapter method keeps affecting its callers.
 */
const guard = (adapter: object, service: object): unknown => {
  const cached = guardCache.get(adapter);
  if (cached) {
    return cached;
  }

  const guarded = new Proxy(service, {
    get: (target, property, receiver) => {
      const own = Reflect.getOwnPropertyDescriptor(adapter, property);
      if (own && typeof own.value === 'function') {
        return liftFunction(adapter, own.value);
      }

      const value = Reflect.get(target, property, receiver);
      if (isPlainObject(value)) {
        const nested = Reflect.get(adapter, property);
        return typeof nested === 'object' && nested !== null ? guard(nested, value) : value;
      }
      if (value === undefined) {
        const fallback = Reflect.get(adapter, property);
        if (typeof fallback === 'function') {
          return liftFunction(adapter, fallback);
        }
      }
      return value;
    },
  });
  guardCache.set(adapter, guarded);
  return guarded;
};

/** The Effect service behind a repository adapter; falls back to lifting the adapter's Promise methods. */
export const effectOf = <S extends AnyService>(_tag: S, adapter: object): Shape<S> => {
  if (typeof adapter !== 'object' || adapter === null) {
    return adapter as never;
  }
  const service = registry.get(adapter);
  return (service ? guard(adapter, service as object) : lift(adapter)) as Shape<S>;
};
