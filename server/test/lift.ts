import { isPlainObject, liftFunction } from 'src/effect/bridge.js';

const liftCache = new WeakMap<object, unknown>();

/**
 * Wraps every function on a Promise-based object (a vitest mock) so it returns an Effect.
 * Installed as the bridge fallback by `test/setup.ts`, which keeps the Promise-based mocks in the service specs working.
 */
export const lift = (target: object): unknown => {
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
