import { Layer, ManagedRuntime } from 'effect';

/**
 * The application's shared Effect runtime. It has no layers yet; add app-wide services here (tracer,
 * logger, metrics) so every `runPromise`/`runSync` call picks them up, and they are released on shutdown.
 */
export const appRuntime = ManagedRuntime.make(Layer.empty);

export const disposeAppRuntime = () => appRuntime.dispose();
