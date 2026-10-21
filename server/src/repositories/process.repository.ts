import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { SpawnOptionsWithoutStdio, fork, spawn } from 'node:child_process';
import { Duplex } from 'node:stream';
import { effectOf, registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runSync } from 'src/effect/run.js';

const createDuplexStream = (
  spawnFn: typeof spawn,
  command: string,
  args?: readonly string[],
  options?: SpawnOptionsWithoutStdio,
): Duplex => {
  let isStdinClosed = false;
  let drainCallback: undefined | (() => void);

  const process = spawnFn(command, args, options);
  const duplex = new Duplex({
    // duplex -> stdin
    write(chunk, encoding, callback) {
      // drain the input if process dies
      if (isStdinClosed) {
        return callback();
      }

      // handle stream backpressure
      if (process.stdin.write(chunk, encoding)) {
        callback();
      } else {
        drainCallback = callback;
        process.stdin.once('drain', () => {
          drainCallback = undefined;
          callback();
        });
      }
    },

    read() {
      // no-op
    },

    final(callback) {
      if (isStdinClosed) {
        callback();
      } else {
        process.stdin.end(callback);
      }
    },

    destroy(error, callback) {
      if (process.exitCode === null && process.signalCode === null) {
        process.kill();
      }
      callback(error);
    },
  });

  // stdout -> duplex
  process.stdout.on('data', (chunk) => {
    // handle stream backpressure
    if (!duplex.push(chunk)) {
      process.stdout.pause();
    }
  });

  duplex.on('resume', () => process.stdout.resume());

  // end handling
  let isStdoutClosed = false;
  function close(error?: Error) {
    isStdinClosed = true;

    if (error) {
      duplex.destroy(error);
    } else if (isStdoutClosed && typeof process.exitCode === 'number') {
      duplex.push(null);
    }
  }

  process.stdout.on('close', () => {
    isStdoutClosed = true;
    close();
  });

  // error handling
  process.on('error', close);
  process.stdout.on('error', close);
  process.stdin.on('error', (error) => {
    if ((error as { code?: 'EPIPE' })?.code === 'EPIPE') {
      try {
        drainCallback!();
      } catch (error) {
        close(error as Error);
      }
    } else {
      close(error);
    }
  });

  let stderr = '';
  process.stderr.on('data', (chunk) => (stderr += chunk));

  process.on('exit', (code) => {
    console.info(`${command} exited (${code})`);

    if (code === 0) {
      close();
    } else {
      close(new Error(`${command} non-zero exit code (${code})\n${stderr}`));
    }
  });

  return Object.assign(duplex, { _process: process });
};

export class ProcessRepo extends Context.Service<
  ProcessRepo,
  {
    spawnDuplexStream(
      command: string,
      args?: readonly string[],
      options?: SpawnOptionsWithoutStdio,
    ): Effect.Effect<Duplex, LibraryError>;
    fork(...args: Parameters<typeof fork>): Effect.Effect<ReturnType<typeof fork>, LibraryError>;
  }
>()('immich/repositories/ProcessRepo') {
  /** `getSpawn` is read per call so a replaced `ProcessRepository.spawn` (tests) is honoured. */
  static readonly make = (getSpawn: () => typeof spawn) =>
    ProcessRepo.of({
      spawnDuplexStream: Effect.fn('Process.spawnDuplexStream')((command, args, options) =>
        Effect.try({
          try: () => createDuplexStream(getSpawn(), command, args, options),
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),

      fork: Effect.fn('Process.fork')((...args: Parameters<typeof fork>) =>
        Effect.try({
          try: () => fork(...args),
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),
    });

  static readonly layer = Layer.sync(ProcessRepo, () => ProcessRepo.make(() => spawn));
}

/** Synchronous adapter for the Nest services (streams and child processes are returned as-is). Delegates to `ProcessRepo`. */
@Injectable()
export class ProcessRepository {
  spawn = spawn;

  constructor() {
    // `getSpawn` is read per call so a replaced `spawn` (tests) is honoured.
    registerEffect(
      this,
      ProcessRepo.make(() => this.spawn),
    );
  }

  spawnDuplexStream(command: string, args?: readonly string[], options?: SpawnOptionsWithoutStdio): Duplex {
    return runSync(effectOf(ProcessRepo, this).spawnDuplexStream(command, args, options));
  }

  fork(...args: Parameters<typeof fork>): ReturnType<typeof fork> {
    return runSync(effectOf(ProcessRepo, this).fork(...args));
  }
}
