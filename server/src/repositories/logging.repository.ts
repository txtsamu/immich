import { ConsoleLogger, Inject, Injectable, Scope } from '@nestjs/common';
import { isLogLevelEnabled } from '@nestjs/common/services/utils/is-log-level-enabled.util';
import { Context, Effect, Layer } from 'effect';
import { ClsService } from 'nestjs-cls';
import { Telemetry } from 'src/decorators.js';
import { registerEffect } from 'src/effect/bridge.js';
import { runSync } from 'src/effect/run.js';
import { LogFormat, LogLevel } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';

type LogDetails = any;
type LogFunction = () => string;

const LOG_LEVELS = [LogLevel.Verbose, LogLevel.Debug, LogLevel.Log, LogLevel.Warn, LogLevel.Error, LogLevel.Fatal];

enum LogColor {
  RED = 31,
  GREEN = 32,
  YELLOW = 33,
  BLUE = 34,
  MAGENTA_BRIGHT = 95,
  CYAN_BRIGHT = 96,
}

let appName: string | undefined;
let logLevels: LogLevel[] = [LogLevel.Log, LogLevel.Warn, LogLevel.Error, LogLevel.Fatal];

export class MyConsoleLogger extends ConsoleLogger {
  private isColorEnabled: boolean;

  constructor(
    private cls: ClsService | undefined,
    options?: { json?: boolean; color?: boolean; context?: string },
  ) {
    super(options?.context || MyConsoleLogger.name, {
      json: options?.json ?? false,
    });
    this.isColorEnabled = !options?.json && (options?.color || false);
  }

  isLevelEnabled(level: LogLevel) {
    return isLogLevelEnabled(level, logLevels);
  }

  formatContext(context: string): string {
    let prefix = appName || '';
    if (context) {
      prefix += (prefix ? ':' : '') + context;
    }

    const correlationId = this.cls?.getId();
    if (correlationId) {
      prefix += `~${correlationId}`;
    }

    if (!prefix) {
      return '';
    }

    return this.colors.yellow(`[${prefix}]`) + ' ';
  }

  private colors = {
    red: (text: string) => this.withColor(text, LogColor.RED),
    green: (text: string) => this.withColor(text, LogColor.GREEN),
    yellow: (text: string) => this.withColor(text, LogColor.YELLOW),
    blue: (text: string) => this.withColor(text, LogColor.BLUE),
    magentaBright: (text: string) => this.withColor(text, LogColor.MAGENTA_BRIGHT),
    cyanBright: (text: string) => this.withColor(text, LogColor.CYAN_BRIGHT),
  };

  private withColor(text: string, color: LogColor) {
    return this.isColorEnabled ? `\u{1B}[${color}m${text}\u{1B}[39m` : text;
  }
}

const emit = (logger: MyConsoleLogger, level: LogLevel, message: string | Error, details: LogDetails[]) => {
  switch (level) {
    case LogLevel.Verbose: {
      logger.verbose(message, ...details);
      break;
    }

    case LogLevel.Debug: {
      logger.debug(message, ...details);
      break;
    }

    case LogLevel.Log: {
      logger.log(message, ...details);
      break;
    }

    case LogLevel.Warn: {
      logger.warn(message, ...details);
      break;
    }

    case LogLevel.Error: {
      logger.error(message, ...details);
      break;
    }

    case LogLevel.Fatal: {
      logger.fatal(message, ...details);
      break;
    }
  }
};

export class LoggingRepo extends Context.Service<
  LoggingRepo,
  {
    setAppName(name: string): Effect.Effect<void>;
    setContext(context: string): Effect.Effect<void>;
    isLevelEnabled(level: LogLevel): Effect.Effect<boolean>;
    setLogLevel(level: LogLevel | false): Effect.Effect<void>;
    getLogLevel(): Effect.Effect<LogLevel>;
    verbose(message: string, ...details: LogDetails): Effect.Effect<void>;
    verboseFn(message: LogFunction, ...details: LogDetails): Effect.Effect<void>;
    debug(message: string, ...details: LogDetails): Effect.Effect<void>;
    debugFn(message: LogFunction, ...details: LogDetails): Effect.Effect<void>;
    log(message: string, ...details: LogDetails): Effect.Effect<void>;
    warn(message: string, ...details: LogDetails): Effect.Effect<void>;
    error(message: string | Error, ...details: LogDetails): Effect.Effect<void>;
    fatal(message: string, ...details: LogDetails): Effect.Effect<void>;
    deprecate(message: string): Effect.Effect<void>;
  }
>()('immich/repositories/LoggingRepo') {
  /**
   * Every method is a synchronous `Effect.sync` so log output stays ordered and immediate when run through `runSync`.
   * Methods are untraced (no spans): logging is a hot path and must not emit spans about itself.
   */
  static readonly make = (logger: MyConsoleLogger) => {
    const write = (level: LogLevel, message: string | Error, details: LogDetails[]) =>
      Effect.sync(() => emit(logger, level, message, details));
    const writeFn = (level: LogLevel, message: LogFunction, details: LogDetails[]) =>
      Effect.suspend(() =>
        logger.isLevelEnabled(level) ? Effect.sync(() => emit(logger, level, message(), details)) : Effect.void,
      );

    return LoggingRepo.of({
      setAppName: (name) =>
        Effect.sync(() => {
          appName = name.charAt(0).toUpperCase() + name.slice(1);
        }),
      setContext: (context) => Effect.sync(() => logger.setContext(context)),
      isLevelEnabled: (level) => Effect.sync(() => logger.isLevelEnabled(level)),
      setLogLevel: (level) =>
        Effect.sync(() => {
          logLevels = level ? LOG_LEVELS.slice(LOG_LEVELS.indexOf(level)) : [];
        }),
      getLogLevel: () => Effect.sync(() => logLevels[0] || LogLevel.Fatal),
      verbose: (message, ...details) => write(LogLevel.Verbose, message, details),
      verboseFn: (message, ...details) => writeFn(LogLevel.Verbose, message, details),
      debug: (message, ...details) => write(LogLevel.Debug, message, details),
      debugFn: (message, ...details) => writeFn(LogLevel.Debug, message, details),
      log: (message, ...details) => write(LogLevel.Log, message, details),
      warn: (message, ...details) => write(LogLevel.Warn, message, details),
      error: (message, ...details) => write(LogLevel.Error, message, details),
      fatal: (message, ...details) => write(LogLevel.Fatal, message, details),
      deprecate: (message) => write(LogLevel.Warn, `[Deprecated] ${message}`, []),
    });
  };

  static readonly layer = Layer.sync(LoggingRepo, () => LoggingRepo.make(new MyConsoleLogger(undefined)));
}

/** Synchronous adapter for the Nest services (statics like `create()` included). Delegates to `LoggingRepo`. */
@Injectable({ scope: Scope.TRANSIENT })
@Telemetry({ enabled: false })
export class LoggingRepository {
  private logger: MyConsoleLogger;
  private readonly effect: Context.Service.Shape<typeof LoggingRepo>;

  constructor(
    @Inject(ClsService) cls: ClsService | undefined,
    @Inject(ConfigRepository) configRepository: ConfigRepository | undefined,
  ) {
    let isNoColor = false;
    let logFormat = LogFormat.Console;
    if (configRepository) {
      const env = configRepository.getEnv();
      isNoColor = env.noColor;
      logFormat = env.logFormat ?? logFormat;
    }
    this.logger = new MyConsoleLogger(cls, {
      context: LoggingRepository.name,
      json: logFormat === LogFormat.Json,
      color: !isNoColor,
    });
    this.effect = LoggingRepo.make(this.logger);
    registerEffect(this, this.effect);
  }

  static create(context?: string) {
    const logger = new LoggingRepository(undefined, undefined);
    if (context) {
      logger.setContext(context);
    }

    return logger;
  }

  setAppName(name: string): void {
    runSync(this.effect.setAppName(name));
  }

  setContext(context: string) {
    runSync(this.effect.setContext(context));
  }

  isLevelEnabled(level: LogLevel) {
    return runSync(this.effect.isLevelEnabled(level));
  }

  setLogLevel(level: LogLevel | false): void {
    runSync(this.effect.setLogLevel(level));
  }

  getLogLevel(): LogLevel {
    return runSync(this.effect.getLogLevel());
  }

  verbose(message: string, ...details: LogDetails) {
    runSync(this.effect.verbose(message, ...details));
  }

  verboseFn(message: LogFunction, ...details: LogDetails) {
    runSync(this.effect.verboseFn(message, ...details));
  }

  debug(message: string, ...details: LogDetails) {
    runSync(this.effect.debug(message, ...details));
  }

  debugFn(message: LogFunction, ...details: LogDetails) {
    runSync(this.effect.debugFn(message, ...details));
  }

  log(message: string, ...details: LogDetails) {
    runSync(this.effect.log(message, ...details));
  }

  warn(message: string, ...details: LogDetails) {
    runSync(this.effect.warn(message, ...details));
  }

  error(message: string | Error, ...details: LogDetails) {
    runSync(this.effect.error(message, ...details));
  }

  fatal(message: string, ...details: LogDetails) {
    runSync(this.effect.fatal(message, ...details));
  }

  deprecate(message: string) {
    runSync(this.effect.deprecate(message));
  }
}
