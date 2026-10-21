import { Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { MetricOptions } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { IORedisInstrumentation } from '@opentelemetry/instrumentation-ioredis';
import { NestInstrumentation } from '@opentelemetry/instrumentation-nestjs-core';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { AggregationType } from '@opentelemetry/sdk-metrics';
import { NodeSDK, contextBase } from '@opentelemetry/sdk-node';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import { Context, Effect } from 'effect';
import { snakeCase, startCase } from 'lodash-es';
import { MetricService } from 'nestjs-otel';
import { copyMetadataFromFunctionToFunction } from 'nestjs-otel/lib/opentelemetry.utils.js';
import { excludePaths, serverVersion } from 'src/constants.js';
import { ConfigError } from 'src/effect/errors.js';
import { runSync } from 'src/effect/run.js';
import { ImmichTelemetry, MetadataKey } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';

type MetricGroupOptions = { enabled: boolean };

export class MetricGroupRepo extends Context.Service<
  MetricGroupRepo,
  {
    addToCounter(name: string, value: number, options?: MetricOptions): Effect.Effect<void>;
    addToGauge(name: string, value: number, options?: MetricOptions): Effect.Effect<void>;
    addToHistogram(name: string, value: number, options?: MetricOptions): Effect.Effect<void>;
    configure(options: MetricGroupOptions): Effect.Effect<void>;
  }
>()('immich/repositories/MetricGroupRepo') {
  /** Untraced and synchronous: metrics are recorded from hot paths (every request/job/query). */
  static readonly make = (metricService: MetricService) => {
    let enabled = false;
    return MetricGroupRepo.of({
      addToCounter: (name, value, options) =>
        Effect.sync(() => {
          if (enabled) {
            metricService.getCounter(name, options).add(value);
          }
        }),
      addToGauge: (name, value, options) =>
        Effect.sync(() => {
          if (enabled) {
            metricService.getUpDownCounter(name, options).add(value);
          }
        }),
      addToHistogram: (name, value, options) =>
        Effect.sync(() => {
          if (enabled) {
            metricService.getHistogram(name, options).record(value);
          }
        }),
      configure: (options) =>
        Effect.sync(() => {
          enabled = options.enabled;
        }),
    });
  };
}

/** Synchronous adapter over `MetricGroupRepo`. */
export class MetricGroupRepository {
  private readonly effect: Context.Service.Shape<typeof MetricGroupRepo>;

  constructor(metricService: MetricService) {
    this.effect = MetricGroupRepo.make(metricService);
  }

  addToCounter(name: string, value: number, options?: MetricOptions): void {
    runSync(this.effect.addToCounter(name, value, options));
  }

  addToGauge(name: string, value: number, options?: MetricOptions): void {
    runSync(this.effect.addToGauge(name, value, options));
  }

  addToHistogram(name: string, value: number, options?: MetricOptions): void {
    runSync(this.effect.addToHistogram(name, value, options));
  }

  configure(options: MetricGroupOptions): this {
    runSync(this.effect.configure(options));
    return this;
  }
}

const aggregationBoundaries = [
  0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10_000,
];

let instance: NodeSDK | undefined;

export const bootstrapTelemetry = (port: number) => {
  if (instance) {
    throw new Error('OpenTelemetry SDK already started');
  }

  const { telemetry } = new ConfigRepository().getEnv();

  instance = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: `immich`,
      [ATTR_SERVICE_VERSION]: serverVersion.toString(),
    }),
    metricReader: new PrometheusExporter({ port }),
    contextManager: new AsyncLocalStorageContextManager(),
    instrumentations: [
      new HttpInstrumentation({
        enabled: telemetry.metrics.has(ImmichTelemetry.Api),
        ignoreIncomingRequestHook: (request) => excludePaths.some((item) => request.url?.startsWith(item)),
      }),
      new IORedisInstrumentation(),
      new NestInstrumentation(),
      new PgInstrumentation(),
    ],
    views: [
      {
        instrumentName: '*',
        instrumentUnit: 'ms',
        aggregation: {
          type: AggregationType.EXPLICIT_BUCKET_HISTOGRAM,
          options: { boundaries: aggregationBoundaries },
        },
      },
    ],
  });

  instance.start();
};

export const teardownTelemetry = async () => {
  if (!instance) {
    return;
  }

  await instance.shutdown();
  instance = undefined;
};

/**
 * Wraps every prototype method of `Repository` with a duration histogram.
 * This is reflection over property descriptors and stays plain synchronous code; the wrapped
 * methods must keep their exact return value (Promise or not), so it is not expressed as an Effect pipeline.
 */
const wrapRepository = (metricService: MetricService, Repository: new (...args: any[]) => unknown) => {
  const className = Repository.name;
  const descriptors = Object.getOwnPropertyDescriptors(Repository.prototype);
  const unit = 'ms';

  for (const [propName, descriptor] of Object.entries(descriptors)) {
    const isMethod = typeof descriptor.value === 'function' && propName !== 'constructor';
    if (!isMethod) {
      continue;
    }

    const method = descriptor.value;
    const propertyName = snakeCase(propName);
    const metricName = `${snakeCase(className).replaceAll(/_(?=(repository)|(controller)|(provider)|(service)|(module))/g, '.')}.${propertyName}.duration`;

    const histogram = metricService.getHistogram(metricName, {
      prefix: 'immich',
      description: `The elapsed time in ${unit} for the ${startCase(className)} to ${propertyName.toLowerCase()}`,
      unit,
      valueType: contextBase.ValueType.DOUBLE,
    });

    descriptor.value = function (...args: any[]) {
      const start = performance.now();
      // eslint-disable-next-line unicorn/no-this-outside-of-class
      const result = method.apply(this, args);

      void Promise.resolve(result)
        .then(() => histogram.record(performance.now() - start, {}))
        .catch(() => {
          // noop
        });

      return result;
    };

    copyMetadataFromFunctionToFunction(method, descriptor.value);
    Object.defineProperty(Repository.prototype, propName, descriptor);
  }
};

export class TelemetryRepo extends Context.Service<
  TelemetryRepo,
  {
    setup(options: { repositories: (new (...args: any[]) => unknown)[] }): Effect.Effect<void, ConfigError>;
  }
>()('immich/repositories/TelemetryRepo') {
  static readonly make = (
    metricService: MetricService,
    reflect: Reflector,
    configRepository: ConfigRepository,
    logger: LoggingRepository,
  ) =>
    TelemetryRepo.of({
      setup: Effect.fn('Telemetry.setup')(function* ({ repositories }) {
        const { telemetry } = yield* Effect.try({
          try: () => configRepository.getEnv(),
          catch: (cause) => new ConfigError({ cause }),
        });
        if (!telemetry.metrics.has(ImmichTelemetry.Repo)) {
          return;
        }

        yield* Effect.sync(() => {
          for (const Repository of repositories) {
            const isEnabled = reflect.get(MetadataKey.TelemetryEnabled, Repository) ?? true;
            if (!isEnabled) {
              logger.debug(`Telemetry disabled for ${Repository.name}`);
              continue;
            }

            wrapRepository(metricService, Repository);
          }
        });
      }),
    });
}

/** Adapter for the Nest services. Delegates to `TelemetryRepo`. */
@Injectable()
export class TelemetryRepository {
  api: MetricGroupRepository;
  host: MetricGroupRepository;
  jobs: MetricGroupRepository;
  repo: MetricGroupRepository;

  private readonly effect: Context.Service.Shape<typeof TelemetryRepo>;

  constructor(
    private metricService: MetricService,
    private reflect: Reflector,
    private configRepository: ConfigRepository,
    private logger: LoggingRepository,
  ) {
    const { telemetry } = this.configRepository.getEnv();
    const { metrics } = telemetry;

    this.api = new MetricGroupRepository(metricService).configure({ enabled: metrics.has(ImmichTelemetry.Api) });
    this.host = new MetricGroupRepository(metricService).configure({ enabled: metrics.has(ImmichTelemetry.Host) });
    this.jobs = new MetricGroupRepository(metricService).configure({ enabled: metrics.has(ImmichTelemetry.Job) });
    this.repo = new MetricGroupRepository(metricService).configure({ enabled: metrics.has(ImmichTelemetry.Repo) });
    this.effect = TelemetryRepo.make(metricService, reflect, configRepository, logger);
  }

  setup({ repositories }: { repositories: (new (...args: any[]) => unknown)[] }) {
    runSync(this.effect.setup({ repositories }));
  }
}
