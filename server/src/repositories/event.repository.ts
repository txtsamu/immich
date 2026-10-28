import { Injectable } from '@nestjs/common';
import { ModuleRef, Reflector } from '@nestjs/core';
import { Context, Effect } from 'effect';
import { orderBy } from 'lodash-es';
import { Socket } from 'socket.io';
import type { JobItem, JobSource, UploadFile } from 'src/types.js';
import { Asset } from 'src/database.js';
import { EventConfig } from 'src/decorators.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { SystemConfig } from 'src/dtos/config.dto.js';
import { registerEffect } from 'src/effect/bridge.js';
import { HandlerError, LibraryError } from 'src/effect/errors.js';
import { runPromise, runSync } from 'src/effect/run.js';
import { ImmichWorker, JobStatus, MetadataKey, QueueName, UserAvatarColor, UserStatus } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';

type EmitHandlers = Partial<{ [T in EmitEvent]: Array<EventItem<T>> }>;

type Item<T extends EmitEvent> = {
  event: T;
  handler: EmitHandler<T>;
  priority: number;
  server: boolean;
  label: string;
};

type EventMap = {
  // app events
  AppBootstrap: [];
  AppShutdown: [];
  AppRestart: [AppRestartEvent];

  ConfigInit: [{ newConfig: SystemConfig }];
  // config events
  ConfigUpdate: [
    {
      newConfig: SystemConfig;
      oldConfig: SystemConfig;
    },
  ];
  ConfigValidate: [{ newConfig: SystemConfig; oldConfig: SystemConfig }];

  // album events
  AlbumUpdate: [{ id: string; userIds: string[]; recipientIds: string[] }];
  AlbumInvite: [{ id: string; userId: string; senderName: string }];

  // asset events
  AssetCreate: [{ asset: Pick<Asset, 'id' | 'ownerId'>; file?: UploadFile }];
  AssetTag: [{ assetId: string; userId: string }];
  AssetUntag: [{ assetId: string }];
  AssetHide: [{ assetId: string; userId: string }];
  AssetShow: [{ assetId: string; userId: string }];
  AssetTrash: [{ assetId: string; userId: string }];
  AssetDelete: [{ assetId: string; userId: string }];
  AssetMetadataExtracted: [{ assetId: string; userId: string; source?: JobSource }];

  // asset bulk events
  AssetTrashAll: [{ assetIds: string[]; userId: string }];
  AssetDeleteAll: [{ assetIds: string[]; userId: string }];
  AssetRestoreAll: [{ assetIds: string[]; userId: string }];

  /** a worker receives a job and emits this event to run it */
  JobRun: [QueueName, JobItem];
  /** job pre-hook */
  JobStart: [QueueName, JobItem];
  /** job post-hook */
  JobComplete: [QueueName, JobItem];
  /** job finishes without error */
  JobSuccess: [JobSuccessEvent];
  /** job finishes with error */
  JobError: [JobErrorEvent];

  // queue events
  QueueStart: [QueueStartEvent];

  // session events
  SessionDelete: [{ sessionId: string }];

  // stack events
  StackCreate: [{ stackId: string; userId: string }];
  StackUpdate: [{ stackId: string; userId: string }];
  StackDelete: [{ stackId: string; userId: string }];

  // stack bulk events
  StackDeleteAll: [{ stackIds: string[]; userId: string }];

  // user events
  UserSignup: [{ notify: boolean; id: string; password?: string }];
  UserCreate: [UserEvent];
  /** user is soft deleted */
  UserTrash: [UserEvent];
  /** user is permanently deleted */
  UserDelete: [UserEvent];
  UserRestore: [UserEvent];

  AuthChangePassword: [{ userId: string; currentSessionId?: string; invalidateSessions?: boolean }];

  // hls streaming events
  HlsSegmentRequest: [{ sessionId: string; assetId: string; variantIndex: number; segmentIndex: number }];
  HlsSegmentResult: [{ sessionId: string; variantIndex: number; segmentIndex: number; error?: string }];
  HlsHeartbeat: [{ sessionId: string; variantIndex?: number; segmentIndex?: number }];
  HlsSessionRequest: [{ sessionId: string; assetId: string; ownerId: string }];
  HlsSessionResult: [{ sessionId: string; error?: string }];
  HlsSessionEnd: [{ sessionId: string }];

  // websocket events
  WebsocketConnect: [{ userId: string }];
};

export type AppRestartEvent = {
  isMaintenanceMode: boolean;
};

type JobSuccessEvent = { job: JobItem; response?: JobStatus };
type JobErrorEvent = { job: JobItem; error: Error | any };

type QueueStartEvent = {
  name: QueueName;
};

type UserEvent = {
  name: string;
  id: string;
  createdAt: Date;
  updatedAt: Date;
  deletedAt: Date | null;
  status: UserStatus;
  email: string;
  profileImagePath: string;
  isAdmin: boolean;
  shouldChangePassword: boolean;
  avatarColor: UserAvatarColor | null;
  oauthId: string | null;
  storageLabel: string | null;
  quotaSizeInBytes: number | null;
  quotaUsageInBytes: number;
  profileChangedAt: Date;
};

export type EmitEvent = keyof EventMap;
export type EmitHandler<T extends EmitEvent> = (...args: ArgsOf<T>) => Promise<void> | void;
export type ArgOf<T extends EmitEvent> = EventMap[T][0];
export type ArgsOf<T extends EmitEvent> = EventMap[T];

export type EventItem<T extends EmitEvent> = {
  event: T;
  handler: EmitHandler<T>;
  server: boolean;
};

export type AuthFn = (client: Socket) => Promise<AuthDto>;

export class EventRepo extends Context.Service<
  EventRepo,
  {
    setup(options: { services: (new (...args: any[]) => unknown)[] }): Effect.Effect<void, LibraryError>;
    emit<T extends EmitEvent>(event: T, ...args: ArgsOf<T>): Effect.Effect<void, HandlerError>;
    onEvent<T extends EmitEvent>(event: {
      name: T;
      args: ArgsOf<T>;
      server: boolean;
    }): Effect.Effect<void, HandlerError>;
  }
>()('immich/repositories/EventRepo') {
  static readonly make = ({
    moduleRef,
    configRepository,
  }: {
    moduleRef: ModuleRef;
    configRepository: ConfigRepository;
  }) => {
    // handler registry; mutated by `setup`, read by `onEvent`
    const emitHandlers: EmitHandlers = {};

    const addHandler = <T extends EmitEvent>(item: Item<T>): void => {
      const event = item.event;

      if (!Object.hasOwn(emitHandlers, event)) {
        emitHandlers[event] = [];
      }

      emitHandlers[event]!.push(item);
    };

    const discover = (services: (new (...args: any[]) => unknown)[]) => {
      const reflector = moduleRef.get(Reflector, { strict: false });
      const items: Item<EmitEvent>[] = [];
      const worker = configRepository.getWorker();
      if (!worker) {
        throw new Error('Unable to determine worker type');
      }

      // discovery
      for (const Service of services) {
        const instance = moduleRef.get<any>(Service);
        const ctx = Object.getPrototypeOf(instance);
        for (const property of Object.getOwnPropertyNames(ctx)) {
          const descriptor = Object.getOwnPropertyDescriptor(ctx, property);
          if (!descriptor || descriptor.get || descriptor.set) {
            continue;
          }

          const handler = instance[property];
          if (typeof handler !== 'function') {
            continue;
          }

          const event = reflector.get<EventConfig>(MetadataKey.EventConfig, handler);
          if (!event) {
            continue;
          }

          const workers = event.workers ?? Object.values(ImmichWorker);
          if (!workers.includes(worker)) {
            continue;
          }

          items.push({
            event: event.name,
            priority: event.priority || 0,
            server: event.server ?? false,
            handler: handler.bind(instance),
            label: `${Service.name}.${handler.name}`,
          });
        }
      }

      return items;
    };

    const onEvent = Effect.fn('Event.onEvent')(function* <T extends EmitEvent>(event: {
      name: T;
      args: ArgsOf<T>;
      server: boolean;
    }) {
      const handlers = emitHandlers[event.name] || [];
      for (const { handler, server } of handlers) {
        // exclude handlers that ignore server events
        if (!server && event.server) {
          continue;
        }

        // handlers run sequentially, each awaited before the next; sync or async
        yield* Effect.tryPromise({
          try: async () => handler(...event.args),
          catch: (cause) => new HandlerError({ cause }),
        });
      }
    });

    return EventRepo.of({
      setup: Effect.fn('Event.setup')(function* ({ services }) {
        const items = yield* Effect.try({
          try: () => discover(services),
          catch: (cause) => new LibraryError({ cause }),
        });

        const handlers = orderBy(items, ['priority'], ['asc']);

        // register by priority
        for (const handler of handlers) {
          addHandler(handler);
        }
      }),

      emit: Effect.fn('Event.emit')(function* <T extends EmitEvent>(event: T, ...args: ArgsOf<T>) {
        yield* onEvent({ name: event, args, server: false });
      }),

      onEvent,
    });
  };
}

/** Promise-facing adapter for the Nest services. Delegates to `EventRepo`. */
@Injectable()
export class EventRepository {
  private readonly effect: Context.Service.Shape<typeof EventRepo>;

  constructor(
    moduleRef: ModuleRef,
    configRepository: ConfigRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(EventRepository.name);
    this.effect = EventRepo.make({ moduleRef, configRepository });
    registerEffect(this, this.effect);
  }

  setup({ services }: { services: (new (...args: any[]) => unknown)[] }) {
    return runSync(this.effect.setup({ services }));
  }

  emit<T extends EmitEvent>(event: T, ...args: ArgsOf<T>): Promise<void> {
    return runPromise(this.effect.emit(event, ...args));
  }

  onEvent<T extends EmitEvent>(event: { name: T; args: ArgsOf<T>; server: boolean }): Promise<void> {
    return runPromise(this.effect.onEvent(event));
  }
}
