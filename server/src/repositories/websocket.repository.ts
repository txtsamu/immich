import { Injectable } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Context, Effect } from 'effect';
import { Server, Socket } from 'socket.io';
import { AssetResponseDto } from 'src/dtos/asset-response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { NotificationDto } from 'src/dtos/notification.dto.js';
import { ReleaseEventV1, ServerVersionResponseDto } from 'src/dtos/server.dto.js';
import { SyncAssetEditV1, SyncAssetExifV1, SyncAssetV2 } from 'src/dtos/sync.dto.js';
import { LibraryError } from 'src/effect/errors.js';
import { runSync } from 'src/effect/run.js';
import { type AppRestartEvent, type ArgsOf, EventRepository } from 'src/repositories/event.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { handlePromiseError } from 'src/utils/misc.js';

export const serverEvents = [
  'ConfigUpdate',
  'AppRestart',
  'HlsSegmentRequest',
  'HlsSegmentResult',
  'HlsHeartbeat',
  'HlsSessionRequest',
  'HlsSessionResult',
  'HlsSessionEnd',
] as const;
export type ServerEvents = (typeof serverEvents)[number];

export interface ClientEventMap {
  on_upload_success: [AssetResponseDto];
  on_user_delete: [string];
  on_asset_delete: [string];
  on_asset_trash: [string[]];
  on_asset_update: [AssetResponseDto];
  on_asset_hidden: [string];
  on_asset_restore: [string[]];
  on_asset_stack_update: string[];
  on_album_update: [string];
  on_person_thumbnail: [string];
  on_server_version: [ServerVersionResponseDto];
  on_config_update: [];
  on_new_release: [ReleaseEventV1];
  on_notification: [NotificationDto];
  on_session_delete: [string];

  AssetUploadReadyV2: [{ asset: SyncAssetV2; exif: SyncAssetExifV1 }];
  AppRestartV1: [AppRestartEvent];
  AssetEditReadyV2: [{ asset: SyncAssetV2; edit: SyncAssetEditV1[] }];
}

export type AuthFn = (client: Socket) => Promise<AuthDto>;

export class WebsocketRepo extends Context.Service<
  WebsocketRepo,
  {
    clientSend<T extends keyof ClientEventMap>(
      event: T,
      room: string,
      ...data: ClientEventMap[T]
    ): Effect.Effect<void, LibraryError>;
    clientBroadcast<T extends keyof ClientEventMap>(
      event: T,
      ...data: ClientEventMap[T]
    ): Effect.Effect<void, LibraryError>;
    serverSend<T extends ServerEvents>(event: T, ...args: ArgsOf<T>): Effect.Effect<void, LibraryError>;
  }
>()('immich/repositories/WebsocketRepo') {
  /** `getServer` is lazy because Nest injects the socket.io server into the gateway after construction. */
  static readonly make = ({ getServer, logger }: { getServer: () => Server | undefined; logger: LoggingRepository }) =>
    WebsocketRepo.of({
      clientSend: Effect.fn('Websocket.clientSend')(function* <T extends keyof ClientEventMap>(
        event: T,
        room: string,
        ...data: ClientEventMap[T]
      ) {
        yield* Effect.try({
          try: () => {
            getServer()
              ?.to(room)
              .emit(event, ...data);
          },
          catch: (cause) => new LibraryError({ cause }),
        });
      }),

      clientBroadcast: Effect.fn('Websocket.clientBroadcast')(function* <T extends keyof ClientEventMap>(
        event: T,
        ...data: ClientEventMap[T]
      ) {
        yield* Effect.try({
          try: () => {
            getServer()?.emit(event, ...data);
          },
          catch: (cause) => new LibraryError({ cause }),
        });
      }),

      serverSend: Effect.fn('Websocket.serverSend')(function* <T extends ServerEvents>(event: T, ...args: ArgsOf<T>) {
        yield* Effect.try({
          try: () => {
            logger.debug(`Server event: ${event} (send)`);
            getServer()?.serverSideEmit(event, ...args);
          },
          catch: (cause) => new LibraryError({ cause }),
        });
      }),
    });
}

@WebSocketGateway({
  cors: true,
  path: '/api/socket.io',
  transports: ['websocket'],
})
@Injectable()
export class WebsocketRepository implements OnGatewayConnection, OnGatewayDisconnect, OnGatewayInit {
  private authFn?: AuthFn;
  private readonly effect: Context.Service.Shape<typeof WebsocketRepo>;

  @WebSocketServer()
  private server?: Server;

  constructor(
    private eventRepository: EventRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(WebsocketRepository.name);
    this.effect = WebsocketRepo.make({ getServer: () => this.server, logger: this.logger });
  }

  afterInit(server: Server) {
    this.logger.log('Initialized websocket server');

    for (const event of serverEvents) {
      server.on(event, (...args: ArgsOf<any>) => {
        this.logger.debug(`Server event: ${event} (receive)`);
        handlePromiseError(this.eventRepository.onEvent({ name: event, args, server: true }), this.logger);
      });
    }
  }

  async handleConnection(client: Socket) {
    try {
      this.logger.log(`Websocket Connect:    ${client.id}`);
      const auth = await this.authenticate(client);
      await client.join(auth.user.id);
      if (auth.session) {
        await client.join(auth.session.id);
      }
      await this.eventRepository.emit('WebsocketConnect', { userId: auth.user.id });
    } catch (error: Error | any) {
      this.logger.error(`Websocket connection error: ${error}`, error?.stack);
      client.emit('error', 'unauthorized');
      client.disconnect();
    }
  }

  async handleDisconnect(client: Socket) {
    this.logger.log(`Websocket Disconnect: ${client.id}`);
    await client.leave(client.nsp.name);
  }

  clientSend<T extends keyof ClientEventMap>(event: T, room: string, ...data: ClientEventMap[T]) {
    runSync(this.effect.clientSend(event, room, ...data));
  }

  clientBroadcast<T extends keyof ClientEventMap>(event: T, ...data: ClientEventMap[T]) {
    runSync(this.effect.clientBroadcast(event, ...data));
  }

  serverSend<T extends ServerEvents>(event: T, ...args: ArgsOf<T>): void {
    runSync(this.effect.serverSend(event, ...args));
  }

  setAuthFn(fn: (client: Socket) => Promise<AuthDto>) {
    this.authFn = fn;
  }

  private async authenticate(client: Socket) {
    if (!this.authFn) {
      throw new Error('Auth function not set');
    }

    return this.authFn(client);
  }
}
