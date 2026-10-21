import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { Insertable } from 'kysely';
import sanitize from 'sanitize-filename';
import type { LoginDetails } from 'src/services/auth.service.js';
import type { ClassConstructor } from 'src/types.js';
import { SALT_ROUNDS } from 'src/constants.js';
import { StorageCore } from 'src/cores/storage.core.js';
import { UserAdmin } from 'src/database.js';
import { mapLoginResponse } from 'src/dtos/auth.dto.js';
import { SystemConfig } from 'src/dtos/config.dto.js';
import { Fx, FxSource, createFx } from 'src/effect/fx.js';
import { runPromise } from 'src/effect/run.js';
import { AccessRepository } from 'src/repositories/access.repository.js';
import { ActivityRepository } from 'src/repositories/activity.repository.js';
import { AlbumUserRepository } from 'src/repositories/album-user.repository.js';
import { AlbumRepository } from 'src/repositories/album.repository.js';
import { ApiKeyRepository } from 'src/repositories/api-key.repository.js';
import { AppRepository } from 'src/repositories/app.repository.js';
import { AssetEditRepository } from 'src/repositories/asset-edit.repository.js';
import { AssetFileRepository } from 'src/repositories/asset-file.repository.js';
import { AssetJobRepository } from 'src/repositories/asset-job.repository.js';
import { AssetRepository } from 'src/repositories/asset.repository.js';
import { AuthChallengeRepository } from 'src/repositories/auth-challenge.repository.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { CronRepository } from 'src/repositories/cron.repository.js';
import { CryptoRepository } from 'src/repositories/crypto.repository.js';
import { DatabaseRepository } from 'src/repositories/database.repository.js';
import { DownloadRepository } from 'src/repositories/download.repository.js';
import { EmailRepository } from 'src/repositories/email.repository.js';
import { EventRepository } from 'src/repositories/event.repository.js';
import { IntegrityRepository } from 'src/repositories/integrity.repository.js';
import { JobRepository } from 'src/repositories/job.repository.js';
import { LibraryRepository } from 'src/repositories/library.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { MapRepository } from 'src/repositories/map.repository.js';
import { MediaRepository } from 'src/repositories/media.repository.js';
import { MemoryRepository } from 'src/repositories/memory.repository.js';
import { MetadataRepository } from 'src/repositories/metadata.repository.js';
import { MoveRepository } from 'src/repositories/move.repository.js';
import { NotificationRepository } from 'src/repositories/notification.repository.js';
import { OAuthRepository } from 'src/repositories/oauth.repository.js';
import { PartnerRepository } from 'src/repositories/partner.repository.js';
import { PasskeyRepository } from 'src/repositories/passkey.repository.js';
import { PluginRepository } from 'src/repositories/plugin.repository.js';
import { ProcessRepository } from 'src/repositories/process.repository.js';
import { SearchRepository } from 'src/repositories/search.repository.js';
import { ServerInfoRepository } from 'src/repositories/server-info.repository.js';
import { SessionRepository } from 'src/repositories/session.repository.js';
import { SharedLinkAssetRepository } from 'src/repositories/shared-link-asset.repository.js';
import { SharedLinkRepository } from 'src/repositories/shared-link.repository.js';
import { StackRepository } from 'src/repositories/stack.repository.js';
import { StorageRepository } from 'src/repositories/storage.repository.js';
import { SyncCheckpointRepository } from 'src/repositories/sync-checkpoint.repository.js';
import { SyncRepository } from 'src/repositories/sync.repository.js';
import { SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { TagRepository } from 'src/repositories/tag.repository.js';
import { TelemetryRepository } from 'src/repositories/telemetry.repository.js';
import { TrashRepository } from 'src/repositories/trash.repository.js';
import { UserRepository } from 'src/repositories/user.repository.js';
import { VersionHistoryRepository } from 'src/repositories/version-history.repository.js';
import { VideoStreamRepository } from 'src/repositories/video-stream.repository.js';
import { ViewRepository } from 'src/repositories/view-repository.js';
import { WebAuthnRepository } from 'src/repositories/webauthn.repository.js';
import { WebsocketRepository } from 'src/repositories/websocket.repository.js';
import { WorkflowRepository } from 'src/repositories/workflow.repository.js';
import { UserTable } from 'src/schema/tables/user.table.js';
import { AccessRequest, checkAccessFx, requireAccessFx } from 'src/utils/access.js';
import { getConfigFx, updateConfigFx } from 'src/utils/config.js';

const fxCache = new WeakMap<BaseService, Fx>();

export const BASE_SERVICE_DEPENDENCIES = [
  LoggingRepository,
  AccessRepository,
  ActivityRepository,
  AlbumRepository,
  AlbumUserRepository,
  ApiKeyRepository,
  AppRepository,
  AssetRepository,
  AssetEditRepository,
  AssetFileRepository,
  AssetJobRepository,
  AuthChallengeRepository,
  ConfigRepository,
  CronRepository,
  CryptoRepository,
  DatabaseRepository,
  DownloadRepository,
  EmailRepository,
  EventRepository,
  IntegrityRepository,
  JobRepository,
  LibraryRepository,
  MapRepository,
  MediaRepository,
  MemoryRepository,
  MetadataRepository,
  MoveRepository,
  NotificationRepository,
  OAuthRepository,
  PartnerRepository,
  PasskeyRepository,
  PluginRepository,
  ProcessRepository,
  SearchRepository,
  ServerInfoRepository,
  SessionRepository,
  SharedLinkRepository,
  SharedLinkAssetRepository,
  StackRepository,
  StorageRepository,
  SyncRepository,
  SyncCheckpointRepository,
  SystemMetadataRepository,
  TagRepository,
  TelemetryRepository,
  TrashRepository,
  UserRepository,
  VersionHistoryRepository,
  VideoStreamRepository,
  ViewRepository,
  WebAuthnRepository,
  WebsocketRepository,
  WorkflowRepository,
] as const;

@Injectable()
export class BaseService {
  protected storageCore: StorageCore;

  constructor(
    protected logger: LoggingRepository,
    protected accessRepository: AccessRepository,
    protected activityRepository: ActivityRepository,
    protected albumRepository: AlbumRepository,
    protected albumUserRepository: AlbumUserRepository,
    protected apiKeyRepository: ApiKeyRepository,
    protected appRepository: AppRepository,
    protected assetRepository: AssetRepository,
    protected assetEditRepository: AssetEditRepository,
    protected assetFileRepository: AssetFileRepository,
    protected assetJobRepository: AssetJobRepository,
    protected authChallengeRepository: AuthChallengeRepository,
    protected configRepository: ConfigRepository,
    protected cronRepository: CronRepository,
    protected cryptoRepository: CryptoRepository,
    protected databaseRepository: DatabaseRepository,
    protected downloadRepository: DownloadRepository,
    protected emailRepository: EmailRepository,
    protected eventRepository: EventRepository,
    protected integrityRepository: IntegrityRepository,
    protected jobRepository: JobRepository,
    protected libraryRepository: LibraryRepository,
    protected mapRepository: MapRepository,
    protected mediaRepository: MediaRepository,
    protected memoryRepository: MemoryRepository,
    protected metadataRepository: MetadataRepository,
    protected moveRepository: MoveRepository,
    protected notificationRepository: NotificationRepository,
    protected oauthRepository: OAuthRepository,
    protected partnerRepository: PartnerRepository,
    protected passkeyRepository: PasskeyRepository,
    protected pluginRepository: PluginRepository,
    protected processRepository: ProcessRepository,
    protected searchRepository: SearchRepository,
    protected serverInfoRepository: ServerInfoRepository,
    protected sessionRepository: SessionRepository,
    protected sharedLinkRepository: SharedLinkRepository,
    protected sharedLinkAssetRepository: SharedLinkAssetRepository,
    protected stackRepository: StackRepository,
    protected storageRepository: StorageRepository,
    protected syncRepository: SyncRepository,
    protected syncCheckpointRepository: SyncCheckpointRepository,
    protected systemMetadataRepository: SystemMetadataRepository,
    protected tagRepository: TagRepository,
    protected telemetryRepository: TelemetryRepository,
    protected trashRepository: TrashRepository,
    protected userRepository: UserRepository,
    protected versionRepository: VersionHistoryRepository,
    protected videoStreamRepository: VideoStreamRepository,
    protected viewRepository: ViewRepository,
    protected webAuthnRepository: WebAuthnRepository,
    protected websocketRepository: WebsocketRepository,
    protected workflowRepository: WorkflowRepository,
  ) {
    this.logger.setContext(this.constructor.name);
    this.storageCore = StorageCore.create(
      assetRepository,
      configRepository,
      cryptoRepository,
      moveRepository,
      storageRepository,
      systemMetadataRepository,
      this.logger,
    );
  }

  static create<T extends ClassConstructor<typeof BaseService>>(Service: T, ctx: BaseService) {
    const service = new Service(
      LoggingRepository.create(),
      ctx.accessRepository,
      ctx.activityRepository,
      ctx.albumRepository,
      ctx.albumUserRepository,
      ctx.apiKeyRepository,
      ctx.appRepository,
      ctx.assetRepository,
      ctx.assetEditRepository,
      ctx.assetFileRepository,
      ctx.assetJobRepository,
      ctx.authChallengeRepository,
      ctx.configRepository,
      ctx.cronRepository,
      ctx.cryptoRepository,
      ctx.databaseRepository,
      ctx.downloadRepository,
      ctx.emailRepository,
      ctx.eventRepository,
      ctx.integrityRepository,
      ctx.jobRepository,
      ctx.libraryRepository,
      ctx.mapRepository,
      ctx.mediaRepository,
      ctx.memoryRepository,
      ctx.metadataRepository,
      ctx.moveRepository,
      ctx.notificationRepository,
      ctx.oauthRepository,
      ctx.partnerRepository,
      ctx.passkeyRepository,
      ctx.pluginRepository,
      ctx.processRepository,
      ctx.searchRepository,
      ctx.serverInfoRepository,
      ctx.sessionRepository,
      ctx.sharedLinkRepository,
      ctx.sharedLinkAssetRepository,
      ctx.stackRepository,
      ctx.storageRepository,
      ctx.syncRepository,
      ctx.syncCheckpointRepository,
      ctx.systemMetadataRepository,
      ctx.tagRepository,
      ctx.telemetryRepository,
      ctx.trashRepository,
      ctx.userRepository,
      ctx.versionRepository,
      ctx.videoStreamRepository,
      ctx.viewRepository,
      ctx.webAuthnRepository,
      ctx.websocketRepository,
      ctx.workflowRepository,
    );

    service.logger.setContext(BaseService.name);

    return service as InstanceType<T>;
  }

  /** The repositories as Effect services (the real ones in production, lifted Promise mocks in specs). */
  protected get fx(): Fx {
    let fx = fxCache.get(this);
    if (!fx) {
      fx = createFx(this as unknown as FxSource);
      fxCache.set(this, fx);
    }
    return fx;
  }

  get worker() {
    return this.configRepository.getWorker();
  }

  private get configFxRepos() {
    return {
      configRepo: this.fx.config,
      metadataRepo: this.fx.systemMetadata,
      logger: this.fx.logger,
    };
  }

  protected getConfigFx(options: { withCache: boolean }) {
    return getConfigFx(this.configFxRepos, options);
  }

  protected updateConfigFx(newConfig: SystemConfig) {
    return updateConfigFx(this.configFxRepos, newConfig);
  }

  getConfig(options: { withCache: boolean }): Promise<SystemConfig> {
    return runPromise(this.getConfigFx(options));
  }

  updateConfig(newConfig: SystemConfig): Promise<SystemConfig> {
    return runPromise(this.updateConfigFx(newConfig));
  }

  protected requireAccessFx(request: AccessRequest) {
    return requireAccessFx(this.fx.access, request);
  }

  protected checkAccessFx(request: AccessRequest) {
    return checkAccessFx(this.fx.access, request);
  }

  requireAccess(request: AccessRequest): Promise<void> {
    return runPromise(this.requireAccessFx(request));
  }

  checkAccess(request: AccessRequest): Promise<Set<string>> {
    return runPromise(this.checkAccessFx(request));
  }

  protected isSetupAvailableFx() {
    const { fx } = this;
    return Effect.gen(function* () {
      const { setup } = yield* fx.config.getEnv();
      return setup.allow && !(yield* fx.user.hasAdmin());
    });
  }

  protected requireSetupAvailableFx() {
    const isSetupAvailable = this.isSetupAvailableFx();
    return Effect.gen(function* () {
      if (!(yield* isSetupAvailable)) {
        return yield* Effect.fail(new BadRequestException('Admin setup is not available'));
      }
    });
  }

  async isSetupAvailable(): Promise<boolean> {
    return runPromise(this.isSetupAvailableFx());
  }

  async requireSetupAvailable(): Promise<void> {
    return runPromise(this.requireSetupAvailableFx());
  }

  protected createUserFx(dto: Insertable<UserTable> & { email: string }) {
    const { fx, logger } = this;
    return Effect.gen(function* () {
      const exists = yield* fx.user.getByEmail(dto.email);
      if (exists) {
        logger.debug('User creation rejected: user already exists');
        return yield* Effect.fail(new BadRequestException('Email is not available'));
      }

      if (!dto.isAdmin) {
        const localAdmin = yield* fx.user.getAdmin();
        if (!localAdmin) {
          return yield* Effect.fail(new BadRequestException('The first registered account must the administrator.'));
        }
      }

      const payload: Insertable<UserTable> = { ...dto };
      if (payload.password) {
        payload.password = yield* fx.crypto.hashBcrypt(payload.password, SALT_ROUNDS);
      }
      if (payload.storageLabel) {
        payload.storageLabel = sanitize(payload.storageLabel);
      }

      const user: UserAdmin = yield* fx.user.create(payload);

      yield* fx.event.emit('UserCreate', user);

      return user;
    });
  }

  async createUser(dto: Insertable<UserTable> & { email: string }): Promise<UserAdmin> {
    return runPromise(this.createUserFx(dto));
  }

  protected createLoginResponseFx(
    user: UserAdmin,
    loginDetails: LoginDetails,
    oauthSid?: string,
    oauthBearerToken?: string,
  ) {
    const { fx } = this;
    return Effect.gen(function* () {
      const token = yield* fx.crypto.randomBytesAsText(32);
      const hashed = yield* fx.crypto.hashSha256(token);

      yield* fx.session.create({
        token: hashed,
        deviceOS: loginDetails.deviceOS,
        deviceType: loginDetails.deviceType,
        appVersion: loginDetails.appVersion,
        userId: user.id,
        oauthSid: oauthSid ?? null,
        oauthBearerToken: oauthBearerToken ?? null,
      });

      return mapLoginResponse(user, token);
    });
  }

  protected async createLoginResponse(
    user: UserAdmin,
    loginDetails: LoginDetails,
    oauthSid?: string,
    oauthBearerToken?: string,
  ) {
    return runPromise(this.createLoginResponseFx(user, loginDetails, oauthSid, oauthBearerToken));
  }
}
