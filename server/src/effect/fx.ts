// Generated from the BaseService constructor: one Effect service per repository, in the same order.
import { effectOf } from 'src/effect/bridge.js';
import { AccessRepo, AccessRepository } from 'src/repositories/access.repository.js';
import { ActivityRepo, ActivityRepository } from 'src/repositories/activity.repository.js';
import { AlbumUserRepo, AlbumUserRepository } from 'src/repositories/album-user.repository.js';
import { AlbumRepo, AlbumRepository } from 'src/repositories/album.repository.js';
import { ApiKeyRepo, ApiKeyRepository } from 'src/repositories/api-key.repository.js';
import { AppRepo, AppRepository } from 'src/repositories/app.repository.js';
import { AssetEditRepo, AssetEditRepository } from 'src/repositories/asset-edit.repository.js';
import { AssetFileRepo, AssetFileRepository } from 'src/repositories/asset-file.repository.js';
import { AssetJobRepo, AssetJobRepository } from 'src/repositories/asset-job.repository.js';
import { AssetRepo, AssetRepository } from 'src/repositories/asset.repository.js';
import { AuthChallengeRepo, AuthChallengeRepository } from 'src/repositories/auth-challenge.repository.js';
import { ConfigRepo, ConfigRepository } from 'src/repositories/config.repository.js';
import { CronRepo, CronRepository } from 'src/repositories/cron.repository.js';
import { CryptoRepo, CryptoRepository } from 'src/repositories/crypto.repository.js';
import { DatabaseRepo, DatabaseRepository } from 'src/repositories/database.repository.js';
import { DownloadRepo, DownloadRepository } from 'src/repositories/download.repository.js';
import { EmailRepo, EmailRepository } from 'src/repositories/email.repository.js';
import { EventRepo, EventRepository } from 'src/repositories/event.repository.js';
import { IntegrityRepo, IntegrityRepository } from 'src/repositories/integrity.repository.js';
import { JobRepo, JobRepository } from 'src/repositories/job.repository.js';
import { LibraryRepo, LibraryRepository } from 'src/repositories/library.repository.js';
import { LoggingRepo, LoggingRepository } from 'src/repositories/logging.repository.js';
import { MapRepo, MapRepository } from 'src/repositories/map.repository.js';
import { MediaRepo, MediaRepository } from 'src/repositories/media.repository.js';
import { MemoryRepo, MemoryRepository } from 'src/repositories/memory.repository.js';
import { MetadataRepo, MetadataRepository } from 'src/repositories/metadata.repository.js';
import { MoveRepo, MoveRepository } from 'src/repositories/move.repository.js';
import { NotificationRepo, NotificationRepository } from 'src/repositories/notification.repository.js';
import { OAuthRepo, OAuthRepository } from 'src/repositories/oauth.repository.js';
import { PartnerRepo, PartnerRepository } from 'src/repositories/partner.repository.js';
import { PasskeyRepo, PasskeyRepository } from 'src/repositories/passkey.repository.js';
import { PluginRepo, PluginRepository } from 'src/repositories/plugin.repository.js';
import { ProcessRepo, ProcessRepository } from 'src/repositories/process.repository.js';
import { SearchRepo, SearchRepository } from 'src/repositories/search.repository.js';
import { ServerInfoRepo, ServerInfoRepository } from 'src/repositories/server-info.repository.js';
import { SessionRepo, SessionRepository } from 'src/repositories/session.repository.js';
import { SharedLinkAssetRepo, SharedLinkAssetRepository } from 'src/repositories/shared-link-asset.repository.js';
import { SharedLinkRepo, SharedLinkRepository } from 'src/repositories/shared-link.repository.js';
import { StackRepo, StackRepository } from 'src/repositories/stack.repository.js';
import { StorageRepo, StorageRepository } from 'src/repositories/storage.repository.js';
import { SyncCheckpointRepo, SyncCheckpointRepository } from 'src/repositories/sync-checkpoint.repository.js';
import { SyncRepo, SyncRepository } from 'src/repositories/sync.repository.js';
import { SystemMetadataRepo, SystemMetadataRepository } from 'src/repositories/system-metadata.repository.js';
import { TagRepo, TagRepository } from 'src/repositories/tag.repository.js';
import { TelemetryRepo, TelemetryRepository } from 'src/repositories/telemetry.repository.js';
import { TrashRepo, TrashRepository } from 'src/repositories/trash.repository.js';
import { UserRepo, UserRepository } from 'src/repositories/user.repository.js';
import { VersionHistoryRepo, VersionHistoryRepository } from 'src/repositories/version-history.repository.js';
import { VideoStreamRepo, VideoStreamRepository } from 'src/repositories/video-stream.repository.js';
import { ViewRepo, ViewRepository } from 'src/repositories/view-repository.js';
import { WebAuthnRepo, WebAuthnRepository } from 'src/repositories/webauthn.repository.js';
import { WebsocketRepo, WebsocketRepository } from 'src/repositories/websocket.repository.js';
import { WorkflowRepo, WorkflowRepository } from 'src/repositories/workflow.repository.js';

/** The repository adapters a service was constructed with. */
export type FxSource = {
  logger: LoggingRepository;
  accessRepository: AccessRepository;
  activityRepository: ActivityRepository;
  albumRepository: AlbumRepository;
  albumUserRepository: AlbumUserRepository;
  apiKeyRepository: ApiKeyRepository;
  appRepository: AppRepository;
  assetRepository: AssetRepository;
  assetEditRepository: AssetEditRepository;
  assetFileRepository: AssetFileRepository;
  assetJobRepository: AssetJobRepository;
  authChallengeRepository: AuthChallengeRepository;
  configRepository: ConfigRepository;
  cronRepository: CronRepository;
  cryptoRepository: CryptoRepository;
  databaseRepository: DatabaseRepository;
  downloadRepository: DownloadRepository;
  emailRepository: EmailRepository;
  eventRepository: EventRepository;
  integrityRepository: IntegrityRepository;
  jobRepository: JobRepository;
  libraryRepository: LibraryRepository;
  mapRepository: MapRepository;
  mediaRepository: MediaRepository;
  memoryRepository: MemoryRepository;
  metadataRepository: MetadataRepository;
  moveRepository: MoveRepository;
  notificationRepository: NotificationRepository;
  oauthRepository: OAuthRepository;
  partnerRepository: PartnerRepository;
  passkeyRepository: PasskeyRepository;
  pluginRepository: PluginRepository;
  processRepository: ProcessRepository;
  searchRepository: SearchRepository;
  serverInfoRepository: ServerInfoRepository;
  sessionRepository: SessionRepository;
  sharedLinkRepository: SharedLinkRepository;
  sharedLinkAssetRepository: SharedLinkAssetRepository;
  stackRepository: StackRepository;
  storageRepository: StorageRepository;
  syncRepository: SyncRepository;
  syncCheckpointRepository: SyncCheckpointRepository;
  systemMetadataRepository: SystemMetadataRepository;
  tagRepository: TagRepository;
  telemetryRepository: TelemetryRepository;
  trashRepository: TrashRepository;
  userRepository: UserRepository;
  versionRepository: VersionHistoryRepository;
  videoStreamRepository: VideoStreamRepository;
  viewRepository: ViewRepository;
  webAuthnRepository: WebAuthnRepository;
  websocketRepository: WebsocketRepository;
  workflowRepository: WorkflowRepository;
};

// Getters, so a repository a service was never constructed with (e.g. in medium tests) only fails when used.
export const createFx = (source: FxSource) => ({
  get logger() {
    return effectOf(LoggingRepo, source.logger);
  },
  get access() {
    return effectOf(AccessRepo, source.accessRepository);
  },
  get activity() {
    return effectOf(ActivityRepo, source.activityRepository);
  },
  get album() {
    return effectOf(AlbumRepo, source.albumRepository);
  },
  get albumUser() {
    return effectOf(AlbumUserRepo, source.albumUserRepository);
  },
  get apiKey() {
    return effectOf(ApiKeyRepo, source.apiKeyRepository);
  },
  get app() {
    return effectOf(AppRepo, source.appRepository);
  },
  get asset() {
    return effectOf(AssetRepo, source.assetRepository);
  },
  get assetEdit() {
    return effectOf(AssetEditRepo, source.assetEditRepository);
  },
  get assetFile() {
    return effectOf(AssetFileRepo, source.assetFileRepository);
  },
  get assetJob() {
    return effectOf(AssetJobRepo, source.assetJobRepository);
  },
  get authChallenge() {
    return effectOf(AuthChallengeRepo, source.authChallengeRepository);
  },
  get config() {
    return effectOf(ConfigRepo, source.configRepository);
  },
  get cron() {
    return effectOf(CronRepo, source.cronRepository);
  },
  get crypto() {
    return effectOf(CryptoRepo, source.cryptoRepository);
  },
  get database() {
    return effectOf(DatabaseRepo, source.databaseRepository);
  },
  get download() {
    return effectOf(DownloadRepo, source.downloadRepository);
  },
  get email() {
    return effectOf(EmailRepo, source.emailRepository);
  },
  get event() {
    return effectOf(EventRepo, source.eventRepository);
  },
  get integrity() {
    return effectOf(IntegrityRepo, source.integrityRepository);
  },
  get job() {
    return effectOf(JobRepo, source.jobRepository);
  },
  get library() {
    return effectOf(LibraryRepo, source.libraryRepository);
  },
  get map() {
    return effectOf(MapRepo, source.mapRepository);
  },
  get media() {
    return effectOf(MediaRepo, source.mediaRepository);
  },
  get memory() {
    return effectOf(MemoryRepo, source.memoryRepository);
  },
  get metadata() {
    return effectOf(MetadataRepo, source.metadataRepository);
  },
  get move() {
    return effectOf(MoveRepo, source.moveRepository);
  },
  get notification() {
    return effectOf(NotificationRepo, source.notificationRepository);
  },
  get oauth() {
    return effectOf(OAuthRepo, source.oauthRepository);
  },
  get partner() {
    return effectOf(PartnerRepo, source.partnerRepository);
  },
  get passkey() {
    return effectOf(PasskeyRepo, source.passkeyRepository);
  },
  get plugin() {
    return effectOf(PluginRepo, source.pluginRepository);
  },
  get process() {
    return effectOf(ProcessRepo, source.processRepository);
  },
  get search() {
    return effectOf(SearchRepo, source.searchRepository);
  },
  get serverInfo() {
    return effectOf(ServerInfoRepo, source.serverInfoRepository);
  },
  get session() {
    return effectOf(SessionRepo, source.sessionRepository);
  },
  get sharedLink() {
    return effectOf(SharedLinkRepo, source.sharedLinkRepository);
  },
  get sharedLinkAsset() {
    return effectOf(SharedLinkAssetRepo, source.sharedLinkAssetRepository);
  },
  get stack() {
    return effectOf(StackRepo, source.stackRepository);
  },
  get storage() {
    return effectOf(StorageRepo, source.storageRepository);
  },
  get sync() {
    return effectOf(SyncRepo, source.syncRepository);
  },
  get syncCheckpoint() {
    return effectOf(SyncCheckpointRepo, source.syncCheckpointRepository);
  },
  get systemMetadata() {
    return effectOf(SystemMetadataRepo, source.systemMetadataRepository);
  },
  get tag() {
    return effectOf(TagRepo, source.tagRepository);
  },
  get telemetry() {
    return effectOf(TelemetryRepo, source.telemetryRepository);
  },
  get trash() {
    return effectOf(TrashRepo, source.trashRepository);
  },
  get user() {
    return effectOf(UserRepo, source.userRepository);
  },
  get version() {
    return effectOf(VersionHistoryRepo, source.versionRepository);
  },
  get videoStream() {
    return effectOf(VideoStreamRepo, source.videoStreamRepository);
  },
  get view() {
    return effectOf(ViewRepo, source.viewRepository);
  },
  get webAuthn() {
    return effectOf(WebAuthnRepo, source.webAuthnRepository);
  },
  get websocket() {
    return effectOf(WebsocketRepo, source.websocketRepository);
  },
  get workflow() {
    return effectOf(WorkflowRepo, source.workflowRepository);
  },
});

export type Fx = ReturnType<typeof createFx>;
