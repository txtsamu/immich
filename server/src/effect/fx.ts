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

export const createFx = (source: FxSource) => ({
  logger: effectOf(LoggingRepo, source.logger),
  access: effectOf(AccessRepo, source.accessRepository),
  activity: effectOf(ActivityRepo, source.activityRepository),
  album: effectOf(AlbumRepo, source.albumRepository),
  albumUser: effectOf(AlbumUserRepo, source.albumUserRepository),
  apiKey: effectOf(ApiKeyRepo, source.apiKeyRepository),
  app: effectOf(AppRepo, source.appRepository),
  asset: effectOf(AssetRepo, source.assetRepository),
  assetEdit: effectOf(AssetEditRepo, source.assetEditRepository),
  assetFile: effectOf(AssetFileRepo, source.assetFileRepository),
  assetJob: effectOf(AssetJobRepo, source.assetJobRepository),
  authChallenge: effectOf(AuthChallengeRepo, source.authChallengeRepository),
  config: effectOf(ConfigRepo, source.configRepository),
  cron: effectOf(CronRepo, source.cronRepository),
  crypto: effectOf(CryptoRepo, source.cryptoRepository),
  database: effectOf(DatabaseRepo, source.databaseRepository),
  download: effectOf(DownloadRepo, source.downloadRepository),
  email: effectOf(EmailRepo, source.emailRepository),
  event: effectOf(EventRepo, source.eventRepository),
  integrity: effectOf(IntegrityRepo, source.integrityRepository),
  job: effectOf(JobRepo, source.jobRepository),
  library: effectOf(LibraryRepo, source.libraryRepository),
  map: effectOf(MapRepo, source.mapRepository),
  media: effectOf(MediaRepo, source.mediaRepository),
  memory: effectOf(MemoryRepo, source.memoryRepository),
  metadata: effectOf(MetadataRepo, source.metadataRepository),
  move: effectOf(MoveRepo, source.moveRepository),
  notification: effectOf(NotificationRepo, source.notificationRepository),
  oauth: effectOf(OAuthRepo, source.oauthRepository),
  partner: effectOf(PartnerRepo, source.partnerRepository),
  passkey: effectOf(PasskeyRepo, source.passkeyRepository),
  plugin: effectOf(PluginRepo, source.pluginRepository),
  process: effectOf(ProcessRepo, source.processRepository),
  search: effectOf(SearchRepo, source.searchRepository),
  serverInfo: effectOf(ServerInfoRepo, source.serverInfoRepository),
  session: effectOf(SessionRepo, source.sessionRepository),
  sharedLink: effectOf(SharedLinkRepo, source.sharedLinkRepository),
  sharedLinkAsset: effectOf(SharedLinkAssetRepo, source.sharedLinkAssetRepository),
  stack: effectOf(StackRepo, source.stackRepository),
  storage: effectOf(StorageRepo, source.storageRepository),
  sync: effectOf(SyncRepo, source.syncRepository),
  syncCheckpoint: effectOf(SyncCheckpointRepo, source.syncCheckpointRepository),
  systemMetadata: effectOf(SystemMetadataRepo, source.systemMetadataRepository),
  tag: effectOf(TagRepo, source.tagRepository),
  telemetry: effectOf(TelemetryRepo, source.telemetryRepository),
  trash: effectOf(TrashRepo, source.trashRepository),
  user: effectOf(UserRepo, source.userRepository),
  version: effectOf(VersionHistoryRepo, source.versionRepository),
  videoStream: effectOf(VideoStreamRepo, source.videoStreamRepository),
  view: effectOf(ViewRepo, source.viewRepository),
  webAuthn: effectOf(WebAuthnRepo, source.webAuthnRepository),
  websocket: effectOf(WebsocketRepo, source.websocketRepository),
  workflow: effectOf(WorkflowRepo, source.workflowRepository),
});

export type Fx = ReturnType<typeof createFx>;
