import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import type { ArgOf } from 'src/repositories/event.repository.js';
import type { EmailImageAttachment, JobOf } from 'src/types.js';
import { OnEvent, OnJob } from 'src/decorators.js';
import { MapAlbumDto } from 'src/dtos/album.dto.js';
import { mapAsset } from 'src/dtos/asset-response.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { SystemConfigSmtpDto } from 'src/dtos/config.dto.js';
import {
  NotificationDeleteAllDto,
  NotificationDto,
  NotificationSearchDto,
  NotificationUpdateAllDto,
  NotificationUpdateDto,
  mapNotification,
} from 'src/dtos/notification.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import {
  AssetFileType,
  JobName,
  JobStatus,
  NotificationLevel,
  NotificationType,
  Permission,
  QueueName,
} from 'src/enum.js';
import { EmailTemplate } from 'src/repositories/email.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { getFilenameExtension } from 'src/utils/file.js';
import { getExternalDomain } from 'src/utils/misc.js';
import { isEqualObject } from 'src/utils/object.js';
import { getPreferences } from 'src/utils/preferences.js';

/** The original thrown value behind a wrapped repository error (`DatabaseError`, `LibraryError`, ...). */
@Injectable()
export class NotificationService extends BaseService {
  private static albumUpdateEmailDelayMs = 300_000;

  async search(auth: AuthDto, dto: NotificationSearchDto): Promise<NotificationDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const items = yield* this.fx.notification.search(auth.user.id, dto);
        return items.map((item) => mapNotification(item));
      }),
    );
  }

  async updateAll(auth: AuthDto, dto: NotificationUpdateAllDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, ids: dto.ids, permission: Permission.NotificationUpdate });
        yield* this.fx.notification.updateAll(dto.ids, {
          readAt: dto.readAt,
        });
      }),
    );
  }

  async deleteAll(auth: AuthDto, dto: NotificationDeleteAllDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, ids: dto.ids, permission: Permission.NotificationDelete });
        yield* this.fx.notification.deleteAll(dto.ids);
      }),
    );
  }

  async get(auth: AuthDto, id: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, ids: [id], permission: Permission.NotificationRead });
        const item = yield* this.fx.notification.get(id);
        if (!item) {
          return yield* Effect.fail(new BadRequestException('Notification not found'));
        }
        return mapNotification(item);
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: NotificationUpdateDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, ids: [id], permission: Permission.NotificationUpdate });
        const item = yield* this.fx.notification.update(id, {
          readAt: dto.readAt,
        });
        return mapNotification(item);
      }),
    );
  }

  async delete(auth: AuthDto, id: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, ids: [id], permission: Permission.NotificationDelete });
        yield* this.fx.notification.delete(id);
      }),
    );
  }

  @OnJob({ name: JobName.NotificationsCleanup, queue: QueueName.BackgroundTask })
  async onNotificationsCleanup() {
    return runPromise(this.fx.notification.cleanup());
  }

  @OnEvent({ name: 'JobError' })
  async onJobError({ job, error }: ArgOf<'JobError'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const admin = yield* this.fx.user.getAdmin();
        if (!admin) {
          return;
        }

        this.logger.error(`Unable to run job handler (${job.name}): ${error}`, error?.stack, JSON.stringify(job.data));

        switch (job.name) {
          case JobName.DatabaseBackup: {
            const errorMessage = error instanceof Error ? error.message : error;
            const item = yield* this.fx.notification.create({
              userId: admin.id,
              type: NotificationType.JobFailed,
              level: NotificationLevel.Error,
              title: 'Job Failed',
              description: `Job ${[job.name]} failed with error: ${errorMessage}`,
            });

            yield* this.fx.websocket.clientSend('on_notification', admin.id, mapNotification(item));
            break;
          }

          default: {
            return;
          }
        }
      }),
    );
  }

  @OnEvent({ name: 'ConfigUpdate' })
  onConfigUpdate({ oldConfig, newConfig }: ArgOf<'ConfigUpdate'>) {
    this.websocketRepository.clientBroadcast('on_config_update');
    this.websocketRepository.serverSend('ConfigUpdate', { oldConfig, newConfig });
  }

  @OnEvent({ name: 'AppRestart' })
  onAppRestart(state: ArgOf<'AppRestart'>) {
    this.websocketRepository.clientBroadcast('AppRestartV1', {
      isMaintenanceMode: state.isMaintenanceMode,
    });

    this.websocketRepository.serverSend('AppRestart', state);
  }

  @OnEvent({ name: 'ConfigValidate', priority: -100 })
  async onConfigValidate({ oldConfig, newConfig }: ArgOf<'ConfigValidate'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        if (
          newConfig.notifications.smtp.enabled &&
          !isEqualObject(oldConfig.notifications.smtp, newConfig.notifications.smtp)
        ) {
          yield* this.fx.email.verifySmtp(newConfig.notifications.smtp.transport);
        }
      }).pipe(
        Effect.catch((error_) => {
          const error: Error | any = unwrap(error_);
          this.logger.error(`Failed to validate SMTP configuration: ${error}`, error?.stack);
          return Effect.fail(new Error('Invalid SMTP configuration', { cause: error }));
        }),
      ),
    );
  }

  @OnEvent({ name: 'AssetHide' })
  onAssetHide({ assetId, userId }: ArgOf<'AssetHide'>) {
    this.websocketRepository.clientSend('on_asset_hidden', userId, assetId);
  }

  @OnEvent({ name: 'AssetShow' })
  async onAssetShow({ assetId }: ArgOf<'AssetShow'>) {
    return runPromise(
      this.fx.job.queue({ name: JobName.AssetGenerateThumbnails, data: { id: assetId, notify: true } }),
    );
  }

  @OnEvent({ name: 'AssetTrash' })
  onAssetTrash({ assetId, userId }: ArgOf<'AssetTrash'>) {
    this.websocketRepository.clientSend('on_asset_trash', userId, [assetId]);
  }

  @OnEvent({ name: 'AssetDelete' })
  onAssetDelete({ assetId, userId }: ArgOf<'AssetDelete'>) {
    this.websocketRepository.clientSend('on_asset_delete', userId, assetId);
  }

  @OnEvent({ name: 'AssetTrashAll' })
  onAssetsTrash({ assetIds, userId }: ArgOf<'AssetTrashAll'>) {
    this.websocketRepository.clientSend('on_asset_trash', userId, assetIds);
  }

  @OnEvent({ name: 'AssetMetadataExtracted' })
  async onAssetMetadataExtracted({ assetId, userId, source }: ArgOf<'AssetMetadataExtracted'>) {
    if (source !== 'sidecar-write') {
      return;
    }

    return runPromise(
      Effect.gen({ self: this }, function* () {
        const [asset] = yield* this.fx.asset.getByIdsWithAllRelationsButStacks([assetId]);
        if (asset) {
          yield* this.fx.websocket.clientSend(
            'on_asset_update',
            userId,
            mapAsset(asset, { auth: { user: { id: userId } } as AuthDto }),
          );
        }
      }),
    );
  }

  @OnEvent({ name: 'AssetRestoreAll' })
  onAssetsRestore({ assetIds, userId }: ArgOf<'AssetRestoreAll'>) {
    this.websocketRepository.clientSend('on_asset_restore', userId, assetIds);
  }

  @OnEvent({ name: 'StackCreate' })
  onStackCreate({ userId }: ArgOf<'StackCreate'>) {
    this.websocketRepository.clientSend('on_asset_stack_update', userId);
  }

  @OnEvent({ name: 'StackUpdate' })
  onStackUpdate({ userId }: ArgOf<'StackUpdate'>) {
    this.websocketRepository.clientSend('on_asset_stack_update', userId);
  }

  @OnEvent({ name: 'StackDelete' })
  onStackDelete({ userId }: ArgOf<'StackDelete'>) {
    this.websocketRepository.clientSend('on_asset_stack_update', userId);
  }

  @OnEvent({ name: 'StackDeleteAll' })
  onStacksDelete({ userId }: ArgOf<'StackDeleteAll'>) {
    this.websocketRepository.clientSend('on_asset_stack_update', userId);
  }

  @OnEvent({ name: 'UserSignup' })
  async onUserSignup({ notify, id, password: password }: ArgOf<'UserSignup'>) {
    if (notify) {
      return runPromise(this.fx.job.queue({ name: JobName.NotifyUserSignup, data: { id, password } }));
    }
  }

  @OnEvent({ name: 'UserDelete' })
  onUserDelete({ id }: ArgOf<'UserDelete'>) {
    this.websocketRepository.clientBroadcast('on_user_delete', id);
  }

  @OnEvent({ name: 'AlbumUpdate' })
  async onAlbumUpdate({ id, userIds, recipientIds }: ArgOf<'AlbumUpdate'>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        for (const userId of userIds) {
          yield* this.fx.websocket.clientSend('on_album_update', userId, id);
        }

        for (const recipientId of recipientIds) {
          yield* this.fx.job.removeJob(JobName.NotifyAlbumUpdate, `${id}/${recipientId}`);
          yield* this.fx.job.queue({
            name: JobName.NotifyAlbumUpdate,
            data: { id, recipientId, delay: NotificationService.albumUpdateEmailDelayMs },
          });
        }
      }),
    );
  }

  @OnEvent({ name: 'AlbumInvite' })
  async onAlbumInvite({ id, userId, senderName }: ArgOf<'AlbumInvite'>) {
    return runPromise(
      this.fx.job.queue({ name: JobName.NotifyAlbumInvite, data: { id, recipientId: userId, senderName } }),
    );
  }

  @OnEvent({ name: 'SessionDelete' })
  onSessionDelete({ sessionId }: ArgOf<'SessionDelete'>) {
    // after the response is sent
    setTimeout(() => this.websocketRepository.clientSend('on_session_delete', sessionId, sessionId), 500);
  }

  async sendTestEmail(id: string, dto: SystemConfigSmtpDto, tempTemplate?: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.fx.user.get(id, { withDeleted: false });
        if (!user) {
          return yield* Effect.fail(new Error('User not found'));
        }

        yield* this.fx.email
          .verifySmtp(dto.transport)
          .pipe(
            Effect.catch((error) =>
              Effect.fail(new BadRequestException('Failed to verify SMTP configuration', { cause: unwrap(error) })),
            ),
          );

        const { server } = yield* this.getConfigFx({ withCache: false });
        const { html, text } = yield* this.fx.email.renderEmail({
          template: EmailTemplate.TEST_EMAIL,
          data: {
            baseUrl: getExternalDomain(server),
            displayName: user.name,
          },
          customTemplate: tempTemplate!,
        });
        const { messageId } = yield* this.fx.email.sendEmail({
          to: user.email,
          subject: 'Test email from Immich',
          html,
          text,
          from: dto.from,
          replyTo: dto.replyTo || dto.from,
          smtp: dto.transport,
        });

        return { messageId };
      }),
    );
  }

  @OnJob({ name: JobName.NotifyUserSignup, queue: QueueName.Notification })
  async handleUserSignup({ id, password }: JobOf<JobName.NotifyUserSignup>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const user = yield* this.fx.user.get(id, { withDeleted: false });
        if (!user) {
          return JobStatus.Skipped;
        }

        const { server, templates } = yield* this.getConfigFx({ withCache: true });
        const { html, text } = yield* this.fx.email.renderEmail({
          template: EmailTemplate.WELCOME,
          data: {
            baseUrl: getExternalDomain(server),
            displayName: user.name,
            username: user.email,
            password,
          },
          customTemplate: templates.email.welcomeTemplate,
        });

        yield* this.fx.job.queue({
          name: JobName.SendMail,
          data: {
            to: user.email,
            subject: 'Welcome to Immich',
            html,
            text,
          },
        });

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.NotifyAlbumInvite, queue: QueueName.Notification })
  async handleAlbumInvite({ id, recipientId, senderName }: JobOf<JobName.NotifyAlbumInvite>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const album = yield* this.fx.album.getById(id);
        if (!album) {
          return JobStatus.Skipped;
        }

        const recipient = yield* this.fx.user.get(recipientId, { withDeleted: false });
        if (!recipient) {
          return JobStatus.Skipped;
        }

        yield* this.sendAlbumLocalNotification(album, recipientId, NotificationType.AlbumInvite, senderName);

        const { emailNotifications } = getPreferences(recipient.metadata);

        if (!emailNotifications.enabled || !emailNotifications.albumInvite) {
          return JobStatus.Skipped;
        }

        const attachment = yield* this.getAlbumThumbnailAttachment(album);

        const { server, templates } = yield* this.getConfigFx({ withCache: false });
        const { html, text } = yield* this.fx.email.renderEmail({
          template: EmailTemplate.ALBUM_INVITE,
          data: {
            baseUrl: getExternalDomain(server),
            albumId: album.id,
            albumName: album.albumName,
            senderName,
            recipientName: recipient.name,
            cid: attachment ? attachment.cid : undefined,
          },
          customTemplate: templates.email.albumInviteTemplate,
        });

        yield* this.fx.job.queue({
          name: JobName.SendMail,
          data: {
            to: recipient.email,
            subject: `You have been added to a shared album - ${album.albumName}`,
            html,
            text,
            imageAttachments: attachment ? [attachment] : undefined,
          },
        });

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.NotifyAlbumUpdate, queue: QueueName.Notification })
  async handleAlbumUpdate({ id, recipientId }: JobOf<JobName.NotifyAlbumUpdate>) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const album = yield* this.fx.album.getById(id);

        if (!album) {
          return JobStatus.Skipped;
        }

        const recipient = yield* this.fx.user.get(recipientId, { withDeleted: false });
        if (!recipient) {
          return JobStatus.Skipped;
        }

        yield* this.sendAlbumLocalNotification(album, recipientId, NotificationType.AlbumUpdate);

        const attachment = yield* this.getAlbumThumbnailAttachment(album);

        const { server, templates } = yield* this.getConfigFx({ withCache: false });

        const user = yield* this.fx.user.get(recipientId, { withDeleted: false });
        if (!user) {
          return JobStatus.Skipped;
        }

        const { emailNotifications } = getPreferences(user.metadata);

        if (!emailNotifications.enabled || !emailNotifications.albumUpdate) {
          return JobStatus.Skipped;
        }

        const { html, text } = yield* this.fx.email.renderEmail({
          template: EmailTemplate.ALBUM_UPDATE,
          data: {
            baseUrl: getExternalDomain(server),
            albumId: album.id,
            albumName: album.albumName,
            recipientName: user.name,
            cid: attachment ? attachment.cid : undefined,
          },
          customTemplate: templates.email.albumUpdateTemplate,
        });

        yield* this.fx.job.queue({
          name: JobName.SendMail,
          data: {
            to: user.email,
            subject: `New media has been added to an album - ${album.albumName}`,
            html,
            text,
            imageAttachments: attachment ? [attachment] : undefined,
          },
        });

        return JobStatus.Success;
      }),
    );
  }

  @OnJob({ name: JobName.SendMail, queue: QueueName.Notification })
  async handleSendEmail(data: JobOf<JobName.SendMail>): Promise<JobStatus> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { notifications } = yield* this.getConfigFx({ withCache: false });
        if (!notifications.smtp.enabled) {
          return JobStatus.Skipped;
        }

        const { to, subject, html, text: plain } = data;
        const response = yield* this.fx.email.sendEmail({
          to,
          subject,
          html,
          text: plain,
          from: notifications.smtp.from,
          replyTo: notifications.smtp.replyTo || notifications.smtp.from,
          smtp: notifications.smtp.transport,
          imageAttachments: data.imageAttachments,
        });

        this.logger.log(`Sent mail with id: ${response.messageId} status: ${response.response}`);

        return JobStatus.Success;
      }),
    );
  }

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private getAlbumThumbnailAttachment = Effect.fn('NotificationService.getAlbumThumbnailAttachment')(function* (
    this: NotificationService,
    album: {
      albumThumbnailAssetId: string | null;
    },
  ) {
    if (!album.albumThumbnailAssetId) {
      return;
    }

    const albumThumbnailFiles = yield* this.fx.assetJob.getAlbumThumbnailFiles(
      album.albumThumbnailAssetId,
      AssetFileType.Thumbnail,
    );

    if (albumThumbnailFiles.length !== 1) {
      return;
    }

    return {
      filename: `album-thumbnail${getFilenameExtension(albumThumbnailFiles[0].path)}`,
      path: albumThumbnailFiles[0].path,
      cid: 'album-thumbnail',
    } satisfies EmailImageAttachment;
  });

  // eslint-disable-next-line unicorn/consistent-function-scoping
  private sendAlbumLocalNotification = Effect.fn('NotificationService.sendAlbumLocalNotification')(function* (
    this: NotificationService,
    album: MapAlbumDto,
    userId: string,
    type: NotificationType.AlbumInvite | NotificationType.AlbumUpdate,
    senderName?: string,
  ) {
    const isInvite = type === NotificationType.AlbumInvite;
    const item = yield* this.fx.notification.create({
      userId,
      type,
      level: isInvite ? NotificationLevel.Success : NotificationLevel.Info,
      title: isInvite ? 'Shared Album Invitation' : 'Shared Album Update',
      description: isInvite
        ? `${senderName} shared an album (${album.albumName}) with you`
        : `New media has been added to the album (${album.albumName})`,
      data: JSON.stringify({ albumId: album.id }),
    });

    yield* this.fx.websocket.clientSend('on_notification', userId, mapNotification(item));
  });
}
