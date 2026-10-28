import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { SystemConfigSmtpDto } from 'src/dtos/config.dto.js';
import { NotificationCreateDto, mapNotification } from 'src/dtos/notification.dto.js';
import { runPromise, unwrap } from 'src/effect/run.js';
import { NotificationLevel, NotificationType } from 'src/enum.js';
import { EmailTemplate } from 'src/repositories/email.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { getExternalDomain } from 'src/utils/misc.js';

/** The original thrown value behind an Effect error that wraps it in `cause` (see `src/effect/errors.ts`). */
@Injectable()
export class NotificationAdminService extends BaseService {
  async create(auth: AuthDto, dto: NotificationCreateDto) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const item = yield* this.fx.notification.create({
          userId: dto.userId,
          type: dto.type ?? NotificationType.Custom,
          level: dto.level ?? NotificationLevel.Info,
          title: dto.title,
          description: dto.description,
          data: dto.data,
        });

        return mapNotification(item);
      }),
    );
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

  async getTemplate(name: EmailTemplate, customTemplate: string) {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { server, templates } = yield* this.getConfigFx({ withCache: false });

        let templateResponse: string;

        switch (name) {
          case EmailTemplate.WELCOME: {
            const { html: _welcomeHtml } = yield* this.fx.email.renderEmail({
              template: EmailTemplate.WELCOME,
              data: {
                baseUrl: getExternalDomain(server),
                displayName: 'John Doe',
                username: 'john@doe.com',
                password: 'thisIsAPassword123',
              },
              customTemplate: customTemplate || templates.email.welcomeTemplate,
            });

            templateResponse = _welcomeHtml;
            break;
          }
          case EmailTemplate.ALBUM_UPDATE: {
            const { html: _updateAlbumHtml } = yield* this.fx.email.renderEmail({
              template: EmailTemplate.ALBUM_UPDATE,
              data: {
                baseUrl: getExternalDomain(server),
                albumId: '1',
                albumName: 'Favorite Photos',
                recipientName: 'Jane Doe',
                cid: undefined,
              },
              customTemplate: customTemplate || templates.email.albumInviteTemplate,
            });
            templateResponse = _updateAlbumHtml;
            break;
          }

          case EmailTemplate.ALBUM_INVITE: {
            const { html } = yield* this.fx.email.renderEmail({
              template: EmailTemplate.ALBUM_INVITE,
              data: {
                baseUrl: getExternalDomain(server),
                albumId: '1',
                albumName: "John Doe's Favorites",
                senderName: 'John Doe',
                recipientName: 'Jane Doe',
                cid: undefined,
              },
              customTemplate: customTemplate || templates.email.albumInviteTemplate,
            });
            templateResponse = html;
            break;
          }
          default: {
            templateResponse = '';
            break;
          }
        }

        return { name, html: templateResponse };
      }),
    );
  }
}
