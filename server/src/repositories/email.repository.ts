import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { createTransport } from 'nodemailer';
import React, { createElement } from 'react';
import { render } from 'react-email';
import type { EmailImageAttachment } from 'src/types.js';
import { registerEffect } from 'src/effect/bridge.js';
import { LibraryError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { AlbumInviteEmail } from 'src/emails/album-invite.email.js';
import { AlbumUpdateEmail } from 'src/emails/album-update.email.js';
import { TestEmail } from 'src/emails/test.email.js';
import { WelcomeEmail } from 'src/emails/welcome.email.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';

export type SendEmailOptions = {
  from: string;
  to: string;
  replyTo?: string;
  subject: string;
  html: string;
  text: string;
  imageAttachments?: EmailImageAttachment[];
  smtp: SmtpOptions;
};

export type SmtpOptions = {
  host: string;
  port?: number;
  secure?: boolean;
  username?: string;
  password?: string;
  ignoreCert?: boolean;
};

export enum EmailTemplate {
  TEST_EMAIL = 'test',

  // AUTH
  WELCOME = 'welcome',
  RESET_PASSWORD = 'reset-password',

  // ALBUM
  ALBUM_INVITE = 'album-invite',
  ALBUM_UPDATE = 'album-update',
}

interface BaseEmailProps {
  baseUrl: string;
  customTemplate?: string;
}

export interface TestEmailProps extends BaseEmailProps {
  displayName: string;
}

export interface WelcomeEmailProps extends BaseEmailProps {
  displayName: string;
  username: string;
  password?: string;
}

export interface AlbumInviteEmailProps extends BaseEmailProps {
  albumName: string;
  albumId: string;
  senderName: string;
  recipientName: string;
  cid?: string;
}

export interface AlbumUpdateEmailProps extends BaseEmailProps {
  albumName: string;
  albumId: string;
  recipientName: string;
  cid?: string;
}

export type EmailRenderRequest =
  | {
      template: EmailTemplate.TEST_EMAIL;
      data: TestEmailProps;
      customTemplate: string;
    }
  | {
      template: EmailTemplate.WELCOME;
      data: WelcomeEmailProps;
      customTemplate: string;
    }
  | {
      template: EmailTemplate.ALBUM_INVITE;
      data: AlbumInviteEmailProps;
      customTemplate: string;
    }
  | {
      template: EmailTemplate.ALBUM_UPDATE;
      data: AlbumUpdateEmailProps;
      customTemplate: string;
    };

export type SendEmailResponse = {
  messageId: string;
  response: any;
};

const renderComponent = ({
  template,
  data,
  customTemplate,
}: EmailRenderRequest): React.FunctionComponentElement<any> => {
  switch (template) {
    case EmailTemplate.TEST_EMAIL: {
      return createElement(TestEmail, { ...data, customTemplate });
    }

    case EmailTemplate.WELCOME: {
      return createElement(WelcomeEmail, { ...data, customTemplate });
    }

    case EmailTemplate.ALBUM_INVITE: {
      return createElement(AlbumInviteEmail, { ...data, customTemplate });
    }

    case EmailTemplate.ALBUM_UPDATE: {
      return createElement(AlbumUpdateEmail, { ...data, customTemplate });
    }
  }
};

const buildTransport = (options: SmtpOptions) =>
  createTransport({
    host: options.host,
    port: options.port,
    tls: { rejectUnauthorized: !options.ignoreCert },
    secure: options.secure,
    auth:
      options.username || options.password
        ? {
            user: options.username,
            pass: options.password,
          }
        : undefined,
    connectionTimeout: 5000,
  });

export class EmailRepo extends Context.Service<
  EmailRepo,
  {
    verifySmtp(options: SmtpOptions): Effect.Effect<true, LibraryError>;
    renderEmail(request: EmailRenderRequest): Effect.Effect<{ html: string; text: string }, LibraryError>;
    sendEmail(options: SendEmailOptions): Effect.Effect<SendEmailResponse, LibraryError>;
  }
>()('immich/repositories/EmailRepo') {
  static readonly make = (logger: LoggingRepository) =>
    EmailRepo.of({
      verifySmtp: Effect.fn('Email.verifySmtp')((options: SmtpOptions) =>
        Effect.tryPromise({
          // the transport is closed right after verification starts, exactly as before the conversion
          try: () => {
            const transport = buildTransport(options);
            try {
              return transport.verify();
            } finally {
              transport.close();
            }
          },
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),

      renderEmail: Effect.fn('Email.renderEmail')((request: EmailRenderRequest) =>
        Effect.tryPromise({
          try: async () => {
            const component = renderComponent(request);
            const html = await render(component, { pretty: false });
            const text = await render(component, { plainText: true });
            return { html, text };
          },
          catch: (cause) => new LibraryError({ cause }),
        }),
      ),

      sendEmail: Effect.fn('Email.sendEmail')(
        ({ to, from, subject, html, text, smtp, imageAttachments }: SendEmailOptions) =>
          Effect.tryPromise({
            try: () => {
              logger.debug(`Sending email to ${to} with subject: ${subject}`);
              const transport = buildTransport(smtp);

              const attachments = imageAttachments?.map((attachment) => ({
                filename: attachment.filename,
                path: attachment.path,
                cid: attachment.cid,
              }));

              try {
                return transport.sendMail({ to, from, subject, html, text, attachments });
              } finally {
                transport.close();
              }
            },
            catch: (cause) => new LibraryError({ cause }),
          }),
      ),
    });

  static readonly layer = (logger: LoggingRepository) => Layer.sync(EmailRepo, () => EmailRepo.make(logger));
}

/** Promise-facing adapter for the Nest services. Delegates to `EmailRepo`. */
@Injectable()
export class EmailRepository {
  private readonly effect: Context.Service.Shape<typeof EmailRepo>;

  constructor(private logger: LoggingRepository) {
    this.logger.setContext(EmailRepository.name);
    this.effect = EmailRepo.make(this.logger);
    registerEffect(this, this.effect);
  }

  verifySmtp(options: SmtpOptions): Promise<true> {
    return runPromise(this.effect.verifySmtp(options));
  }

  renderEmail(request: EmailRenderRequest): Promise<{ html: string; text: string }> {
    return runPromise(this.effect.renderEmail(request));
  }

  sendEmail(options: SendEmailOptions): Promise<SendEmailResponse> {
    return runPromise(this.effect.sendEmail(options));
  }
}
