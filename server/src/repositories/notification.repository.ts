import { Context, Effect, Layer } from 'effect';
import { DateTime } from 'luxon';
import { InjectKysely } from 'nestjs-kysely';
import type { Insertable, Kysely, Updateable } from 'kysely';
import { columns } from 'src/database.js';
import { DummyValue, GenerateSql } from 'src/decorators.js';
import { NotificationSearchDto } from 'src/dtos/notification.dto.js';
import { Db, query } from 'src/effect/db.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { NotificationTable } from 'src/schema/tables/notification.table.js';

const makeNotification = (db: Kysely<DB>) => ({
  cleanup: Effect.fn('Notification.cleanup')(function* () {
    return yield* query(db, (db) =>
      db
        .deleteFrom('notification')
        .where((eb) =>
          eb.or([
            // remove soft-deleted notifications
            eb.and([
              eb('deletedAt', 'is not', null),
              eb('deletedAt', '<', DateTime.now().minus({ days: 3 }).toJSDate()),
            ]),

            // remove old, read notifications
            eb.and([
              // keep recently read messages around for a few days
              eb('readAt', '>', DateTime.now().minus({ days: 2 }).toJSDate()),
              eb('createdAt', '<', DateTime.now().minus({ days: 15 }).toJSDate()),
            ]),

            eb.and([
              // remove super old, unread notifications
              eb('readAt', '=', null),
              eb('createdAt', '<', DateTime.now().minus({ days: 30 }).toJSDate()),
            ]),
          ]),
        )
        .execute(),
    );
  }),

  search: Effect.fn('Notification.search')(function* (userId: string, dto: NotificationSearchDto) {
    return yield* query(db, (db) =>
      db
        .selectFrom('notification')
        .select(columns.notification)
        .where((qb) =>
          qb.and({
            userId,
            id: dto.id,
            level: dto.level,
            type: dto.type,
            readAt: dto.unread ? null : undefined,
          }),
        )
        .where('deletedAt', 'is', null)
        .orderBy('createdAt', 'desc')
        .execute(),
    );
  }),

  create: Effect.fn('Notification.create')(function* (notification: Insertable<NotificationTable>) {
    return yield* query(db, (db) =>
      db.insertInto('notification').values(notification).returning(columns.notification).executeTakeFirstOrThrow(),
    );
  }),

  get: Effect.fn('Notification.get')(function* (id: string) {
    return yield* query(db, (db) =>
      db
        .selectFrom('notification')
        .select(columns.notification)
        .where('id', '=', id)
        .where('deletedAt', 'is not', null)
        .executeTakeFirst(),
    );
  }),

  update: Effect.fn('Notification.update')(function* (id: string, notification: Updateable<NotificationTable>) {
    return yield* query(db, (db) =>
      db
        .updateTable('notification')
        .set(notification)
        .where('deletedAt', 'is', null)
        .where('id', '=', id)
        .returning(columns.notification)
        .executeTakeFirstOrThrow(),
    );
  }),

  updateAll: Effect.fn('Notification.updateAll')(function* (
    ids: string[],
    notification: Updateable<NotificationTable>,
  ) {
    yield* query(db, (db) => db.updateTable('notification').set(notification).where('id', 'in', ids).execute());
  }),

  delete: Effect.fn('Notification.delete')(function* (id: string) {
    yield* query(db, (db) =>
      db.updateTable('notification').set({ deletedAt: DateTime.now().toJSDate() }).where('id', '=', id).execute(),
    );
  }),

  deleteAll: Effect.fn('Notification.deleteAll')(function* (ids: string[]) {
    yield* query(db, (db) =>
      db.updateTable('notification').set({ deletedAt: DateTime.now().toJSDate() }).where('id', 'in', ids).execute(),
    );
  }),
});

export class NotificationRepo extends Context.Service<NotificationRepo, ReturnType<typeof makeNotification>>()(
  'immich/repositories/NotificationRepo',
) {
  static readonly make = (db: Kysely<DB>) => NotificationRepo.of(makeNotification(db));

  static readonly layer = Layer.effect(
    NotificationRepo,
    Effect.gen(function* () {
      return NotificationRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `NotificationRepo`. */
export class NotificationRepository {
  private readonly effect: Context.Service.Shape<typeof NotificationRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = NotificationRepo.make(db);
  }

  cleanup() {
    return runPromise(this.effect.cleanup());
  }

  @GenerateSql({ params: [DummyValue.UUID, {}] }, { name: 'unread', params: [DummyValue.UUID, { unread: true }] })
  search(userId: string, dto: NotificationSearchDto) {
    return runPromise(this.effect.search(userId, dto));
  }

  create(notification: Insertable<NotificationTable>) {
    return runPromise(this.effect.create(notification));
  }

  get(id: string) {
    return runPromise(this.effect.get(id));
  }

  update(id: string, notification: Updateable<NotificationTable>) {
    return runPromise(this.effect.update(id, notification));
  }

  updateAll(ids: string[], notification: Updateable<NotificationTable>): Promise<void> {
    return runPromise(this.effect.updateAll(ids, notification));
  }

  delete(id: string): Promise<void> {
    return runPromise(this.effect.delete(id));
  }

  deleteAll(ids: string[]): Promise<void> {
    return runPromise(this.effect.deleteAll(ids));
  }
}
