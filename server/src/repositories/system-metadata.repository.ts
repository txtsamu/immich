import { Injectable } from '@nestjs/common';
import { Context, Effect, Layer } from 'effect';
import { InjectKysely } from 'nestjs-kysely';
import { readFile } from 'node:fs/promises';
import type { Insertable, Kysely } from 'kysely';
import type { SystemMetadata } from 'src/types.js';
import { GenerateSql } from 'src/decorators.js';
import { Db, query } from 'src/effect/db.js';
import { DatabaseError, FileSystemError } from 'src/effect/errors.js';
import { runPromise } from 'src/effect/run.js';
import { DB } from 'src/schema/index.js';
import { SystemMetadataTable } from 'src/schema/tables/system-metadata.table.js';

type Upsert = Insertable<SystemMetadataTable>;

export class SystemMetadataRepo extends Context.Service<
  SystemMetadataRepo,
  {
    get<T extends keyof SystemMetadata>(key: T): Effect.Effect<SystemMetadata[T] | null, DatabaseError>;
    set<T extends keyof SystemMetadata>(key: T, value: SystemMetadata[T]): Effect.Effect<void, DatabaseError>;
    delete<T extends keyof SystemMetadata>(key: T): Effect.Effect<void, DatabaseError>;
    readFile(filename: string): Effect.Effect<string, FileSystemError>;
  }
>()('immich/repositories/SystemMetadataRepo') {
  static readonly make = (db: Kysely<DB>) =>
    SystemMetadataRepo.of({
      get: Effect.fn('SystemMetadata.get')(function* <T extends keyof SystemMetadata>(key: T) {
        const metadata = yield* query(db, (db) =>
          db.selectFrom('system_metadata').select('value').where('key', '=', key).executeTakeFirst(),
        );
        return metadata ? (metadata.value as SystemMetadata[T]) : null;
      }),

      set: Effect.fn('SystemMetadata.set')(function* <T extends keyof SystemMetadata>(
        key: T,
        value: SystemMetadata[T],
      ) {
        yield* query(db, (db) =>
          db
            .insertInto('system_metadata')
            .values({ key, value } as Upsert)
            .onConflict((oc) => oc.columns(['key']).doUpdateSet({ value } as Upsert))
            .execute(),
        );
      }),

      delete: Effect.fn('SystemMetadata.delete')(function* <T extends keyof SystemMetadata>(key: T) {
        yield* query(db, (db) => db.deleteFrom('system_metadata').where('key', '=', key).execute());
      }),

      readFile: Effect.fn('SystemMetadata.readFile')((filename: string) =>
        Effect.tryPromise({
          try: () => readFile(filename, { encoding: 'utf8' }),
          catch: (cause) => new FileSystemError({ cause }),
        }),
      ),
    });

  static readonly layer = Layer.effect(
    SystemMetadataRepo,
    Effect.gen(function* () {
      return SystemMetadataRepo.make(yield* Db);
    }),
  );
}

/** Promise-facing adapter for the Nest services. Delegates to `SystemMetadataRepo`. */
@Injectable()
export class SystemMetadataRepository {
  private readonly effect: Context.Service.Shape<typeof SystemMetadataRepo>;

  constructor(@InjectKysely() db: Kysely<DB>) {
    this.effect = SystemMetadataRepo.make(db);
  }

  @GenerateSql({ params: ['metadata_key'] })
  get<T extends keyof SystemMetadata>(key: T): Promise<SystemMetadata[T] | null> {
    return runPromise(this.effect.get(key));
  }

  set<T extends keyof SystemMetadata>(key: T, value: SystemMetadata[T]): Promise<void> {
    return runPromise(this.effect.set(key, value));
  }

  @GenerateSql({ params: ['metadata_key'] })
  delete<T extends keyof SystemMetadata>(key: T): Promise<void> {
    return runPromise(this.effect.delete(key));
  }

  readFile(filename: string): Promise<string> {
    return runPromise(this.effect.readFile(filename));
  }
}
