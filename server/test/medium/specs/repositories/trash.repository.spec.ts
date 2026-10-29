import { Kysely } from 'kysely';
import { AssetStatus } from 'src/enum.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { TrashRepository } from 'src/repositories/trash.repository.js';
import { DB } from 'src/schema/index.js';
import { BaseService } from 'src/services/base.service.js';
import { newMediumService } from 'test/medium.factory.js';
import { getKyselyDB } from 'test/utils.js';

let defaultDatabase: Kysely<DB>;

const statusOf = async (db: Kysely<DB>, id: string) => {
  const { status } = await db.selectFrom('asset').select('status').where('id', '=', id).executeTakeFirstOrThrow();
  return status;
};

const setup = (db?: Kysely<DB>) => {
  const { ctx } = newMediumService(BaseService, {
    database: db || defaultDatabase,
    real: [],
    mock: [LoggingRepository],
  });
  return { ctx, sut: ctx.get(TrashRepository) };
};

beforeAll(async () => {
  defaultDatabase = await getKyselyDB();
});

describe(TrashRepository.name, () => {
  describe('empty', () => {
    it('should mark trashed assets and assets taken offline as deleted, and nothing else', async () => {
      const { ctx, sut } = setup();
      const { user } = await ctx.newUser();
      const { user: otherUser } = await ctx.newUser();
      const { asset: trashed } = await ctx.newAsset({
        ownerId: user.id,
        status: AssetStatus.Trashed,
        deletedAt: new Date(),
      });
      const { asset: offline } = await ctx.newAsset({ ownerId: user.id, isOffline: true, deletedAt: new Date() });
      const { asset: active } = await ctx.newAsset({ ownerId: user.id });
      const { asset: otherUsers } = await ctx.newAsset({
        ownerId: otherUser.id,
        status: AssetStatus.Trashed,
        deletedAt: new Date(),
      });

      await expect(sut.empty(user.id)).resolves.toBe(2);

      await expect(statusOf(defaultDatabase, trashed.id)).resolves.toBe(AssetStatus.Deleted);
      await expect(statusOf(defaultDatabase, offline.id)).resolves.toBe(AssetStatus.Deleted);
      await expect(statusOf(defaultDatabase, active.id)).resolves.toBe(AssetStatus.Active);
      await expect(statusOf(defaultDatabase, otherUsers.id)).resolves.toBe(AssetStatus.Trashed);
    });
  });
});
