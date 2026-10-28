import { Context, Effect } from 'effect';
import { Tag } from 'src/database.js';
import { effectOf } from 'src/effect/bridge.js';
import { runPromise } from 'src/effect/run.js';
import { TagRepo, TagRepository } from 'src/repositories/tag.repository.js';

type UpsertRequest = { userId: string; tags: string[] };

export const upsertTagsFx = (repository: Context.Service.Shape<typeof TagRepo>, { userId, tags }: UpsertRequest) =>
  Effect.gen(function* () {
    tags = [...new Set(tags)];

    const results: Tag[] = [];

    for (const tag of tags) {
      const parts = tag.split('/').filter(Boolean);
      let parent: Tag | undefined;

      for (const part of parts) {
        const value = parent ? `${parent.value}/${part}` : part;
        parent = yield* repository.upsertValue({ userId, value, parentId: parent?.id });
      }

      if (parent) {
        results.push(parent);
      }
    }

    return results;
  });

export const upsertTags = (repository: TagRepository, request: UpsertRequest): Promise<Tag[]> =>
  runPromise(upsertTagsFx(effectOf(TagRepo, repository), request));
