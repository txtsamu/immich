import { getExploreData, MemorySearchOrder } from '@immich/sdk';
import { memoryManager } from '$lib/managers/memory-manager.svelte';
import { authenticate } from '$lib/utils/auth';
import { getFormatter } from '$lib/utils/i18n';
import type { PageLoad } from './$types';

export const load = (async ({ url }) => {
  await authenticate(url);
  memoryManager.setFilters({ size: 12, order: MemorySearchOrder.Desc });
  await memoryManager.applyPreferences();

  const [explore] = await Promise.all([getExploreData(), memoryManager.refresh()]);
  const $t = await getFormatter();

  return {
    explore,
    memories: memoryManager.memories,
    meta: {
      title: $t('explore'),
    },
  };
}) satisfies PageLoad;
