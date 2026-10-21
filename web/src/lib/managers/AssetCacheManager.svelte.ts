import { getAssetInfo } from '@immich/sdk';
import { authManager } from '$lib/managers/auth-manager.svelte';
import { eventManager } from '$lib/managers/event-manager.svelte';

const defaultSerializer = <K>(params: K) => JSON.stringify(params);

class AsyncCache<K, V> {
  #cache = new Map<string, V>();

  constructor(private fetcher: (params: K) => Promise<V>) {}

  async getOrFetch(params: K, updateCache: boolean): Promise<V> {
    const cacheKey = defaultSerializer(params);

    const cached = this.#cache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const value = await this.fetcher(params);
    if (value && updateCache) {
      this.#cache.set(cacheKey, value);
    }

    return value;
  }

  clearKey(params: K) {
    const cacheKey = defaultSerializer(params);
    this.#cache.delete(cacheKey);
  }

  clear() {
    this.#cache.clear();
  }
}

class AssetCacheManager {
  #assetCache = new AsyncCache(getAssetInfo);

  constructor() {
    eventManager.on({
      AssetEditsApplied: (assetId) => {
        this.invalidateAsset(assetId);
      },
      AssetUpdate: (asset) => {
        this.invalidateAsset(asset.id);
      },
    });
  }

  async getAsset({ id, key, slug }: { id: string; key?: string; slug?: string }, updateCache = true) {
    return this.#assetCache.getOrFetch({ id, key, slug }, updateCache);
  }

  invalidateAsset(id: string) {
    const { key, slug } = authManager.params;
    this.#assetCache.clearKey({ id, key, slug });
  }

  clearAssetCache() {
    this.#assetCache.clear();
  }

  invalidate() {
    this.clearAssetCache();
  }
}

export const assetCacheManager = new AssetCacheManager();
