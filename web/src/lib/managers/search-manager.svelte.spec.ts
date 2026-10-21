import { searchManager } from '$lib/managers/search-manager.svelte';

describe('SearchManager', () => {
  beforeEach(() => {
    localStorage.clear();
    searchManager.reset();
  });

  it('defaults to metadata search', () => {
    expect(searchManager.filter.queryType).toBe('metadata');
  });

  it('remembers the selected query type', () => {
    searchManager.setQueryType('description');
    expect(searchManager.filter.queryType).toBe('description');

    searchManager.reset();
    expect(searchManager.filter.queryType).toBe('description');
  });

  it('overrides a previously stored query type', () => {
    localStorage.setItem('searchQueryType', 'metadata');
    searchManager.reset();

    searchManager.setQueryType('fullPath');
    searchManager.reset();
    expect(searchManager.filter.queryType).toBe('fullPath');
  });

  it('ignores an invalid stored query type', () => {
    localStorage.setItem('searchQueryType', 'invalid');
    searchManager.reset();
    expect(searchManager.filter.queryType).toBe('metadata');
  });
});
