<script lang="ts">
  import { getSearchTypeTitle } from './search-bar-utils';
  import SearchButton from './SearchButton.svelte';
  import { searchManager } from '$lib/managers/search-manager.svelte';
  import { Text } from '@immich/ui';
  import { t } from 'svelte-i18n';

  let queryType = $derived(searchManager.filter.queryType);

  const setType = (type: 'metadata' | 'description' | 'fullPath') => {
    searchManager.setQueryType(type);
  };
</script>

<section>
  <fieldset>
    <Text class="mb-5">{$t('search_type_description')}</Text>
    <div class="flex flex-wrap gap-2">
      <SearchButton checked active={queryType === 'metadata'} onclick={() => setType('metadata')}>
        {getSearchTypeTitle('metadata')}
      </SearchButton>
      <SearchButton checked active={queryType === 'description'} onclick={() => setType('description')}>
        {getSearchTypeTitle('description')}
      </SearchButton>
      <SearchButton checked active={queryType === 'fullPath'} onclick={() => setType('fullPath')}>
        {getSearchTypeTitle('fullPath')}
      </SearchButton>
    </div>
  </fieldset>
</section>
