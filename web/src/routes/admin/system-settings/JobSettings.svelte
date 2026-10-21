<script lang="ts">
  import { createPreloadedQuery } from '$lib/queries/client';
  import { getConfigQuery } from '$lib/queries/system-config.query';
  import { cloneDeep } from 'lodash-es';
  import SettingButtonsRow from '$lib/components/shared-components/settings/SystemConfigButtonRow.svelte';
  import SettingInputField from '$lib/components/shared-components/settings/SettingInputField.svelte';
  import { SettingInputFieldType } from '$lib/constants';
  import { featureFlagsManager } from '$lib/managers/feature-flags-manager.svelte';
  import { QueueName, type AdminConfigJobDto } from '@immich/sdk';
  import { t } from 'svelte-i18n';
  import { fade } from 'svelte/transition';

  const query = createPreloadedQuery(getConfigQuery);
  const config = $derived(query.data);

  const disabled = $derived(featureFlagsManager.value.configFile);
  let configToEdit = $state(cloneDeep(config));

  const queueNames = [
    QueueName.ThumbnailGeneration,
    QueueName.MetadataExtraction,
    QueueName.Library,
    QueueName.Sidecar,
    QueueName.VideoConversion,
    QueueName.StorageTemplateMigration,
    QueueName.Migration,
  ];

  function isSystemConfigJobDto(jobName: string): jobName is keyof AdminConfigJobDto {
    return Object.hasOwn(configToEdit.job, jobName);
  }

  const queueTitles: Record<QueueName, string> = $derived({
    [QueueName.ThumbnailGeneration]: $t('admin.thumbnail_generation_job'),
    [QueueName.MetadataExtraction]: $t('admin.metadata_extraction_job'),
    [QueueName.Sidecar]: $t('admin.sidecar_job'),
    [QueueName.VideoConversion]: $t('admin.video_conversion_job'),
    [QueueName.StorageTemplateMigration]: $t('admin.storage_template_migration'),
    [QueueName.Migration]: $t('admin.migration_job'),
    [QueueName.BackgroundTask]: $t('admin.background_task_job'),
    [QueueName.Search]: $t('search'),
    [QueueName.Library]: $t('external_libraries'),
    [QueueName.Notifications]: $t('notifications'),
    [QueueName.BackupDatabase]: $t('admin.backup_database'),
    [QueueName.Workflow]: $t('workflows'),
    [QueueName.Editor]: $t('editor'),
    [QueueName.IntegrityCheck]: $t('integrity_checks'),
  });
</script>

<div>
  <div in:fade={{ duration: 500 }}>
    <form autocomplete="off" onsubmit={(event) => event.preventDefault()}>
      {#each queueNames as queueName (queueName)}
        <div class="ms-4 mt-4 flex flex-col gap-4">
          {#if isSystemConfigJobDto(queueName)}
            <SettingInputField
              inputType={SettingInputFieldType.NUMBER}
              {disabled}
              label={$t('admin.job_concurrency', { values: { job: queueTitles[queueName] } })}
              description=""
              bind:value={configToEdit.job[queueName].concurrency}
              required={true}
              isEdited={configToEdit.job[queueName].concurrency !== config.job[queueName].concurrency}
            />
          {:else}
            <SettingInputField
              inputType={SettingInputFieldType.NUMBER}
              label={$t('admin.job_concurrency', { values: { job: queueTitles[queueName] } })}
              description=""
              value={1}
              disabled={true}
              title={$t('admin.job_not_concurrency_safe')}
            />
          {/if}
        </div>
      {/each}

      <div class="ms-4">
        <SettingButtonsRow bind:configToEdit keys={['job']} {disabled} />
      </div>
    </form>
  </div>
</div>
