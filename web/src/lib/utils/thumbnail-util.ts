import { t } from 'svelte-i18n';
import { derived, get } from 'svelte/store';
import type { TimelineAsset } from '$lib/managers/timeline-manager/types';
import { locale } from '$lib/stores/preferences.store';
import { fromTimelinePlainDateTime } from '$lib/utils/timeline-util';

export const getAltText = derived(t, ($t) => {
  return (asset: TimelineAsset) => {
    const date = fromTimelinePlainDateTime(asset.localDateTime).toJSDate().toLocaleString(get(locale), {
      dateStyle: 'long',
      timeZone: 'UTC',
    });
    const hasPlace = asset.city && asset.country;

    const values = {
      date,
      city: asset.city,
      country: asset.country,
      isVideo: asset.isVideo,
    };

    if (hasPlace) {
      return $t('image_alt_text_date_place', { values });
    }

    return $t('image_alt_text_date', { values });
  };
});
