import { AssetVisibility } from '@immich/sdk';
import { init, register, waitLocale } from 'svelte-i18n';
import type { TimelineAsset } from '$lib/managers/timeline-manager/types';
import { getAltText } from '$lib/utils/thumbnail-util';

describe('getAltText', () => {
  beforeAll(async () => {
    await init({ fallbackLocale: 'en-US' });
    register('en-US', () => import('$i18n/en.json'));
    await waitLocale('en-US');
  });

  it.each`
    isVideo  | city         | country      | expected
    ${false} | ${undefined} | ${'country'} | ${'Image taken on January 1, 2024'}
    ${true}  | ${'city'}    | ${undefined} | ${'Video taken on January 1, 2024'}
    ${false} | ${'city'}    | ${'country'} | ${'Image taken in city, country on January 1, 2024'}
    ${true}  | ${'city'}    | ${'country'} | ${'Video taken in city, country on January 1, 2024'}
  `(
    'generates correctly formatted alt text when isVideo=$isVideo, city=$city, country=$country',
    ({ isVideo, city, country, expected }: { isVideo: boolean; city?: string; country?: string; expected: string }) => {
      const testDate = new Date('2024-01-01T12:00:00.000Z');
      const asset: TimelineAsset = {
        id: 'test-id',
        ownerId: 'test-owner',
        ratio: 1,
        thumbhash: null,
        fileCreatedAt: {
          year: testDate.getUTCFullYear(),
          month: testDate.getUTCMonth() + 1, // Note: getMonth() is 0-based
          day: testDate.getUTCDate(),
          hour: testDate.getUTCHours(),
          minute: testDate.getUTCMinutes(),
          second: testDate.getUTCSeconds(),
          millisecond: testDate.getUTCMilliseconds(),
        },
        createdAt: {
          year: testDate.getUTCFullYear(),
          month: testDate.getUTCMonth() + 1, // Note: getMonth() is 0-based
          day: testDate.getUTCDate(),
          hour: testDate.getUTCHours(),
          minute: testDate.getUTCMinutes(),
          second: testDate.getUTCSeconds(),
          millisecond: testDate.getUTCMilliseconds(),
        },
        localDateTime: {
          year: testDate.getUTCFullYear(),
          month: testDate.getUTCMonth() + 1, // Note: getMonth() is 0-based
          day: testDate.getUTCDate(),
          hour: testDate.getUTCHours(),
          minute: testDate.getUTCMinutes(),
          second: testDate.getUTCSeconds(),
          millisecond: testDate.getUTCMilliseconds(),
        },

        visibility: AssetVisibility.Timeline,
        isFavorite: false,
        isTrashed: false,
        isVideo,
        isImage: !isVideo,
        stack: null,
        duration: null,
        projectionType: null,
        livePhotoVideoId: null,
        city: city ?? null,
        country: country ?? null,
      };

      getAltText.subscribe((fn) => {
        expect(fn(asset)).toEqual(expected);
      });
    },
  );
});
