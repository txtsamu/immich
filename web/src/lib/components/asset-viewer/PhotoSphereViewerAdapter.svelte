<script lang="ts">
  import { shortcuts } from '$lib/actions/shortcut';
  import AssetViewerEvents from '$lib/components/AssetViewerEvents.svelte';
  import { assetViewerManager } from '$lib/managers/asset-viewer-manager.svelte';
  import { alwaysLoadOriginalFile } from '$lib/stores/preferences.store';
  import {
    EquirectangularAdapter,
    Viewer,
    events,
    type AdapterConstructor,
    type PluginConstructor,
  } from '@photo-sphere-viewer/core';
  import '@photo-sphere-viewer/core/index.css';
  import { ResolutionPlugin } from '@photo-sphere-viewer/resolution-plugin';
  import { SettingsPlugin } from '@photo-sphere-viewer/settings-plugin';
  import '@photo-sphere-viewer/settings-plugin/index.css';
  import { onDestroy, onMount } from 'svelte';

  type Props = {
    panorama: string | { source: string };
    originalPanorama?: string | { source: string };
    adapter?: AdapterConstructor | [AdapterConstructor, unknown];
    plugins?: (PluginConstructor | [PluginConstructor, unknown])[];
    navbar?: boolean;
  };

  let { panorama, originalPanorama, adapter = EquirectangularAdapter, plugins = [], navbar = false }: Props = $props();

  let container: HTMLDivElement | undefined = $state();
  let viewer: Viewer;

  const onZoom = () => {
    viewer?.animate({ zoom: assetViewerManager.zoom > 1 ? 50 : 83.3, speed: 250 });
  };

  let hasChangedResolution: boolean = false;
  onMount(() => {
    if (!container) {
      return;
    }

    viewer = new Viewer({
      adapter,
      plugins: [
        SettingsPlugin,
        [
          ResolutionPlugin,
          {
            defaultResolution: $alwaysLoadOriginalFile && originalPanorama ? 'original' : 'default',
            resolutions: [
              {
                id: 'default',
                label: 'Default',
                panorama,
              },
              ...(originalPanorama
                ? [
                    {
                      id: 'original',
                      label: 'Original',
                      panorama: originalPanorama,
                    },
                  ]
                : []),
            ],
          },
        ],
        ...plugins,
      ],
      container,
      touchmoveTwoFingers: false,
      mousewheelCtrlKey: false,
      navbar,
      minFov: 15,
      maxFov: 90,
      zoomSpeed: 0.5,
      fisheye: false,
    });
    const resolutionPlugin = viewer.getPlugin<ResolutionPlugin>(ResolutionPlugin);
    const zoomHandler = ({ zoomLevel }: events.ZoomUpdatedEvent) => {
      // zoomLevel is 0-100
      assetViewerManager.zoom = zoomLevel / 50;

      if (Math.round(zoomLevel) >= 75 && !hasChangedResolution) {
        // Replace the preview with the original
        void resolutionPlugin.setResolution('original');
        hasChangedResolution = true;
      }
    };

    if (originalPanorama && !$alwaysLoadOriginalFile) {
      viewer.addEventListener(events.ZoomUpdatedEvent.type, zoomHandler, { passive: true });
    }

    return () => {
      viewer.removeEventListener(events.ZoomUpdatedEvent.type, zoomHandler);
    };
  });

  onDestroy(() => {
    if (viewer) {
      viewer.destroy();
    }
    assetViewerManager.zoom = 1;
  });
</script>

<AssetViewerEvents {onZoom} />

<svelte:document use:shortcuts={[{ shortcut: { key: 'z' }, onShortcut: onZoom, preventDefault: true }]} />
<div class="mb-0 size-full" bind:this={container}></div>
