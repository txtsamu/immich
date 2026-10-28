import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import {
  PluginMethodResponseDto,
  PluginMethodSearchDto,
  PluginResponseDto,
  PluginSearchDto,
  PluginTemplateResponseDto,
  mapMethod,
  mapPlugin,
  mapTemplate,
} from 'src/dtos/plugin.dto.js';
import { runPromise } from 'src/effect/run.js';
import { BaseService } from 'src/services/base.service.js';
import { isMethodCompatible } from 'src/utils/workflow.js';

@Injectable()
export class PluginService extends BaseService {
  async search(dto: PluginSearchDto): Promise<PluginResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const plugins = yield* this.fx.plugin.search(dto);
        return plugins.map((plugin) => mapPlugin(plugin));
      }),
    );
  }

  async get(id: string): Promise<PluginResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const plugin = yield* this.fx.plugin.get(id);
        if (!plugin) {
          return yield* Effect.fail(new BadRequestException('Plugin not found'));
        }
        return mapPlugin(plugin);
      }),
    );
  }

  async searchMethods(dto: PluginMethodSearchDto): Promise<PluginMethodResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const methods = yield* this.fx.plugin.searchMethods(dto);
        return methods
          .filter((method) => !dto.trigger || isMethodCompatible(method, dto.trigger))
          .map((method) => mapMethod(method));
      }),
    );
  }

  async searchTemplates(): Promise<PluginTemplateResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const plugins = yield* this.fx.plugin.search();
        return plugins.flatMap((plugin) => plugin.templates.map((template) => mapTemplate(plugin, template)));
      }),
    );
  }
}
