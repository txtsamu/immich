import { WorkflowStepConfig, WorkflowTrigger } from '@immich/plugin-sdk';
import { BadRequestException, Injectable } from '@nestjs/common';
import { Effect } from 'effect';
import { AuthDto } from 'src/dtos/auth.dto.js';
import {
  WorkflowCreateDto,
  WorkflowGetLogsDto,
  WorkflowLogEntryDto,
  WorkflowResponseDto,
  WorkflowSearchDto,
  WorkflowShareResponseDto,
  WorkflowTriggerResponseDto,
  WorkflowUpdateDto,
  mapWorkflow,
  mapWorkflowShare,
} from 'src/dtos/workflow.dto.js';
import { runPromise } from 'src/effect/run.js';
import { Permission } from 'src/enum.js';
import { PluginMethodSearchResponse } from 'src/repositories/plugin.repository.js';
import { BaseService } from 'src/services/base.service.js';
import { getWorkflowTriggers, isMethodCompatible, resolveMethod } from 'src/utils/workflow.js';

@Injectable()
export class WorkflowService extends BaseService {
  getTriggers(): WorkflowTriggerResponseDto[] {
    return getWorkflowTriggers();
  }

  async search(auth: AuthDto, dto: WorkflowSearchDto): Promise<WorkflowResponseDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const workflows = yield* this.fx.workflow.search({ ...dto, userId: auth.user.id });
        return workflows.map((workflow) => mapWorkflow(workflow));
      }),
    );
  }

  async get(auth: AuthDto, id: string): Promise<WorkflowResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.WorkflowRead, ids: [id] });
        const workflow = yield* this.findOrFail(id);
        return mapWorkflow(workflow);
      }),
    );
  }

  async share(auth: AuthDto, id: string): Promise<WorkflowShareResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.WorkflowRead, ids: [id] });
        const workflow = yield* this.findOrFail(id);
        return mapWorkflowShare(workflow);
      }),
    );
  }

  async create(auth: AuthDto, dto: WorkflowCreateDto): Promise<WorkflowResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        const { steps: stepsDto, ...workflowDto } = dto;
        const steps = yield* this.resolveAndValidateSteps(stepsDto ?? [], workflowDto.trigger);

        const workflow = yield* this.fx.workflow.create(
          {
            ...workflowDto,
            ownerId: auth.user.id,
          },
          steps.map((step) => ({
            enabled: step.enabled ?? true,
            config: step.config as WorkflowStepConfig,
            pluginMethodId: step.pluginMethod.id,
          })),
        );

        return mapWorkflow({ ...workflow, steps: [] });
      }),
    );
  }

  async update(auth: AuthDto, id: string, dto: WorkflowUpdateDto): Promise<WorkflowResponseDto> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.WorkflowUpdate, ids: [id] });

        const { steps: stepsDto, ...workflowDto } = dto;
        const current = yield* this.findOrFail(id);
        const steps = stepsDto
          ? yield* this.resolveAndValidateSteps(stepsDto, dto.trigger ?? current.trigger)
          : undefined;
        const workflow = yield* this.fx.workflow.update(
          id,
          workflowDto,
          steps?.map((step) => ({
            enabled: step.enabled ?? true,
            config: step.config as WorkflowStepConfig,
            pluginMethodId: step.pluginMethod.id,
          })),
        );

        return mapWorkflow(workflow);
      }),
    );
  }

  async delete(auth: AuthDto, id: string): Promise<void> {
    await runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.WorkflowDelete, ids: [id] });
        yield* this.fx.workflow.delete(id);
      }),
    );
  }

  async getLogs(auth: AuthDto, id: string, dto: WorkflowGetLogsDto): Promise<WorkflowLogEntryDto[]> {
    return runPromise(
      Effect.gen({ self: this }, function* () {
        yield* this.requireAccessFx({ auth, permission: Permission.WorkflowLogs, ids: [id] });
        const logs = yield* this.fx.workflow.getLogs(id, dto);
        return logs.map((entry) => ({
          id: entry.id,
          at: entry.createdAt,
          result: entry.result,
          triggerDataId: entry.triggerDataId ?? undefined,
          lastStep: entry.step
            ? {
                index: entry.step.order,
                method: `${entry.step.pluginId}#${entry.step.methodName}`,
              }
            : undefined,
        }));
      }),
    );
  }

  private resolveAndValidateSteps = Effect.fn('WorkflowService.resolveAndValidateSteps')(function* <
    T extends { method: string },
  >(this: WorkflowService, steps: T[], trigger: WorkflowTrigger) {
    const methods = yield* this.fx.plugin.getForValidation();
    const results: Array<T & { pluginMethod: PluginMethodSearchResponse }> = [];

    for (const step of steps) {
      const pluginMethod = resolveMethod(methods, step.method);
      if (!pluginMethod) {
        return yield* Effect.fail(new BadRequestException(`Unknown method ${step.method}`));
      }

      if (!isMethodCompatible(pluginMethod, trigger)) {
        return yield* Effect.fail(
          new BadRequestException(`Method "${step.method}" is incompatible with workflow trigger: "${trigger}"`),
        );
      }

      results.push({ ...step, pluginMethod });
    }

    // TODO make sure all steps can use a common WorkflowType

    return results;
  });

  private findOrFail = Effect.fn('WorkflowService.findOrFail')(function* (this: WorkflowService, id: string) {
    const workflow = yield* this.fx.workflow.get(id);
    if (!workflow) {
      return yield* Effect.fail(new BadRequestException('Workflow not found'));
    }
    return workflow;
  });
}
