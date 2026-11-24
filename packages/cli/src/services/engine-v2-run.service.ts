import { Container, Service } from '@n8n/di';
import { UnexpectedError } from '@n8n/errors';
import type { IWorkflowExecutionDataProcess } from 'n8n-workflow';

import { createExecutionIdV2, type ExecutionIdV2 } from '@/executions/execution-id';
import type {
	EngineV2CompletionOutcome,
	EngineV2CompletionWaitOptions,
} from '@/modules/engine-v2/engine-v2-execution-completions';
import { EngineDataPlaneProxyService } from '@/services/engine-data-plane-proxy.service';
import { EngineV2Dispatcher } from '@/services/engine-v2-dispatcher.service';
import { WorkflowRunner } from '@/workflow-runner';

/**
 * Starts engine v2 runs for control-plane code that waits for their end.
 * It is not part of {@link EngineV2Dispatcher}, because `WorkflowRunner`
 * depends on the dispatcher.
 */
@Service()
export class EngineV2RunService {
	constructor(
		private readonly runner: WorkflowRunner,
		private readonly dispatcher: EngineV2Dispatcher,
		private readonly proxy: EngineDataPlaneProxyService,
	) {}

	/** Whether a run of this data goes to engine v2. */
	routesToEngineV2(data: IWorkflowExecutionDataProcess): boolean {
		return this.dispatcher.routesToEngineV2(data);
	}

	/**
	 * Starts an engine v2 run and waits for its end.
	 *
	 * It never cancels the run and never reads the data plane. On `timeout` or
	 * `aborted`, the caller decides what to do with the run. Events can be lost,
	 * so after a `timeout` the caller reads the status from the data plane. With
	 * `stopWhenWaiting`, the wait can end with `waiting`. The run then stays
	 * paused or continues, so the caller must not treat it as ended.
	 *
	 * @throws {UnexpectedError} If the run does not go to engine v2.
	 * @throws {OperationalError} If the watch cannot subscribe to the lifecycle events.
	 * @throws The errors of `WorkflowRunner.run` when the run does not start.
	 */
	async runAndWait(
		data: IWorkflowExecutionDataProcess,
		options: EngineV2CompletionWaitOptions,
	): Promise<{ executionId: ExecutionIdV2; outcome: EngineV2CompletionOutcome }> {
		if (!this.routesToEngineV2(data)) {
			throw new UnexpectedError('Only a run that goes to engine v2 can be awaited');
		}

		if (!this.proxy.isAvailable()) {
			// The runner refuses the run, with the reason the user can act on.
			await this.runner.run(data);
			throw new UnexpectedError('Engine v2 started a run while it is not available');
		}

		// Lazily imported: the completions belong to the engine-v2 module.
		const { EngineV2ExecutionCompletions } = await import(
			'@/modules/engine-v2/engine-v2-execution-completions.js'
		);
		const executionId = createExecutionIdV2();
		const watch = await Container.get(EngineV2ExecutionCompletions).watch(executionId);
		try {
			const startedId = await this.runner.run({ ...data, engineV2ExecutionId: executionId });
			if (startedId !== executionId) {
				throw new UnexpectedError('Engine v2 started the run under a different id', {
					extra: { executionId, startedId },
				});
			}

			return { executionId, outcome: await watch.wait(options) };
		} finally {
			watch.stop();
		}
	}
}
