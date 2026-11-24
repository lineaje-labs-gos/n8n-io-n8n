import { mockInstance } from '@n8n/backend-test-utils';
import { UnexpectedError, UserError } from '@n8n/errors';
import type { IWorkflowExecutionDataProcess } from 'n8n-workflow';
import { mock } from 'vitest-mock-extended';

import {
	type EngineV2CompletionOutcome,
	type EngineV2CompletionWatch,
	EngineV2ExecutionCompletions,
} from '@/modules/engine-v2/engine-v2-execution-completions';
import type { EngineDataPlaneProxyService } from '@/services/engine-data-plane-proxy.service';
import type { EngineV2Dispatcher } from '@/services/engine-v2-dispatcher.service';
import { EngineV2RunService } from '@/services/engine-v2-run.service';
import type { WorkflowRunner } from '@/workflow-runner';

const data = {
	executionMode: 'manual',
	workflowData: { id: 'wf-1' },
} as IWorkflowExecutionDataProcess;
const options = { timeoutMs: 1_000 };

describe('EngineV2RunService', () => {
	const runner = mock<WorkflowRunner>();
	const dispatcher = mock<EngineV2Dispatcher>();
	const proxy = mock<EngineDataPlaneProxyService>();
	const completions = mockInstance(EngineV2ExecutionCompletions);
	const watch = mock<EngineV2CompletionWatch>();
	const runs = new EngineV2RunService(runner, dispatcher, proxy);

	/** The id the run got: the one `watch` was called with. */
	const watchedId = () => completions.watch.mock.calls[0][0];

	beforeEach(() => {
		vi.clearAllMocks();
		dispatcher.routesToEngineV2.mockReturnValue(true);
		proxy.isAvailable.mockReturnValue(true);
		completions.watch.mockResolvedValue(watch);
		runner.run.mockImplementation(async ({ engineV2ExecutionId }) => engineV2ExecutionId ?? '');
		watch.wait.mockResolvedValue({ kind: 'finished', status: 'success' });
	});

	it('routes a run as the dispatcher does', () => {
		dispatcher.routesToEngineV2.mockReturnValue(false);

		expect(runs.routesToEngineV2(data)).toBe(false);
		expect(dispatcher.routesToEngineV2).toHaveBeenCalledWith(data);
	});

	it('refuses a run that does not go to engine v2 before anything starts', async () => {
		dispatcher.routesToEngineV2.mockReturnValue(false);

		await expect(runs.runAndWait(data, options)).rejects.toThrow(UnexpectedError);
		expect(completions.watch).not.toHaveBeenCalled();
		expect(runner.run).not.toHaveBeenCalled();
	});

	it('passes on the refusal of the runner, and watches nothing, when engine v2 is not available', async () => {
		proxy.isAvailable.mockReturnValue(false);
		const refusal = new UserError('Engine v2 is not available.');
		runner.run.mockRejectedValue(refusal);

		await expect(runs.runAndWait(data, options)).rejects.toBe(refusal);
		expect(completions.watch).not.toHaveBeenCalled();
		expect(runner.run).toHaveBeenCalledWith(data);
	});

	it('throws an UnexpectedError when the runner starts a run although engine v2 is not available', async () => {
		proxy.isAvailable.mockReturnValue(false);
		runner.run.mockResolvedValue('exec-1');

		await expect(runs.runAndWait(data, options)).rejects.toThrow(UnexpectedError);
		expect(completions.watch).not.toHaveBeenCalled();
	});

	it('watches the execution before it starts the run under the same id', async () => {
		await runs.runAndWait(data, options);

		expect(completions.watch.mock.invocationCallOrder[0]).toBeLessThan(
			runner.run.mock.invocationCallOrder[0],
		);
		expect(runner.run).toHaveBeenCalledWith({ ...data, engineV2ExecutionId: watchedId() });
	});

	it.each<EngineV2CompletionOutcome>([
		{ kind: 'finished', status: 'error' },
		{
			kind: 'waiting',
			wait: { at: '2026-11-24T10:00:00.000Z', awaitsRequest: true, awaitsSubExecution: false },
		},
		{ kind: 'timeout' },
		{ kind: 'aborted' },
	])('returns the id and the outcome $kind of the wait', async (outcome) => {
		watch.wait.mockResolvedValue(outcome);

		const result = await runs.runAndWait(data, options);

		expect(result).toEqual({ executionId: watchedId(), outcome });
	});

	it('passes the timeout, the abort signal and the wait predicate to the wait', async () => {
		const waitOptions = {
			timeoutMs: 42,
			abortSignal: new AbortController().signal,
			stopWhenWaiting: () => true,
		};

		await runs.runAndWait(data, waitOptions);

		expect(watch.wait).toHaveBeenCalledWith(waitOptions);
	});

	it('stops the watch after the wait ends', async () => {
		await runs.runAndWait(data, options);

		expect(watch.stop).toHaveBeenCalledTimes(1);
		expect(watch.wait.mock.invocationCallOrder[0]).toBeLessThan(
			watch.stop.mock.invocationCallOrder[0],
		);
	});

	it('stops the watch when the run does not start', async () => {
		const failure = new Error('refused');
		runner.run.mockRejectedValue(failure);

		await expect(runs.runAndWait(data, options)).rejects.toBe(failure);
		expect(watch.stop).toHaveBeenCalledTimes(1);
		expect(watch.wait).not.toHaveBeenCalled();
	});

	it('starts no run when the watch fails', async () => {
		const failure = new Error('no subscription');
		completions.watch.mockRejectedValue(failure);

		await expect(runs.runAndWait(data, options)).rejects.toBe(failure);
		expect(runner.run).not.toHaveBeenCalled();
	});

	it('refuses a run that started under a different id, and stops the watch', async () => {
		runner.run.mockResolvedValue('other-id');

		await expect(runs.runAndWait(data, options)).rejects.toThrow(UnexpectedError);
		expect(watch.wait).not.toHaveBeenCalled();
		expect(watch.stop).toHaveBeenCalledTimes(1);
	});
});
