package com.myexam.app.qesm;

import com.myexam.app.qesm.analysis.TransientSimulation;
import com.myexam.app.qesm.analysis.TransientSimulation.RewardType;

/** Runs the transient experiments defined in the project report. */
public final class App {

	private static final int BASELINE_BATCH_SIZE = 20;
	private static final String BASELINE_TIMEOUT = "25";
	private static final int OVERHEAD = 2;
	private static final int STABILITY = 30;

	private static final String TIME_STEP = "0.1";
	private static final String TIME_HORIZON = "250";
	private static final String RUNS = "1";

	private App() {
	}

	public static void main(String[] args) {
		// Baseline: BatchSize=20, Timeout=25, Overhead=2, Stability=30.
		runExperiment(BASELINE_BATCH_SIZE, BASELINE_TIMEOUT, STABILITY, RewardType.NORMAL_TRANSIENT);

		// BatchSize study: change only BatchSize. The baseline covers 20.
		runExperiment(10, BASELINE_TIMEOUT, STABILITY, RewardType.NORMAL_TRANSIENT);
		runExperiment(30, BASELINE_TIMEOUT, STABILITY, RewardType.NORMAL_TRANSIENT);

		// Timeout study: change only Timeout. The baseline covers 25.
		runExperiment(BASELINE_BATCH_SIZE, "12.5", STABILITY, RewardType.NORMAL_TRANSIENT);
		runExperiment(BASELINE_BATCH_SIZE, "35", STABILITY, RewardType.NORMAL_TRANSIENT);

		// Batching-induced idle study: use the cumulative watcher rewards.
		runExperiment(BASELINE_BATCH_SIZE, BASELINE_TIMEOUT, STABILITY, RewardType.CUMULATIVE_WATCHER);
	}

	private static void runExperiment(int batchSize, String timeout, int stability, RewardType rewardType) {
		TransientSimulation.main(new String[] { Integer.toString(batchSize), timeout, Integer.toString(OVERHEAD),
				Integer.toString(stability), TIME_STEP, TIME_HORIZON, RUNS, rewardType.name() });
	}
}
