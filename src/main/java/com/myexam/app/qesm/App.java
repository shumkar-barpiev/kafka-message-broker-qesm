package com.myexam.app.qesm;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.myexam.app.qesm.analysis.TransientSimulation;
import com.myexam.app.qesm.analysis.TransientSimulation.RewardType;
import com.myexam.app.qesm.analysis.TransientSimulation.Result;

/** Runs the transient experiments defined in the project report. */
public final class App {

	private static final int BASELINE_BATCH_SIZE = 20;
	private static final String BASELINE_TIMEOUT = "25";
	private static final int OVERHEAD = 2;
	private static final int STABILITY = 30;

	private static final String TIME_STEP = "0.1";
	private static final String TIME_HORIZON = "250";
	private static final long RUNS = 1L;
	private static final Path OUTCOMES_DIRECTORY = Paths.get("outcomes");

	private App() {
	}

	public static void main(String[] args) {
		// Baseline: BatchSize=20, Timeout=25, Overhead=2, Stability=30.
		runExperiment("baseline_queue_behaviour", BASELINE_BATCH_SIZE, BASELINE_TIMEOUT, STABILITY,
				RewardType.NORMAL_TRANSIENT);

		// BatchSize study: change only BatchSize. The baseline covers 20.
		runExperiment("batch_size_10_backlog", 10, BASELINE_TIMEOUT, STABILITY, RewardType.NORMAL_TRANSIENT);
		runExperiment("batch_size_30_backlog", 30, BASELINE_TIMEOUT, STABILITY, RewardType.NORMAL_TRANSIENT);

		// Timeout study: change only Timeout. The baseline covers 25.
		runExperiment("timeout_12_5_backlog", BASELINE_BATCH_SIZE, "12.5", STABILITY, RewardType.NORMAL_TRANSIENT);
		runExperiment("timeout_35_backlog", BASELINE_BATCH_SIZE, "35", STABILITY, RewardType.NORMAL_TRANSIENT);

		// Batching-induced idle study: use the cumulative watcher rewards.
		runExperiment("batching_induced_idle", BASELINE_BATCH_SIZE, BASELINE_TIMEOUT, STABILITY,
				RewardType.CUMULATIVE_WATCHER);
	}

	private static void runExperiment(String experimentName, int batchSize, String timeout, int stability,
			RewardType rewardType) {
		TransientSimulation simulation = new TransientSimulation(batchSize, new BigDecimal(timeout), OVERHEAD,
				stability, new BigDecimal(TIME_STEP), new BigDecimal(TIME_HORIZON), RUNS, rewardType);
		Result result = simulation.run();
		Path outputFile = OUTCOMES_DIRECTORY.resolve(experimentName + ".csv");
		result.writeCsv(outputFile);
		System.out.println("Saved " + experimentName + " to " + outputFile.toAbsolutePath());
	}
}
