package com.myexam.app.qesm.analysis;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collection;

import com.myexam.app.qesm.model.KafkaBrokerModel;
import com.myexam.app.qesm.model.KafkaBrokerModel.BuiltModel;
import com.myexam.app.qesm.model.KafkaBrokerModel.Parameters;

import org.oristool.analyzer.log.NoOpLogger;
import org.oristool.petrinet.Marking;
import org.oristool.petrinet.MarkingCondition;
import org.oristool.simulator.Sequencer;
import org.oristool.simulator.TimeSeriesRewardResult;
import org.oristool.simulator.rewards.ContinuousRewardTime;
import org.oristool.simulator.rewards.RewardEvaluator;
import org.oristool.simulator.stpn.STPNSimulatorComponentsFactory;
import org.oristool.simulator.stpn.TransientMarkingConditionProbability;

/**
 * Runs one transient simulation experiment for a Kafka broker configuration.
 */
public final class TransientSimulation {

	public enum RewardType {
		NORMAL_TRANSIENT, CUMULATIVE_WATCHER
	}

	private static final BigDecimal ARRIVAL_RATE = BigDecimal.ONE;
	private static final BigDecimal DEFAULT_TIME_STEP = new BigDecimal("0.1");
	private static final BigDecimal DEFAULT_TIME_HORIZON = new BigDecimal("250");
	private static final long DEFAULT_RUNS = 1L;

	private final int batchSize;
	private final BigDecimal timeout;
	private final int overhead;
	private final int stability;
	private final BigDecimal timeStep;
	private final BigDecimal timeHorizon;
	private final long runs;
	private final RewardType rewardType;

	public TransientSimulation(int batchSize, BigDecimal timeout, int overhead, int stability, BigDecimal timeStep,
			BigDecimal timeHorizon, long runs, RewardType rewardType) {
		this.batchSize = batchSize;
		this.timeout = requirePositive(timeout, "timeout");
		this.overhead = overhead;
		this.stability = stability;
		this.timeStep = requirePositive(timeStep, "timeStep");
		this.timeHorizon = requirePositive(timeHorizon, "timeHorizon");
		if (runs <= 0) {
			throw new IllegalArgumentException("runs must be greater than zero");
		}
		this.runs = runs;
		if (rewardType == null) {
			throw new IllegalArgumentException("rewardType must not be null");
		}
		this.rewardType = rewardType;

		parameters();
		sampleCount();
	}

	/** Runs the configured experiment and returns its transient mean curves. */
	public Result run() {
		BuiltModel model = KafkaBrokerModel.create(parameters());
		Sequencer sequencer = new Sequencer(model.getNet(), model.getInitialMarking(),
				new STPNSimulatorComponentsFactory(), NoOpLogger.INSTANCE);

		TransientMarkingConditionProbability reward = new TransientMarkingConditionProbability(sequencer,
				new ContinuousRewardTime(timeStep), sampleCount(), MarkingCondition.ANY);
		RewardEvaluator evaluator = new RewardEvaluator(reward, runs);

		sequencer.simulate();

		TimeSeriesRewardResult distribution = (TimeSeriesRewardResult) evaluator.getResult();
		if (distribution == null) {
			throw new IllegalStateException("The transient simulation did not produce a result");
		}

		switch (rewardType) {
		case NORMAL_TRANSIENT:
			return aggregateNormalTransient(distribution);
		case CUMULATIVE_WATCHER:
			return aggregateCumulativeWatcher(distribution);
		default:
			throw new IllegalStateException("Unsupported reward type: " + rewardType);
		}
	}

	private Parameters parameters() {
		return new Parameters(ARRIVAL_RATE, batchSize, timeout, overhead, stability);
	}

	private int sampleCount() {
		BigDecimal[] quotientAndRemainder = timeHorizon.divideAndRemainder(timeStep);
		if (quotientAndRemainder[1].compareTo(BigDecimal.ZERO) != 0) {
			throw new IllegalArgumentException("timeHorizon must be an exact multiple of timeStep");
		}

		try {
			return quotientAndRemainder[0].intValueExact() + 1;
		} catch (ArithmeticException exception) {
			throw new IllegalArgumentException("The requested number of samples is too large", exception);
		}
	}

	private Result aggregateNormalTransient(TimeSeriesRewardResult distribution) {
		int samples = sampleCount();
		BigDecimal[] gatewayMean = zeroArray(samples);
		BigDecimal[] serviceMean = zeroArray(samples);
		Collection<Marking> markings = distribution.getMarkings();

		for (Marking marking : markings) {
			BigDecimal[] probabilities = distribution.getTimeSeries(marking);
			BigDecimal gatewayTokens = BigDecimal.valueOf(marking.getTokens(KafkaBrokerModel.MSGS_AT_GATEWAY));
			BigDecimal serviceTokens = BigDecimal.valueOf(marking.getTokens(KafkaBrokerModel.AT_SERVICE));
			for (int sample = 0; sample < samples; sample++) {
				BigDecimal probability = probabilities[sample];
				gatewayMean[sample] = gatewayMean[sample]
						.add(probability.multiply(gatewayTokens, MathContext.DECIMAL128));
				serviceMean[sample] = serviceMean[sample]
						.add(probability.multiply(serviceTokens, MathContext.DECIMAL128));
			}
		}

		return Result.normalTransient(timeStep, gatewayMean, serviceMean);
	}

	private Result aggregateCumulativeWatcher(TimeSeriesRewardResult distribution) {
		int samples = sampleCount();
		BigDecimal[] emptyRewardRate = zeroArray(samples);
		BigDecimal[] batchingIdleRewardRate = zeroArray(samples);

		for (Marking marking : distribution.getMarkings()) {
			BigDecimal[] probabilities = distribution.getTimeSeries(marking);
			BigDecimal emptyValue = BigDecimal.valueOf(marking.getTokens(KafkaBrokerModel.EMPTY));
			boolean batchingIdle = marking.getTokens(KafkaBrokerModel.IDLE) == 1
					&& marking.getTokens(KafkaBrokerModel.NOT_EMPTY) == 1;

			for (int sample = 0; sample < samples; sample++) {
				BigDecimal probability = probabilities[sample];
				emptyRewardRate[sample] = emptyRewardRate[sample]
						.add(probability.multiply(emptyValue, MathContext.DECIMAL128));
				if (batchingIdle) {
					batchingIdleRewardRate[sample] = batchingIdleRewardRate[sample]
							.add(probability.multiply(new BigDecimal("2"), MathContext.DECIMAL128));
				}
			}
		}

		BigDecimal[] cumulativeEmpty = cumulativeIntegral(emptyRewardRate);
		BigDecimal[] cumulativeBatchingIdleReward = cumulativeIntegral(batchingIdleRewardRate);
		BigDecimal[] batchingIdleTime = zeroArray(samples);
		BigDecimal[] batchingIdleFraction = zeroArray(samples);
		BigDecimal elapsedTime = BigDecimal.ZERO;

		for (int sample = 1; sample < samples; sample++) {
			elapsedTime = elapsedTime.add(timeStep);
			batchingIdleTime[sample] = cumulativeBatchingIdleReward[sample].divide(new BigDecimal("2"),
					MathContext.DECIMAL128);
			batchingIdleFraction[sample] = batchingIdleTime[sample].divide(elapsedTime, MathContext.DECIMAL128);
		}

		return Result.cumulativeWatcher(timeStep, cumulativeEmpty, cumulativeBatchingIdleReward, batchingIdleTime,
				batchingIdleFraction);
	}

	private BigDecimal[] cumulativeIntegral(BigDecimal[] rewardRate) {
		BigDecimal[] cumulative = zeroArray(rewardRate.length);
		BigDecimal two = new BigDecimal("2");

		for (int sample = 1; sample < rewardRate.length; sample++) {
			BigDecimal intervalReward = rewardRate[sample - 1].add(rewardRate[sample])
					.multiply(timeStep, MathContext.DECIMAL128).divide(two, MathContext.DECIMAL128);
			cumulative[sample] = cumulative[sample - 1].add(intervalReward);
		}

		return cumulative;
	}

	private static BigDecimal[] zeroArray(int size) {
		BigDecimal[] values = new BigDecimal[size];
		for (int i = 0; i < size; i++) {
			values[i] = BigDecimal.ZERO;
		}
		return values;
	}

	private static BigDecimal requirePositive(BigDecimal value, String name) {
		if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
			throw new IllegalArgumentException(name + " must be greater than zero");
		}
		return value;
	}

	/**
	 * Runs the report baseline when no arguments are supplied.
	 *
	 * <p>
	 * Custom usage:
	 * </p>
	 * 
	 * <pre>
	 * TransientSimulation BatchSize Timeout Overhead Stability [Step] [Horizon] [Runs] [RewardType]
	 * </pre>
	 */
	public static void main(String[] args) {
		if (args.length != 0 && (args.length < 4 || args.length > 8)) {
			throw new IllegalArgumentException("Usage: TransientSimulation BatchSize Timeout Overhead Stability "
					+ "[Step] [Horizon] [Runs] [NORMAL_TRANSIENT|CUMULATIVE_WATCHER]");
		}

		int batchSize = args.length == 0 ? 20 : Integer.parseInt(args[0]);
		BigDecimal timeout = args.length == 0 ? new BigDecimal("25") : new BigDecimal(args[1]);
		int overhead = args.length == 0 ? 2 : Integer.parseInt(args[2]);
		int stability = args.length == 0 ? 30 : Integer.parseInt(args[3]);
		BigDecimal step = args.length >= 5 ? new BigDecimal(args[4]) : DEFAULT_TIME_STEP;
		BigDecimal horizon = args.length >= 6 ? new BigDecimal(args[5]) : DEFAULT_TIME_HORIZON;
		long runs = args.length >= 7 ? Long.parseLong(args[6]) : DEFAULT_RUNS;
		RewardType rewardType = args.length >= 8 ? RewardType.valueOf(args[7]) : RewardType.NORMAL_TRANSIENT;

		TransientSimulation simulation = new TransientSimulation(batchSize, timeout, overhead, stability, step, horizon,
				runs, rewardType);
		Result result = simulation.run();

		System.out.println("# ArrivalRate=" + ARRIVAL_RATE + ", BatchSize=" + batchSize + ", Timeout=" + timeout
				+ ", Overhead=" + overhead + ", Stability=" + stability + ", Step=" + step + ", Horizon=" + horizon
				+ ", Runs=" + runs + ", RewardType=" + rewardType);
		result.printCsv();
	}

	/** Reward values at each configured time sample. */
	public static final class Result {
		private final RewardType rewardType;
		private final BigDecimal timeStep;
		private final BigDecimal[] gatewayMean;
		private final BigDecimal[] serviceMean;
		private final BigDecimal[] cumulativeEmpty;
		private final BigDecimal[] cumulativeBatchingIdleReward;
		private final BigDecimal[] batchingIdleTime;
		private final BigDecimal[] batchingIdleFraction;

		private Result(RewardType rewardType, BigDecimal timeStep, BigDecimal[] gatewayMean, BigDecimal[] serviceMean,
				BigDecimal[] cumulativeEmpty, BigDecimal[] cumulativeBatchingIdleReward, BigDecimal[] batchingIdleTime,
				BigDecimal[] batchingIdleFraction) {
			this.rewardType = rewardType;
			this.timeStep = timeStep;
			this.gatewayMean = gatewayMean;
			this.serviceMean = serviceMean;
			this.cumulativeEmpty = cumulativeEmpty;
			this.cumulativeBatchingIdleReward = cumulativeBatchingIdleReward;
			this.batchingIdleTime = batchingIdleTime;
			this.batchingIdleFraction = batchingIdleFraction;
		}

		private static Result normalTransient(BigDecimal timeStep, BigDecimal[] gatewayMean, BigDecimal[] serviceMean) {
			return new Result(RewardType.NORMAL_TRANSIENT, timeStep, gatewayMean, serviceMean, null, null, null, null);
		}

		private static Result cumulativeWatcher(BigDecimal timeStep, BigDecimal[] cumulativeEmpty,
				BigDecimal[] cumulativeBatchingIdleReward, BigDecimal[] batchingIdleTime,
				BigDecimal[] batchingIdleFraction) {
			return new Result(RewardType.CUMULATIVE_WATCHER, timeStep, null, null, cumulativeEmpty,
					cumulativeBatchingIdleReward, batchingIdleTime, batchingIdleFraction);
		}

		public RewardType getRewardType() {
			return rewardType;
		}

		public BigDecimal getTimeStep() {
			return timeStep;
		}

		public BigDecimal[] getGatewayMean() {
			return gatewayMean.clone();
		}

		public BigDecimal[] getServiceMean() {
			return serviceMean.clone();
		}

		public BigDecimal[] getCumulativeEmpty() {
			return cumulativeEmpty.clone();
		}

		public BigDecimal[] getCumulativeBatchingIdleReward() {
			return cumulativeBatchingIdleReward.clone();
		}

		public BigDecimal[] getBatchingIdleTime() {
			return batchingIdleTime.clone();
		}

		public BigDecimal[] getBatchingIdleFraction() {
			return batchingIdleFraction.clone();
		}

		public void printCsv() {
			if (rewardType == RewardType.NORMAL_TRANSIENT) {
				printNormalTransientCsv();
			} else {
				printCumulativeWatcherCsv();
			}
		}

		private void printNormalTransientCsv() {
			System.out.println("time,meanMsgsAtGateway,meanAtService");
			BigDecimal time = BigDecimal.ZERO;
			for (int sample = 0; sample < gatewayMean.length; sample++) {
				System.out.println(time.toPlainString() + "," + gatewayMean[sample].toPlainString() + ","
						+ serviceMean[sample].toPlainString());
				time = time.add(timeStep);
			}
		}

		private void printCumulativeWatcherCsv() {
			System.out
					.println("time,cumulativeEmpty,cumulativeBatchingIdleReward,batchingIdleTime,batchingIdleFraction");
			BigDecimal time = BigDecimal.ZERO;
			for (int sample = 0; sample < cumulativeEmpty.length; sample++) {
				System.out.println(time.toPlainString() + "," + cumulativeEmpty[sample].toPlainString() + ","
						+ cumulativeBatchingIdleReward[sample].toPlainString() + ","
						+ batchingIdleTime[sample].toPlainString() + ","
						+ batchingIdleFraction[sample].toPlainString());
				time = time.add(timeStep);
			}
		}
	}
}
