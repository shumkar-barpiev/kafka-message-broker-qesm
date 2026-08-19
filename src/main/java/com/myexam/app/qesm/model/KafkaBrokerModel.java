package com.myexam.app.qesm.model;

import java.math.BigDecimal;
import java.util.Objects;

import org.oristool.models.pn.PostUpdater;
import org.oristool.models.pn.Priority;
import org.oristool.models.pn.ResetSet;
import org.oristool.models.stpn.MarkingExpr;
import org.oristool.models.stpn.trees.StochasticTransitionFeature;
import org.oristool.petrinet.EnablingFunction;
import org.oristool.petrinet.Marking;
import org.oristool.petrinet.PetriNet;
import org.oristool.petrinet.Place;
import org.oristool.petrinet.Transition;

/**
 * Builds the STPN used to study Kafka-style message batching.
 *
 * <p>
 * The timeout is enabled only while the gateway contains messages. Its clock
 * therefore starts when the first message enters an empty gateway and is reset
 * when a full batch is pushed.
 * </p>
 */
public final class KafkaBrokerModel {

	public static final String AT_SERVICE = "AtService";
	public static final String BATCH_SIZE = "BatchSize";
	public static final String EMPTY = "Empty";
	public static final String IDLE = "Idle";
	public static final String MSGS_AT_GATEWAY = "MsgsAtGateway";
	public static final String NOT_EMPTY = "NotEmpty";
	public static final String NOT_IDLE = "NotIdle";
	public static final String OVERHEAD = "Overhead";
	public static final String STABILITY = "Stability";

	public static final String MSG_ARRIVAL = "msgArrival";
	public static final String PUSH_AT_BATCH_SIZE = "pushAtBatchSize";
	public static final String PUSH_AT_TIMEOUT = "pushAtTimeout";
	public static final String SERVICE = "service";
	public static final String SERVICE_BECOMES_BUSY = "t0";
	public static final String SERVICE_BECOMES_IDLE = "t1";
	public static final String GATEWAY_BECOMES_NON_EMPTY = "t2";
	public static final String GATEWAY_BECOMES_EMPTY = "t3";

	public static final String TRANSIENT_REWARD_EXPRESSION = MSGS_AT_GATEWAY + ";" + AT_SERVICE;
	public static final String BATCHING_IDLE_REWARD_EXPRESSION = EMPTY + ";If(" + IDLE + "==1&&" + NOT_EMPTY
			+ "==1,2,0)";

	private KafkaBrokerModel() {
	}

	/** Backwards-compatible entry point using the report's baseline parameters. */
	public static void build(PetriNet net, Marking marking) {
		build(net, marking, Parameters.baseline());
	}

	/** Creates a new net and initial marking with the supplied parameters. */
	public static BuiltModel create(Parameters parameters) {
		return build(new PetriNet(), new Marking(), parameters);
	}

	/** Populates a net and marking and returns the complete model instance. */
	public static BuiltModel build(PetriNet net, Marking marking, Parameters parameters) {
		Objects.requireNonNull(net, "net");
		Objects.requireNonNull(marking, "marking");
		Objects.requireNonNull(parameters, "parameters");

		Place atService = net.addPlace(AT_SERVICE);
		Place batchSize = net.addPlace(BATCH_SIZE);
		Place empty = net.addPlace(EMPTY);
		Place idle = net.addPlace(IDLE);
		Place msgsAtGateway = net.addPlace(MSGS_AT_GATEWAY);
		Place notEmpty = net.addPlace(NOT_EMPTY);
		Place notIdle = net.addPlace(NOT_IDLE);
		Place overhead = net.addPlace(OVERHEAD);
		Place stability = net.addPlace(STABILITY);

		Transition msgArrival = net.addTransition(MSG_ARRIVAL);
		Transition pushAtBatchSize = net.addTransition(PUSH_AT_BATCH_SIZE);
		Transition pushAtTimeout = net.addTransition(PUSH_AT_TIMEOUT);
		Transition service = net.addTransition(SERVICE);
		Transition serviceBecomesBusy = net.addTransition(SERVICE_BECOMES_BUSY);
		Transition serviceBecomesIdle = net.addTransition(SERVICE_BECOMES_IDLE);
		Transition gatewayBecomesNonEmpty = net.addTransition(GATEWAY_BECOMES_NON_EMPTY);
		Transition gatewayBecomesEmpty = net.addTransition(GATEWAY_BECOMES_EMPTY);

		// Main batching and service flow.
		net.addPostcondition(msgArrival, msgsAtGateway);
		net.addPrecondition(atService, service);

		// Watcher self-loops and zero tests.
		net.addPrecondition(atService, serviceBecomesBusy);
		net.addPostcondition(serviceBecomesBusy, atService);
		net.addPrecondition(idle, serviceBecomesBusy);

		net.addPrecondition(notIdle, serviceBecomesIdle);
		net.addInhibitorArc(atService, serviceBecomesIdle);

		net.addPrecondition(msgsAtGateway, gatewayBecomesNonEmpty);
		net.addPostcondition(gatewayBecomesNonEmpty, msgsAtGateway);
		net.addPrecondition(empty, gatewayBecomesNonEmpty);

		net.addPrecondition(notEmpty, gatewayBecomesEmpty);
		net.addInhibitorArc(msgsAtGateway, gatewayBecomesEmpty);

		marking.setTokens(atService, 0);
		marking.setTokens(batchSize, parameters.getBatchSize());
		marking.setTokens(empty, 1);
		marking.setTokens(idle, 1);
		marking.setTokens(msgsAtGateway, 0);
		marking.setTokens(notEmpty, 0);
		marking.setTokens(notIdle, 0);
		marking.setTokens(overhead, parameters.getOverhead());
		marking.setTokens(stability, parameters.getStability());

		msgArrival.addFeature(
				StochasticTransitionFeature.newExponentialInstance(parameters.getArrivalRate(), MarkingExpr.ONE));

		pushAtBatchSize.addFeature(new EnablingFunction(MSGS_AT_GATEWAY + ">=" + BATCH_SIZE));
		pushAtBatchSize.addFeature(batchPushUpdater(net));
		pushAtBatchSize.addFeature(new ResetSet(pushAtTimeout));
		makeImmediate(pushAtBatchSize);

		// Prevent empty timeout pushes and start the timeout on the first message.
		pushAtTimeout.addFeature(new EnablingFunction(MSGS_AT_GATEWAY + ">0"));
		pushAtTimeout.addFeature(batchPushUpdater(net));
		pushAtTimeout.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(parameters.getTimeout(), MarkingExpr.ONE));
		pushAtTimeout.addFeature(new Priority(0));

		// The effective rate is arrivalRate * (1 + Stability/100).
		service.addFeature(StochasticTransitionFeature.newExponentialInstance(parameters.getArrivalRate(),
				MarkingExpr.from("1+" + STABILITY + "/100.0", net)));

		serviceBecomesBusy.addFeature(new EnablingFunction(IDLE + "==1 && " + AT_SERVICE + ">0"));
		serviceBecomesBusy.addFeature(new PostUpdater(IDLE + "=0;" + NOT_IDLE + "=1", net));
		makeImmediate(serviceBecomesBusy);

		serviceBecomesIdle.addFeature(new EnablingFunction(NOT_IDLE + "==1 && " + AT_SERVICE + "==0"));
		serviceBecomesIdle.addFeature(new PostUpdater(IDLE + "=1;" + NOT_IDLE + "=0", net));
		makeImmediate(serviceBecomesIdle);

		gatewayBecomesNonEmpty.addFeature(new EnablingFunction(EMPTY + "==1 && " + MSGS_AT_GATEWAY + ">0"));
		gatewayBecomesNonEmpty.addFeature(new PostUpdater(EMPTY + "=0;" + NOT_EMPTY + "=1", net));
		makeImmediate(gatewayBecomesNonEmpty);

		gatewayBecomesEmpty.addFeature(new EnablingFunction(NOT_EMPTY + "==1 && " + MSGS_AT_GATEWAY + "==0"));
		gatewayBecomesEmpty.addFeature(new PostUpdater(EMPTY + "=1;" + NOT_EMPTY + "=0", net));
		makeImmediate(gatewayBecomesEmpty);

		return new BuiltModel(net, marking, parameters);
	}

	private static PostUpdater batchPushUpdater(PetriNet net) {
		return new PostUpdater(
				AT_SERVICE + "=" + AT_SERVICE + "+" + MSGS_AT_GATEWAY + "+" + OVERHEAD + ";" + MSGS_AT_GATEWAY + "=0",
				net);
	}

	private static void makeImmediate(Transition transition) {
		transition.addFeature(StochasticTransitionFeature.newDeterministicInstance(BigDecimal.ZERO, MarkingExpr.ONE));
		transition.addFeature(new Priority(0));
	}

	/** Immutable parameters for one model configuration. */
	public static final class Parameters {
		private final BigDecimal arrivalRate;
		private final int batchSize;
		private final BigDecimal timeout;
		private final int overhead;
		private final int stability;

		public Parameters(BigDecimal arrivalRate, int batchSize, BigDecimal timeout, int overhead, int stability) {
			this.arrivalRate = requirePositive(arrivalRate, "arrivalRate");
			if (batchSize <= 0) {
				throw new IllegalArgumentException("batchSize must be greater than zero");
			}
			this.batchSize = batchSize;
			this.timeout = requirePositive(timeout, "timeout");
			if (overhead < 0) {
				throw new IllegalArgumentException("overhead must not be negative");
			}
			this.overhead = overhead;
			if (stability < 0) {
				throw new IllegalArgumentException("stability must not be negative");
			}
			this.stability = stability;
		}

		public static Parameters baseline() {
			return new Parameters(BigDecimal.ONE, 20, new BigDecimal("25"), 2, 30);
		}

		private static BigDecimal requirePositive(BigDecimal value, String name) {
			Objects.requireNonNull(value, name);
			if (value.compareTo(BigDecimal.ZERO) <= 0) {
				throw new IllegalArgumentException(name + " must be greater than zero");
			}
			return value;
		}

		public BigDecimal getArrivalRate() {
			return arrivalRate;
		}

		public int getBatchSize() {
			return batchSize;
		}

		public BigDecimal getTimeout() {
			return timeout;
		}

		public int getOverhead() {
			return overhead;
		}

		public int getStability() {
			return stability;
		}

		public double getServiceRate() {
			return arrivalRate.doubleValue() * (1.0 + stability / 100.0);
		}

		public Parameters withBatchSize(int value) {
			return new Parameters(arrivalRate, value, timeout, overhead, stability);
		}

		public Parameters withTimeout(BigDecimal value) {
			return new Parameters(arrivalRate, batchSize, value, overhead, stability);
		}

		public Parameters withStability(int value) {
			return new Parameters(arrivalRate, batchSize, timeout, overhead, value);
		}
	}

	/** A complete model instance ready to be passed to Sirio analyses. */
	public static final class BuiltModel {
		private final PetriNet net;
		private final Marking initialMarking;
		private final Parameters parameters;

		private BuiltModel(PetriNet net, Marking initialMarking, Parameters parameters) {
			this.net = net;
			this.initialMarking = initialMarking;
			this.parameters = parameters;
		}

		public PetriNet getNet() {
			return net;
		}

		public Marking getInitialMarking() {
			return initialMarking;
		}

		public Parameters getParameters() {
			return parameters;
		}

		public Place getPlace(String name) {
			return net.getPlace(name);
		}

		public Transition getTransition(String name) {
			return net.getTransition(name);
		}
	}
}
