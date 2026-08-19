package com.myexam.app.qesm.model;

import java.math.BigDecimal;
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

public class KafkaBrokerModel {
	public static void build(PetriNet net, Marking marking) {

		// Generating Nodes
		Place AtService = net.addPlace("AtService");
		Place BatchSize = net.addPlace("BatchSize");
		Place Empty = net.addPlace("Empty");
		Place Idle = net.addPlace("Idle");
		Place MsgsAtGateway = net.addPlace("MsgsAtGateway");
		Place NotEmpty = net.addPlace("NotEmpty");
		Place NotIdle = net.addPlace("NotIdle");
		Place Overhead = net.addPlace("Overhead");
		Place Stability = net.addPlace("Stability");
		Transition msgArrival = net.addTransition("msgArrival");
		Transition pushAtBatchSize = net.addTransition("pushAtBatchSize");
		Transition pushAtTimeout = net.addTransition("pushAtTimeout");
		Transition service = net.addTransition("service");
		Transition t0 = net.addTransition("t0");
		Transition t1 = net.addTransition("t1");
		Transition t2 = net.addTransition("t2");
		Transition t3 = net.addTransition("t3");

		// Generating Connectors
		net.addInhibitorArc(MsgsAtGateway, t3);
		net.addInhibitorArc(AtService, t1);
		net.addPrecondition(AtService, t0);
		net.addPostcondition(t0, NotIdle);
		net.addPrecondition(NotIdle, t1);
		net.addPostcondition(t2, MsgsAtGateway);
		net.addPrecondition(NotEmpty, t3);
		net.addPostcondition(t0, AtService);
		net.addPrecondition(AtService, service);
		net.addPrecondition(Idle, t0);
		net.addPrecondition(Empty, t2);
		net.addPrecondition(MsgsAtGateway, t2);
		net.addPostcondition(msgArrival, MsgsAtGateway);
		net.addPostcondition(t1, Idle);
		net.addPostcondition(t2, NotEmpty);
		net.addPostcondition(t3, Empty);

		// Generating Properties
		marking.setTokens(AtService, 0);
		marking.setTokens(BatchSize, 20);
		marking.setTokens(Empty, 1);
		marking.setTokens(Idle, 1);
		marking.setTokens(MsgsAtGateway, 0);
		marking.setTokens(NotEmpty, 0);
		marking.setTokens(NotIdle, 0);
		marking.setTokens(Overhead, 2);
		marking.setTokens(Stability, 30);
		msgArrival.addFeature(
				StochasticTransitionFeature.newExponentialInstance(new BigDecimal("1"), MarkingExpr.from("1", net)));
		pushAtBatchSize.addFeature(new EnablingFunction("MsgsAtGateway>=BatchSize"));
		pushAtBatchSize.addFeature(new PostUpdater("AtService=AtService+MsgsAtGateway+Overhead; MsgsAtGateway=0", net));
		pushAtBatchSize.addFeature(new ResetSet(pushAtTimeout));
		pushAtBatchSize.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		pushAtBatchSize.addFeature(new Priority(0));
		pushAtTimeout.addFeature(new PostUpdater("AtService=AtService+MsgsAtGateway+Overhead; MsgsAtGateway=0", net));
		pushAtTimeout.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("25"), MarkingExpr.from("1", net)));
		pushAtTimeout.addFeature(new Priority(0));
		service.addFeature(StochasticTransitionFeature.newExponentialInstance(new BigDecimal("1"),
				MarkingExpr.from("1+Stability/100", net)));
		t0.addFeature(new EnablingFunction("Idle==1 && AtService>0"));
		t0.addFeature(new PostUpdater("Idle=0;NotIdle=1", net));
		t0.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		t0.addFeature(new Priority(0));
		t1.addFeature(new EnablingFunction("NotIdle==1 && AtService==0"));
		t1.addFeature(new PostUpdater("Idle=1;NotIdle=0", net));
		t1.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		t1.addFeature(new Priority(0));
		t2.addFeature(new EnablingFunction("Empty==1 && MsgsAtGateway>0"));
		t2.addFeature(new PostUpdater("Empty=0;NotEmpty=1;", net));
		t2.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		t2.addFeature(new Priority(0));
		t3.addFeature(new EnablingFunction("NotEmpty==1 && MsgsAtGateway==0"));
		t3.addFeature(new PostUpdater("Empty=1;NotEmpty=0", net));
		t3.addFeature(
				StochasticTransitionFeature.newDeterministicInstance(new BigDecimal("0"), MarkingExpr.from("1", net)));
		t3.addFeature(new Priority(0));
	}
}
