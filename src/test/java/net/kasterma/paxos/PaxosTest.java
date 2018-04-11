package net.kasterma.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActor;
import akka.testkit.javadsl.TestKit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Testing paxos using testkit.
 */
@Slf4j
class PaxosTest {
    private static ActorSystem system;

    @BeforeEach
    void setup() {
        system = ActorSystem.create();
    }

    @AfterEach
    void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    /**
     * Repeated proposals use the right sequence of proposal indices.
     */
    @Test
    void repeatedProposalIndices() {
        TestKit probe = new TestKit(system);
        List<ActorRef> acceptorList = Collections.singletonList(probe.getRef());
        GenerateProposalIdx gen = new GenerateProposalIdx(2, 3);
        ActorRef prop = system.actorOf(Proposer.props(gen, acceptorList),
                "proposer-under-test");

        prop.tell(new Proposer.DoPropose(), ActorRef.noSender());
        probe.expectMsg(new Acceptor.Prepare(2));
        prop.tell(new Proposer.DoPropose(), ActorRef.noSender());
        probe.expectMsg(new Acceptor.Prepare(5));
        prop.tell(new Proposer.DoPropose(), ActorRef.noSender());
        probe.expectMsg(new Acceptor.Prepare(8));
    }

    /**
     * Acceptor makes promise when it can.
     */
    @Test
    void acceptorPromises() {
        ActorRef acceptor = system.actorOf(Acceptor.props(),
                "acceptor-under-test");

        TestKit probe = new TestKit(system);

        // fresh acceptor Promises
        Acceptor.Prepare prepare = new Acceptor.Prepare(33);
        acceptor.tell(prepare, probe.getRef());
        probe.expectMsg(new Proposer.Promise(null, 33));

        // acceptor Promises if large enough
        prepare = new Acceptor.Prepare(36);
        acceptor.tell(prepare, probe.getRef());
        probe.expectMsg(new Proposer.Promise(null, 36));
        prepare = new Acceptor.Prepare(3);
        acceptor.tell(prepare, probe.getRef());
        probe.expectMsg(new Proposer.TooSmall(3));

        // repeated message should be alright
        prepare = new Acceptor.Prepare(36);
        acceptor.tell(prepare, probe.getRef());
        probe.expectMsg(new Proposer.Promise(null, 36));

    }

    /**
     * Check Promise messages.
     */
    @Test
    void checkPromiseMessages() {
        new TestKit(system) {{

            class ForwardToActor extends TestActor.AutoPilot {
                /**
                 * The proposer (or in this context more likely a wrapper
                 * for the proposer on which we can expect messages).
                 *
                 * <p>Note: we can't set this in the construtor b/c the
                 * proposer needs the acceptors in its constructor, and hence
                 * we first need to wrap the acceptors *after which* we have
                 * a proposer to fill in here.
                 */
                private ActorRef proposer;
                /**
                 * The actual acceptor actor.
                 */
                private final ActorRef acceptor;

                public ForwardToActor(final ActorRef acceptor) {
                    this.acceptor = acceptor;
                }

                void setProposer(final ActorRef p) {
                    proposer = p;
                }

                @Override
                public TestActor.AutoPilot run(ActorRef sender, Object msg) {
                    if (proposer == null) {
                        acceptor.tell(msg, sender);
                    } else {
                        acceptor.tell(msg, proposer);
                    }
                    return this;
                }
            }

            List<ActorRef> acceptorList =
                    IntStream.range(0, 3)
                            .mapToObj(i -> system.actorOf(Acceptor.props(), "acceptor-under-test-" + i))
                            .collect(Collectors.toList());

            List<TestKit> probes = new ArrayList<>();
            List<ForwardToActor> wraps = new ArrayList<>();

            List<ActorRef> acceptorForwards =
                    acceptorList.stream().map(a -> {
                        TestKit probe = new TestKit(system);
                        ForwardToActor wrap = new ForwardToActor(a);
                        wraps.add(wrap);
                        probe.setAutoPilot(wrap);
                        probes.add(probe);
                        return probe.getRef();
                    }).collect(Collectors.toList());


            GenerateProposalIdx gen = new GenerateProposalIdx(2, 3);
            ActorRef proposer = system.actorOf(Proposer.props(gen, acceptorForwards),
                    "proposer-under-test");

            TestKit proposerProbe = new TestKit(system);
            ForwardToActor proposerForward = new ForwardToActor(proposer);
            proposerProbe.setAutoPilot(proposerForward);

            wraps.forEach(w -> w.setProposer(proposerProbe.getRef()));

            proposer.tell(new Proposer.DoPropose(), ActorRef.noSender());

            probes.get(0).expectMsg(new Acceptor.Prepare(2));
            probes.get(1).expectMsg(new Acceptor.Prepare(2));
            probes.get(2).expectMsg(new Acceptor.Prepare(2));
            proposerProbe.expectMsg(new Proposer.Promise(null, 2));
            proposerProbe.expectMsg(new Proposer.Promise(null, 2));
            proposerProbe.expectMsg(new Proposer.Promise(null, 2));
            probes.get(0).expectMsgClass(Acceptor.Proposal.class); //new Acceptor.Proposal(2, 99));

        }};
    }
}
