package net.kasterma.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.sys.Prop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
        GenerateProposalIdx gen = new GenerateProposalIdx(2,3);
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
}
