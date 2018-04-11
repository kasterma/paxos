package net.kasterma.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Set up paxos network of agents, and run a determination of a value through
 * it.
 */
@Slf4j
public class Paxos {
    public static void main(final String[] args) throws InterruptedException {
        log.info("running");
        ActorSystem system = ActorSystem.create("paxosSystem");

        // Generate set of acceptors
        int acceptorCt = 3;
        List<ActorRef> acceptors = new ArrayList<>();
        for (int idx = 0; idx < acceptorCt; idx++) {
            acceptors.add(system.actorOf(Acceptor.props(),
                    "acceptor-" + idx));
        }

        // Generate set of proposers
        int proposerCt = 1;
        List<ActorRef> proposers = new ArrayList<>();
        for (int idx = 0; idx < proposerCt; idx++) {
            GenerateProposalIdx genIdx =
                    new GenerateProposalIdx(0,proposerCt);
            proposers.add(system.actorOf(Proposer.props(genIdx, acceptors, 42),
                    "proposer-" + idx));
        }


        // Generate set of learners
        int learnerCt = 1;
        List<ActorRef> learners = new ArrayList<>();
        for (int idx = 0; idx < learnerCt; idx++) {
            learners.add(system.actorOf(Learner.props(acceptors),
                    "learner-" + idx));
        }

        // Fire up the making of a proposal
        proposers.get(0).tell(new Proposer.DoPropose(), ActorRef.noSender());

        Thread.sleep(10 * 1000L);
        log.info("terminating");
        system.terminate();
    }
}
