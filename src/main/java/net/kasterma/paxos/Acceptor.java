package net.kasterma.paxos;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.DiagnosticLoggingAdapter;
import akka.event.Logging;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Actor that implements the Acceptor role from the Paxos algorithm.
 */

public class Acceptor extends AbstractActor {
    private final DiagnosticLoggingAdapter log = Logging.getLogger(this);

    // No proposal with id <= maxPromise should be accepted.
    private int maxPromise = -1;
    // Proposal with max idx that his Acceptor has accepted.
    private Proposal accepted = null;

    /**
     * Mapped diagnostic context for logging.
     */
    private Map<String, Object> mdc = new HashMap<>();

    /**
     * Get the props for creating an Acceptor.
     *
     * @return Acceptor Props for creating Acceptor.
     */
    static Props props() {
        return Props.create(Acceptor.class);
    }

    /**
     * Message representing a proposal from a Proposer.
     */
    @AllArgsConstructor
    @Data
    static class Proposal {
        private final int idx;
        private final int val;
    }

    /**
     * Act on a received Proposal.
     *
     * @param p the received Proposal.
     */
    private void proposal(final Proposal p) {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            log.debug("proposal");
            if (p.getIdx() > maxPromise) {
                assert p.getIdx() - 1 == maxPromise;
                accepted = p;
                sender().tell(new Proposer.Accept(p, p.getIdx()), getSelf());
            } else {
                sender().tell(new Proposer.TooSmall(p.getIdx()), getSelf());
            }
        } finally {
            log.clearMDC();
        }
    }


    @AllArgsConstructor
    @Data
    static class Prepare {
        private final int idx;
    }

    private void prepare(final Prepare p) {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            log.debug("prepare");
            if (p.getIdx() > maxPromise) {
                // the id in the prepare is the smallest that should still be
                // accepted
                maxPromise = p.getIdx() - 1;
                getSender().tell(new Proposer.Promise(accepted, p.getIdx()),
                        getSelf());
            } else {
                log.info("Recvd prepare for too small value");
                getSender().tell(new Proposer.TooSmall(p.getIdx()), getSelf());
            }
        } finally {
            log.clearMDC();
        }
    }

    /**
     * Request from Learner to find out of this Acceptor has accepted a
     * proposal.
     */
    static class Accepted {
    }

    /**
     * If something has been accepted send it back to a learner so it can decide
     * if a value has been chosen.
     */
    private void accepted() {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            if (accepted != null) {
                sender().tell(accepted, getSelf());
            }
        } finally {
            log.clearMDC();
        }
    }

    /**
     * Set up the actor behavior.
     *
     * @return Recieve object to be used to set up initial interaction.
     */
    @Override
    public final Receive createReceive() {
        return receiveBuilder()
                .match(Prepare.class, this::prepare)
                .match(Proposal.class, this::proposal)
                .match(Accepted.class, d -> accepted())
                .build();
    }
}
