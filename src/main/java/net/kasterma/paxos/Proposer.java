package net.kasterma.paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.DiagnosticLoggingAdapter;
import akka.event.Logging;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;

public class Proposer extends AbstractActor {
    private final DiagnosticLoggingAdapter log = Logging.getLogger(this);

    private final GenerateProposalIdx genIdx;

    /**
     * List of all acceptors that we are working with.
     */
    private final List<ActorRef> acceptorList;

    /**
     * index for the proposal currently working on.
     */
    private int idx;

    /**
     * Keep track of the acceptances from the different acceptors.
     */
    private Map<ActorRef, Accept> acceptances = null;

    /**
     * Keep track of promises we have received not to accept certain types of
     * Proposals.
     */
    private Map<ActorRef, Acceptor.Proposal> promisesReceived = null;

    /**
     * Mapped diagnostic context for logging.
     */
    private Map<String, Object> mdc = new HashMap<>();

    static Props props(final GenerateProposalIdx genIdx,
                       final List<ActorRef> acceptorList) {
        return Props.create(Proposer.class, genIdx, acceptorList);
    }

    Proposer(final GenerateProposalIdx genIdx,
             final List<ActorRef> acceptorList) {
        this.genIdx = genIdx;
        this.acceptorList = acceptorList;
    }

    /**
     * From the already received promises, compute the proposal to make and
     * send it to all the acceptors.
     */
    private void propose() {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            Optional<Acceptor.Proposal> maxAccept =
                    promisesReceived.values().stream()
                            .filter(Objects::nonNull)
                            .max(Comparator.comparingInt(Acceptor.Proposal::getIdx));
            // If this is called there should be a max present.
            Acceptor.Proposal p;
            if (maxAccept.isPresent()) {
                int val = maxAccept.get().getVal();
                p = new Acceptor.Proposal(idx, val);
            } else {
                // For now just propose random value
                p = new Acceptor.Proposal(idx, new Random().nextInt());
                log.info("Making proposal with new random value {}", p.getVal());
            }

            promisesReceived.forEach((key, value) -> key.tell(p, getSelf()));
            promisesReceived = null; // No longer waiting for pomises
        } finally {
            log.clearMDC();
        }
    }

    private void prepare() {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            idx = genIdx.get();
            acceptances = new HashMap<>();
            promisesReceived = new HashMap<>();
            Acceptor.Prepare prep = new Acceptor.Prepare(idx);
            log.debug("Sending Prepare {}", prep);
            acceptorList.forEach(a -> a.tell(prep, getSelf()));
        } finally {
            log.clearMDC();
        }
    }

    /**
     * Message that indicates we are to make a proposal.
     *
     * TODO: get this somehow set up with a leader election
     * For now the main running chooses a Proposer.
     */
    static class DoPropose { }

    /**
     * Act on message from main running that we should be the Proposer to
     * propose a value.
     */
    private void doPropose() {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            log.debug("Received request to make proposal");
            prepare();
        } finally {
            log.clearMDC();
        }
    }

    /**
     * Message from an Acceptor indicating they are prepared for our proposal.
     */
    @AllArgsConstructor
    @Data
    static class Accept implements Comparable<Accept> {
        final Acceptor.Proposal prop;
        final int idx;

        @Override
        public int compareTo(Accept accept) {
            return Integer.compare(idx, accept.idx);
        }
    }

    private void accept(Accept a) {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            assert acceptorList != null;
            assert acceptances != null;

            if (a.idx != idx || a.getProp().getIdx() != idx) {
                log.debug("Received acceptance for prepare that is not current"
                        + " prepare or prop invalid for this accep");
                return;
            }

            ActorRef sender = getSender();
            if (acceptorList.contains(sender)) {
                acceptances.put(sender, a);
            } else {
                log.debug("Received acceptance from unknown acceptor");
                return;
            }

            // if (acceptances.size() * 2 > acceptorList.size()) then we could know
            // the proposal has been accepted.  Since messages can get lost, we
            // can't be sure (and since we will not request more info we'll not act
            // on this).  To get a safe algorithm we don't have to do anything, to
            // get liveness as well we'll need to maybe request more acceptances.
            log.info("accept run");
        } finally {
            log.clearMDC();
        }
    }

    /**
     * Message for getting promise in reaction to prepare from Acceptor.
     */
    @AllArgsConstructor
    @Data
    static class Promise {
        final Acceptor.Proposal p;
        final int idx;
    }

    private void promise(final Promise p) {
        try {
            mdc.put("sender", getSender().path());
            log.setMDC(mdc);
            if (p.getIdx() != idx) {
                log.debug("got promise for not (no longer) expected idx (recv {}, "
                        + "expected {})", p.getIdx(), idx);
                return;
            }
            if (promisesReceived == null) {
                log.debug("No longer needed promise received");
                return;
            }
            if (promisesReceived.containsKey(getSender())) {
                log.debug("double recv promise");
                return;
            }
            if (acceptorList.contains(getSender())) {
                promisesReceived.put(getSender(), p.getP());
            } else {
                log.debug("Got promise from non-acceptor");
                return;
            }
            if (promisesReceived.size() > acceptorList.size() / 2) {
                log.debug("PROPOSE");
                propose();
            }
        } finally {
            log.clearMDC();
        }
    }

    @AllArgsConstructor
    @Data
    static class TooSmall {
        final int idx;
    }

    private void tooSmall(final TooSmall ts) {
        if (ts.getIdx() == idx) {
            log.debug("Recvd info that our proposal is too small");
            prepare();
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
                .match(DoPropose.class, dop -> doPropose())
                .match(Accept.class, this::accept)
                .match(Promise.class, this::promise)
                .match(TooSmall.class, this::tooSmall)
                .build();
    }
}
