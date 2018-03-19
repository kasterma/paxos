package net.kasterma.paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class Proposer extends AbstractActor {
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
    private List<ActorRef> promiseList = null;

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
     * From the already received acceptances, compute the proposal to make and
     * send it to all the acceptors.
     */
    private void propose() {
        Optional<Accept> maxAccept =
                acceptances.values().stream().max(Accept::compareTo);
        // If this is called there should be a max present.
        Acceptor.Proposal p;
        if (maxAccept.isPresent()) {
            int val = maxAccept.get().getProp().getVal();
            p = new Acceptor.Proposal(idx, val);
        } else {
            // For now just propose random value
            p = new Acceptor.Proposal(idx, new Random().nextInt());
            log.info("Making proposal with new random value {}", p.getVal());
        }

        promiseList.forEach(a -> a.tell(p, getSelf()));
        promiseList = null; // No longer waiting for pomises
    }

    private void prepare() {
        idx = genIdx.get();
        acceptances = new HashMap<>();
        promiseList = new ArrayList<>();
        Acceptor.Prepare prep = new Acceptor.Prepare(idx);
        log.info("Sending Prepare {}", prep);
        acceptorList.forEach(a -> a.tell(prep, getSelf()));
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
        log.info("Received request to make proposal");
        prepare();
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
        assert acceptorList != null;
        assert acceptances != null;
        log.info("accept");
        // The check that the index for the proposal is then then our prepare index if to check my programming, it would
        // be an invalid action on the part of the Acceptor to send a too large idx.
        if (a.idx == idx && a.getProp().getIdx() == idx) {
            ActorRef sender = getSender();
            // This check is only needed to check my programming; this kind of error would be byzantine or
            // malicious.  Not part of the standard paxos algo to deal with.
            if (acceptorList.contains(sender)) {
                acceptances.put(getSender(), a);
                if (acceptances.size() * 2 > acceptorList.size()) {
                    log.info("decided");
                } else {
                    log.info("not enough");
                }
            } else {
                log.info("Received acceptance from acceptor that is not on acceptor list");
            }
        } else {
            log.info("Received acceptance for prepare that is not current prepare or prop invalid for this accep");
        }
    }

    /**
     * Message for getting promise in reaction to prepare from Acceptor.
     */
    @AllArgsConstructor
    @Data
    static class Promise {
        final int idx;
    }

    private void promise(final Promise p) {
        if (p.getIdx() != idx) {
            log.info("got promise for not (no longer) expected idx (recv {}, " +
                    "expected {})", p.getIdx(), idx);
            return;
        }
        if (promiseList == null) {
            log.info("No longer needed promise received");
            return;
        }
        if (promiseList.contains(getSender())) {
            log.info("double recv promise");
            return;
        }
        if (acceptorList.contains(getSender())) {
            promiseList.add(getSender());
        } else {
            log.error("Got promise from non-acceptor");
            return;
        }
        if (promiseList.size() > acceptorList.size() / 2) {
            log.info("PROPOSE");
            propose();
        }
    }

    @AllArgsConstructor
    @Data
    static class TooSmall {
        final int idx;
    }

    private void tooSmall(final TooSmall ts) {
        if (ts.getIdx() == idx) {
            log.info("Recvd info that our proposal is too small");
            prepare();
        }
    }

    public final Receive createReceive() {
        return receiveBuilder()
                .match(DoPropose.class, dop -> doPropose())
                .match(Accept.class, this::accept)
                .match(Promise.class, this::promise)
                .match(TooSmall.class, this::tooSmall)
                .build();
    }
}
