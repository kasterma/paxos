package net.kasterma.paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A Learner queries the Acceptors until a value has been chosen.
 */
@Slf4j
public class Learner extends AbstractActor {
    private final List<ActorRef> acceptorList;
    private final Map<ActorRef, Acceptor.Proposal> accepted = new HashMap<>();
    private final FiniteDuration duration =
            Duration.create(1, TimeUnit.SECONDS);

    static Props props(final List<ActorRef> acceptorList) {
        return Props.create(Learner.class, acceptorList);
    }

    Learner(final List<ActorRef> acceptorList) {
        this.acceptorList = acceptorList;
        scheduleCheckValue(duration);
    }

    void scheduleCheckValue(FiniteDuration duration) {
        getContext().getSystem().scheduler().scheduleOnce(duration,
                getSelf(), new CheckValue(),
                getContext().getSystem().dispatcher(), null);
    }

    private static class CheckValue {}

    private void checkValue() {
        log.info("Check if value has been decicded");
        acceptorList.forEach(a -> a.tell(new Acceptor.Accepted(), getSelf()));
    }

    /**
     * Receiving an accepted proposal, record it, and check if a value has been
     * chosen.
     *
     * From the paper: a value has been chosen if a sinle Proposal with that
     * value has been accepted by a majority of Acceptors.
     *
     * @param p Message containing accepted proposal
     */
    private void decided(final Acceptor.Proposal p) {
        if (acceptorList.contains(sender())) {
            accepted.put(sender(), p);
            Map<Acceptor.Proposal, Integer> cts = new HashMap<>();
            accepted.values()
                    .forEach(pp -> cts.merge(pp, 1, Integer::sum));
            Optional<Map.Entry<Acceptor.Proposal, Integer>> e =
                    cts.entrySet().stream()
                            .max(Comparator.comparingInt(Map.Entry::getValue));
            if (e.isPresent() && e.get().getValue() > acceptorList.size() / 2) {
                log.info("We have a chosen value: {}",
                        e.get().getKey().getVal());
            } else {
                log.info("No evidence of chosen value yet");
                scheduleCheckValue(duration);
            }
        } else {
            log.error("Recvd accepted proposal from acceptor not in our list");
        }
    }

    public final Receive createReceive() {
        return receiveBuilder()
                .match(Acceptor.Proposal.class, this::decided)
                .match(CheckValue.class, cv -> checkValue())
                .build();
    }
}
