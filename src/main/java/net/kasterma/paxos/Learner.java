package net.kasterma.paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Learner queries the Acceptors until a value has been chosen.
 */
@Slf4j
public class Learner extends AbstractActor {
    /**
     * List of acceptors to work with; needs to be complete b/c its length
     * determines the size of a majority.
     */
    private final List<ActorRef> acceptorList;
    /**
     * Record of the received accepted Proposals (currently unused).
     */
    private final Map<ActorRef, Acceptor.Proposal> accepted = new HashMap<>();
    /**
     * Count of receipt of different accepted Proposals.
     */
    private final Map<Acceptor.Proposal, Integer> cts = new HashMap<>();
    /**
     * Duration of interval to send out CheckValue messages.
     */
    private final FiniteDuration duration =
            Duration.create(1, TimeUnit.SECONDS);

    /**
     * Store the chosen value here; remaind null as long as we have not
     * discovered the chosen value yet.
     */
    private Integer chosenValue = null;

    static Props props(final List<ActorRef> acceptorList) {
        return Props.create(Learner.class, acceptorList);
    }

    Learner(final List<ActorRef> acceptorList) {
        this.acceptorList = acceptorList;
        scheduleCheckValue(duration);
    }

    /**
     * Schedule a message to myself to send messages checking again if a value
     * has been chosen.
     *
     * @param duration A FiniteDuration with delay before checking again.
     */
    private void scheduleCheckValue(final FiniteDuration duration) {
        getContext().getSystem().scheduler().scheduleOnce(duration,
                getSelf(), new CheckValue(),
                getContext().getSystem().dispatcher(), null);
    }

    /**
     * Message used by the scheduler to send to myself to initiate another
     * round of querying Acceptors.
     */
    private static class CheckValue {}

    /**
     * Check for a chosen value; i.e. query the Acceptors to find out what they
     * have accepted so far.
     */
    private void checkValue() {
        log.info("Check if value has been decicded");
        if (chosenValue != null) {
            acceptorList
                    .forEach(a -> a.tell(new Acceptor.Accepted(), getSelf()));
            // and immediately schedule another check.
            scheduleCheckValue(duration);
        }
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
        if (!acceptorList.contains(sender())) {
            log.error("Recvd accepted proposal from acceptor not in our list");
            return;
        }

        // record the Proposal
        accepted.put(sender(), p);
        cts.merge(p, 1, Integer::sum);

        // check if we have found a chosen value yet
        Optional<Map.Entry<Acceptor.Proposal, Integer>> e =
                cts.entrySet().stream()
                        .max(Comparator.comparingInt(Map.Entry::getValue));
        if (e.isPresent() && e.get().getValue() > acceptorList.size() / 2) {
            log.info("We have a chosen value: {}",
                    e.get().getKey().getVal());
            chosenValue = e.get().getKey().getVal();
        } else {
            log.info("No evidence of chosen value yet");
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
                .match(Acceptor.Proposal.class, this::decided)
                .match(CheckValue.class, cv -> checkValue())
                .build();
    }
}
