package de.adrianbartnik.operator;

import de.adrianbartnik.data.nexmark.AuctionEvent;
import de.adrianbartnik.data.nexmark.NewPersonEvent;
import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class JoiningNewUsersWithAuctionsCoGroupFunction extends RichCoGroupFunction<NewPersonEvent, AuctionEvent, Query8WindowOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(JoiningNewUsersWithAuctionsCoGroupFunction.class);

    /**
     * CoGroups Auction and Person on person id and return the Persons name as well as ID.
     * Finding every person that created a new auction.
     *
     * Currently, when execution on the simple generator, it most certainly will happen, that the same person
     * appears multiple times in a window. Currently, simple ignore that case.
     */
    @Override
    public void coGroup(Iterable<NewPersonEvent> persons,
                        Iterable<AuctionEvent> auctions,
                        Collector<Query8WindowOutput> out) {

        LOG.debug("{} was called with {} and {}", this.getClass(), persons, auctions);

        Iterator<NewPersonEvent> personIterator = persons.iterator();
        Iterator<AuctionEvent> auctionIterator = auctions.iterator();

        if (!personIterator.hasNext()) {
            LOG.debug("Has not received person for auction events. Person there has already been created before the last window");
        } else if (!auctionIterator.hasNext()) {
            LOG.debug("Has not received auction for person events. Person has not yet created a auction");
        } else {

            NewPersonEvent person = personIterator.next();

            Long personCreationTimestamp = person.getTimestamp();
            Long personIngestionTimestamp = person.getIngestionTimestamp();

            long personId = person.getPersonId();
            String personName = person.getName();

            for (AuctionEvent auction : auctions) {
                Long auctionCreationTimestamp = auction.getTimestamp();
                Long auctionIngestionTimestamp = auction.getIngestionTimestamp();

                out.collect(new Query8WindowOutput(
                        System.currentTimeMillis(),
                        personCreationTimestamp,
                        personIngestionTimestamp,
                        auctionCreationTimestamp,
                        auctionIngestionTimestamp,
                        personId,
                        personName));
            }
        }
    }
}

