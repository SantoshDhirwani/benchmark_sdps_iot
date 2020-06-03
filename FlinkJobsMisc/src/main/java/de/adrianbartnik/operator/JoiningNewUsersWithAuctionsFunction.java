package de.adrianbartnik.operator;

import de.adrianbartnik.data.nexmark.AuctionEvent;
import de.adrianbartnik.data.nexmark.NewPersonEvent;
import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import org.apache.flink.api.common.functions.RichJoinFunction;

public class JoiningNewUsersWithAuctionsFunction extends RichJoinFunction<NewPersonEvent, AuctionEvent, Query8WindowOutput> {


    /**
     * Join Auction and Person on person id and return the Persons name as well as ID.
     * Finding every person that created a new auction.
     */
    @Override
    public Query8WindowOutput join(NewPersonEvent person, AuctionEvent auction) {

        long evictingTimestamp = System.currentTimeMillis();

        Long personCreationTimestamp = person.getTimestamp();
        Long personIngestionTimestamp = person.getIngestionTimestamp();

        Long auctionCreationTimestamp = person.getTimestamp();
        Long auctionIngestionTimestamp = person.getIngestionTimestamp();

        long personId = person.getPersonId();
        String personName = person.getName();

        return new Query8WindowOutput(evictingTimestamp,personCreationTimestamp, personIngestionTimestamp, auctionCreationTimestamp, auctionIngestionTimestamp, personId, personName);
    }
}

