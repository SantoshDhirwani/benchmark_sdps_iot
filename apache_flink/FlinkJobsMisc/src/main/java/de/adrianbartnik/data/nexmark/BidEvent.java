package de.adrianbartnik.data.nexmark;

import java.io.Serializable;

/**
 * Class needs public field with default, no-argument constructor to be serializable.
 */
public class BidEvent implements Serializable {

    public long timestamp;
    public int auctionId;
    public int personId;
    public int bidId;
    public double bid;

    public BidEvent() {
    }

    public BidEvent(long timestamp, int auctionId, int personId, int bidId, double bid) {
        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.bidId = bidId;
        this.bid = bid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Integer getAuctionId() {
        return auctionId;
    }

    public Integer getPersonId() {
        return personId;
    }

    public Integer getBidId() {
        return bidId;
    }

    public Double getBid() {
        return bid;
    }
}