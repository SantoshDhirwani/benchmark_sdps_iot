package de.adrianbartnik.data.nexmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Class needs public field with default, no-argument constructor to be serializable.
 */
public class AuctionEvent implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AuctionEvent.class);


    public long timestamp;
    public long auctionId;
    public long personId;
    public long itemId;
    public double initialPrice;
    public long start;
    public long end;
    public long categoryId;
    public long ingestionTimestamp;

    public AuctionEvent() {
        LOG.debug("Created person event with default constructor");
    }

    public AuctionEvent(long timestamp, long auctionId, long itemId, long personId, double initialPrice, long categoryID, long start, long end) {
        this(timestamp, auctionId, itemId, personId, initialPrice, categoryID, start, end, System.currentTimeMillis());
    }

    public AuctionEvent(long timestamp, long auctionId, long itemId, long personId, double initialPrice, long categoryID, long start, long end, long ingestionTimestamp) {
        LOG.debug("Created person event with auctionId {} and personId {}", auctionId, personId);

        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.itemId = itemId;
        this.initialPrice = initialPrice;
        this.categoryId = categoryID;
        this.start = start;
        this.end = end;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public long getAuctionId() {
        return auctionId;
    }

    public Long getPersonId() {
        return personId;
    }

    public long getItemId() {
        return itemId;
    }

    public Double getInitialPrice() {
        return initialPrice;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public long getIngestionTimestamp() {
        return ingestionTimestamp;
    }
}
