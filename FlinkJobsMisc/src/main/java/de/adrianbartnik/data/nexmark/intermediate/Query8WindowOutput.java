package de.adrianbartnik.data.nexmark.intermediate;

public class Query8WindowOutput {

    private final Long windowEvictingTimestamp;
    private final Long personCreationTimestamp;
    private final Long personIngestionTimestamp;
    private final Long auctionCreationTimestamp;
    private final Long auctionIngestionTimestamp;
    private final long personId;
    private final String personName;

    public Query8WindowOutput(Long windowEvictingTimestamp,
                              Long personCreationTimestamp,
                              Long personIngestionTimestamp,
                              Long auctionCreationTimestamp,
                              Long auctionIngestionTimestamp,
                              long personId,
                              String personName) {
        this.windowEvictingTimestamp = windowEvictingTimestamp;
        this.personCreationTimestamp = personCreationTimestamp;
        this.personIngestionTimestamp = personIngestionTimestamp;
        this.auctionCreationTimestamp = auctionCreationTimestamp;
        this.auctionIngestionTimestamp = auctionIngestionTimestamp;
        this.personId = personId;
        this.personName = personName;
    }

    public Long getAuctionCreationTimestamp() {
        return auctionCreationTimestamp;
    }

    public Long getPersonCreationTimestamp() {
        return personCreationTimestamp;
    }

    public Long getPersonIngestionTimestamp() {
        return personIngestionTimestamp;
    }

    public Long getAuctionIngestionTimestamp() {
        return auctionIngestionTimestamp;
    }

    public long getPersonId() {
        return personId;
    }

    public String getPersonName() {
        return personName;
    }

    public Long getWindowEvictingTimestamp() {
        return windowEvictingTimestamp;
    }
}
