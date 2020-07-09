package de.adrianbartnik.job.data;

public class ZipCodeGeoEvent {
    public final int stateID;
    public final String category;

    public ZipCodeGeoEvent(int stateID, String category) {
        this.stateID = stateID;
        this.category = category;
    }
}
