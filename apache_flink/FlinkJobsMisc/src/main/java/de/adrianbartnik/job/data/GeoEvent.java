package de.adrianbartnik.job.data;

public class GeoEvent {
    public final Long lat;
    public final Long lng;
    public final String category;

    public GeoEvent(Long lat, Long lng, String category) {
        this.lat = lat;
        this.lng = lng;
        this.category = category;
    }
}
