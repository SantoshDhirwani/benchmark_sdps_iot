package de.adrianbartnik.job.data;

public class GeoWindow {
    public final int state;
    public final Long count;

    public GeoWindow(Long count, int state) {
        this.state = state;
        this.count = count;
    }
}
