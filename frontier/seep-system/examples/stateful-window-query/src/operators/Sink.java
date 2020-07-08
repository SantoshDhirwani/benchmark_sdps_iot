/*******************************************************************************
 *
 * Contributors:
 *     Santosh Dhirwani
 ******************************************************************************/
package operators;

import java.util.List;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;

public class Sink implements StatelessOperator {

    private static final long serialVersionUID = 1L;

    public void setUp() {

    }

    public void processData(DataTuple dt) {
        long windowTS = dt.getLong(Constants.TIMESTAMP);
        int eventsCount = dt.getInt(Constants.EVENTS_IN_WINDOW);
        long payload = dt.getLong(Constants.LATENCY);
        long currTS = System.currentTimeMillis() - windowTS;

        System.out.println("WindowTimestamp: " + windowTS + " EventsInWindowCount: " + eventsCount + " Latency: " + currTS + " ms.");
    }

    public void processData(List<DataTuple> arg0) {
    }
}
