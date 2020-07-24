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

    long latestUpdate = System.currentTimeMillis();

    public void processData(DataTuple dt) {
        long windowTS = dt.getLong(Constants.TIMESTAMP);
        int eventsCount = dt.getInt(Constants.EVENTS_IN_WINDOW);
        long payload = dt.getLong(Constants.LATENCY);
        long currTS = System.currentTimeMillis() - windowTS;

        if (System.currentTimeMillis() - latestUpdate >= 1000){
            System.out.println("output-window-lat," + windowTS + "," + currTS);
            latestUpdate = System.currentTimeMillis();
        }

    }

    public void processData(List<DataTuple> arg0) {
    }
}
