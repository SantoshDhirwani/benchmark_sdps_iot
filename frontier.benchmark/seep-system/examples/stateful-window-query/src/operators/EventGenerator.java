/*******************************************************************************
 *
 * Contributors:
 *     Santosh Dhirwani
 ******************************************************************************/
package operators;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.comm.serialization.messages.TuplePayload;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;

import java.util.UUID;

public class EventGenerator implements StatelessOperator {

    private static final long serialVersionUID = 1L;

    public EventGenerator(int eventsInSec) {
        this.eventsInSec = eventsInSec;
    }

    private int eventsInSec;
    private final String uuid = UUID.randomUUID().toString();
    private static final int workerUUID = (int) (System.currentTimeMillis() - Constants.timeOffset);

    public void setUp() {

    }

    public void processData(DataTuple dt) {
        Map<String, Integer> mapper = api.getDataMapper();
        DataTuple data = new DataTuple(mapper, new TuplePayload());

        while (true) {
            Event ev = generateEvent();

            DataTuple output = data.newTuple(ev.timestamp, ev.userId, ev.pageId, ev.ad_id, ev.ad_type, ev.event_type, ev.ip_address);

            api.send(output);

            int calculatedDelay = 1000 / eventsInSec;

            if(calculatedDelay >= 3) {
                try {
                    Thread.sleep(calculatedDelay);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    private Event generateEvent() {
        return new Event(
                System.currentTimeMillis(),
                uuid,
                uuid,
                uuid + "adId",
                uuid + "adType",
                uuid + "evType",
                "255.255.255.255"
        );
    }

    public void processData(List<DataTuple> arg0) {
        // TODO Auto-generated method stub
    }
}
