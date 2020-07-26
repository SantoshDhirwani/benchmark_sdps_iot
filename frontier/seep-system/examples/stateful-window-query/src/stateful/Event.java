
package operators;

import java.util.List;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatefulOperator;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;
import uk.ac.imperial.lsds.seep.state.StateWrapper;

public class Event {
    public long timestamp;
    public String userId;
    public String pageId;
    public String ad_id;
    public String ad_type;
    public String event_type;
    public String ip_address;

    public Event(long timestamp, String userId, String pageId, String ad_id, String ad_type, String event_type, String ip_address) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.pageId = pageId;
        this.ad_id = ad_id;
        this.ad_type = ad_type;
        this.event_type = event_type;
        this.ip_address = ip_address;
    }
}