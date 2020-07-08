/*******************************************************************************
 *
 * Contributors:
 *     Santosh Dhirwani
 ******************************************************************************/
package operators;

import java.util.List;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;

public class Constants{

	public static final long timeOffset = 1000000L;
	public static final String TIMESTAMP = "timestamp";
	public static final String LATENCY = "latency";
	public static final String EVENTS_IN_WINDOW = "eventsInWindow";

	public static final String userId = "userId";
	public static final String pageId = "pageId";
	public static final String ad_id = "ad_id";
	public static final String ad_type = "ad_type";
	public static final String event_type = "event_type";
	public static final String ip_address = "ip_address";
}
