/*******************************************************************************
 *
 * Contributors:
 *     Santosh Dhirwani
 ******************************************************************************/

import java.util.ArrayList;

import operators.StatefulWindowOverStateless;
import operators.Sink;
import operators.EventGenerator;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryPlan;
import uk.ac.imperial.lsds.seep.operator.Connectable;
import uk.ac.imperial.lsds.seep.state.StateWrapper;
import operators.Constants;

public class Base implements QueryComposer {

    public QueryPlan compose() {

        int eventsInSecond = 100; // Declare EVENTS/SEC rate here

        /** Declare operators **/

        // Declare Source
        ArrayList<String> srcFields = new ArrayList<String>();
        srcFields.add(Constants.TIMESTAMP);
        srcFields.add(Constants.userId);
        srcFields.add(Constants.pageId);
        srcFields.add(Constants.ad_id);
        srcFields.add(Constants.ad_type);
        srcFields.add(Constants.event_type);
        srcFields.add(Constants.ip_address);
        Connectable src = QueryBuilder.newStatelessSource(new EventGenerator(eventsInSecond), 13, srcFields);

        int windowSizeMS = 1000;
        int windowSlideMS = 1;
        StateWrapper sw = new StateWrapper(20, windowSlideMS);
        Connectable statefulOverStateless = QueryBuilder.newStatelessOperator(new StatefulWindowOverStateless(windowSizeMS, windowSlideMS, sw), 20, srcFields);

        // Declare sink
        ArrayList<String> snkFields = new ArrayList<String>();
        snkFields.add(Constants.TIMESTAMP);
        snkFields.add(Constants.EVENTS_IN_WINDOW);
        snkFields.add(Constants.LATENCY);
        Connectable snk = QueryBuilder.newStatelessSink(new Sink(), 25, snkFields);

        /** Connect operators **/
        src.connectTo(statefulOverStateless, true, 0);
        statefulOverStateless.connectTo(snk, true, 0);

        return QueryBuilder.build();
    }
}
