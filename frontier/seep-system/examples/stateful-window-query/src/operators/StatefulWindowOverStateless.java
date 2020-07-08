/*******************************************************************************
 *
 * Contributors:
 *     Santosh Dhirwani
 ******************************************************************************/
package operators;

import java.util.List;
import java.util.ArrayList;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatefulOperator;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;
import uk.ac.imperial.lsds.seep.state.StateWrapper;

public class StatefulWindowOverStateless implements StatefulWrapper {

    private static final long serialVersionUID = 1L;
    private int payload = 0;

    private int slide;
    private StateWrapper stateWrapper;

    private long lastTriggerTime = -1;
    private int size;
    private List<Event> currentWindow;

    public StatefulWindowOverStateless(int size, int slide, StateWrapper stateWrapper) {
        this.size = size;
        this.slide = slide;
        this.stateWrapper = stateWrapper;
    }

    public void processData(DataTuple data) {
        long messageTS = data.getLong(Constants.TIMESTAMP);
        String userId = data.getString(Constants.userId);
        String pageId = data.getString(Constants.pageId);
        String ad_id = data.getString(Constants.ad_id);
        String ad_type = data.getString(Constants.ad_type);
        String event_type = data.getString(Constants.event_type);
        String ip_address = data.getString(Constants.ip_address);

        payload++;
        Event newEvent = new Event(messageTS, userId, pageId, ad_id, ad_type, event_type, ip_address);
        replaceState(stateWrapper);
        currentWindow.add(newEvent);
        if (lastTriggerTime < messageTS - slide) {
            // send window
            DataTuple outputTuple = doWindowStatefullAggregation(currentWindow, data, messageTS, lastTriggerTime);
            api.send(outputTuple);

            currentWindow.removeIf(e -> e.timestamp < (messageTS - size));
            lastTriggerTime = messageTS;
        }
    }

    @Override
    public void processData(List<DataTuple> arg0) {

    }

    @Override
    public void replaceState(StateWrapper arg0) {
        stateWrapper = arg0;
    }

    private DataTuple doWindowStatefullAggregation(List<Event> events, DataTuple data, long windowTS, long lastTriggerTime) {

        int eventsInWindow = events.size();
        long lastUpdate = 0;
        if(windowTS > lastTriggerTime){
            lastUpdate = windowTS;
        }else{
            lastUpdate = windowTS - lastTriggerTime;
        }

        return data.setValues(windowTS, eventsInWindow, windowTS - lastTriggerTime);
    }

    @Override
    public StateWrapper getState() {

        return new StateWrapper(20, 20);
    }

    @Override
    public void setUp() {
        currentWindow = new ArrayList<Event>();
    }
}
