package operators;

import java.util.Queue;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.largestateimpls.SeepMap;
import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatefulOperator;
import uk.ac.imperial.lsds.seep.state.StateWrapper;


public class Distinct implements StatefulOperator, WindowOperator {

    private static final long serialVersionUID = 1L;

    private SeepMap<String, String> state;

    private Window window;

    public Distinct(Window window) {
        this.window = window;
    }

    @Override
    public void setUp() {
        state = new SeepMap<>();
        this.window.registerCallback(this);
    }

    @Override
    public void processData(DataTuple data) {
        this.window.updateWindow(data);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Distinct");
        return sb.toString();
    }

    @Override
    public void processData(List<DataTuple> dataList) {
        this.window.updateWindow(dataList);
    }

    @Override
    public void evaluateWindow(Queue<DataTuple> dataList) {
        //DataTuple data = dataList.get(0);

        //System.out.println("DATAAAAAAAAAAAAAAA!!!!: " + data);
        System.out.println("DATAAAAAAAAAAAAAAA!!!!: ");

        //api.send(data);
    }

    @Override
    public StateWrapper getState() {

        return new StateWrapper(2, 1, this.state);
        //return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void replaceState(StateWrapper state) {

        this.state = (SeepMap<String, String>) state.getStateImpl();
    }

}
