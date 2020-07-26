
package operators;

import java.util.List;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatefulOperator;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;
import uk.ac.imperial.lsds.seep.state.StateWrapper;

public interface StatefulWrapper extends StatelessOperator{

	 void processData(List<DataTuple> arg0);

	 void replaceState(StateWrapper arg0);

	 StateWrapper getState();

	 void setUp();
}