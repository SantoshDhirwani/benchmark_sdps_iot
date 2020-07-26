/*******************************************************************************
 * Copyright (c) 2014 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Raul Castro Fernandez - initial API and implementation
 ******************************************************************************/
import java.util.ArrayList;

import operators.Sink;
import operators.Source;
import operators.RangeWindow;
import operators.Window;
import operators.Constants;
import operators.Distinct;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryPlan;
import uk.ac.imperial.lsds.seep.operator.Connectable;
import uk.ac.imperial.lsds.seep.state.StateWrapper;


public class Base implements QueryComposer{

	public QueryPlan compose() {
		/** Declare operators **/
		
		// Declare Source
		ArrayList<String> srcFields = new ArrayList<String>();
		srcFields.add(Constants.TIMESTAMP);
		srcFields.add("value2");
		srcFields.add("value3");
		Connectable src = QueryBuilder.newStatelessSource(new Source(), 0, srcFields);


		StateWrapper sw = new StateWrapper(1, 5000, null); // let's use this only if we do dynamic scale out
		Window w = new RangeWindow(10000,5000);
		Distinct op = new Distinct(w);
		Connectable q1 = QueryBuilder.newStatefulOperator(op, 1, sw, srcFields);
		
		// Declare sink
		ArrayList<String> snkFields = new ArrayList<String>();
		snkFields.add(Constants.TIMESTAMP);
		snkFields.add("value2");
		snkFields.add("value3");
		Connectable snk = QueryBuilder.newStatelessSink(new Sink(), 2, snkFields);
		
		/** Connect operators **/
		src.connectTo(q1, true, 0);
		q1.connectTo(snk, true, 0);
		
		return QueryBuilder.build();
	}
}
