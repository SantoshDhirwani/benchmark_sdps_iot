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
package operators;

import java.util.List;

import uk.ac.imperial.lsds.seep.comm.serialization.DataTuple;
import uk.ac.imperial.lsds.seep.operator.StatelessOperator;

public class Sink implements StatelessOperator {

	private static final long serialVersionUID = 1L;
	
	public void setUp() {

	}

	// time control variables
	int c = 0;
	long init = 0;
	int sec = 0;
	
	public void processData(DataTuple dt) {
		long messageTS = dt.getLong(Constants.TIMESTAMP);
		int workerUUID = dt.getInt("value2");
		int payload = dt.getInt("value3");
		int currTS = (int) (System.currentTimeMillis() - Constants.timeOffset);
		int delta = currTS - (int)messageTS;
		// TIME CONTROL
		c++;
		if((System.currentTimeMillis() - init) > 1000){
			//System.out.println("Latency for: "+workerUUID+" is "+delta+" ms. Payload: " + payload );
			System.out.println("messageTS: "+ messageTS +" workerUUID: "+ workerUUID + "  Payload: " + payload );
			c = 0;
			sec++;
			init = System.currentTimeMillis();
		}
	}
	
	public void processData(List<DataTuple> arg0) {
	}
}
