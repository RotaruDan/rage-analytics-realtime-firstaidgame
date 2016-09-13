/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.firstaidgame.realtime.topologies;

import backtype.storm.tuple.Fields;
import es.eucm.firstaidgame.realtime.states.DocumentBuilder;
import es.eucm.firstaidgame.realtime.states.ESStateFactory;
import es.eucm.firstaidgame.realtime.states.TraceStateUpdater;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;

public class RealtimeTopology extends TridentTopology {

	public void prepareTest(ITridentSpout spout,
			ESStateFactory elasticStateFactory) {
		prepare(newStream("traces", spout), elasticStateFactory);
	}

	public void prepare(Stream traces, ESStateFactory elasticStateFactory) {

		Stream tracesStream = createTracesStream(traces);

		/** ---> Analysis definition <--- **/

		tracesStream.each(
				new Fields("trace"),
				new DocumentBuilder(elasticStateFactory.getConfig()
						.getSessionId(), "trace"), new Fields("document"))
				.partitionPersist(elasticStateFactory, new Fields("document"),
						new TraceStateUpdater());

	}

	protected Stream createTracesStream(Stream stream) {
		return stream;
	}
}
