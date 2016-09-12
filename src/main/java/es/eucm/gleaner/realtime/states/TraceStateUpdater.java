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
package es.eucm.gleaner.realtime.states;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class TraceStateUpdater implements StateUpdater<ESGameplayState> {

	@Override
	public void updateState(ESGameplayState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		state.bulkUpdateIndices(tuples);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
