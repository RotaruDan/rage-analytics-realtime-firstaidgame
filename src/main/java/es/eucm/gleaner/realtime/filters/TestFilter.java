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
package es.eucm.gleaner.realtime.filters;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class TestFilter implements Filter {

	public static String NOTA_DT;

	public TestFilter() {
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
		Object traceObj = objects.getValueByField("trace");
		if(traceObj instanceof Map) {
			Map<String, Object> trace = (Map) traceObj;
			for (Map.Entry<String, Object> stringObjectEntry : trace.entrySet()) {
				String key = stringObjectEntry.getKey();
				if(key.startsWith("nota")) {

					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {
	}

	@Override
	public void cleanup() {

	}
}
