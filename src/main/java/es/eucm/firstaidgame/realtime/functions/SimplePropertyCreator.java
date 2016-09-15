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
package es.eucm.firstaidgame.realtime.functions;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

public class SimplePropertyCreator implements Function {

	private static final Logger LOG = Logger
			.getLogger(SimplePropertyCreator.class.getSimpleName());

	private String valueField;

	private String key;

	public SimplePropertyCreator(String valueField, String key) {
		this.valueField = valueField;
		this.key = key;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.info("extractinng key " + key + ", value field "
				+ tuple.getValueByField(valueField));
		collector.emit(Arrays.asList(key, tuple.getValueByField(valueField)));
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
}
