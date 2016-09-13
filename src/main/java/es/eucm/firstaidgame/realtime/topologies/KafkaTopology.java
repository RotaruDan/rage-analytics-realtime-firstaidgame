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

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import es.eucm.firstaidgame.realtime.states.ESStateFactory;
import es.eucm.firstaidgame.realtime.functions.JsonToTrace;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;

public class KafkaTopology extends RealtimeTopology {

	private String sessionId;

	public KafkaTopology(String sessionId) {
		this.sessionId = sessionId;
	}

	public void prepare(String zookeeperUrl, ESStateFactory elasticStateFactory) {
		BrokerHosts zk = new ZkHosts(zookeeperUrl);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, sessionId,
				sessionId);
		spoutConf.forceFromStart = true;
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		super.prepare(newStream("kafka-spout", spout), elasticStateFactory);
	}

	@Override
	protected Stream createTracesStream(Stream stream) {
		return stream.each(new Fields("str"), new JsonToTrace(sessionId),
				new Fields("versionId", "trace"));
	}
}
