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
package es.eucm.gleaner.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import es.eucm.gleaner.realtime.states.ESStateFactory;
import es.eucm.gleaner.realtime.utils.EsConfig;
import es.eucm.gleaner.realtime.topologies.KafkaTopology;
import es.eucm.gleaner.realtime.utils.DBUtils;

import java.util.Map;

public class RealTime {

	private static StormTopology buildTopology(Map conf, String sessionId,
			String zookeeperUrl) {
		DBUtils.startRealtime(DBUtils.getMongoDB(conf), sessionId);

		KafkaTopology kafkaTopology = new KafkaTopology(sessionId);

		String elasticsearchUrl = conf.get("elasticsearchUrl").toString();
		EsConfig esConfig = new EsConfig(elasticsearchUrl, sessionId);
		kafkaTopology.prepare(zookeeperUrl, new ESStateFactory(esConfig));
		return kafkaTopology.build();
	}

	/**
	 * Configures the default values for the 'conf' parameter.
	 * 
	 * @param conf
	 */
	private static void setUpConfig(Config conf) {
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(500);
	}

	/**
	 * 
	 * @param args
	 *
	 */
	public static void main(String[] args) {

		Config conf = new Config();
		String sessionId = args[0];
		String zookeeperUrl = args[2];
		setUpConfig(conf);

		if (args.length == 4 && "debug".equals(args[3])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(sessionId, conf,
					buildTopology(conf, sessionId, zookeeperUrl));
		} else {
			try {
				System.out.println("Starting analysis of session " + sessionId);
				StormSubmitter.submitTopology(sessionId, conf,
						buildTopology(conf, sessionId, zookeeperUrl));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
	}

	// Storm flux function
	public StormTopology getTopology(Map<String, Object> conf) {
		String sessionId = conf.get("sessionId").toString();
		DBUtils.startRealtime(DBUtils.getMongoDB(conf), sessionId);
		KafkaTopology kafkaTopology = new KafkaTopology(sessionId);
		String elasticsearchUrl = conf.get("elasticsearchUrl").toString();
		EsConfig esConfig = new EsConfig(elasticsearchUrl, sessionId);
		kafkaTopology.prepare(conf.get("zookeeperUrl")
				.toString(), new ESStateFactory(esConfig));

		return kafkaTopology.build();
	}
}
