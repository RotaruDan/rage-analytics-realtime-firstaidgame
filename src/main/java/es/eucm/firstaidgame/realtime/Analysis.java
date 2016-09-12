/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.firstaidgame.realtime;

import backtype.storm.generated.StormTopology;
import es.eucm.firstaidgame.realtime.topologies.KafkaTopology;
import es.eucm.firstaidgame.realtime.utils.DBUtils;
import es.eucm.firstaidgame.realtime.states.ESStateFactory;
import es.eucm.firstaidgame.realtime.utils.EsConfig;

import java.util.Map;

public class Analysis {

    /**
     * Builds a KafkaTopology
     * @param conf
     * @param sessionId
     * @param zookeeperUrl
     * @return a topology that connects to kafka and performs the analysis
     */
    private static StormTopology buildTopology(Map conf, String sessionId,
                                               String zookeeperUrl) {
        DBUtils.startRealtime(sessionId);
        KafkaTopology kafkaTopology = new KafkaTopology(sessionId);
        String elasticSearchUrl = conf.get("elasticsearchUrl").toString();
        EsConfig esConfig = new EsConfig(elasticSearchUrl, sessionId);
        kafkaTopology.prepare(zookeeperUrl, new ESStateFactory(esConfig));
        return kafkaTopology.build();
    }

    // Storm flux Start-up function
    public StormTopology getTopology(Map<String, Object> conf) {
        String sessionId = conf.get("sessionId").toString();
        String zookeeperUrl = conf.get("zookeeperUrl").toString();
        return buildTopology(conf, sessionId, zookeeperUrl);
    }
}
