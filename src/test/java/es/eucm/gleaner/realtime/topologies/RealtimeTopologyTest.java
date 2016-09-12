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
package es.eucm.gleaner.realtime.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.IMetricsContext;
import es.eucm.gleaner.realtime.states.ESGameplayState;
import es.eucm.gleaner.realtime.states.ESStateFactory;
import es.eucm.gleaner.realtime.states.GameplayState;
import es.eucm.gleaner.realtime.states.NoteBuilder;
import es.eucm.gleaner.realtime.utils.Document;
import es.eucm.gleaner.realtime.utils.EsConfig;
import org.bson.BSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class RealtimeTopologyTest {

    @Test
    public void test() throws IOException {
        FeederBatchSpout tracesSpout = new FeederBatchSpout(Arrays.asList(
                "versionId", "trace"));

        RealtimeTopology topology = new RealtimeTopology();
        Factory factory = new Factory(new EsConfig("testEsHost", "testSessionId"));
        topology.prepareTest(tracesSpout, factory);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("realtime", conf, topology.build());

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                ClassLoader.getSystemResourceAsStream("traces.txt")));

        String line;
        ArrayList<List<Object>> tuples = new ArrayList<List<Object>>();
        while ((line = reader.readLine()) != null) {
            tuples.add(Arrays.asList("version", buildTrace(line)));
            Logger.getLogger("test").info(line);

        }
        tracesSpout.feed(tuples);
        for (Document document : Factory.state.documents) {
            Logger.getLogger("test").info("document = " + document);
        }
        assertEquals(9, Factory.state.documents.size());
        Player player = Factory.state.getPlayer("1");
        assertEquals(13f, player.properties.get("meanScore"));
        Player player2 = Factory.state.getPlayer("2");
        assertEquals(46f, player2.properties.get("meanScore"));
    }

    private Map<String, Object> buildTrace(String line) {
        String[] parts = line.split(",");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("gameplayId", parts[0]);
        map.put("timestamp", parts[1]);
        map.put("event", parts[2]);
        map.put("type", parts[3]);
        map.put("target", parts[4]);
        map.put("response", parts[5]);
        map.put(parts[6], parts[7]);
        return map;
    }

    public static class TestState extends ESGameplayState {

        private Map<String, Player> players = new HashMap<String, Player>();

        public List<Document> documents = new ArrayList<>();

        public TestState(TransportClient client, String sessionId) {
            super(client, sessionId);
        }

        @Override
        public void setProperty(String versionId, String gameplayId,
                                String key, Object value) {
            getPlayer(gameplayId).setProperty(key, value);
        }

        @Override
        public void setOpaqueValue(String versionId, String gameplayId,
                                   List<Object> key, OpaqueValue value) {
            getPlayer(gameplayId).setValue(keyFromList(key), value);
        }

        @Override
        public OpaqueValue getOpaqueValue(String versionId, String gameplayId,
                                          List<Object> key) {
            return getPlayer(gameplayId).getValue(keyFromList(key));
        }

        private String keyFromList(List<Object> keys) {
            String key = "";
            for (Object o : keys) {
                key += o;
            }
            return key;
        }

        @Override
        public void bulkUpdateIndices(List<TridentTuple> inputs) {

            for (TridentTuple input : inputs) {
                Document<Map> doc = (Document<Map>) input
                        .getValueByField("document");
                documents.add(doc);

                Object meanScore = doc.getSource().get("meanScore");
                if (meanScore != null) {
                    getPlayer(doc.getSource().get("gameplayId").toString()).setProperty("meanScore", doc.getSource().get("meanScore"));
                }
            }
        }

        private Player getPlayer(String gameplayId) {
            Player player = players.get(gameplayId);
            if (player == null) {
                player = new Player();
                players.put(gameplayId, player);
            }
            return player;
        }
    }

    public static class Player {

        public Map<String, Object> properties = new HashMap<String, Object>();

        public Map<String, OpaqueValue> values = new HashMap<String, OpaqueValue>();

        public void setProperty(String key, Object value) {
            System.out.println("KKKKKKKKKKKKKKKKKKKKKKKKKKK key = [" + key + "], value = [" + value + "]");
            Object o = properties.get(key);
            if (o instanceof BSONObject && value instanceof BSONObject) {
                ((BSONObject) o).putAll((Map) value);
            } else {
                properties.put(key, value);
            }
        }

        public void setValue(String key, OpaqueValue value) {
            values.put(key, value);
        }

        public OpaqueValue getValue(String key) {
            return values.get(key);
        }
    }

    public static class Factory extends ESStateFactory {

        public static TestState state;

        public Factory(EsConfig config) {
            super(config);
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics,
                               int partitionIndex, int numPartitions) {
            if (state == null) {
                state = new TestState(null, getConfig().getSessionId());
            }
            return state;
        }
    }

}
