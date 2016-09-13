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
package es.eucm.firstaidgame.realtime.topologies;

import backtype.storm.tuple.Fields;
import es.eucm.firstaidgame.realtime.filters.FieldValueFilter;
import es.eucm.firstaidgame.realtime.functions.PropertyCreator;
import es.eucm.firstaidgame.realtime.functions.TraceFieldExtractor;
import es.eucm.firstaidgame.realtime.states.DocumentBuilder;
import es.eucm.firstaidgame.realtime.states.ESStateFactory;
import es.eucm.firstaidgame.realtime.states.GameplayStateUpdater;
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

        GameplayStateUpdater gameplayStateUpdater = new GameplayStateUpdater();
        Stream tracesStream = createTracesStream(traces);

        /** ---> Analysis definition <--- **/

        tracesStream.each(
                new Fields("trace"),
                new DocumentBuilder(elasticStateFactory.getConfig()
                        .getSessionId(), "trace"), new Fields("document"))
                .partitionPersist(elasticStateFactory, new Fields("document"),
                        new TraceStateUpdater());

        Stream eventStream = tracesStream.each(new Fields("trace"),
                new TraceFieldExtractor("gameplayId", "event"), new Fields(
                        "gameplayId", "event"));

        // Compute last accessed value
        Stream accessedTridentStream = eventStream
                .each(new Fields("event", "trace"),
                        new FieldValueFilter("event", "accessed"))
                .each(new Fields("trace"), new TraceFieldExtractor("target"),
                        new Fields("target"))
                .each(new Fields("event", "target"),
                        new PropertyCreator("target", "event"),
                        new Fields("p", "v"));

        accessedTridentStream.partitionPersist(elasticStateFactory,
                new Fields("versionId", "gameplayId", "p", "v"), gameplayStateUpdater);


        // Compute last accessed value
        Stream skippedTridentStream = eventStream
                .each(new Fields("event", "trace"),
                        new FieldValueFilter("event", "skipped"))
                .each(new Fields("trace"), new TraceFieldExtractor("target"),
                        new Fields("target"))
                .each(new Fields("event", "target"),
                        new PropertyCreator("target", "event"),
                        new Fields("p", "v"));

        skippedTridentStream.partitionPersist(elasticStateFactory,
                new Fields("versionId", "gameplayId", "p", "v"), gameplayStateUpdater);

    }

    protected Stream createTracesStream(Stream stream) {
        return stream;
    }
}
