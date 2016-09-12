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
package es.eucm.gleaner.realtime.states;

import es.eucm.gleaner.realtime.utils.DBUtils;
import es.eucm.gleaner.realtime.utils.Document;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class NoteBuilder implements Function {

    private Map<String, GameplayInfo> gameplaysInfo = new HashMap<String, GameplayInfo>();

    public class GameplayInfo extends HashMap<String, Object> {
        public static final String NOTA_DT = "NotaDT";
        public static final String NOTA_AT = "NotaAT";
        public static final String NOTA_INC = "NotaINC";

    }

    public NoteBuilder() {

    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        Map<String, Object> trace = (Map<String, Object>) tuple.getValueByField("trace");

        if (buildTrace(trace)) {
            ArrayList<Object> object = new ArrayList<Object>(1);
            object.add(trace);

            collector.emit(object);
        }
    }

    private boolean buildTrace(Map<String, Object> trace) {

        Logger.getLogger("test").info("buildTrace buildTracebuildTracebuildTracebuildTracebuild" +
                "TracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebuild" +
                "TracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTr" +
                "acebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebui" +
                "ldTracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebuild" +
                "TracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTracebuildTrac" +
                "buildTracebuildTracebuildTracebuildTracebuildTracebuildTraceebuildTracetrace = " + trace);

        for (Map.Entry<String, Object> stringObjectEntry : trace.entrySet()) {
            String key = stringObjectEntry.getKey();
            if (key.startsWith("Nota")) {
                String gameplayId = trace.get("gameplayId").toString();
                GameplayInfo gameplayInfo = gameplaysInfo.get(gameplayId);
                if (gameplayInfo == null) {
                    gameplayInfo = new GameplayInfo();
                    gameplaysInfo.put(gameplayId, gameplayInfo);
                }

                gameplayInfo.put(key, stringObjectEntry.getValue());
                try {
                    Object objectAT = gameplayInfo.get(GameplayInfo.NOTA_AT);
                    Object objectINC = gameplayInfo.get(GameplayInfo.NOTA_INC);
                    Object objectDT = gameplayInfo.get(GameplayInfo.NOTA_DT);

                    Float valueAT = 0f;
                    if(objectAT != null) {
                        valueAT = Float.valueOf(objectAT.toString());
                    }

                    Float valueDT = 0f;
                    if(objectDT != null) {
                        valueDT = Float.valueOf(objectDT.toString());
                    }

                    Float valueINC = 0f;
                    if(objectINC != null) {
                        valueINC = Float.valueOf(objectINC.toString());
                    }

                    float mean = (valueINC + valueAT + valueDT) / 3f;
                    trace.put("meanScore", mean);
                } catch (NumberFormatException nfe) {
                    System.out.println("Number Format Exception " + nfe);
                    return false;
                }

                return true;
            }
        }

        return false;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
