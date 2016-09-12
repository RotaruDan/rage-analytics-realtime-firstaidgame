#
# Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Builds a jar-with-dependencies for the realtime analysis support
# Usage: 
#   mkdir output && chmod 0777 output \
#   && docker run -v $(pwd)/output:/app/output eucm/rage-analytics-realtime
#
FROM maven

ENV USER_NAME="user" \
    WORK_DIR="/app" \
    OUTPUT_VOL="/app/output" \
    OUTPUT_JAR="realtime/target/realtime-jar-with-dependencies.jar"

# setup sources, user, group and workdir
COPY ./ ${WORK_DIR}/realtime
RUN groupadd -r ${USER_NAME} \
    && mkdir ${OUTPUT_VOL}\
    && useradd -r -d ${WORK_DIR} -g ${USER_NAME} ${USER_NAME} \
    && chown -R ${USER_NAME}:${USER_NAME} ${WORK_DIR}
ENV HOME=${WORK_DIR}
USER ${USER_NAME}
WORKDIR ${WORK_DIR}

# build, remove downloaded/unneeded jars, and expose results
RUN cd realtime && mvn license:format && mvn install  \
    && rm -rf ../.m2

VOLUME ${OUTPUT_VOL}

CMD cp ${OUTPUT_JAR} ${OUTPUT_VOL}
