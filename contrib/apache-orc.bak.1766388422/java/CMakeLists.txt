# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(NO_DOWNLOAD_MSG
  --batch-mode
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn)

# set the version in the POM file to match the CMake version string
execute_process(COMMAND ./mvnw versions:set -DnewVersion=${ORC_VERSION}
                                         -DgenerateBackupPoms=false
                                         -Pbenchmark
                                         ${NO_DOWNLOAD_MSG}
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# show maven & java version
execute_process(COMMAND ./mvnw --version
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(ORC_JARS
  ${CMAKE_CURRENT_BINARY_DIR}/shims/orc-shims-${ORC_VERSION}.jar
  ${CMAKE_CURRENT_BINARY_DIR}/core/orc-core-${ORC_VERSION}.jar
  ${CMAKE_CURRENT_BINARY_DIR}/mapreduce/orc-mapreduce-${ORC_VERSION}.jar
  ${CMAKE_CURRENT_BINARY_DIR}/tools/orc-tools-${ORC_VERSION}-uber.jar
)

if (ANALYZE_JAVA)
  set(JAVA_PROFILE "-Pcmake,analyze,benchmark")
else()
  set(JAVA_PROFILE "-Pcmake,benchmark")
endif()

execute_process(
  COMMAND java --add-opens java.base/java.nio=ALL-UNNAMED -version
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  RESULT_VARIABLE RET
  OUTPUT_QUIET
  ERROR_QUIET)
if(RET EQUAL 0)
  set(ADD_OPENS --add-opens)
  set(JAVA_NIO java.base/java.nio=ALL-UNNAMED)
endif()

add_custom_command(
   OUTPUT ${ORC_JARS}
   COMMAND ./mvnw ${NO_DOWNLOAD_MSG} ${JAVA_PROFILE}
             -Dbuild.dir=${CMAKE_CURRENT_BINARY_DIR} -Dcyclonedx.skip -DskipTests package
   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
   COMMENT "Build the java directory"
   VERBATIM)

add_custom_target(java_build ALL DEPENDS ${ORC_JARS})

add_test(
  NAME java-test
  COMMAND ./mvnw ${NO_DOWNLOAD_MSG} -Pcmake
           -Dbuild.dir=${CMAKE_CURRENT_BINARY_DIR} test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# TOOD(ORC-1003)
# add_test(
#   NAME java-examples-test
#   COMMAND java -jar examples/orc-examples-${ORC_VERSION}-uber.jar write
#   WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_test(
  NAME java-tools-test
  COMMAND java -jar tools/orc-tools-${ORC_VERSION}-uber.jar version
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_test(
  NAME java-bench-gen-test
  COMMAND java ${ADD_OPENS} ${JAVA_NIO} -jar bench/core/orc-benchmarks-core-${ORC_VERSION}-uber.jar generate data -d sales -s 1
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_test(
  NAME java-bench-scan-test
  COMMAND java ${ADD_OPENS} ${JAVA_NIO} -jar bench/core/orc-benchmarks-core-${ORC_VERSION}-uber.jar scan data -d sales
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_test(
  NAME java-bench-hive-test
  COMMAND java ${ADD_OPENS} ${JAVA_NIO} -jar bench/hive/orc-benchmarks-hive-${ORC_VERSION}-uber.jar write -i 1 -I 0 -t 1 data
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_test(
  NAME java-bench-spark-test
  COMMAND java ${ADD_OPENS} ${JAVA_NIO} -jar bench/spark/orc-benchmarks-spark-${ORC_VERSION}.jar spark data -d sales -c zstd -f orc
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

install(
  FILES ${ORC_JARS}
  DESTINATION share)
