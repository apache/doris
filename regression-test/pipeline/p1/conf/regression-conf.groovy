// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/* ******* Do not commit this file unless you know what you are doing ******* */

// **Note**: default db will be create if not exist
defaultDb = "regression_test"

jdbcUrl = "jdbc:mysql://172.19.0.2:9132/?useLocalSessionState=true&allowLoadLocalInfile=true"
targetJdbcUrl = "jdbc:mysql://172.19.0.2:9132/?useLocalSessionState=true&allowLoadLocalInfile=true"
jdbcUser = "root"
jdbcPassword = ""

ccrDownstreamUrl = "jdbc:mysql://172.19.0.2:9132/?useLocalSessionState=true&allowLoadLocalInfile=true"
ccrDownstreamUser = "root"
ccrDownstreamPassword = ""
ccrDownstreamFeThriftAddress = "127.0.0.1:9020"

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
feSyncerUser = "root"
feSyncerPassword = ""

feHttpAddress = "172.19.0.2:8132"
feHttpUser = "root"
feHttpPassword = ""

beHttpAddress = "172.19.0.2:8142"

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"

// will test <group>/<suite>.groovy
// empty group will test all group
testGroups = ""
// empty suite will test all suite
testSuites = ""
// this suites will not be executed
excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "test_analyze_stats_p1," +
    "test_broker_load," +
    "test_profile," +
    "test_refresh_mtmv," +
    "test_spark_load," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

// this dir will not be executed
excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "fault_injection_p0," +
    "workload_manager_p1," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

cacheDataPath="/data/regression/"

s3Source="aliyun"

max_failure_num=50

externalEnvIp="127.0.0.1"

enableBrokerLoad=true
