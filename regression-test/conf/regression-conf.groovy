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

jdbcUrl = "jdbc:mysql://rd-dev-hk-ali02:9118/?"
jdbcUser = "root"
jdbcPassword = "Liqf@051116_Bigdata"

feHttpAddress = "rd-dev-hk-ali02:8019"
feHttpUser = "root"
feHttpPassword = "Liqf@051116_Bigdata"

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"
// sf1DataPath can be url like "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com" or local path like "/data"
sf1DataPath = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com"

// will test <group>/<suite>.groovy
// empty group will test all group
testGroups = ""
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = ""
// this suites will not be executed
excludeSuites = "test_create_table_with_bloom_filter"
// this directories will not be executed
excludeDirectories = ""

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"
