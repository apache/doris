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

// add useLocalSessionState so that the jdbc will not send
// init cmd like: select @@session.tx_read_only
// at each time we connect.
jdbcUrl = "jdbc:mysql://127.0.0.1:9235/?useLocalSessionState=true&allowLoadLocalInfile=true"
jdbcUser = "root"
jdbcPassword = ""

feHttpAddress = "127.0.0.1:8335"
feHttpUser = "root"
feHttpPassword = ""

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"

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
excludeSuites = ""
// this directories will not be executed
excludeDirectories = ""

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// broker load test config
enableBrokerLoad=true
ak="AKIDPhmeKo46sPhkZ9USWQEqGTSDsW5Xr6IL"
sk="xmT2Uoz0vgGkbKr6A4mGWcWCiBVcYJSV"

// jdbc connector test config
// To enable jdbc test, you need first start mysql/pg container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableJdbcTest=false
mysql_57_port=3316
pg_14_port=5442
oracle_11_port=1521

// hive catalog test config
// To enable jdbc test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=false
hms_port=9183
hdfs_port=8120

// elasticsearch catalog test config
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableEsTest=false
es_6_port=19200
es_7_port=29200
es_8_port=39200


//hive  catalog test config for bigdata
enableExternalHiveTest = true
extHiveHmsHost = "172.21.16.47"
extHiveHmsPort = 7004
extHiveHmsUser = "root"
extHiveHmsPassword= "Cfplhys@2022"
extHdfsPort = 4007

//mysql jdbc connector test config for bigdata
enableExternalMysqlTest = true
extMysqlHost = "172.21.48.3"
extMysqlPort = 3306
extMysqlUser = "root"
extMysqlPassword = "Cfplhys@2022"

//postgresql jdbc connector test config for bigdata
enableExternalPgTest = true
extPgHost = "172.21.48.9"
extPgPort = 5432
extPgUser = "root"
extPgPassword = "Cfplhys@2022"

// elasticsearch external test config for bigdata
enableExternalEsTest = true
extEsHost = "172.21.0.138"
extEsPort = 9002
extEsUser = "elastic"
extEsPassword = "Cfplhys@2022"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

sf1DataPath = "/mnt/disk2/yunyou/git/doris1.2/test-data-cache"
cacheDataPath = "/mnt/disk2/yunyou/git/doris1.2/test-data-cache"
