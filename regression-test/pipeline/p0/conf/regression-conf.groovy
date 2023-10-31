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

jdbcUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true"
targetJdbcUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true"
jdbcUser = "root"
jdbcPassword = ""

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
feSyncerUser = "root"
feSyncerPassword = ""

feHttpAddress = "172.19.0.2:8131"
feHttpUser = "root"
feHttpPassword = ""

// set DORIS_HOME by system properties
// e.g. java -DDORIS_HOME=./
suitePath = "${DORIS_HOME}/regression-test/suites"
dataPath = "${DORIS_HOME}/regression-test/data"
pluginPath = "${DORIS_HOME}/regression-test/plugins"
realDataPath = "${DORIS_HOME}/regression-test/realdata"
// sf1DataPath can be url like "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com" or local path like "/data"
//sf1DataPath = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com"

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
excludeSuites = "test_hive_read_orc_complex_type,test_hive_read_parquet,test_hive_read_parquet_complex_type,test_with_and_two_phase_agg,explode,test_cast_function,test_pk_uk_index_change,test_pk_uk_case,window_function,test_profile,test_broker_load_p2,test_spark_load,test_analyze_stats_p1,test_refresh_mtmv,test_bitmap_filter,test_export_parquet,test_doris_jdbc_catalog"

// this directories will not be executed
excludeDirectories = "nereids_tpcds_shape_sf100_p0,nereids_tpch_shape_sf1000_p0,nereids_tpch_shape_sf500_p0,workload_manager_p1"

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// broker load test config
enableBrokerLoad=true

// jdbc connector test config
// To enable jdbc test, you need first start mysql/pg container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableJdbcTest=false
mysql_57_port=7111
pg_14_port=7121
mariadb_10_port=3326
// hive catalog test config
// To enable jdbc test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=false
hms_port=7141
hiveServerPort=10000

// kafka test config
// to enable kafka test, you need firstly to start kafka container
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableKafkaTest=true
kafka_port=19193

// iceberg test config
iceberg_rest_uri_port=18181

enableEsTest=false
es_6_port=19200
es_7_port=29200
es_8_port=39200

cacheDataPath = "/data/regression/"

s3Endpoint = "cos.ap-hongkong.myqcloud.com"
s3BucketName = "doris-build-hk-1308700295"
s3Region = "ap-hongkong"

max_failure_num=50

externalEnvIp="127.0.0.1"
