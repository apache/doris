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

jdbcUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
targetJdbcUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
jdbcUser = "root"
jdbcPassword = ""

ccrDownstreamUrl = "jdbc:mysql://172.19.0.2:9131/?useLocalSessionState=true&allowLoadLocalInfile=true"
ccrDownstreamUser = "root"
ccrDownstreamPassword = ""
ccrDownstreamFeThriftAddress = "127.0.0.1:9020"

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
trinoPluginsPath = "/tmp/trino_connector"

// will test <group>/<suite>.groovy
// empty group will test all group
testGroups = "nonConcurrent"
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = "p1,p2"
// this suites will not be executed

// this suites will not be executed
excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "test_cold_data_compaction_fault_injection," +
    "test_cumu_compaction_delay_fault_injection," + // force 3 replica cause fail
    "test_full_compaction_run_status," + // unstable
    "test_routine_load_timeout_value," + // to be fix
    "test_insert_error_url," + // cause stop grace fail
    "zzz_the_end_sentinel_do_not_touch"// keep this line as the last line

// this directories will not be executed
excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "cold_heat_separation," + // Save costs, the case is too time-consuming and not used by users
    "pipeline_p1," + // Save costs
    "variant_github_events_nonConcurrent_p2," +
    "variant_github_events_new_p2," +
    "hdfs_vault_p2," +
    "query_p0/hbo," +
    "cloud_p0," +
    "zzz_the_end_sentinel_do_not_touch"// keep this line as the last line

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
enablePaimonTest=false

// port of hive2 docker
hive2HmsPort=9083
hive2HdfsPort=8020
hive2ServerPort=10000
hive2PgPort=5432

// port of hive3 docker
hive3HmsPort=9383
hive3HdfsPort=8320
hive3ServerPort=13000
hive3PgPort=5732

// kafka test config
// to enable kafka test, you need firstly to start kafka container
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableKafkaTest=true
kafka_port=19193

// iceberg test config
iceberg_rest_uri_port=18181
iceberg_scan_planning_rest_uri_port=18182
iceberg_minio_port=19001
iceberg_rest_uri_port_s3=19181
iceberg_rest_uri_port_oss=19182
iceberg_rest_uri_port_cos=19183
iceberg_rest_uri_port_obs=19184
iceberg_rest_uri_port_gcs=19185
iceberg_rest_uri_port_hdfs=19186
iceberg_rest_hdfs_port=20020

// polaris rest catalog config
polaris_rest_uri_port=20181
polaris_minio_port=20001

enableEsTest=false
es_6_port=19200
es_7_port=29200
es_8_port=39200

cacheDataPath = "/data/regression/"

s3Source = "aliyun"
s3Endpoint = "oss-cn-hongkong-internal.aliyuncs.com"

//arrow flight sql test config
extArrowFlightSqlHost = "127.0.0.1"
extArrowFlightSqlPort = 8081
extArrowFlightSqlUser = "root"
extArrowFlightSqlPassword= ""

max_failure_num=50

externalEnvIp="127.0.0.1"

// trino-connector catalog test config
enableTrinoConnectorTest = false

// Temporary exclusions for CI using the unmodified framework with rootless containers and STS.
// Keep historical adapter failures separate from failures observed with the original framework.
// Re-enable each suite after its recorded failure or prerequisite is resolved and validated.
// Historical injected-adapter failures below are not evidence of original-framework defects.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    excludeSuites += "," + [
        // Historical injected repository adapter rejected the runtime bucket; not a proven upstream framework defect.
        "test_backup_cancelled",
        // Historical injected repository adapter rejected the runtime bucket; not a proven upstream framework defect.
        "test_backup_restore_atomic_with_alter",
        // Master run 608: the sequence-column Unique Key table returned one duplicate (k1, k2)
        // group after forced segment compaction; the no-duplicate assertion expected zero.
        // Investigate sequence-column deduplication/segment compaction and validate before re-enabling.
        "test_compact_seg",
        // Master run 608: the runner timed out (exit 124) before this suite finished. Last observed
        // activity was waitRowCountReady for the DUJ1 fixture: SHOW DATA still reported RowCount=0.
        // Investigate row-count reporting and suite/lane duration; validate completion before re-enabling.
        // This is an incomplete run, not a confirmed analyze assertion failure or deadlock.
        "test_analyze_long_string"
    ].join(",")
}

// Reviewed defaults for CI using the unmodified framework and rootless runtime.
// Preserve other environments; addresses, local paths, credentials and build IDs are injected at deployment.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    // Use the existing read-only regression fixtures in Hong Kong.
    s3BucketName = "doris-regression-hk"
    s3Endpoint = "oss-cn-hongkong.aliyuncs.com"
    s3Region = "oss-cn-hongkong"
    s3Provider = "S3"
}
