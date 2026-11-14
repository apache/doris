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
testGroups = ""
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = "p1,p2"
// this suites will not be executed
// load_stream_fault_injection may cause bad disk

excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "test_dump_image," +
    "test_index_failure_injection," +
    "test_profile," +
    "test_refresh_mtmv," +
    "test_spark_load," +
    "test_index_compaction_failure_injection," +
    "test_full_compaction_run_status," +
    "test_topn_fault_injection," + 
    "rec_cte_with_delete_test," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

// this directories will not be executed
excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "cloud," +
    "cloud_p0," +
    "external_table_p0/remote_doris," + // ubsan issue, need to investigate
    "workload_manager_p1," +
    "plsql_p0," + // plsql is not developped any more, add by sk
    "restore_p0," +
    "variant_p0/nested," +
    "variant_p0/nested/sql," +
    "zzz_the_end_sentinel_do_not_touch"// keep this line as the last line

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

max_failure_num=-1

externalEnvIp="127.0.0.1"

// trino-connector catalog test config
enableTrinoConnectorTest = false

// Temporary exclusions for CI using the unmodified framework with rootless containers and STS.
// CI exclusions describe environment prerequisites only. Product assertions,
// timeouts and historical failures of removed adapters must execute normally.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    // Suite-name filtering also selects same-named export_p0 outfile suites.
    excludeSuites += "," + [
        // Requires the shared parent-database fixture; restore when that initializer runs in this lane.
        "test_nested_types_insert_into_with_s3",
        // Requires the shared parent-database fixture; restore when that initializer runs in this lane.
        "test_nestedtypes_csv_insert_into_with_s3",
        // Requires the shared parent-database fixture; restore when that initializer runs in this lane.
        "test_nestedtypes_json_insert_into_with_s3",
        // Requires root SSH/SCP or remote filesystem access; restore with the rootless filesystem contract.
        "test_export_view",
        // Requires sudo/Docker fixtures; restore after the original suite supports rootless CI.
        "test_single_hive_kerberos",
        // Requires sudo/Docker fixtures; restore after the original suite supports rootless CI.
        "test_two_hive_kerberos",
        // Requires root SSH/SCP or remote filesystem access; restore with the rootless filesystem contract.
        "test_parquet_meta_tvf",
        // Requires a mysqldump client with compatible column-statistics options; restore after the client probe passes.
        "test_mysqldump",
        // Requires root SSH/SCP or remote filesystem access; restore with the rootless filesystem contract.
        "test_outfile",
        // Requires root SSH/SCP or remote filesystem access; restore with the rootless filesystem contract.
        "test_outfile_expr",
        // Requires root SSH/SCP or remote filesystem access; restore with the rootless filesystem contract.
        "test_outfile_parquet",
        // Requires root SSH/SCP or remote filesystem access; restore with the rootless filesystem contract.
        "test_install_plugin_command",
        // Requires regressionAliyunStsRegion, regressionAliyunStsRoleArn and AssumeRole access; restore once supplied.
        "test_s3_load_with_tmp_token"
    ].join(",")
    // Preserve the Hong Kong regression fixtures and their public HTTPS endpoint.
    s3BucketName = "doris-regression-hk"
    s3Endpoint = "oss-cn-hongkong.aliyuncs.com"
    s3Region = "oss-cn-hongkong"
    // OSS requires its hostname addressing; CI must not silently replace the S3 provider.
    s3Provider = "OSS"
}
