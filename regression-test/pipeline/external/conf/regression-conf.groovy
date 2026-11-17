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

//arrow flight sql test config
extArrowFlightSqlHost = "172.19.0.2"
extArrowFlightSqlPort = 8081
extArrowFlightSqlUser = "root"
extArrowFlightSqlPassword= ""
extArrowFlightHttpPort= 8131
extFeThriftPort = 9020

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
testGroups = "external"
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
    "test_information_schema_external," +
    "test_profile," +
    "test_paimon_gcs," +
    "test_refresh_mtmv," +
    "test_spark_load," +
    "test_paimon_gcs," +
    "test_stream_stub_fault_injection," +
    "test_iceberg_overwrite_with_wrong_partition," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

// this directories will not be executed
excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "cloud," +
    "cloud_p0," +
    "nereids_rules_p0/subquery," +
    "workload_manager_p1," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

customConf1 = "test_custom_conf_value"

// for test csv with header
enableHdfs=false // set to true if hdfs is ready
hdfsFs = "hdfs://127.0.0.1:9000"
hdfsUser = "doris-test"
hdfsPasswd = ""
brokerName = "broker_name"

// for JuiceFS(hadoop-compatible) regression cases
// first step: enable case execution in external pipeline
enableJfsTest=true
jfsFs = "jfs://cluster"
jfsImpl = "io.juicefs.JuiceFileSystem"
jfsMeta = "mysql://root:123456@(127.0.0.1:3316)/juicefs_meta"
jfsHiveMetastoreUris = "thrift://127.0.0.1:9383"
jfsHadoopUser = "root"

// broker load test config
enableBrokerLoad=true

// jdbc connector test config
// To enable jdbc test, you need first start mysql/pg container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableJdbcTest=true
mysql_57_port=3316
pg_14_port=5442
mariadb_10_port=3326
// hive catalog test config
// To enable jdbc test, you need first start hive container.
// See `docker/thirdparties/start-thirdparties-docker.sh`
enableHiveTest=true

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

// The external regression pipeline starts the fluss component for these suites.
enableFlussTest=true
fluss_coordinator_port=19123
// The object store the tiered half of a fluss table lives in; the suites give
// its endpoint and the minio default credentials to their own catalog, because
// fluss strips every credential-bearing option before answering a client.
fluss_minio_port=19125
// refactor params
enableRefactorParamsTest=true

// iceberg test config
iceberg_rest_uri_port=18181
iceberg_scan_planning_rest_uri_port=18182
// Avoid the hms_port suffix: shared CI rewrites it for the default Hive service.
iceberg_encryption_metastore_port=18383
iceberg_encryption_kms_port=18384
iceberg_encryption_fixture_port=18385
iceberg_rest_uri_port_s3=19181
iceberg_rest_uri_port_oss=19182
iceberg_rest_uri_port_cos=19183
iceberg_rest_uri_port_obs=19184
iceberg_rest_uri_port_gcs=19185
iceberg_rest_uri_port_hdfs=19186
iceberg_rest_hdfs_port=20020
iceberg_minio_port=19001
enableIcebergTest=true

// polaris rest catalog config
polaris_rest_uri_port=20181
polaris_minio_port=20001

enableEsTest=true
es_5_port=59200
es_6_port=19200
es_7_port=29200
es_8_port=39200

cacheDataPath = "/data/regression/"

s3Source = "aliyun"
s3Endpoint = "oss-cn-hongkong-internal.aliyuncs.com"

// for multi cloud test case, eg. paimon_base_filesystem
aliYunAk="***********"
aliYunSk="***********"
txYunAk="***********"
txYunSk="***********"

// max compute catalog test config
enableMaxComputeTest=true

max_failure_num=50

externalEnvIp="127.0.0.1"

enablePaimonTest=true

hdfs_port=8020

oracle_11_port=1521
sqlserver_2022_port=1433
clickhouse_22_port=8123
oceanbase_port=2881
oceanbase_cdc_port=2883
db2_11_port=50000

// trino-connector catalog test config
enableTrinoConnectorTest = true

// kerberos docker config
enableKerberosTest = true
kerberosHmsPort=9883
kerberosHdfsPort=8820
enableNonCatalogKerberosTest = true


// AWS iam role config

// hudi p0 external regression test config
// To enable hudi test, you need first start hudi container.
// See `docker/thirdparties/run-thirdparties-docker.sh -c hudi`
enableHudiTest=true
// hudi catalog config
hudiHmsPort=19083
hudiMinioPort=19100
hudiMinioAccessKey="minio"
hudiMinioSecretKey="minio123"

// Keep rootless CI exclusions opt-in; other external-test environments are unchanged.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    // Temporary environment exclusions using the existing suite-name filter.
    // Re-enable after the rootless execution prerequisites are reviewed and validated.
    excludeSuites += "," + [
        // sudo/Docker fixture; requires case changes for rootless CI
        "test_single_hive_kerberos",
        // sudo/Docker fixture; requires case changes for rootless CI
        "test_two_hive_kerberos",
        // root SSH/SCP or remote filesystem prerequisite
        "test_parquet_meta_tvf"
    ].join(",")
}

// Reviewed defaults for CI using the unmodified framework and rootless runtime.
// Preserve other environments; addresses, local paths, credentials and build IDs are injected at deployment.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    // Use the existing read-only regression fixtures in Hong Kong.
    s3BucketName = "doris-regression-hk"
    s3Endpoint = "oss-cn-hongkong.aliyuncs.com"
    s3Region = "oss-cn-hongkong"
    s3FixtureProtocol = "https"
    ciFixtureBaseUrl = "https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com"
    ciFixtureStsEnabled = true
    ciScopedDynamicFixturesEnabled = true
    ciFixtureProfile = "external"
    // Preserve the existing local filesystem contract for the rootless external lane.
    backendFilesystemTransport = "local-rootless"
    ciTrinoLocalDispatchEnabled = true
    backendLocalPathAllowlist = "[\"/tmp/test_export_view\"]"
    // Preserve the existing build-scoped backup repository; its build ID is injected separately.
    ciBackupRepositoryEnabled = true
    ciBackupRepositoryBucket = "doris-ci-artifacts-1949087886039699-hk"
    ciBackupRepositoryEndpoint = "https://oss-cn-hongkong-internal.aliyuncs.com"
    ciBackupRepositoryRegion = "oss-cn-hongkong"
    ciBackupRepositoryProfile = "external"
    ciBackupRepositoryPrefix = "external"
}
