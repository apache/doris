jdbcUrl = "jdbc:mysql://127.0.0.1:9033/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9033/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"

testGroups = "p0"
// empty suite will test all suite
testSuites = ""
// empty directories will test all directories
testDirectories = ""

// this groups will not be executed
excludeGroups = ""
// this suites will not be executed
// load_stream_fault_injection may cause bad disk

excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "test_dump_image," +
    "test_index_failure_injection," +
    "test_profile," +
    "test_refresh_mtmv," +
    "test_spark_load," +
    "test_broker_load_func," +
    "test_index_compaction_failure_injection," +
    "test_full_compaction_run_status," +
    "test_topn_fault_injection," + 
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

// this directories will not be executed
excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "cloud," +
    "cloud_p0," +
    "workload_manager_p1," +
    "plsql_p0," + // plsql is not developped any more, add by sk
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
iceberg_minio_port=19001

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
