CREATE DATABASE IF NOT EXISTS hdfs_db;

USE hdfs_db;

CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'hdfs:///user/hive/warehouse/hdfs_db.db/external_test_table';

CREATE DATABASE IF NOT EXISTS ali_db;

USE ali_db;

CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'oss://__OSS_BUCKET__/regression/paimon_warehouse/ali_db.db/hive_test_table';
