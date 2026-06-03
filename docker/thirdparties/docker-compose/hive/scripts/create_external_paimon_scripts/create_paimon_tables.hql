CREATE DATABASE IF NOT EXISTS hdfs_db;
USE hdfs_db;
drop table if exists external_test_table;
create table external_test_table(
    a INT COMMENT 'The a field',
    b STRING COMMENT 'The b field'
)
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler';
INSERT INTO external_test_table VALUES(11111111, "hdfs_db_test");

SET hive.metastore.warehouse.dir=s3a://selectdb-qa-datalake-test-hk/paimon_warehouse;
CREATE DATABASE IF NOT EXISTS aws_db;
USE aws_db;
drop table if exists external_test_table;
create external table external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 's3a://selectdb-qa-datalake-test-hk/paimon_warehouse/aws_db.db/hive_test_table';


SET hive.metastore.warehouse.dir=oss://doris-regression-bj/regression/paimon_warehouse;
CREATE DATABASE if not exists ali_db;
USE ali_db;
drop table if exists external_test_table;
create external table external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'oss://doris-regression-bj/regression/paimon_warehouse/ali_db.db/hive_test_table';


SET hive.metastore.warehouse.dir=obs://doris-build/regression/paimon_warehouse;
CREATE DATABASE if not exists hw_db;
USE hw_db;
drop table if exists external_test_table;
create external table external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'obs://doris-build/regression/paimon_warehouse/hw_db.db/hive_test_table';


SET hive.metastore.warehouse.dir=cosn://sdb-qa-datalake-test-1308700295/paimon_warehouse;
CREATE DATABASE if not exists tx_db;
USE tx_db;
drop table if exists external_test_table;
create external table external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'cosn://sdb-qa-datalake-test-1308700295/paimon_warehouse/tx_db.db/hive_test_table';


SET hive.metastore.warehouse.dir=gs://selectdb-qa-datalake-test/paimon_warehouse;
CREATE DATABASE if not exists gcs_db;
USE gcs_db;
drop table if exists external_test_table;
create external table external_test_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'gs://selectdb-qa-datalake-test/paimon_warehouse/gcs_db.db/hive_test_table';
