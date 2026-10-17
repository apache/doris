-- Paimon tables registered in the kerberized Hive Metastore, consumed by the
-- hdfs_kerberos / hdfs_new_kerberos and oss_hms_kerberos cases of
-- external_table_p2/paimon/test_paimon_hms_catalog.groovy.
--
-- hdfs_db is backed by ../paimon_data, which the entrypoint uploads into HDFS.
-- ali_db points at the existing OSS fixture used by the ordinary Hive Metastore.
--
-- The table declares no columns, so the metastore derives them through the
-- Paimon storage handler (metastore.storage.schema.reader.impl is
-- SerDeStorageSchemaReader) -- the Paimon jar has to be on the metastore's own
-- classpath, not just on the classpath of whoever runs this script.

CREATE DATABASE IF NOT EXISTS hdfs_db;

USE hdfs_db;

DROP TABLE IF EXISTS external_test_table;

CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'hdfs:///user/hive/warehouse/hdfs_db.db/external_test_table';

SET hive.metastore.warehouse.dir=oss://doris-regression-bj/regression/paimon_warehouse;
CREATE DATABASE IF NOT EXISTS ali_db;
USE ali_db;
DROP TABLE IF EXISTS external_test_table;
CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'oss://doris-regression-bj/regression/paimon_warehouse/ali_db.db/hive_test_table';
