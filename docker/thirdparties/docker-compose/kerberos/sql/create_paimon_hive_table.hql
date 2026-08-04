-- Paimon tables registered in the kerberized Hive Metastore, consumed by
-- external_table_p2/paimon/test_paimon_hms_catalog.groovy.
--
-- hdfs_db is backed by ../paimon_data, which the entrypoint uploads into HDFS.
-- ali_db carries no data of its own: it points at the same OSS warehouse the
-- Hive3 stack registers as ali_db, so this is a second metastore entry over one
-- copy of the data. __OSS_BUCKET__ is substituted with ${OSSBucket}.
--
-- Neither table declares columns, so the metastore derives them through the
-- Paimon storage handler (metastore.storage.schema.reader.impl is
-- SerDeStorageSchemaReader) -- the Paimon jar has to be on the metastore's own
-- classpath, not just on the classpath of whoever runs this script.

CREATE DATABASE IF NOT EXISTS hdfs_db;

USE hdfs_db;

DROP TABLE IF EXISTS external_test_table;

CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'hdfs:///user/hive/warehouse/hdfs_db.db/external_test_table';

CREATE DATABASE IF NOT EXISTS ali_db;

USE ali_db;

DROP TABLE IF EXISTS external_test_table;

CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'oss://__OSS_BUCKET__/regression/paimon_warehouse/ali_db.db/hive_test_table';
