-- Paimon table registered in the kerberized Hive Metastore, consumed by the
-- hdfs_kerberos / hdfs_new_kerberos cases of
-- external_table_p2/paimon/test_paimon_hms_catalog.groovy.
--
-- hdfs_db is backed by ../paimon_data, which the entrypoint uploads into HDFS.
-- (branch-4.0 additionally registers ali_db over OSS here; master has no case
-- that reads it through this metastore, so it is deliberately not provisioned.)
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
