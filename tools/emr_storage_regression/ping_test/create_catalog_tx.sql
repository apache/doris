CREATE CATALOG IF NOT EXISTS hms_hdfs PROPERTIES ( "type" = "hms", "hive.metastore.uris" = "META_URI" );
CREATE CATALOG IF NOT EXISTS hms_cos PROPERTIES ( "type" = "hms", "hive.metastore.uris" = "META_URI", "cos.secret_key" = "SK_INPUT", "cos.endpoint" = "ENDPOINT", "cos.access_key" = "AK_INPUT" );

CREATE CATALOG IF NOT EXISTS iceberg_hms PROPERTIES ( "type" = "iceberg", "iceberg.catalog.type" = "hms", "hive.metastore.uris" = "META_URI" );
CREATE CATALOG IF NOT EXISTS iceberg_hms_cos PROPERTIES ( "type" = "iceberg", "iceberg.catalog.type" = "hms", "hive.metastore.uris" = "META_URI", "cos.secret_key" = "SK_INPUT", "cos.endpoint" = "ENDPOINT", "cos.access_key" = "AK_INPUT" );
