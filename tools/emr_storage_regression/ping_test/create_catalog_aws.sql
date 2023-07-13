CREATE CATALOG hms_hdfs PROPERTIES ( "type" = "hms", "hive.metastore.uris" = "META_URI" );
CREATE CATALOG hms_s3 PROPERTIES ( "type" = "hms", "hive.metastore.uris" = "META_URI", "s3.secret_key" = "SK_INPUT", "s3.endpoint" = "ENDPOINT", "s3.access_key" = "AK_INPUT" );

CREATE CATALOG iceberg_hms PROPERTIES ( "type" = "iceberg", "iceberg.catalog.type" = "hms", "hive.metastore.uris" = "META_URI" );
CREATE CATALOG iceberg_hms_s3 PROPERTIES ( "type" = "iceberg", "iceberg.catalog.type" = "hms", "hive.metastore.uris" = "META_URI", "s3.secret_key" = "SK_INPUT", "s3.endpoint" = "ENDPOINT", "s3.access_key" = "AK_INPUT" );

-- glue s3