CREATE CATALOG IF NOT EXISTS hms_hdfs PROPERTIES ( "type" = "hms", "hive.metastore.uris" = "META_URI" );
CREATE CATALOG IF NOT EXISTS hms_obs PROPERTIES ( "type" = "hms", "hive.metastore.uris" = "META_URI", "obs.secret_key" = "SK_INPUT", "obs.endpoint" = "ENDPOINT", "obs.access_key" = "AK_INPUT" );

CREATE CATALOG IF NOT EXISTS iceberg_hms PROPERTIES ( "type" = "iceberg", "iceberg.catalog.type" = "hms", "hive.metastore.uris" = "META_URI" );
CREATE CATALOG IF NOT EXISTS iceberg_hms_obs PROPERTIES ( "type" = "iceberg", "iceberg.catalog.type" = "hms", "hive.metastore.uris" = "META_URI", "obs.secret_key" = "SK_INPUT", "obs.endpoint" = "ENDPOINT", "obs.access_key" = "AK_INPUT" );
