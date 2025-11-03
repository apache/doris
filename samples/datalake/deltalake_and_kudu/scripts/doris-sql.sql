create catalog delta_lake properties (
  "type"="trino-connector",
  "trino.connector.name"="delta_lake",
  "trino.hive.metastore.uri"="thrift://hive-metastore:9083",
  "trino.hive.s3.endpoint"="http://minio:9000",
  "trino.hive.s3.region"="us-east-1",
  "trino.hive.s3.aws-access-key"="minio",
  "trino.hive.s3.aws-secret-key"="minio123",
  "trino.hive.s3.path-style-access"="true"
);


CREATE CATALOG `kudu_catalog` PROPERTIES (
    "type" = "trino-connector",
    "trino.connector.name" = "kudu",
    "trino.kudu.authentication.type" = "NONE",
    "trino.kudu.client.master-addresses" = "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251"
);

ALTER SYSTEM ADD BACKEND 'doris-env:9050';
