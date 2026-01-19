use multi_catalog;

CREATE EXTERNAL TABLE `hive_parquet_migrate_iceberg`(
  `new_id` int, 
  `user_info` struct<new_name:string,age:int,tags:array<string>,address:struct<city:string,location:struct<lon:double,new_lat:double>>>, 
  `metrics` map<string,struct<cnt:int,scores:array<int>>>, 
  `dt` string)
STORED AS PARQUET
LOCATION
  'hdfs://127.0.0.1:8320/user/doris/preinstalled_data/parquet_table/hive_parquet_migrate_iceberg'
TBLPROPERTIES (
  'DO_NOT_UPDATE_STATS'='true', 
  'bucketing_version'='2', 
  'current-schema'='{"type":"struct","schema-id":4,"fields":[{"id":1,"name":"new_id","required":false,"type":"int"},{"id":2,"name":"user_info","required":false,"type":{"type":"struct","fields":[{"id":5,"name":"new_name","required":false,"type":"string"},{"id":6,"name":"age","required":false,"type":"int"},{"id":7,"name":"tags","required":false,"type":{"type":"list","element-id":9,"element":"string","element-required":false}},{"id":8,"name":"address","required":false,"type":{"type":"struct","fields":[{"id":10,"name":"city","required":false,"type":"string"},{"id":11,"name":"location","required":false,"type":{"type":"struct","fields":[{"id":13,"name":"lon","required":false,"type":"double"},{"id":12,"name":"new_lat","required":false,"type":"double"}]}}]}}]}},{"id":3,"name":"metrics","required":false,"type":{"type":"map","key-id":14,"key":"string","value-id":15,"value":{"type":"struct","fields":[{"id":16,"name":"cnt","required":false,"type":"int"},{"id":17,"name":"scores","required":false,"type":{"type":"list","element-id":18,"element":"int","element-required":false}}]},"value-required":false}},{"id":4,"name":"dt","required":false,"type":"string"}]}', 
  'current-snapshot-id'='6954944780205564732', 
  'current-snapshot-summary'='{"spark.app.id":"local-1768834818135","added-data-files":"1","added-records":"2","added-files-size":"3839","changed-partition-count":"1","total-records":"4","total-files-size":"3839","total-data-files":"2","total-delete-files":"0","total-position-deletes":"0","total-equality-deletes":"0","engine-version":"3.5.1","app-id":"local-1768834818135","engine-name":"spark","iceberg-version":"Apache Iceberg 1.10.0 (commit 2114bf631e49af532d66e2ce148ee49dd1dd1f1f)"}', 
  'current-snapshot-timestamp-ms'='1768834979913', 
  'default-partition-spec'='{"spec-id":0,"fields":[{"name":"dt","transform":"identity","source-id":4,"field-id":1000}]}', 
  'metadata_location'='hdfs://127.0.0.1:8320/user/doris/preinstalled_data/parquet_table/hive_parquet_migrate_iceberg/metadata/00005-1ac61e8e-6e1b-4b69-a51f-9d465fb7c8b1.metadata.json', 
  'migrated'='true', 
  'previous_metadata_location'='hdfs://127.0.0.1:8320/user/doris/preinstalled_data/parquet_table/hive_parquet_migrate_iceberg/metadata/00004-0ae4bed7-9f69-4400-abcd-a0a584e039fa.metadata.json', 
  'schema.name-mapping.default'='[ {\n  "field-id" : 1,\n  "names" : [ "new_id", "id" ]\n}, {\n  "field-id" : 2,\n  "names" : [ "user_info" ],\n  "fields" : [ {\n    "field-id" : 5,\n    "names" : [ "name", "new_name" ]\n  }, {\n    "field-id" : 6,\n    "names" : [ "age" ]\n  }, {\n    "field-id" : 7,\n    "names" : [ "tags" ],\n    "fields" : [ {\n      "field-id" : 9,\n      "names" : [ "element" ]\n    } ]\n  }, {\n    "field-id" : 8,\n    "names" : [ "address" ],\n    "fields" : [ {\n      "field-id" : 10,\n      "names" : [ "city" ]\n    }, {\n      "field-id" : 11,\n      "names" : [ "location" ],\n      "fields" : [ {\n        "field-id" : 12,\n        "names" : [ "new_lat", "lat" ]\n      }, {\n        "field-id" : 13,\n        "names" : [ "lon" ]\n      } ]\n    } ]\n  } ]\n}, {\n  "field-id" : 3,\n  "names" : [ "metrics" ],\n  "fields" : [ {\n    "field-id" : 14,\n    "names" : [ "key" ]\n  }, {\n    "field-id" : 15,\n    "names" : [ "value" ],\n    "fields" : [ {\n      "field-id" : 16,\n      "names" : [ "cnt" ]\n    }, {\n      "field-id" : 17,\n      "names" : [ "scores" ],\n      "fields" : [ {\n        "field-id" : 18,\n        "names" : [ "element" ]\n      } ]\n    } ]\n  } ]\n}, {\n  "field-id" : 4,\n  "names" : [ "dt" ]\n} ]', 
  'snapshot-count'='2', 
  'table_type'='ICEBERG', 
  'transient_lastDdlTime'='1768834964', 
  'uuid'='36183bd2-9cde-4a4e-87a4-48c8a546a7c9', 
  'write.parquet.compression-codec'='zstd');


-- CREATE TABLE hive_parquet_migrate_iceberg (
--   id INT,
--   user_info STRUCT<
--     name: STRING,
--     age: INT,
--     tags: ARRAY<STRING>,
--     address: STRUCT<
--       city: STRING,
--       location: STRUCT<
--         lat: DOUBLE,
--         lon: DOUBLE
--       >
--     >
--   >,
--   metrics MAP<
--     STRING,
--     STRUCT<
--       cnt: INT,
--       scores: ARRAY<INT>
--     >
--   >
-- ) partitioned by (dt string)
-- STORED AS PARQUET
-- location '/user/doris/preinstalled_data/parquet_table/hive_parquet_migrate_iceberg';

-- -- migrate
-- INSERT INTO hive_parquet_migrate_iceberg PARTITION(dt='100') VALUES
-- (
--   1,
--   named_struct(
--     'name', 'alice',
--     'age', 30,
--     'tags', array('engineer', 'hadoop'),
--     'address', named_struct(
--       'city', 'beijing',
--       'location', named_struct(
--         'lat', CAST(39.9042 AS DOUBLE),
--         'lon', CAST(116.4074 AS DOUBLE)
--       )
--     )
--   ),
--   map(
--     'click', named_struct(
--       'cnt', 10,
--       'scores', array(1, 2, 3)
--     ),
--     'view', named_struct(
--       'cnt', 100,
--       'scores', array(5, 6)
--     )
--   )
-- ),
-- (
--   2,
--   named_struct(
--     'name', 'bob',
--     'age', 25,
--     'tags', array('spark', 'sql'),
--     'address', named_struct(
--       'city', 'shanghai',
--       'location', named_struct(
--         'lat', CAST(31.2304 AS DOUBLE),
--         'lon', CAST(121.4737 AS DOUBLE)
--       )
--     )
--   ),
--   map(
--     'purchase', named_struct(
--       'cnt', 3,
--       'scores', array(88, 90)
--     )
--   )
-- );

-- -- spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions   --hiveconf hive.metastore.uris=thrift://xxx:9383  --conf spark.sql.catalog.spark_catalog.type=hive  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog  --conf spark.sql.catalogImplementation=hive

-- CALL system.migrate(
-- table => 'hive_parquet_migrate_iceberg'
-- );

-- alter table hive_parquet_migrate_iceberg rename column id to new_id;
-- alter table hive_parquet_migrate_iceberg rename column user_info.name to new_name;
-- alter table hive_parquet_migrate_iceberg rename column user_info.address.location.lat to new_lat;
-- alter table hive_parquet_migrate_iceberg alter column user_info.address.location.lon first;

-- INSERT INTO hive_parquet_migrate_iceberg VALUES
-- (
--   3,
--   named_struct(
--     'new_name', 'carol',
--     'age', 28,
--     'tags', array('flink', 'stream'),
--     'address', named_struct(
--       'city', 'shenzhen',
--       'location', named_struct(
--         'lon', CAST(114.0579 AS DOUBLE),
--         'new_lat', CAST(22.5431 AS DOUBLE)
--       )
--     )
--   ),
--   map(
--     'click', named_struct(
--       'cnt', 56,
--       'scores', array(2, 4, 6)
--     )
--   ),"200"
-- ),
-- (
--   4,
--   named_struct(
--     'new_name', 'dave',
--     'age', 35,
--     'tags', array('hive', 'warehouse'),
--     'address', named_struct(
--       'city', 'guangzhou',
--       'location', named_struct(
--         'lon', CAST(113.2644 AS DOUBLE),
--         'new_lat', CAST(23.1291 AS DOUBLE)
--       )
--     )
--   ),
--   map(
--     'view', named_struct(
--       'cnt', 1000,
--       'scores', array(1)
--     )
--   ),"200"
-- );
