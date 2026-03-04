// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

val dbName    = "format_v3"
val tableName = "dv_test"
val fullTable = s"$dbName.$tableName"

spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")


spark.sql(s""" drop table if exists $fullTable """)
spark.sql(s"""
CREATE TABLE $fullTable (
  id INT,
  batch INT,
  data STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
""")

import spark.implicits._

val batch1 = Seq(
  (1, 1, "a"), (2, 1, "b"), (3, 1, "c"), (4, 1, "d"),
  (5, 1, "e"), (6, 1, "f"), (7, 1, "g"), (8, 1, "h")
).toDF("id", "batch", "data")
 .coalesce(1)

batch1.writeTo(fullTable).append()

spark.sql(s"""
DELETE FROM $fullTable
WHERE batch = 1 AND id IN (3, 4, 5)
""")

val batch2 = Seq(
  (9,  2, "i"), (10, 2, "j"), (11, 2, "k"), (12, 2, "l"),
  (13, 2, "m"), (14, 2, "n"), (15, 2, "o"), (16, 2, "p")
).toDF("id", "batch", "data")
 .coalesce(1)

batch2.writeTo(fullTable).append()

spark.sql(s"""
DELETE FROM $fullTable
WHERE batch = 2 AND id >= 14
""")

spark.sql(s"""
DELETE FROM $fullTable
WHERE id % 2 = 1
""")


// spark.sql(s""" select count(*) from $fullTable """).show()


// v2 to v3.

val tableName = "dv_test_v2"
val fullTable = s"$dbName.$tableName"
spark.sql(s""" drop table if exists $fullTable """)

spark.sql(s"""
CREATE TABLE $fullTable (
  id INT,
  batch INT,
  data STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
""")

batch1.writeTo(fullTable).append()

spark.sql(s"""
DELETE FROM $fullTable
WHERE batch = 1 AND id IN (3, 4, 5)
""")

spark.sql(s"""
ALTER TABLE $fullTable
SET TBLPROPERTIES ('format-version' = '3')
""")

spark.sql(s"""
DELETE FROM $fullTable
WHERE id % 2 = 1
""")


// spark.sql(s""" select * from $fullTable order by id """).show()


val tableName = "dv_test_1w"
val fullTable = s"$dbName.$tableName"
spark.sql(s""" drop table if exists $fullTable """)

spark.sql(s"""
CREATE TABLE $fullTable (
  id        BIGINT,
  grp       INT,
  value     INT,
  ts        TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
  'format-version'='3',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read',
  'write.parquet.row-group-size-bytes'='10240'
)
""")

import org.apache.spark.sql.functions._

val df = spark.range(0, 100000).select(
    col("id"),
    (col("id") % 100).cast("int").as("grp"),
    (rand() * 1000).cast("int").as("value"),
    current_timestamp().as("ts")
  )

df.repartition(10).writeTo(fullTable).append()


spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.adaptive.enabled", "false")


spark.sql(s"""
DELETE FROM $fullTable
WHERE id%2 = 1
""")

spark.sql(s"""
DELETE FROM $fullTable
WHERE id%3 = 1
""")

// spark.sql(s""" select count(*) from $fullTable """).show()
