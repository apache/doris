#!usr/bin/python3
# -*- coding=utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession

# source hive dataname.tablename
origin_db_table = "zgq.t2"

target_db_name = "parquet_db"
target_table_name = "table_01"

# create SparkSession object
spark = SparkSession.builder.appName("parquet_format_conversion_tool").config("spark.master", "local").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("spark.hadoop.hive.metastore.uris", "thrift://127.0.0.1:9083").enableHiveSupport().getOrCreate()

output_path = "hdfs://127.0.0.1:8020/user/hive/warehouse/" + target_db_name + "/" + target_table_name

print("target parquet file path", output_path)

# read  Hive table
hive_table_df = spark.table(origin_db_table)
print("source data: ")
hive_table_df.show(10)

# Specify different property configurations
# parquet.compression: Set the compression algorithm, optional values include uncompressed, gzip, snappy, lz4, brotli, zstd, etc.
# parquet.enable.dictionary: Whether to enable dictionary encoding, default is true.
# parquet.writer.version: Parquet write version, optional values are 1 or 2.
# parquet.enable.data.page.v2: Whether to enable data page version 2, default is true.
# parquet.page.size: Data page size, default is 1MB.
# parquet.block.size: Data block size, default is 128MB.
# parquet.dictionary.page.size: Dictionary page size, default is 8KB.
# parquet.enable.dictionary.compression: Whether to enable dictionary page compression, default is false.
# parquet.filter.dictionary: Whether to enable dictionary filtering, default is false.
# parquet.avro.write-old-list-structure: Whether to write old Avro list structure, default is false.
# save as parquet format file Different properties can be set according to requirements
# These properties can be set using the .option("property_name", "property_value") method, for example:

hive_table_df.write.format("parquet").option("compression", "snappy").option("parquet.block.size", "131072").option("parquet.enable.dictionary", "true").mode("overwrite").save(output_path)

# read Parquet file
parquet_df = spark.read.parquet(output_path)

# Displays the schema and the first few rows of data for the DataFrame
print("DataFrame Schema:")
parquet_df.printSchema()

print("\nFirst 5 rows of DataFrame:")
parquet_df.show(5)

# Get statistics for a DataFrame
print("\nDataFrame Summary Statistics:")
parquet_df.describe().show()

spark.stop()