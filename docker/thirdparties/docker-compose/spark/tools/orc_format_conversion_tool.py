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


# dataname.tablename format in hive
origin_db_table = "zgq.t2"
target_db_name = "orc_db"
target_table_name = "table_01"

# create SparkSession object
spark = SparkSession.builder.appName("parquet_format_conversion_tool").config("spark.master", "local").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("spark.hadoop.hive.metastore.uris", "thrift://127.0.0.1:9083").enableHiveSupport().getOrCreate()

orc_output_path = "hdfs://127.0.0.1:8020/user/hive/warehouse/" + target_db_name + "/" + target_table_name
print("target parquet file path: ", orc_output_path)

hive_table_df = spark.table(origin_db_table)
print("source data: ")
hive_table_df.show(10)

# orc.compress: Compression method. Optional values: NONE, ZLIB, SNAPPY, LZO. Default value: ZLIB.
# orc.compress.size: Compression block size. Default value: 262144.
# orc.stripe.size: Stripe size of ORC file. Default value: 268435456.
# orc.row.index.stride: Row index stride. Default value: 10000.
# orc.bloom.filter.columns: Names of columns using Bloom filter.
# orc.bloom.filter.fpp: False positive probability of Bloom filter. Default value: 0.05.
# orc.dictionary.key.threshold: Threshold for dictionary encoding columns. Default value: 1.0.
# orc.enable.indexes: Whether to enable indexes. Default value: true.
# orc.create.index: Whether to create indexes when writing ORC files. Default value: true.
# orc.bloom.filter.storage.bitset: Storage method of Bloom filter, BitSet or BitArray. Default value: true.
# orc.bloom.filter.write.max.memory: Maximum memory for writing Bloom filter. Default value: 268435456.
# orc.bloom.filter.page.size: Page size of Bloom filter. Default value: 1024.
# save as orc format file Different properties can be set according to requirements
# These properties can be set using the .option("property_name", "property_value") method, for example:


hive_table_df.write.format("orc").option("compression", "zlib").option("orc.create.index", "true").option("orc.stripe.size", "268435456").option("orc.row.index.stride", "10000").option("orc.bloom.filter.columns", "col1,col2").option("orc.bloom.filter.fpp", "0.05").option("orc.dictionary.key.threshold", "1.0").option("orc.encoding.strategy", "SPEED").mode("overwrite").save(orc_output_path)

# Read the ORC file
orc_df = spark.read.orc(orc_output_path)

# Displays the schema and the first few rows of data for the DataFrame
print("DataFrame Schema:")
orc_df.printSchema()

print("\nFirst 5 rows of DataFrame:")
orc_df.show(5)

# Get statistics for a DataFrame
print("\nDataFrame Summary Statistics:")
orc_df.describe().show()

spark.stop()
