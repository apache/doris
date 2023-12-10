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
from pyspark.sql.types import *

# create SparkSession object
spark = SparkSession.builder.appName("parquet_format_conversion_tool").config("spark.master", "local").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("spark.hadoop.hive.metastore.uris", "thrift://127.0.0.1:9083").enableHiveSupport().getOrCreate()

# Define the path to the local csv file
csv_file_path = "./sample.csv"
print("source csv_file_path: ", csv_file_path)

# Defines the table structure of the generated file
csv_schema = StructType([
    StructField("k1", StringType(), False),
    StructField("k2", StringType(), False)
])

# csv field delimiter
df = spark.read.format("csv").schema(csv_schema).option("delimiter", "|").load(csv_file_path)

# Displays the first 10 rows of the DataFrame
df.show(10)

# orc file
orc_file_path = "./sample_orc"
print("target orc_file_path: ", orc_file_path)
df.write.format("orc").option("delimiter", ",").option("compression", "snappy").mode("overwrite").save(orc_file_path)

# read ORC file
orc_df = spark.read.orc(orc_file_path)

# show DataFrame schema and A few rows of the DataFrame
print("DataFrame Schema:")
orc_df.printSchema()

print("\nFirst 5 rows of DataFrame:")
orc_df.show(5)

# 获取 DataFrame 的统计信息
print("\nDataFrame Summary Statistics:")
orc_df.describe().show()

print("****************************************************************")

# parquet file
parquet_file_path = "./sample_parquet"
print("target parquet_file_path: ", parquet_file_path)
df.write.format("parquet").option("delimiter", ",").option("compression", "snappy").mode("overwrite").save(parquet_file_path)

# read Parquet file
parquet_df = spark.read.parquet(parquet_file_path)

# Displays the schema and the first few rows of data for the DataFrame
print("DataFrame Schema:")
parquet_df.printSchema()

print("\nFirst 5 rows of DataFrame:")
parquet_df.show(5)

# Get statistics for a DataFrame
print("\nDataFrame Summary Statistics:")
parquet_df.describe().show()

spark.stop()
