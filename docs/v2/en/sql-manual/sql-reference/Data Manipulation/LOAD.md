---
{
    "title": "LOAD",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# LOAD
## Description

Palo currently supports the following four import methods:

1. Hadoop Load: Importing ETL based on MR.
2. Broker Load: Use broker to import data.
3. Mini Load: Upload files through HTTP protocol for batch data import.
4. Stream Load: Stream data import through HTTP protocol.
5. S3 Load: Directly access the storage system supporting the S3 protocol for data import through the S3 protocol. The import syntax is basically the same as that of Broker Load.

This help mainly describes the first import method, namely Hadoop Load related help information. The rest of the import methods can use the following commands to view help:

This import method may not be supported in a subsequent version. It is recommended that other import methods be used for data import. !!!

1. help broker load;
2. help mini load;
3. help stream load;

Hadoop Load is only applicable to Baidu's internal environment. Public, private and open source environments cannot use this import approach.
The import method must set up a Hadoop computing queue for ETL, which can be viewed through the help set property command.

Grammar:

LOAD LABEL load_label
(
Date of date of date of entry
)
[opt_properties];

1. load label

The label of the current imported batch. Unique in a database.
Grammar:
[database_name.]your_label

2. data_desc

Used to describe a batch of imported data.
Grammar:
DATA INFILE
(
"file_path1"[, file_path2, ...]
)
[NEGATIVE]
INTO TABLE `table_name`
[PARTITION (p1, P2)]
[COLUMNS TERMINATED BY "column_separator"]
[FORMAT AS "file_type"]
[(column_list)]
[set (k1 = fun (k2)]]

Explain:
file_path:

File paths can be specified to a file, or * wildcards can be used to specify all files in a directory. Wildcards must match to files, not directories.

PARTICIPATION:

If this parameter is specified, only the specified partition will be imported, and data outside the imported partition will be filtered out.
If not specified, all partitions of the table are imported by default.

NEGATIVE:
If this parameter is specified, it is equivalent to importing a batch of "negative" data. Used to offset the same batch of data imported before.
This parameter applies only to the case where there are value columns and the aggregation type of value columns is SUM only.

Column U separator:

Used to specify the column separator in the import file. Default tot
If the character is invisible, it needs to be prefixed with \x, using hexadecimal to represent the separator.
For example, the separator X01 of the hive file is specified as "\ x01"

File type:

Used to specify the type of imported file, such as parquet, orc, csv. The default value is determined by the file suffix name.

column_list:

Used to specify the correspondence between columns in the import file and columns in the table.
When you need to skip a column in the import file, specify it as a column name that does not exist in the table.
Grammar:
(col_name1, col_name2, ...)

SET:

If this parameter is specified, a column of the source file can be transformed according to a function, and then the transformed result can be imported into the table.
The functions currently supported are:

Strftime (fmt, column) date conversion function
Fmt: Date format, such as% Y% m% d% H% M% S (year, month, day, hour, second)
Column: Column in column_list, which is the column in the input file. Storage content should be a digital timestamp.
If there is no column_list, the columns of the input file are entered by default in the column order of the Palo table.

time_format(output_fmt, input_fmt, column) 日期格式转化
Output_fmt: Converted date format, such as% Y% m% d% H% M% S (year, month, day, hour, second)
Input_fmt: The date format of the column before transformation, such as% Y% m% d% H% M% S (days, hours, seconds, months, years)
Column: Column in column_list, which is the column in the input file. Storage content should be a date string in input_fmt format.
If there is no column_list, the columns of the input file are entered by default in the column order of the Palo table.

alignment_timestamp(precision, column) 将时间戳对齐到指定精度
Precision: year 124month;124day;124hour;
Column: Column in column_list, which is the column in the input file. Storage content should be a digital timestamp.
If there is no column_list, the columns of the input file are entered by default in the column order of the Palo table.
Note: When the alignment accuracy is year and month, only the time stamps in the range of 20050101-20191231 are supported.

Default_value (value) sets the default value for a column import
Use default values of columns when creating tables without specifying

Md5sum (column1, column2,...) evaluates the value of the specified imported column to md5sum, returning a 32-bit hexadecimal string

Replace_value (old_value [, new_value]) replaces old_value specified in the import file with new_value
New_value, if not specified, uses the default value of the column when building the table

Hll_hash (column) is used to transform a column in a table or data into a data structure of a HLL column

3. opt_properties

Used to specify some special parameters.
Grammar:
[PROPERTIES ("key"="value", ...)]

The following parameters can be specified:
Cluster: Import the Hadoop computed queue used.
Timeout: Specifies the timeout time of the import operation. The default timeout is 3 days. Unit seconds.
Max_filter_ratio: The ratio of data that is most tolerant of being filterable (for reasons such as data irregularities). Default zero tolerance.
Load_delete_flag: Specifies whether the import deletes data by importing the key column, which applies only to UNIQUE KEY.
Value column is not specified when importing. The default is false.

5. Import data format sample

Integer classes (TINYINT/SMALLINT/INT/BIGINT/LARGEINT): 1,1000,1234
Floating Point Class (FLOAT/DOUBLE/DECIMAL): 1.1, 0.23, 356
Date class (DATE/DATETIME): 2017-10-03, 2017-06-13 12:34:03.
(Note: If it's in other date formats, you can use strftime or time_format functions to convert in the import command)
字符串类（CHAR/VARCHAR）: "I am a student", "a"
NULL value: N

6. S3 Storage
   fs.s3a.access.key  user AK，required
   fs.s3a.secret.key  user SK，required
   fs.s3a.endpoint  user endpoint，required
   fs.s3a.impl.disable.cache  whether disable cache，default true，optional

'35;'35; example

1. Import a batch of data, specify timeout time and filtering ratio. Specify the import queue as my_cluster.

LOAD LABEL example db.label1
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
)
PROPERTIES
(
"cluster" ="my" cluster,
Timeout ="3600",
"max_filter_ratio" = "0.1"
);

Where hdfs_host is the host of the namenode and hdfs_port is the fs.defaultFS port (default 9000)

2. Import a batch of data, including multiple files. Import different tables, specify separators, and specify column correspondences

LOAD LABEL example db.label2
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file1")
INTO TABLE `my_table_1`
COLUMNS TERMINATED BY ","
(k1, k3, k2, v1, v2),
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file2")
INTO TABLE `my_table_2`
COLUMNS TERMINATED BY "\t"
(k1, k2, k3, v2, v1)
);

3. Import a batch of data, specify hive's default delimiter x01, and use wildcard * to specify all files in the directory

LOAD LABEL example db.label3
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
NEGATIVE
INTO TABLE `my_table`
COLUMNS TERMINATED BY "\\x01"
);

4. Import a batch of "negative" data

LOAD LABEL example db.label4
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file)
NEGATIVE
INTO TABLE `my_table`
COLUMNS TERMINATED BY "\t"
);

5. Import a batch of data and specify partitions

LOAD LABEL example db.label5
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
(k1, k3, k2, v1, v2)
);

6. Import a batch of data, specify partitions, and make some transformations to the columns of the imported files, as follows:
The table structure is as follows:
K1 date
date
k3 bigint
k4 varchar (20)
k5 varchar (64)
k6 int

Assume that the data file has only one row of data, five columns, and comma-separated:

1537002087,2018-08-09 11:12:13,1537002087,-,1

The columns in the data file correspond to the columns specified in the import statement:
tmp -u k1, tmp -u k2, tmp u k3, k6, v1

The conversion is as follows:

1) k1: Transform tmp_k1 timestamp column into datetime type data
2) k2: Converting tmp_k2 datetime-type data into date data
3) k3: Transform tmp_k3 timestamp column into day-level timestamp
4) k4: Specify import default value of 1
5) k5: Calculate MD5 values from tmp_k1, tmp_k2, tmp_k3 columns
6) k6: Replace the - value in the imported file with 10

LOAD LABEL example db.label6
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
(tmp /u k1, tmp /u k2, tmp /u k3, k6, v1)
SET (
K1 = strftime (%Y -%m -%d%H:%M:%S ", TMP u K1),
K2 = Time = UFormat ("% Y-% M-% D% H:% M:% S", "% Y-% M-% D", "TMP = UK2),
k3 = alignment_timestamp("day", tmp_k3),
k4 = default_value("1"),
K5 = MD5Sum (TMP = UK1, TMP = UK2, TMP = UK3)
k6 = replace value ("-", "10")
)
);

7. Import data into tables containing HLL columns, which can be columns in tables or columns in data

LOAD LABEL example db.label7
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
SET (
v1 = hll, u hash (k1),
v2 = hll, u hash (k2)
)
);

## keyword
LOAD

