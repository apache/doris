# BROKER LOAD
Description

Broker load accesses data from corresponding data sources and imports data through broker deployed with Palo cluster.
You can view the deployed broker through the show broker command.
The following four data sources are currently supported:

1. Baidu HDFS: Baidu's internal HDFS are limited to Baidu's internal use.
2. Baidu AFS: Baidu's internal AFs are limited to Baidu's internal use.
3. Baidu Object Storage (BOS): Baidu Object Storage. Only Baidu internal users, public cloud users or other users who can access BOS.
Four. Apache HDFS

Grammar:

LOAD LABEL load_label
(
Date of date of date of entry
)
WITH BROKER broker_name
[broker_properties]
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

NEGATIVE：
If this parameter is specified, it is equivalent to importing a batch of "negative" data. Used to offset the same batch of data imported before.
This parameter applies only to the case where there are value columns and the aggregation type of value columns is SUM only.

Column U separator:

Used to specify the column separator in the import file. Default tot
If the character is invisible, it needs to be prefixed with \x, using hexadecimal to represent the separator.
For example, the separator X01 of the hive file is specified as "\ x01"

File type:

Used to specify the type of imported file, such as parquet, csv. The default value is determined by the file suffix name.

column_list：

Used to specify the correspondence between columns in the import file and columns in the table.
When you need to skip a column in the import file, specify it as a column name that does not exist in the table.
Grammar:
(col_name1, col_name2, ...)

SET:

If this parameter is specified, a column of the source file can be transformed according to a function, and then the transformed result can be imported into the table.
The functions currently supported are:

Strftime (fmt, column) date conversion function
Fmt: Date format, such as% Y% m% d% H% i% S (year, month, day, hour, second)
Column: Column in column_list, which is the column in the input file. Storage content should be a digital timestamp.
If there is no column_list, the columns of the input file are entered by default in the column order of the Palo table.
Note: The digital timestamp is in seconds.

time_format(output_fmt, input_fmt, column) 日期格式转化
Output_fmt: Converted date format, such as% Y% m% d% H% i% S (year, month, day, hour, second)
Input_fmt: The date format of the column before transformation, such as% Y%m%d%H%i%S (year, month, day, hour, second)
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

Now () sets the data imported by a column to the point at which the import executes. The column must be of DATE/DATETIME type.

Three. broker name

The name of the broker used can be viewed through the show broker command.

4. broker_properties

Used to provide information to access data sources through broker. Different brokers, as well as different access methods, need to provide different information.

1. HDFS /AFS Baidu

Access to Baidu's internal hdfs/afs currently only supports simple authentication, which needs to be provided:
Username: HDFS username
password -hdfs

2. BOS

Need to provide:
Bos_endpoint: endpoint of BOS
Bos_accesskey: Accesskey for public cloud users
Bos_secret_access key: secret_access key for public cloud users

Three. Apache HDFS

Community version of HDFS supports simple authentication and Kerberos authentication. And support HA configuration.
Simple authentication:
hadoop.security.authentication = simple (默认)
Username: HDFS username
password -hdfs

Kerberos authentication:
hadoop.security.authentication = kerberos
Kerberos_principal: Specifies the principal of Kerberos
Kerberos_keytab: Specifies the KeyTab file path for kerberos. This file must be a file on the server where the broker process resides.
Kerberos_keytab_content: Specifies the content of the KeyTab file in Kerberos after base64 encoding. This is a choice from the kerberos_keytab configuration.

HA code
By configuring namenode HA, new namenode can be automatically identified when the namenode is switched
Dfs. nameservices: Specify the name of the HDFS service and customize it, such as: "dfs. nameservices" = "my_ha"
Dfs.ha.namenodes.xxx: Customize the name of the namenode, with multiple names separated by commas. Where XXX is a custom name in dfs. name services, such as "dfs. ha. namenodes. my_ha" = "my_nn"
Dfs.namenode.rpc-address.xxx.nn: Specify RPC address information for namenode. Where NN denotes the name of the namenode configured in dfs.ha.namenodes.xxx, such as: "dfs.namenode.rpc-address.my_ha.my_nn"= "host:port"
Dfs.client.failover.proxy.provider: Specifies the provider that client connects to namenode by default: org.apache.hadoop.hdfs.server.namenode.ha.Configured Failover ProxyProvider

4. opt_properties

Used to specify some special parameters.
Grammar:
[PROPERTIES ("key"="value", ...)]

The following parameters can be specified:
Timeout: Specifies the timeout time of the import operation. The default timeout is 4 hours. Unit seconds.
Max_filter_ratio: The ratio of data that is most tolerant of being filterable (for reasons such as data irregularities). Default zero tolerance.
Exec_mem_limit: Sets the upper memory limit for import use. Default is 2G, unit byte. This refers to the upper memory limit of a single BE node.
An import may be distributed across multiple BEs. We assume that processing 1GB data at a single node requires up to 5GB of memory. Assuming that a 1GB file is distributed among two nodes, then theoretically, each node needs 2.5GB of memory. Then the parameter can be set to 268454560, or 2.5GB.
Strict mode: is there a strict restriction on data? The default is true.

5. Import data format sample

Integer classes (TINYINT/SMALLINT/INT/BIGINT/LARGEINT): 1,1000,1234
Floating Point Class (FLOAT/DOUBLE/DECIMAL): 1.1, 0.23, 356

(Note: If it's in other date formats, you can use strftime or time_format functions to convert in the import command)
字符串类（CHAR/VARCHAR）："I am a student", "a"
NULL value: N

'35;'35; example

1. Import a batch of data from HDFS, specifying the timeout time and filtering ratio. Use the broker with the inscription my_hdfs_broker. Simple authentication.

LOAD LABEL example db.label1
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
)
WITH BROKER my_hdfs_broker
(
"Username" = "HDFS\\ user"
"password" = "hdfs_passwd"
)
PROPERTIES
(
Timeout ="3600",
"max_filter_ratio" = "0.1"
);

Where hdfs_host is the host of the namenode and hdfs_port is the fs.defaultFS port (default 9000)

2. A batch of data from AFS, including multiple files. Import different tables, specify separators, and specify column correspondences.

LOAD LABEL example db.label2
(
DATA INFILE ("afs http://afs host:hdfs /u port /user /palo /data /input /file1")
INTO TABLE `my_table_1`
COLUMNS TERMINATED BY ","
(k1, k3, k2, v1, v2),
DATA INFILE ("afs http://afs host:hdfs /u port /user /palo /data /input /file2")
INTO TABLE `my_table_2`
COLUMNS TERMINATED BY "\t"
(k1, k2, k3, v2, v1)
)
WITH BROKER my_afs_broker
(
"username" ="abu user",
"password" = "afs_passwd"
)
PROPERTIES
(
Timeout ="3600",
"max_filter_ratio" = "0.1"
);


3. Import a batch of data from HDFS, specify hive's default delimiter x01, and use wildcard * to specify all files in the directory.
Use simple authentication and configure namenode HA at the same time

LOAD LABEL example db.label3
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
INTO TABLE `my_table`
COLUMNS TERMINATED BY "\\x01"
)
WITH BROKER my_hdfs_broker
(
"Username" = "HDFS\\ user"
"password" = "hdfs_passwd",
"dfs.nameservices" = "my_ha",
"dfs.ha.namodes.my -ha" ="we named1, we named2",
"dfs.namode.rpc -address.my ha.my name1" ="nn1 guest:rpc port",
"dfs.namode.rpc -address.my ha.my name2" ="nn2 guest:rpc port",
"dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)

4. Import a batch of "negative" data from HDFS. At the same time, Kerberos authentication is used. Provide KeyTab file path.

LOAD LABEL example db.label4
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file)
NEGATIVE
INTO TABLE `my_table`
COLUMNS TERMINATED BY "\t"
)
WITH BROKER my_hdfs_broker
(
"hadoop.security.authentication" = "kerberos",
"kerberos" principal ="doris @YOUR.COM",
"kerberos" keytab ="/home /palo /palo.keytab"
)

5. Import a batch of data from HDFS and specify partitions. At the same time, Kerberos authentication is used. Provides the KeyTab file content encoded by base64.

LOAD LABEL example db.label5
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
(k1, k3, k2, v1, v2)
)
WITH BROKER my_hdfs_broker
(
"hadoop.security.authentication"="kerberos",
"kerberos" principal ="doris @YOUR.COM",
"kerberos" keytab "content"="BQIAAABEAAEACUJBSURVLkNPTQAEcGFsbw"
)

6. Import a batch of data from BOS, specify partitions, and make some transformations to the columns of imported files, as follows:
The table structure is as follows:
K1 date
date
k3 bigint
k4 varchar (20)
k5 varchar (64)
k6 int

Assume that the data file has only one row of data:

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
DATA INFILE("bos://my_bucket/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
(tmp /u k1, tmp /u k2, tmp /u k3, k6, v1)
SET (

K2 = Time = UFormat ("% Y -% M -% D% H:% I = S", "% Y -% M -% D", TMP = UK2),
k3 = alignment_timestamp("day", tmp_k3),
k4 = default_value("1"),
K5 = MD5Sum (TMP = UK1, TMP = UK2, TMP = UK3)
k6 = replace value ("-", "10")
)
)
WITH BROKER my_bos_broker
(
"bosu endpoint" ="http://bj.bcebos.com",
"bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
"bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyy"
)

7. Import data into tables containing HLL columns, which can be columns in tables or columns in data

If there are three columns in the table (id, v1, v2). Where V1 and V2 columns are HLL columns. The imported source file has three columns. In column_list, it is declared that the first column is ID and the second and third column is K1 and k2, which are temporarily named.
In SET, the HLL column in the table must be specifically declared hll_hash. The V1 column in the table is equal to the hll_hash (k1) column in the original data.
LOAD LABEL example db.label7
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
(id, k1, k2)
SET (
v1 = hll, u hash (k1),
v2 = hll, u hash (k2)
)
)
WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

LOAD LABEL example db.label8
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
PARTITION (p1, P2)
COLUMNS TERMINATED BY ","
(k1, k2, tmp u k3, tmp u k4, v1, v2)
SET (
v1 = hll, u hash (tmp
v2 = hll, u hash (tmp
)
)
WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

8. Importing data into Parquet file specifies FORMAT as parquet, which is judged by file suffix by default.
LOAD LABEL example db.label9
(
DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
INTO TABLE `my_table`
FORMAT AS "parquet"
(k1, k2, k3)
)
WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

## keyword
BROKER,LOAD
