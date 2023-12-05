---
{
    "title": "Spark Load",
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

# Spark Load

Spark load realizes the preprocessing of load data by spark, improves the performance of loading large amount of Doris data and saves the computing resources of Doris cluster. It is mainly used for the scene of initial migration and large amount of data imported into Doris.

Spark load uses the resources of the spark cluster to sort the data to be imported, and Doris be writes files directly, which can greatly reduce the resource usage of the Doris cluster, and is very good for historical mass data migration to reduce the resource usage and load of the Doris cluster. Effect.

If users do not have the resources of Spark cluster and want to complete the migration of external storage historical data conveniently and quickly, they can use [Broker load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) . Compared with Spark load, importing Broker load will consume more resources on the Doris cluster.

Spark load is an asynchronous load method. Users need to create spark type load job by MySQL protocol and view the load results by `show load`.

## Applicable scenarios

* The source data is in a file storage system that spark can access, such as HDFS.

* The data volume ranges from tens of GB to TB.

## Explanation of terms

1. Spark ETL: in the load process, it is mainly responsible for ETL of data, including global dictionary construction (bitmap type), partition, sorting, aggregation, etc.

2. Broker: broker is an independent stateless process. It encapsulates the file system interface and provides the ability of Doris to read the files in the remote storage system.

3. Global dictionary: it stores the data structure from the original value to the coded value. The original value can be any data type, while the encoded value is an integer. The global dictionary is mainly used in the scene of precise de duplication precomputation.

## Basic principles

### Basic process

The user submits spark type load job by MySQL client, Fe records metadata and returns that the user submitted successfully.

The implementation of spark load task is mainly divided into the following five stages.


1. Fe schedules and submits ETL tasks to spark cluster for execution.

2. Spark cluster executes ETL to complete the preprocessing of load data. It includes global dictionary building (bitmap type), partitioning, sorting, aggregation, etc.

3. After the ETL task is completed, Fe obtains the data path of each partition that has been preprocessed, and schedules the related be to execute the push task.

4. Be reads data through broker and converts it into Doris underlying storage format.

5. Fe schedule the effective version and complete the load job.

```
                 +
                 | 0. User create spark load job
            +----v----+
            |   FE    |---------------------------------+
            +----+----+                                 |
                 | 3. FE send push tasks                |
                 | 5. FE publish version                |
    +------------+------------+                         |
    |            |            |                         |
+---v---+    +---v---+    +---v---+                     |
|  BE   |    |  BE   |    |  BE   |                     |1. FE submit Spark ETL job
+---^---+    +---^---+    +---^---+                     |
    |4. BE push with broker   |                         |
+---+---+    +---+---+    +---+---+                     |
|Broker |    |Broker |    |Broker |                     |
+---^---+    +---^---+    +---^---+                     |
    |            |            |                         |
+---+------------+------------+---+ 2.ETL +-------------v---------------+
|               HDFS              +------->       Spark cluster         |
|                                 <-------+                             |
+---------------------------------+       +-----------------------------+

```

## Global dictionary

### Applicable scenarios

At present, the bitmap column in Doris is implemented using the class library '`roaingbitmap`', while the input data type of '`roaringbitmap`' can only be integer. Therefore, if you want to pre calculate the bitmap column in the import process, you need to convert the type of input data to integer.

In the existing Doris import process, the data structure of global dictionary is implemented based on hive table, which stores the mapping from original value to encoded value.

### Build process

1. Read the data from the upstream data source and generate a hive temporary table, which is recorded as `hive_table`.

2. Extract the de duplicated values of the fields to be de duplicated from the `hive_table`, and generate a new hive table, which is marked as `distinct_value_table`.

3. Create a new global dictionary table named `dict_table`; one column is the original value, and the other is the encoded value.

4. Left join the `distinct_value_table` and `dict_table`, calculate the new de duplication value set, and then code this set with window function. At this time, the original value of the de duplication column will have one more column of encoded value. Finally, the data of these two columns will be written back to `dict_table`.

5. Join the `dict_table` with the `hive_table` to replace the original value in the `hive_table` with the integer encoded value.

6. `hive_table` will be read by the next data preprocessing process and imported into Doris after calculation.

## Data preprocessing (DPP)

### Basic process

1. Read data from the data source. The upstream data source can be HDFS file or hive table.

2. Map the read data, calculate the expression, and generate the bucket field `bucket_id` according to the partition information.

3. Generate rolluptree according to rollup metadata of Doris table.

4. Traverse rolluptree to perform hierarchical aggregation. The rollup of the next level can be calculated from the rollup of the previous level.

5. After each aggregation calculation, the data will be calculated according to the `bucket_id`is divided into buckets and then written into HDFS.

6. Subsequent brokers will pull the files in HDFS and import them into Doris be.

## Hive Bitmap UDF

Spark supports loading hive-generated bitmap data directly into Doris, see [hive-bitmap-udf documentation](../../../ecosystem/hive-bitmap-udf.md)

## Basic operation

### Configure ETL cluster

As an external computing resource, spark is used to complete ETL work in Doris. In the future, there may be other external resources that will be used in Doris, such as spark / GPU for query, HDFS / S3 for external storage, MapReduce for ETL, etc. Therefore, we introduce resource management to manage these external resources used by Doris.

Before submitting the spark import task, you need to configure the spark cluster that performs the ETL task.

Grammar:

```sql
-- create spark resource
CREATE EXTERNAL RESOURCE resource_name
PROPERTIES
(
  type = spark,
  spark_conf_key = spark_conf_value,
  working_dir = path,
  broker = broker_name,
  broker.property_key = property_value,
  broker.hadoop.security.authentication = kerberos,
  broker.kerberos_principal = doris@YOUR.COM,
  broker.kerberos_keytab = /home/doris/my.keytab
  broker.kerberos_keytab_content = ASDOWHDLAWIDJHWLDKSALDJSDIWALD
)

-- drop spark resource
DROP RESOURCE resource_name

-- show resources
SHOW RESOURCES
SHOW PROC "/resources"

-- privileges
GRANT USAGE_PRIV ON RESOURCE resource_name TO user_identity
GRANT USAGE_PRIV ON RESOURCE resource_name TO ROLE role_name

REVOKE USAGE_PRIV ON RESOURCE resource_name FROM user_identity
REVOKE USAGE_PRIV ON RESOURCE resource_name FROM ROLE role_name
```

**Create resource**

`resource_name` is the name of the spark resource configured in Doris.

`Properties` are the parameters related to spark resources, as follows:

- `type`: resource type, required. Currently, only spark is supported.
- Spark related parameters are as follows:
  - `spark.master`: required, yarn is supported at present, `spark://host:port`.
  - `spark.submit.deployMode`: the deployment mode of Spark Program. It is required and supports cluster and client.
  - `spark.hadoop.fs.defaultfs`: required when master is yarn.
  - `spark.submit.timeout`：spark task timeout, default 5 minutes
  - Other parameters are optional, refer to `http://spark.apache.org/docs/latest/configuration.html`
- YARN RM related parameters are as follows：
    - If Spark is a single-point RM, you need to configure `spark.hadoop.yarn.resourcemanager.address`，address of the single point resource manager.
    - If Spark is RM-HA, it needs to be configured (where hostname and address are optional)：
        - `spark.hadoop.yarn.resourcemanager.ha.enabled`: ResourceManager enables HA, set to true.
        - `spark.hadoop.yarn.resourcemanager.ha.rm-ids`: List of ResourceManager logical IDs.
        - `spark.hadoop.yarn.resourcemanager.hostname.rm-id`: For each rm-id, specify the hostname of the ResourceManager.
        - `spark.hadoop.yarn.resourcemanager.address.rm-id`: For each rm-id, specify the host:port for clients to submit jobs.
- HDFS HA related parameters are as follows：
    - `spark.hadoop.fs.defaultFS`, hdfs client default path prefix.
    - `spark.hadoop.dfs.nameservices`, hdfs cluster logical name.
    - `spark.hadoop.dfs.ha.namenodes.nameservices01` , unique identifier for each NameNode in the nameservice.
    - `spark.hadoop.dfs.namenode.rpc-address.nameservices01.mynamenode1`, fully qualified RPC address for each NameNode.
    - `spark.hadoop.dfs.namenode.rpc-address.nameservices01.mynamenode2`, fully qualified RPC address for each NameNode.
    - `spark.hadoop.dfs.client.failover.proxy.provider` = `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`, set the implementation class.
-`working_dir`: directory used by ETL. Spark is required when used as an ETL resource. For example: `hdfs://host :port/tmp/doris`.
- `broker.hadoop.security.authentication`: Specify the authentication method as kerberos.
- `broker.kerberos_principal`: Specify the principal of kerberos.
- `broker.kerberos_keytab`: Specify the path to the keytab file for kerberos. The file must be an absolute path to a file on the server where the broker process is located. And can be accessed by the Broker process.
- `broker.kerberos_keytab_content`: Specify the content of the keytab file in kerberos after base64 encoding. You can choose one of these with `broker.kerberos_keytab` configuration.
- `broker`: the name of the broker. Spark is required when used as an ETL resource. You need to use the 'alter system add broker' command to complete the configuration in advance.
- `broker.property_key`: the authentication information that the broker needs to specify when reading the intermediate file generated by ETL.
- `env`: Specify the spark environment variable and support dynamic setting. For example, when the authentication mode of Hadoop is simple, set the Hadoop user name and password
  - `env.HADOOP_USER_NAME`: user name
  - `env.HADOOP_USER_PASSWORD`: user password

Example:

```sql
-- yarn cluster mode
CREATE EXTERNAL RESOURCE "spark0"
PROPERTIES
(
  "type" = "spark",
  "spark.master" = "yarn",
  "spark.submit.deployMode" = "cluster",
  "spark.jars" = "xxx.jar,yyy.jar",
  "spark.files" = "/tmp/aaa,/tmp/bbb",
  "spark.executor.memory" = "1g",
  "spark.yarn.queue" = "queue0",
  "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
  "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
  "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
  "broker" = "broker0",
  "broker.username" = "user0",
  "broker.password" = "password0"
);

-- spark standalone client mode
CREATE EXTERNAL RESOURCE "spark1"
PROPERTIES
(
  "type" = "spark",
  "spark.master" = "spark://127.0.0.1:7777",
  "spark.submit.deployMode" = "client",
  "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
  "broker" = "broker1"
);

-- yarn HA mode
CREATE EXTERNAL RESOURCE sparkHA
PROPERTIES
(
  "type" = "spark",
  "spark.master" = "yarn",
  "spark.submit.deployMode" = "cluster",
  "spark.executor.memory" = "1g",
  "spark.yarn.queue" = "default",
  "spark.hadoop.yarn.resourcemanager.ha.enabled" = "true",
  "spark.hadoop.yarn.resourcemanager.ha.rm-ids" = "rm1,rm2",
  "spark.hadoop.yarn.resourcemanager.address.rm1" = "xxxx:8032",
  "spark.hadoop.yarn.resourcemanager.address.rm2" = "xxxx:8032",
  "spark.hadoop.fs.defaultFS" = "hdfs://nameservices01",
  "spark.hadoop.dfs.nameservices" = "nameservices01",
  "spark.hadoop.dfs.ha.namenodes.nameservices01" = "mynamenode1,mynamenode2",
  "spark.hadoop.dfs.namenode.rpc-address.nameservices01.mynamenode1" = "xxxx:8020",
  "spark.hadoop.dfs.namenode.rpc-address.nameservices01.mynamenode2" = "xxxx:8020",
  "spark.hadoop.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
  "working_dir" = "hdfs://nameservices01/doris_prd_data/sinan/spark_load/",
  "broker" = "broker_name",
  "broker.username" = "username",
  "broker.password" = "",
  "broker.dfs.nameservices" = "nameservices01",
  "broker.dfs.ha.namenodes.nameservices01" = "mynamenode1, mynamenode2",
  "broker.dfs.namenode.rpc-address.nameservices01.mynamenode1" = "xxxx:8020",
  "broker.dfs.namenode.rpc-address.nameservices01.mynamenode2" = "xxxx:8020",
  "broker.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

**Spark Load supports Kerberos authentication**

If Spark load accesses Hadoop cluster resources with Kerberos authentication, we only need to specify the following parameters when creating Spark resources:

- `spark.hadoop.hadoop.security.authentication` Specify the authentication method as Kerberos for Yarn。
- `spark.hadoop.yarn.resourcemanager.principal` Specify the principal of kerberos for Yarn.
- `spark.hadoop.yarn.resourcemanager.keytab` Specify the path to the keytab file of kerberos for Yarn. The file must be an absolute path to a file on the server where the frontend process is located. And can be accessed by the frontend process.
- `broker.hadoop.security.authentication`: Specify the authentication method as kerberos.
- `broker.kerberos_principal`: Specify the principal of kerberos.
- `broker.kerberos_keytab`: Specify the path to the keytab file for kerberos. The file must be an absolute path to a file on the server where the broker process is located. And can be accessed by the Broker process.
- `broker.kerberos_keytab_content`: Specify the content of the keytab file in kerberos after base64 encoding. You can choose one of these with `kerberos_keytab` configuration.

Example：

```sql
CREATE EXTERNAL RESOURCE "spark_on_kerberos"
PROPERTIES
(
  "type" = "spark",
  "spark.master" = "yarn",
  "spark.submit.deployMode" = "cluster",
  "spark.jars" = "xxx.jar,yyy.jar",
  "spark.files" = "/tmp/aaa,/tmp/bbb",
  "spark.executor.memory" = "1g",
  "spark.yarn.queue" = "queue0",
  "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
  "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
  "spark.hadoop.hadoop.security.authentication" = "kerberos",
  "spark.hadoop.yarn.resourcemanager.principal" = "doris@YOUR.YARN.COM",
  "spark.hadoop.yarn.resourcemanager.keytab" = "/home/doris/yarn.keytab",
  "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
  "broker" = "broker0",
  "broker.hadoop.security.authentication" = "kerberos",
  "broker.kerberos_principal" = "doris@YOUR.COM",
  "broker.kerberos_keytab" = "/home/doris/my.keytab"
);
```

**Show resources**

Ordinary accounts can only see the resources that they have `USAGE_PRIV` to use.

The root and admin accounts can see all the resources.

**Resource privilege**

Resource permissions are managed by grant revoke. Currently, only `USAGE_PRIV` permission is supported.

You can use the `USAGE_PRIV` permission is given to a user or a role, and the role is used the same as before.

```sql
-- Grant permission to the spark0 resource to user user0

GRANT USAGE_PRIV ON RESOURCE "spark0" TO "user0"@"%";

-- Grant permission to the spark0 resource to role ROLE0

GRANT USAGE_PRIV ON RESOURCE "spark0" TO ROLE "role0";

-- Grant permission to all resources to user user0

GRANT USAGE_PRIV ON RESOURCE * TO "user0"@"%";

-- Grant permission to all resources to role ROLE0

GRANT USAGE_PRIV ON RESOURCE * TO ROLE "role0";

-- Revoke the spark0 resource permission of user user0

REVOKE USAGE_PRIV ON RESOURCE "spark0" FROM "user0"@"%";

```

### Configure spark client

The Fe submits the spark task by executing the spark submit command. Therefore, it is necessary to configure the spark client for Fe. It is recommended to use the official version of spark 2 above 2.4.3, [download spark here](https://archive.apache.org/dist/spark/). After downloading, please follow the steps to complete the following configuration.

**Configure SPARK_HOME environment variable**

Place the spark client on the same machine as Fe and configure `spark_home_default_dir` in the `fe.conf`. This configuration item defaults to the `fe/lib/spark2x` path. This config cannot be empty.

**Configure spark dependencies**

Package all jar packages in jars folder under spark client root path into a zip file, and configure `spark_resource_patj` in `fe.conf` as this zip file's path.

When the spark load task is submitted, this zip file will be uploaded to the remote repository, and the default repository path will be hung in `working_dir/{cluster_ID}` directory named as `__spark_repository__{resource_name}`, which indicates that a resource in the cluster corresponds to a remote warehouse. The directory structure of the remote warehouse is as follows:

```
__spark_repository__spark0/
    |-__archive_1.0.0/
    |        |-__lib_990325d2c0d1d5e45bf675e54e44fb16_spark-dpp-1.0.0-jar-with-dependencies.jar
    |        |-__lib_7670c29daf535efe3c9b923f778f61fc_spark-2x.zip
    |-__archive_1.1.0/
    |        |-__lib_64d5696f99c379af2bee28c1c84271d5_spark-dpp-1.1.0-jar-with-dependencies.jar
    |        |-__lib_1bbb74bb6b264a270bc7fca3e964160f_spark-2x.zip
    |-__archive_1.2.0/
    |        |-...
```

In addition to spark dependency (named by `spark-2x.zip` by default), Fe will also upload DPP's dependency package to the remote repository. If all the dependency files submitted by spark load already exist in the remote repository, then there is no need to upload dependency, saving the time of repeatedly uploading a large number of files each time.

### Configure yarn client

The Fe obtains the running application status and kills the application by executing the yarn command. Therefore, you need to configure the yarn client for Fe. It is recommended to use the official version of Hadoop above 2.5.2, [download hadoop](https://archive.apache.org/dist/hadoop/common/). After downloading, please follow the steps to complete the following configuration.

**Configure the yarn client path**

Place the downloaded yarn client in the same machine as Fe, and configure `yarn_client_path` in the `fe.conf` as the executable file of yarn, which is set as the `fe/lib/yarn-client/hadoop/bin/yarn` by default.

(optional) when Fe obtains the application status or kills the application through the yarn client, the configuration files required for executing the yarn command will be generated by default in the `lib/yarn-config` path in the Fe root directory. This path can be configured by configuring `yarn-config-dir` in the `fe.conf`. The currently generated configuration yarn config files include `core-site.xml` and `yarn-site.xml`.

### Create Load

Grammar:

```sql
LOAD LABEL load_label
    (data_desc, ...)
    WITH RESOURCE resource_name resource_properties
    [PROPERTIES (key1=value1, ... )]

* load_label:
	db_name.label_name

* data_desc:
    DATA INFILE ('file_path', ...)
    [NEGATIVE]
    INTO TABLE tbl_name
    [PARTITION (p1, p2)]
    [COLUMNS TERMINATED BY separator ]
    [(col1, ...)]
    [SET (k1=f1(xx), k2=f2(xx))]
    [WHERE predicate]

* resource_properties:
    (key2=value2, ...)
```

Example 1: when the upstream data source is HDFS file

```sql
LOAD LABEL db1.label1
(
    DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/file1")
    INTO TABLE tbl1
    COLUMNS TERMINATED BY ","
    (tmp_c1,tmp_c2)
    SET
    (
        id=tmp_c2,
        name=tmp_c1
    ),
    DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/file2")
    INTO TABLE tbl2
    COLUMNS TERMINATED BY ","
    (col1, col2)
    where col1 > 1
)
WITH RESOURCE 'spark0'
(
    "spark.executor.memory" = "2g",
    "spark.shuffle.compress" = "true"
)
PROPERTIES
(
    "timeout" = "3600"
);

```

Example 2: when the upstream data source is hive table

```sql
step 1:新建hive外部表
CREATE EXTERNAL TABLE hive_t1
(
    k1 INT,
    K2 SMALLINT,
    k3 varchar(50),
    uuid varchar(100)
)
ENGINE=hive
properties
(
"database" = "tmp",
"table" = "t1",
"hive.metastore.uris" = "thrift://0.0.0.0:8080"
);

step 2: 提交load命令
LOAD LABEL db1.label1
(
    DATA FROM TABLE hive_t1
    INTO TABLE tbl1
    (k1,k2,k3)
    SET
    (
		uuid=bitmap_dict(uuid)
    )
)
WITH RESOURCE 'spark0'
(
    "spark.executor.memory" = "2g",
    "spark.shuffle.compress" = "true"
)
PROPERTIES
(
    "timeout" = "3600"
);

```

Example 3: when the upstream data source is hive binary type table

```sql
step 1: create hive external table
CREATE EXTERNAL TABLE hive_t1
(
    k1 INT,
    K2 SMALLINT,
    k3 varchar(50),
    uuid varchar(100)
)
ENGINE=hive
properties
(
"database" = "tmp",
"table" = "t1",
"hive.metastore.uris" = "thrift://0.0.0.0:8080"
);

step 2: submit load command
LOAD LABEL db1.label1
(
    DATA FROM TABLE hive_t1
    INTO TABLE tbl1
    (k1,k2,k3)
    SET
    (
		uuid=binary_bitmap(uuid)
    )
)
WITH RESOURCE 'spark0'
(
    "spark.executor.memory" = "2g",
    "spark.shuffle.compress" = "true"
)
PROPERTIES
(
    "timeout" = "3600"
);

```

Example 4: Import data from hive partitioned table

```sql
-- hive create table statement
create table test_partition(
id int,
name string,
age int
)
partitioned by (dt string)
row format delimited fields terminated by ','
stored as textfile;

-- doris create table statement
CREATE TABLE IF NOT EXISTS test_partition_04
(
dt date,
id int,
name string,
age int
)
UNIQUE KEY(`dt`, `id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
-- spark load 
CREATE EXTERNAL RESOURCE "spark_resource"
PROPERTIES
(
"type" = "spark",
"spark.master" = "yarn",
"spark.submit.deployMode" = "cluster",
"spark.executor.memory" = "1g",
"spark.yarn.queue" = "default",
"spark.hadoop.yarn.resourcemanager.address" = "localhost:50056",
"spark.hadoop.fs.defaultFS" = "hdfs://localhost:9000",
"working_dir" = "hdfs://localhost:9000/tmp/doris",
"broker" = "broker_01"
);
LOAD LABEL demo.test_hive_partition_table_18
(
    DATA INFILE("hdfs://localhost:9000/user/hive/warehouse/demo.db/test/dt=2022-08-01/*")
    INTO TABLE test_partition_04
    COLUMNS TERMINATED BY ","
    FORMAT AS "csv"
    (id,name,age)
    COLUMNS FROM PATH AS (`dt`)
    SET
    (
        dt=dt,
        id=id,
        name=name,
        age=age
    )
)
WITH RESOURCE 'spark_resource'
(
    "spark.executor.memory" = "1g",
    "spark.shuffle.compress" = "true"
)
PROPERTIES
(
    "timeout" = "3600"
);
````



You can view the details syntax about creating load by input `help spark load`. This paper mainly introduces the parameter meaning and precautions in the creation and load syntax of spark load.

**Label**

Identification of the import task. Each import task has a unique label within a single database. The specific rules are consistent with `broker load`.

**Data description parameters**

Currently, the supported data sources are CSV and hive table. Other rules are consistent with `broker load`.

**Load job parameters**

Load job parameters mainly refer to the `opt_properties` in the spark load. Load job parameters are applied to the entire load job. The rules are consistent with `broker load`.

**Spark resource parameters**

Spark resources need to be configured into the Doris system in advance, and users should be given `USAGE_PRIV`. Spark load can only be used after priv permission.

When users have temporary requirements, such as adding resources for tasks and modifying spark configs, you can set them here. The settings only take effect for this task and do not affect the existing configuration in the Doris cluster.

```sql
WITH RESOURCE 'spark0'
(
  "spark.driver.memory" = "1g",
  "spark.executor.memory" = "3g"
)
```

**Load when data source is hive table**

At present, if you want to use hive table as a data source in the import process, you need to create an external table of type hive,

Then you can specify the table name of the external table when submitting the Load command.

**Load process to build global dictionary**

The data type applicable to the aggregate columns of the Doris table is of type bitmap.

In the load command, you can specify the field to build a global dictionary. The format is: '```doris field name=bitmap_dict(hive_table field name)```

It should be noted that the construction of global dictionary is supported only when the upstream data source is hive table.

**Load when data source is hive binary type table**

The data type applicable to the aggregate column of the doris table is bitmap type, and the data type of the corresponding column in the hive table of the data source is binary (through the org.apache.doris.load.loadv2.dpp.BitmapValue (FE spark-dpp) class serialized) type.

There is no need to build a global dictionary, just specify the corresponding field in the load command, the format is: ```doris field name=binary_bitmap (hive table field name)```

Similarly, the binary (bitmap) type of data import is currently only supported when the upstream data source is a hive table,You can refer to the use of hive bitmap [hive-bitmap-udf](../../../ecosystem/hive-bitmap-udf.md)

### Show Load

Spark load is asynchronous just like broker load, so the user must create the load label record and use label in the **show load command to view the load result**. The show load command is common in all load types. The specific syntax can be viewed by executing help show load.

Example:

```mysql
mysql> show load order by createtime desc limit 1\G
*************************** 1. row ***************************
         JobId: 76391
         Label: label1
         State: FINISHED
      Progress: ETL:100%; LOAD:100%
          Type: SPARK
       EtlInfo: unselected.rows=4; dpp.abnorm.ALL=15; dpp.norm.ALL=28133376
      TaskInfo: cluster:cluster0; timeout(s):10800; max_filter_ratio:5.0E-5
      ErrorMsg: N/A
    CreateTime: 2019-07-27 11:46:42
  EtlStartTime: 2019-07-27 11:46:44
 EtlFinishTime: 2019-07-27 11:49:44
 LoadStartTime: 2019-07-27 11:49:44
LoadFinishTime: 2019-07-27 11:50:16
           URL: http://1.1.1.1:8089/proxy/application_1586619723848_0035/
    JobDetails: {"ScannedRows":28133395,"TaskNumber":1,"FileNumber":1,"FileSize":200000}
```

Refer to broker load for the meaning of parameters in the returned result set. The differences are as follows:

+ State

  The current phase of the load job. After the job is submitted, the status is pending. After the spark ETL is submitted, the status changes to ETL. After ETL is completed, Fe schedules be to execute push operation, and the status changes to finished after the push is completed and the version takes effect.

  There are two final stages of the load job: cancelled and finished. When the load job is in these two stages, the load is completed. Among them, cancelled is load failure, finished is load success.

+ Progress

  Progress description of the load job. There are two kinds of progress: ETL and load, corresponding to the two stages of the load process, ETL and loading.

  The progress range of load is 0 ~ 100%.

  ```Load progress = the number of tables that have completed all replica imports / the total number of tables in this import task * 100%```

  **If all load tables are loaded, the progress of load is 99%**, the load enters the final effective stage. After the whole load is completed, the load progress will be changed to 100%.

  The load progress is not linear. Therefore, if the progress does not change over a period of time, it does not mean that the load is not in execution.

+ Type

  Type of load job. Spark load is spark.

+ CreateTime/EtlStartTime/EtlFinishTime/LoadStartTime/LoadFinishTime

  These values represent the creation time of the load, the start time of the ETL phase, the completion time of the ETL phase, the start time of the loading phase, and the completion time of the entire load job.

+ JobDetails

  Display the detailed running status of some jobs, which will be updated when ETL ends. It includes the number of loaded files, the total size (bytes), the number of subtasks, the number of processed original lines, etc.

  ```{"ScannedRows":139264,"TaskNumber":1,"FileNumber":1,"FileSize":940754064}```

+ URL

  Copy this url to the browser and jump to the web interface of the corresponding application.

### View spark launcher commit log

Sometimes users need to view the detailed logs generated during the spark submission process. The logs are saved in the `log/spark_launcher_log` under the Fe root directory named as `spark_launcher_{load_job_id}_{label}.log`. The log will be saved in this directory for a period of time. When the load information in Fe metadata is cleaned up, the corresponding log will also be cleaned. The default saving log time is 3 days.

### cancel load

When the spark load job status is not cancelled or finished, it can be manually cancelled by the user. When canceling, you need to specify the label to cancel the load job. The syntax of the cancel load command can be viewed by executing `help cancel load`. 

## Related system configuration

### FE configuration

The following configuration belongs to the system level configuration of spark load, that is, the configuration for all spark load import tasks. Mainly through modification``` fe.conf ``` to modify the configuration value.

+ `enable_spark_load`

  Open spark load and create resource. The default value is false. This feature is turned off.

+ `spark_load_default_timeout_second`

  The default timeout for tasks is 259200 seconds (3 days).

+ `spark_home_default_dir`

  Spark client path (`Fe/lib/spark2x`).

+ `spark_resource_path`

  The path of the packaged spark dependent file (empty by default).

+ `spark_launcher_log_dir`

  The directory where the spark client's commit log is stored (`Fe/log/spark)_launcher_log`）.

+ `yarn_client_path`

  The path of the yarn binary executable file (`Fe/lib/yarn-client/Hadoop/bin/yarn').

+ `yarn_config_dir`

  The path to generate the yarn configuration file (`Fe/lib/yarn-config`).

## Best practices

### Application scenarios

The most suitable scenario to use spark load is that the raw data is in the file system (HDFS), and the amount of data is tens of GB to TB. Stream load or broker load is recommended for small amount of data.

## FAQ

* Spark load does not yet support the import of Doris table fields that are of type String. If your table fields are of type String, please change them to type varchar, otherwise the import will fail, prompting `type:ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel`
* When using spark load, the `HADOOP_CONF_DIR` environment variable is no set in the `spark-env.sh`.

    If the `HADOOP_CONF_DIR` environment variable is not set, the error `When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment` will be reported.

* When using spark load, the `spark_home_default_dir` does not specify correctly.

    The spark submit command is used when submitting a spark job. If `spark_home_default_dir` is set incorrectly, an error `Cannot run program 'xxx/bin/spark_submit', error = 2, no such file or directory` will be reported.

* When using spark load, `spark_resource_path` does not point to the packaged zip file.

    If `spark_resource_path` is not set correctly. An error `file XXX/jars/spark-2x.zip` does not exist will be reported.

* When using spark load `yarn_client_path` does not point to a executable file of yarn.

    If `yarn_client_path` is not set correctly. An error `yarn client does not exist in path: XXX/yarn-client/hadoop/bin/yarn` will be reported.

* When using spark load, the `JAVA_HOME` environment variable is no set in the `hadoop-config.sh` on the yarn clinet.

    If the `JAVA_HOME`  environment variable is not set, the error `yarn application kill failed. app id: xxx, load job id: xxx, msg: which: no xxx/lib/yarn-client/hadoop/bin/yarn in ((null))  Error: JAVA_HOME is not set and could not be found` will be reported.

* When using spark load, the launch log for `SparkLauncher` is not printed or report an error `start spark app failed. error: Waiting too much time to get appId from handle. spark app state: UNKNOWN, loadJobId:xxx`

    In `<`SPARK_HOME`>`/conf, add the log4j.properties configuration file and set the log level to INFO.

* When using spark load, `SparkLauncher` fails to launch.

    Copy the spark-launcher_`<`xxx`>`.jar from `<`SPARK_HOME`>`/lib to the lib of FE and restart the FE process.

* Error with `Compression codec com.hadoop.compression.lzo.LzoCodec not found`.

    Copy `<`HADOOP_HOME`>`/share/hadoop/yarn/lib/hadoop-lzo-.`<`xxx`>`jar to `<`SPARK_HOME`>`/lib and repackage it into a zip and upload it to hdfs.

* Error with `NoClassDefFoundError com/sun/jersey/api/client/config/ClientConfig`.

    Delete or rename the jersey-client-`<`xxx`>`.jar in the `<`SPARK_HOME`>`/lib, copy `<`HADOOP_HOME`>`/share/hadoop/yarn/lib/jersey-client-.`<`xxx`>`jar to `<`SPARK_HOME`>`/lib, and repackage it into a zip and upload it to hdfs.

## More Help

For more detailed syntax used by **Spark Load**,  you can enter `HELP SPARK LOAD` on the Mysql client command line for more help.
