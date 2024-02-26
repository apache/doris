---
{
    "title": "Spark Load",
    "language": "zh-CN"
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

Spark Load 通过外部的 Spark 资源实现对导入数据的预处理，提高 Doris 大数据量的导入性能并且节省 Doris 集群的计算资源。主要用于初次迁移，大数据量导入 Doris 的场景。

Spark Load 是利用了 Spark 集群的资源对要导入的数据的进行了排序，Doris BE 直接写文件，这样能大大降低 Doris 集群的资源使用，对于历史海量数据迁移降低 Doris 集群资源使用及负载有很好的效果。

如果用户在没有 Spark 集群这种资源的情况下，又想方便、快速的完成外部存储历史数据的迁移，可以使用 [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) 。相对 Spark Load 导入，Broker Load 对 Doris 集群的资源占用会更高。

Spark Load 是一种异步导入方式，用户需要通过 MySQL 协议创建 Spark 类型导入任务，并通过 `SHOW LOAD` 查看导入结果。

## 适用场景

- 源数据在 Spark 可以访问的存储系统中，如 HDFS。
- 数据量在 几十 GB 到 TB 级别。

## 基本原理

### 基本流程

用户通过 MySQL 客户端提交 Spark 类型导入任务，FE 记录元数据并返回用户提交成功。

Spark Load 任务的执行主要分为以下 5 个阶段。

1. FE 调度提交 ETL 任务到 Spark 集群执行。
2. Spark 集群执行 ETL 完成对导入数据的预处理，包括全局字典构建（ Bitmap 类型）、分区、排序、聚合等。
3. ETL 任务完成后，FE 获取预处理过的每个分片的数据路径，并调度相关的 BE 执行 Push 任务。
4. BE 通过 Broker 读取数据，转化为 Doris 底层存储格式。
5. FE 调度生效版本，完成导入任务。

```text
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

## 全局字典

### 适用场景

目前 Doris 中 Bitmap 列是使用类库 `Roaringbitmap` 实现的，而 `Roaringbitmap` 的输入数据类型只能是整型，因此如果要在导入流程中实现对于 Bitmap 列的预计算，那么就需要将输入数据的类型转换成整型。

在 Doris 现有的导入流程中，全局字典的数据结构是基于 Hive 表实现的，保存了原始值到编码值的映射。

### 构建流程

1. 读取上游数据源的数据，生成一张 Hive 临时表，记为 `hive_table`。
2. 从 `hive_table `中抽取待去重字段的去重值，生成一张新的 Hive 表，记为 `distinct_value_table`。
3. 新建一张全局字典表，记为 `dict_table` ，一列为原始值，一列为编码后的值。
4. 将 `distinct_value_table` 与 `dict_table` 做 Left Join，计算出新增的去重值集合，然后对这个集合使用窗口函数进行编码，此时去重列原始值就多了一列编码后的值，最后将这两列的数据写回 `dict_table`。
5. 将 `dict_table `与 `hive_table` 进行 Join，完成 `hive_table` 中原始值替换成整型编码值的工作。
6. `hive_table `会被下一步数据预处理的流程所读取，经过计算后导入到 Doris 中。

## 数据预处理（DPP）

### 基本流程

1. 从数据源读取数据，上游数据源可以是 HDFS 文件，也可以是 Hive 表。
2. 对读取到的数据进行字段映射，表达式计算以及根据分区信息生成分桶字段 `bucket_id`。
3. 根据 Doris 表的 Rollup 元数据生成 RollupTree。
4. 遍历 RollupTree，进行分层的聚合操作，下一个层级的 Rollup 可以由上一个层的 Rollup 计算得来。
5. 每次完成聚合计算后，会对数据根据 `bucket_id `进行分桶然后写入 HDFS 中。
6. 后续 Broker 会拉取 HDFS 中的文件然后导入 Doris Be 中。

## Hive Bitmap UDF

Spark 支持将 Hive 生成的 Bitmap 数据直接导入到 Doris。详见 [hive-bitmap-udf 文档](../../../ecosystem/hive-bitmap-udf.md)

## 基本操作

### 配置 ETL 集群

Spark 作为一种外部计算资源在 Doris 中用来完成 ETL 工作，未来可能还有其他的外部资源会加入到 Doris 中使用，如 Spark/GPU 用于查询，HDFS/S3 用于外部存储，MapReduce 用于 ETL 等，因此我们引入 Resource Management 来管理 Doris 使用的这些外部资源。

提交 Spark 导入任务之前，需要配置执行 ETL 任务的 Spark 集群。

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

**创建资源**

`resource_name` 为 Doris 中配置的 Spark 资源的名字。

`PROPERTIES` 是 Spark 资源相关参数，如下：

- `type`：资源类型，必填，目前仅支持 Spark。
- Spark 相关参数如下：
  - `spark.master`: 必填，目前支持 Yarn，Spark://host:port。
  - `spark.submit.deployMode`: Spark 程序的部署模式，必填，支持 Cluster、Client 两种。
  - `spark.hadoop.fs.defaultFS`: Master 为 Yarn 时必填。
  - `spark.submit.timeout`：spark任务超时时间，默认5分钟
- YARN RM 相关参数如下：
  - 如果 Spark 为单点 RM，则需要配置`spark.hadoop.yarn.resourcemanager.address`，表示单点 ResourceManager 地址。
  - 如果 Spark 为 RM-HA，则需要配置（其中 hostname 和 address 任选一个配置）：
    - `spark.hadoop.yarn.resourcemanager.ha.enabled`: ResourceManager 启用 HA，设置为 True。
    - `spark.hadoop.yarn.resourcemanager.ha.rm-ids`: ResourceManager 逻辑 ID 列表。
    - `spark.hadoop.yarn.resourcemanager.hostname.rm-id`: 对于每个 rm-id，指定 ResourceManager 对应的主机名。
    - `spark.hadoop.yarn.resourcemanager.address.rm-id`: 对于每个 rm-id，指定 host:port 以供客户端提交作业。
- HDFS HA 相关参数如下：
  - `spark.hadoop.fs.defaultFS`, HDFS 客户端默认路径前缀
  - `spark.hadoop.dfs.nameservices`, HDFS 集群逻辑名称
  - `spark.hadoop.dfs.ha.namenodes.nameservices01` , nameservice 中每个 NameNode 的唯一标识符
  - `spark.hadoop.dfs.namenode.rpc-address.nameservices01.mynamenode1`, 每个 NameNode 的完全限定的 RPC 地址
  - `spark.hadoop.dfs.namenode.rpc-address.nameservices01.mynamenode2`, 每个 NameNode 的完全限定的 RPC 地址
  - `spark.hadoop.dfs.client.failover.proxy.provider` = `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`, 设置实现类
- `working_dir`: ETL 使用的目录。Spark 作为 ETL 资源使用时必填。例如：hdfs://host:port/tmp/doris。
  - 其他参数为可选，参考 http://spark.apache.org/docs/latest/configuration.html
- `working_dir`: ETL 使用的目录。Spark 作为 ETL 资源使用时必填。例如：hdfs://host:port/tmp/doris。
- `broker.hadoop.security.authentication`：指定认证方式为 Kerberos。
- `broker.kerberos_principal`：指定 Kerberos 的 Principal。
- `broker.kerberos_keytab`：指定 Kerberos 的 Keytab 文件路径。该文件必须为 Broker 进程所在服务器上的文件的绝对路径，并且可以被 Broker 进程访问。
- `broker.kerberos_keytab_content`：指定 Kerberos 中 Keytab 文件内容经过 Base64 编码之后的内容。这个跟 `broker.kerberos_keytab` 配置二选一即可。
- `broker`: Broker 名字。Spark 作为 ETL 资源使用时必填。需要使用 `ALTER SYSTEM ADD BROKER` 命令提前完成配置。
  - `broker.property_key`: Broker 读取 ETL 生成的中间文件时需要指定的认证信息等。
- `env`: 指定 Spark 环境变量,支持动态设置,比如当认证 Hadoop 认为方式为 Simple 时，设置 Hadoop 用户名和密码
  - `env.HADOOP_USER_NAME`: 访问 Hadoop 用户名
  - `env.HADOOP_USER_PASSWORD`:密码

示例：

```sql
-- yarn cluster 模式
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

-- spark standalone client 模式
CREATE EXTERNAL RESOURCE "spark1"
PROPERTIES
(
  "type" = "spark",
  "spark.master" = "spark://127.0.0.1:7777",
  "spark.submit.deployMode" = "client",
  "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
  "broker" = "broker1"
);

-- yarn HA 模式
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

**Spark Load 支持 Kerberos 认证**

如果是 Spark Load 访问带有 Kerberos 认证的 Hadoop 集群资源，我们只需要在创建 Spark resource 的时候指定以下参数即可：

- `spark.hadoop.hadoop.security.authentication` 指定 Yarn 认证方式为 Kerberos。
- `spark.hadoop.yarn.resourcemanager.principal` 指定 Yarn 的 Kerberos Principal。
- `spark.hadoop.yarn.resourcemanager.keytab` 指定 Yarn 的 Kerberos Keytab 文件路径。该文件必须为 FE 进程所在服务器上的文件的绝对路径。并且可以被 FE 进程访问。
- `broker.hadoop.security.authentication`：指定认证方式为 Kerberos。
- `broker.kerberos_principal`：指定 Kerberos 的 Principal。
- `broker.kerberos_keytab`：指定 Kerberos 的 Keytab 文件路径。该文件必须为 Broker 进程所在服务器上的文件的绝对路径。并且可以被 Broker 进程访问。
- `broker.kerberos_keytab_content`：指定 Kerberos 中 Keytab 文件内容经过 Base64 编码之后的内容。这个跟 `kerberos_keytab` 配置二选一即可。

示例：

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

**查看资源**

普通账户只能看到自己有 USAGE_PRIV 使用权限的资源。

Root 和 Admin 账户可以看到所有的资源。

**资源权限**

资源权限通过 GRANT REVOKE 来管理，目前仅支持 USAGE_PRIV 使用权限。

可以将 USAGE_PRIV 权限赋予某个用户或者某个角色，角色的使用与之前一致。

```sql
-- 授予spark0资源的使用权限给用户user0
GRANT USAGE_PRIV ON RESOURCE "spark0" TO "user0"@"%";

-- 授予spark0资源的使用权限给角色role0
GRANT USAGE_PRIV ON RESOURCE "spark0" TO ROLE "role0";

-- 授予所有资源的使用权限给用户user0
GRANT USAGE_PRIV ON RESOURCE * TO "user0"@"%";

-- 授予所有资源的使用权限给角色role0
GRANT USAGE_PRIV ON RESOURCE * TO ROLE "role0";

-- 撤销用户user0的spark0资源使用权限
REVOKE USAGE_PRIV ON RESOURCE "spark0" FROM "user0"@"%";
```

### 配置 Spark 客户端

FE 底层通过执行 spark-submit 的命令去提交 Spark 任务，因此需要为 FE 配置 Spark 客户端，建议使用 2.4.5 或以上的 Spark2 官方版本，[Spark 下载地址](https://archive.apache.org/dist/spark/)，下载完成后，请按步骤完成以下配置。

**配置 SPARK_HOME 环境变量**

将 Spark 客户端放在 FE 同一台机器上的目录下，并在 FE 的配置文件配置 `spark_home_default_dir` 项指向此目录，此配置项默认为 FE 根目录下的 `lib/spark2x ` 路径，此项不可为空。

**配置 Spark 依赖包**

将 Spark 客户端下的 jars 文件夹内所有 jar 包归档打包成一个 Zip 文件，并在 FE 的配置文件配置 `spark_resource_path` 项指向此 Zip 文件，若此配置项为空，则FE会尝试寻找 FE 根目录下的 `lib/spark2x/jars/spark-2x.zip` 文件，若没有找到则会报文件不存在的错误。

当提交 Spark Load 任务时，会将归档好的依赖文件上传至远端仓库，默认仓库路径挂在 `working_dir/{cluster_id}` 目录下，并以`__spark_repository__{resource_name} `命名，表示集群内的一个 Resource 对应一个远端仓库，远端仓库目录结构参考如下:

```text
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

除了 Spark 依赖(默认以 `spark-2x.zip `命名)，FE 还会上传 DPP 的依赖包至远端仓库，若此次 Spark Load 提交的所有依赖文件都已存在远端仓库，那么就不需要在上传依赖，省下原来每次重复上传大量文件的时间。

### 配置 Yarn 客户端

FE 底层通过执行 Yarn 命令去获取正在运行的 Application 的状态以及杀死 Application，因此需要为 FE 配置 Yarn 客户端，建议使用 2.5.2 或以上的 Hadoop2 官方版本，[Hadoop 下载地址](https://archive.apache.org/dist/hadoop/common/) ，下载完成后，请按步骤完成以下配置。

**配置 Yarn 可执行文件路径**

将下载好的 Yarn 客户端放在 FE 同一台机器的目录下，并在 FE 配置文件配置 `yarn_client_path` 项指向 Yarn 的二进制可执行文件，默认为 FE 根目录下的 `lib/yarn-client/hadoop/bin/yarn` 路径。

(可选) 当 FE 通过 Yarn 客户端去获取 Application 的状态或者杀死 Application 时，默认会在 FE 根目录下的 `lib/yarn-config` 路径下生成执行 Yarn 命令所需的配置文件，此路径可通过在 FE 配置文件配置 `yarn_config_dir` 项修改，目前生成的配置文件包括 `core-site.xml` 和`yarn-site.xml`。

### 创建导入

语法：

```sql
LOAD LABEL load_label
    (data_desc, ...)
    WITH RESOURCE resource_name 
    [resource_properties]
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
    [COLUMNS FROM PATH AS (col2, ...)]
    [SET (k1=f1(xx), k2=f2(xx))]
    [WHERE predicate]
    
    DATA FROM TABLE hive_external_tbl
    [NEGATIVE]
    INTO TABLE tbl_name
    [PARTITION (p1, p2)]
    [SET (k1=f1(xx), k2=f2(xx))]
    [WHERE predicate]

* resource_properties:
    (key2=value2, ...)
```

示例1：上游数据源为 HDFS 文件的情况

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

示例2：上游数据源是 Hive 表的情况

```sql
step 1:新建 Hive 外部表
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

step 2: 提交 Load 命令，要求导入的 Doris 表中的列必须在 Hive 外部表中存在。
LOAD LABEL db1.label1
(
    DATA FROM TABLE hive_t1
    INTO TABLE tbl1
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

示例3：上游数据源是 Hive binary 类型情况

```sql
step 1:新建 Hive 外部表
CREATE EXTERNAL TABLE hive_t1
(
    k1 INT,
    K2 SMALLINT,
    k3 varchar(50),
    uuid varchar(100) // Hive 中的类型为 binary
)
ENGINE=hive
properties
(
"database" = "tmp",
"table" = "t1",
"hive.metastore.uris" = "thrift://0.0.0.0:8080"
);

step 2: 提交 Load 命令，要求导入的 Doris 表中的列必须在 Hive 外部表中存在。
LOAD LABEL db1.label1
(
    DATA FROM TABLE hive_t1
    INTO TABLE tbl1
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

示例4： 导入 Hive 分区表的数据

```sql
--Hive 建表语句
create table test_partition(
	id int,
	name string,
	age int
)
partitioned by (dt string)
row format delimited fields terminated by ','
stored as textfile;

--Doris 建表语句
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
--Spark Load 语句
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
```



创建导入的详细语法执行 `HELP SPARK LOAD` 查看语法帮助。这里主要介绍 Spark Load 的创建导入语法中参数意义和注意事项。

**Label**

导入任务的标识。每个导入任务，都有一个在单 Database 内部唯一的 Label。具体规则与 [`Broker Load`](broker-load-manual.md) 一致。

**数据描述类参数**

目前支持的数据源有 CSV 和 Hive Table。其他规则与 [`Broker Load`](broker-load-manual.md) 一致。

**导入作业参数**

导入作业参数主要指的是 Spark Load 创建导入语句中的属于 `opt_properties` 部分的参数。导入作业参数是作用于整个导入作业的。规则与 [`Broker Load`](broker-load-manual.md) 一致。

**Spark资源参数**

Spark 资源需要提前配置到 Doris 系统中并且赋予用户 USAGE_PRIV 权限后才能使用 Spark Load。

当用户有临时性的需求，比如增加任务使用的资源而修改 Spark Configs，可以在这里设置，设置仅对本次任务生效，并不影响 Doris 集群中已有的配置。

```sql
WITH RESOURCE 'spark0'
(
  "spark.driver.memory" = "1g",
  "spark.executor.memory" = "3g"
)
```

**数据源为 Hive 表时的导入**

目前如果期望在导入流程中将 Hive 表作为数据源，那么需要先新建一张类型为 Hive 的外部表， 然后提交导入命令时指定外部表的表名即可。

**导入流程构建全局字典**

适用于 Doris 表聚合列的数据类型为 Bitmap 类型。 在 Load 命令中指定需要构建全局字典的字段即可，格式为：`Doris 字段名称=bitmap_dict(Hive 表字段名称)` 需要注意的是目前只有在上游数据源为 Hive 表时才支持全局字典的构建。

**Hive binary（bitmap）类型列的导入**

适用于 Doris 表聚合列的数据类型为 Bitmap 类型，且数据源 Hive 表中对应列的数据类型为 binary（通过 FE 中 spark-dpp 中的 `org.apache.doris.load.loadv2.dpp.BitmapValue` 类序列化）类型。 无需构建全局字典，在 Load 命令中指定相应字段即可，格式为：`Doris 字段名称= binary_bitmap( Hive 表字段名称)` 同样，目前只有在上游数据源为 Hive 表时才支持 binary（ bitmap ）类型的数据导入 Hive bitmap 使用可参考 [hive-bitmap-udf](../../../ecosystem/hive-bitmap-udf.md) 。

### 查看导入

Spark Load 导入方式同 Broker Load 一样都是异步的，所以用户必须将创建导入的 Label 记录，并且在**查看导入命令中使用 Label 来查看导入结果**。查看导入命令在所有导入方式中是通用的，具体语法可执行 `HELP SHOW LOAD` 查看。

示例：

```sql
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

返回结果集中参数意义可以参考 [Broker Load](./broker-load-manual.md)。不同点如下：

- State

  导入任务当前所处的阶段。任务提交之后状态为 PENDING，提交 Spark ETL 之后状态变为 ETL，ETL 完成之后 FE 调度 BE 执行 push 操作状态变为 LOADING，push 完成并且版本生效后状态变为 FINISHED。

  导入任务的最终阶段有两个：CANCELLED 和 FINISHED，当 Load Job 处于这两个阶段时导入完成。其中 CANCELLED 为导入失败，FINISHED 为导入成功。

- Progress

  导入任务的进度描述。分为两种进度：ETL 和 LOAD，对应了导入流程的两个阶段 ETL 和 LOADING。

  LOAD 的进度范围为：0~100%。

  `LOAD 进度 = 当前已完成所有 Replica 导入的 Tablet 个数 / 本次导入任务的总 Tablet 个数 * 100%`

  **如果所有导入表均完成导入，此时 LOAD 的进度为 99%** 导入进入到最后生效阶段，整个导入完成后，LOAD 的进度才会改为 100%。

  导入进度并不是线性的。所以如果一段时间内进度没有变化，并不代表导入没有在执行。

- Type

  导入任务的类型。Spark load 为 SPARK。

- CreateTime/EtlStartTime/EtlFinishTime/LoadStartTime/LoadFinishTime

  这几个值分别代表导入创建的时间，ETL 阶段开始的时间，ETL 阶段完成的时间，LOADING 阶段开始的时间和整个导入任务完成的时间。

- JobDetails

  显示一些作业的详细运行状态，ETL 结束的时候更新。包括导入文件的个数、总大小（字节）、子任务个数、已处理的原始行数等。

  `{"ScannedRows":139264,"TaskNumber":1,"FileNumber":1,"FileSize":940754064}`

- URL

  可复制输入到浏览器，跳转至相应 Application 的 Web 界面

### 查看 Spark Launcher 提交日志

有时用户需要查看 Spark 任务提交过程中产生的详细日志，日志默认保存在FE根目录下 `log/spark_launcher_log` 路径下，并以 `spark_launcher_{load_job_id}_{label}.log `命名，日志会在此目录下保存一段时间，当FE元数据中的导入信息被清理时，相应的日志也会被清理，默认保存时间为3天。

### 取消导入

当 Spark Load 作业状态不为 CANCELLED 或 FINISHED 时，可以被用户手动取消。取消时需要指定待取消导入任务的 Label 。取消导入命令语法可执行  `HELP CANCEL LOAD` 查看。

## 相关系统配置

### FE 配置

下面配置属于 Spark Load 的系统级别配置，也就是作用于所有 Spark Load 导入任务的配置。主要通过修改 `fe.conf`来调整配置值。

- `enable_spark_load`

  开启 Spark Load 和创建 Resource 功能。默认为 False，关闭此功能。

- `spark_load_default_timeout_second`

  任务默认超时时间为 259200 秒（3 天）。

- `spark_home_default_dir`

  Spark 客户端路径 (`fe/lib/spark2x`) 。

- `spark_resource_path`

  打包好的 Spark 依赖文件路径（默认为空）。

- `spark_launcher_log_dir`

  Spark 客户端的提交日志存放的目录（`fe/log/spark_launcher_log`）。

- `yarn_client_path`

  Yarn 二进制可执行文件路径 (`fe/lib/yarn-client/hadoop/bin/yarn`) 。

- `yarn_config_dir`

  Yarn 配置文件生成路径 (`fe/lib/yarn-config`) 。

## 最佳实践

### 应用场景

使用 Spark Load 最适合的场景就是原始数据在文件系统（HDFS）中，数据量在 几十 GB 到 TB 级别。小数据量还是建议使用  [Stream Load](./stream-load-manual.md) 或者 [Broker Load](./broker-load-manual.md)。

## 常见问题

- 现在 Spark Load 还不支持 Doris 表字段是 String 类型的导入，如果你的表字段有 String 类型的请改成 Varchar 类型，不然会导入失败，提示 `type:ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel`
- 使用 Spark Load 时没有在 Spark 客户端的 `spark-env.sh` 配置 `HADOOP_CONF_DIR` 环境变量。

  如果 `HADOOP_CONF_DIR` 环境变量没有设置，会报 `When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.` 错误。

- 使用 Spark Load 时`spark_home_default_dir`配置项没有指定 Spark 客户端根目录。

  提交 Spark Job 时用到 spark-submit 命令，如果 `spark_home_default_dir` 设置错误，会报 `Cannot run program "xxx/bin/spark-submit": error=2, No such file or directory` 错误。

- 使用 Spark Load 时 `spark_resource_path` 配置项没有指向打包好的 zip 文件。

  如果 `spark_resource_path `没有设置正确，会报 `File xxx/jars/spark-2x.zip does not exist` 错误。

- 使用 Spark Load 时 `yarn_client_path` 配置项没有指定 Yarn 的可执行文件。

  如果 `yarn_client_path `没有设置正确，会报 `yarn client does not exist in path: xxx/yarn-client/hadoop/bin/yarn` 错误

- 使用 Spark Load 时没有在 Yarn 客户端的 `hadoop-config.sh` 配置 `JAVA_HOME` 环境变量。

  如果 `JAVA_HOME` 环境变量没有设置，会报 `yarn application kill failed. app id: xxx, load job id: xxx, msg: which: no xxx/lib/yarn-client/hadoop/bin/yarn in ((null))  Error: JAVA_HOME is not set and could not be found` 错误

- 使用 Spark Load 时没有打印 SparkLauncher 的启动日志或者报错`start spark app failed. error: Waiting too much time to get appId from handle. spark app state: UNKNOWN, loadJobId:xxx`

  在`<`SPARK_HOME`>`/conf下，添加log4j.properties配置文件，并配置日志级别为INFO。

- 使用 Spark Load 时 SparkLauncher 启动失败。

  将`<`SPARK_HOME`>`/lib下的spark-launcher_`<`xxx`>`.jar包复制到fe的lib下，并重启fe进程。

- 报错：`Compression codec com.hadoop.compression.lzo.LzoCodec not found`。

  将`<`HADOOP_HOME`>`/share/hadoop/yarn/lib/hadoop-lzo-`<`xxx`>`.jar复制到`<`SPARK_HOME`>`/lib下，并重新打包成zip上传到hdfs。

- 报错：`NoClassDefFoundError com/sun/jersey/api/client/config/ClientConfig`。

  将原来的`<`SPARK_HOME`>`/lib下的jersey-client-`<`xxx`>`.jar删除或者重命名，将`<`HADOOP_HOME`>`/share/hadoop/yarn/lib/jersey-client-`<`xxx`>`.jar复制到`<`SPARK_HOME`>`/lib下，并重新打包成zip上传到hdfs。

## 更多帮助

关于**Spark Load** 使用的更多详细语法，可以在 MySQL 客户端命令行下输入 `HELP SPARK LOAD` 获取更多帮助信息。

