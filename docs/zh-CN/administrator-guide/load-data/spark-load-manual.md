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

Spark load 通过 Spark 实现对导入数据的预处理，提高 Doris 大数据量的导入性能并且节省 Doris 集群的计算资源。主要用于初次迁移，大数据量导入 Doris 的场景。

Spark load 是一种异步导入方式，用户需要通过 MySQL 协议创建 Spark 类型导入任务，并通过 `SHOW LOAD` 查看导入结果。



## 适用场景

* 源数据在 Spark 可以访问的存储系统中，如 HDFS。
* 数据量在 几十 GB 到 TB 级别。



## 名词解释

1. Frontend（FE）：Doris 系统的元数据和调度节点。在导入流程中主要负责导入任务的调度工作。
2. Backend（BE）：Doris 系统的计算和存储节点。在导入流程中主要负责数据写入及存储。
3. Spark ETL：在导入流程中主要负责数据的 ETL 工作，包括全局字典构建（BITMAP类型）、分区、排序、聚合等。
4. Broker：Broker 为一个独立的无状态进程。封装了文件系统接口，提供 Doris 读取远端存储系统中文件的能力。


## 基本原理

### 基本流程

用户通过 MySQL 客户端提交 Spark 类型导入任务，FE记录元数据并返回用户提交成功。

Spark load 任务的执行主要分为以下5个阶段。

1. FE 调度提交 ETL 任务到 Spark 集群执行。
2. Spark 集群执行 ETL 完成对导入数据的预处理。包括全局字典构建（BITMAP类型）、分区、排序、聚合等。
3. ETL 任务完成后，FE 获取预处理过的每个分片的数据路径，并调度相关的 BE 执行 Push 任务。
4. BE 通过 Broker 读取数据，转化为 Doris 底层存储格式。
5. FE 调度生效版本，完成导入任务。

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



### 全局字典

待补



### 数据预处理（DPP）

待补



## 基本操作

### 配置 ETL 集群

Spark作为一种外部计算资源在Doris中用来完成ETL工作，未来可能还有其他的外部资源会加入到Doris中使用，如Spark/GPU用于查询，HDFS/S3用于外部存储，MapReduce用于ETL等，因此我们引入resource management来管理Doris使用的这些外部资源。

提交 Spark 导入任务之前，需要配置执行 ETL 任务的 Spark 集群。

语法：

```sql
-- create spark resource
CREATE EXTERNAL RESOURCE resource_name
PROPERTIES 
(                 
  type = spark,
  spark_conf_key = spark_conf_value,
  working_dir = path,
  broker = broker_name,
  broker.property_key = property_value
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

#### 创建资源

`resource_name` 为 Doris 中配置的 Spark 资源的名字。

`PROPERTIES` 是 Spark 资源相关参数，如下：

- `type`：资源类型，必填，目前仅支持 spark。

- Spark 相关参数如下：
  - `spark.master`: 必填，目前支持yarn，spark://host:port。
  - `spark.submit.deployMode`:  Spark 程序的部署模式，必填，支持 cluster，client 两种。
  - `spark.hadoop.yarn.resourcemanager.address`: master为yarn时必填。
  - `spark.hadoop.fs.defaultFS`: master为yarn时必填。
  - 其他参数为可选，参考http://spark.apache.org/docs/latest/configuration.html 
- `working_dir`: ETL 使用的目录。spark作为ETL资源使用时必填。例如：hdfs://host:port/tmp/doris。
- `broker`: broker 名字。spark作为ETL资源使用时必填。需要使用`ALTER SYSTEM ADD BROKER` 命令提前完成配置。
  - `broker.property_key`: broker读取ETL生成的中间文件时需要指定的认证信息等。

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
```

#### 查看资源

普通账户只能看到自己有USAGE_PRIV使用权限的资源。

root和admin账户可以看到所有的资源。

#### 资源权限

资源权限通过GRANT REVOKE来管理，目前仅支持USAGE_PRIV使用权限。

可以将USAGE_PRIV权限赋予某个用户或者某个角色，角色的使用与之前一致。
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



### 创建导入

语法：

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
示例：

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

创建导入的详细语法执行 ```HELP SPARK LOAD``` 查看语法帮助。这里主要介绍 Spark load 的创建导入语法中参数意义和注意事项。

#### Label

导入任务的标识。每个导入任务，都有一个在单 database 内部唯一的 Label。具体规则与 `Broker Load` 一致。

#### 数据描述类参数

目前支持的数据源有CSV和hive table。其他规则与 `Broker Load` 一致。

#### 导入作业参数

导入作业参数主要指的是 Spark load 创建导入语句中的属于 ```opt_properties```部分的参数。导入作业参数是作用于整个导入作业的。规则与 `Broker Load` 一致。

#### Spark资源参数

Spark资源需要提前配置到 Doris系统中并且赋予用户USAGE_PRIV权限后才能使用 Spark load。

当用户有临时性的需求，比如增加任务使用的资源而修改 Spark configs，可以在这里设置，设置仅对本次任务生效，并不影响 Doris 集群中已有的配置。

```sql
WITH RESOURCE 'spark0'
(
  "spark.driver.memory" = "1g",
  "spark.executor.memory" = "3g"
)
```



### 查看导入

Spark load 导入方式同 Broker load 一样都是异步的，所以用户必须将创建导入的 Label 记录，并且在**查看导入命令中使用 Label 来查看导入结果**。查看导入命令在所有导入方式中是通用的，具体语法可执行 ```HELP SHOW LOAD``` 查看。

示例：

```
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

返回结果集中参数意义可以参考 Broker load。不同点如下：

+ State

    导入任务当前所处的阶段。任务提交之后状态为 PENDING，提交 Spark ETL 之后状态变为 ETL，ETL 完成之后 FE 调度 BE 执行 push 操作状态变为 LOADING，push 完成并且版本生效后状态变为 FINISHED。
    
    导入任务的最终阶段有两个：CANCELLED 和 FINISHED，当 Load job 处于这两个阶段时导入完成。其中 CANCELLED 为导入失败，FINISHED 为导入成功。
    
+ Progress

    导入任务的进度描述。分为两种进度：ETL 和 LOAD，对应了导入流程的两个阶段 ETL 和 LOADING。
    
    LOAD 的进度范围为：0~100%。
    
    ```LOAD 进度 = 当前已完成所有replica导入的tablet个数 / 本次导入任务的总tablet个数 * 100%``` 
    
    **如果所有导入表均完成导入，此时 LOAD 的进度为 99%** 导入进入到最后生效阶段，整个导入完成后，LOAD 的进度才会改为 100%。
    
    导入进度并不是线性的。所以如果一段时间内进度没有变化，并不代表导入没有在执行。
    
+ Type

    导入任务的类型。Spark load 为 SPARK。    

+ CreateTime/EtlStartTime/EtlFinishTime/LoadStartTime/LoadFinishTime

    这几个值分别代表导入创建的时间，ETL 阶段开始的时间，ETL 阶段完成的时间，LOADING 阶段开始的时间和整个导入任务完成的时间。

+ JobDetails

    显示一些作业的详细运行状态，ETL 结束的时候更新。包括导入文件的个数、总大小（字节）、子任务个数、已处理的原始行数等。

    ```{"ScannedRows":139264,"TaskNumber":1,"FileNumber":1,"FileSize":940754064}```

### 取消导入

当 Spark load 作业状态不为 CANCELLED 或 FINISHED 时，可以被用户手动取消。取消时需要指定待取消导入任务的 Label 。取消导入命令语法可执行 ```HELP CANCEL LOAD```查看。



## 相关系统配置

### FE 配置

下面配置属于 Spark load 的系统级别配置，也就是作用于所有 Spark load 导入任务的配置。主要通过修改 ``` fe.conf```来调整配置值。

+ spark_load_default_timeout_second
  
    任务默认超时时间为259200秒（3天）。
    
    

## 最佳实践

### 应用场景

使用 Spark load 最适合的场景就是原始数据在文件系统（HDFS）中，数据量在 几十 GB 到 TB 级别。小数据量还是建议使用 Stream load 或者 Broker load。



## 常见问题

* 使用Spark load时需要在FE机器设置SPARK_HOME和HADOOP_CONF_DIR环境变量。

提交Spark job时用到spark-submit命令，如果SPARK_HOME环境变量没有设置，会报 `Spark home not found; set it explicitly or use the SPARK_HOME environment variable` 错误。

如果HADOOP_CONF_DIR环境变量没有设置，会报 `When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.` 错误。


