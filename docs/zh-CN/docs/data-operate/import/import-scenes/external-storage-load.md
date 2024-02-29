---
{
    "title": "外部存储数据导入",
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

# 外部存储数据导入

本文档主要介绍如何导入外部系统中存储的数据。例如（HDFS，所有支持S3协议的对象存储）

## HDFS LOAD

### 准备工作

上传需要导入的文件到HDFS上，具体命令可参阅[HDFS上传命令](https://hadoop.apache.org/docs/r3.3.2/hadoop-project-dist/hadoop-common/FileSystemShell.html#put)

### 开始导入

Hdfs load 创建导入语句，导入方式和[Broker Load](../../../data-operate/import/import-way/broker-load-manual) 基本相同，只需要将 `WITH BROKER broker_name ()` 语句替换成如下部分

```
  LOAD LABEL db_name.label_name 
  (data_desc, ...)
  WITH HDFS
  [PROPERTIES (key1=value1, ... )]
```



1. 创建一张表

   通过 `CREATE TABLE` 命令在`demo`创建一张表用于存储待导入的数据。具体的导入方式请查阅 [CREATE TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) 命令手册。示例如下：

   ```sql
   CREATE TABLE IF NOT EXISTS load_hdfs_file_test
   (
       id INT,
       age TINYINT,
       name VARCHAR(50)
   )
   unique key(id)
   DISTRIBUTED BY HASH(id) BUCKETS 3;
   ```
   
2. 导入数据执行以下命令导入HDFS文件：

   ```sql
   LOAD LABEL demo.label_20220402
       (
       DATA INFILE("hdfs://host:port/tmp/test_hdfs.txt")
       INTO TABLE `load_hdfs_file_test`
       COLUMNS TERMINATED BY "\t"            
       (id,age,name)
       )
       with HDFS (
       "fs.defaultFS"="hdfs://testFs",
       "hdfs_user"="user"
       )
       PROPERTIES
       (
       "timeout"="1200",
       "max_filter_ratio"="0.1"
       );
   ```
    关于参数介绍，请参阅[Broker Load](../../../data-operate/import/import-way/broker-load-manual)，HA集群的创建语法，通过`HELP BROKER LOAD`查看
  
3. 查看导入状态
   
   Broker load 是一个异步的导入方式，具体导入结果可以通过[SHOW LOAD](../../../sql-manual/sql-reference/Show-Statements/SHOW-LOAD)命令查看
   
   ```
   mysql> show load order by createtime desc limit 1\G;
   *************************** 1. row ***************************
            JobId: 41326624
            Label: broker_load_2022_04_15
            State: FINISHED
         Progress: ETL:100%; LOAD:100%
             Type: BROKER
          EtlInfo: unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=27
         TaskInfo: cluster:N/A; timeout(s):1200; max_filter_ratio:0.1
         ErrorMsg: NULL
       CreateTime: 2022-04-01 18:59:06
     EtlStartTime: 2022-04-01 18:59:11
    EtlFinishTime: 2022-04-01 18:59:11
    LoadStartTime: 2022-04-01 18:59:11
   LoadFinishTime: 2022-04-01 18:59:11
              URL: NULL
       JobDetails: {"Unfinished backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[]},"ScannedRows":27,"TaskNumber":1,"All backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[36728051]},"FileNumber":1,"FileSize":5540}
   1 row in set (0.01 sec)
   ```
   
   


## S3 LOAD

从0.14 版本开始，Doris 支持通过S3协议直接从支持S3协议的在线存储系统导入数据。

下面主要介绍如何导入 AWS S3 中存储的数据。也支持导入其他支持S3协议的对象存储系统导入。

### 适用场景

* 源数据在 支持S3协议的存储系统中，如 S3 等。
* 数据量在 几十到百GB 级别。

### 准备工作
1. 准本AK 和 SK
   首先需要找到或者重新生成 AWS `Access keys`，可以在 AWS console 的 `My Security Credentials` 找到生成方式， 如下图所示：
   [AK_SK](/images/aws_ak_sk.png)
   选择 `Create New Access Key` 注意保存生成 AK和SK.
2. 准备 REGION 和 ENDPOINT
   REGION 可以在创建桶的时候选择也可以在桶列表中查看到。ENDPOINT 可以通过如下页面通过 REGION 查到 [AWS 文档](https://docs.aws.amazon.com/general/latest/gr/s3.html#s3_region)

其他云存储系统可以相应的文档找到与S3兼容的相关信息

### 开始导入
导入方式和 [Broker Load](../../../data-operate/import/import-way/broker-load-manual) 基本相同，只需要将 `WITH BROKER broker_name ()` 语句替换成如下部分
```
    WITH S3
    (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION"
    )
```

完整示例如下
```
    LOAD LABEL example_db.exmpale_label_1
    (
        DATA INFILE("s3://your_bucket_name/your_file.txt")
        INTO TABLE load_test
        COLUMNS TERMINATED BY ","
    )
    WITH S3
    (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION"
    )
    PROPERTIES
    (
        "timeout" = "3600"
    );
```

### 常见问题

1. S3 SDK 默认使用 `virtual-hosted style` 方式。但某些对象存储系统可能没开启或没支持 `virtual-hosted style` 方式的访问，此时我们可以添加 `use_path_style` 参数来强制使用 `path style` 方式：

```
  WITH S3
  (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION",
        "use_path_style" = "true"
  )
```

<version since="1.2">

2. 支持使用临时秘钥（TOKEN) 访问所有支持 S3 协议的对象存储，用法如下：

```
  WITH S3
  (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_TEMP_ACCESS_KEY",
        "AWS_SECRET_KEY" = "AWS_TEMP_SECRET_KEY",
        "AWS_TOKEN" = "AWS_TEMP_TOKEN",
        "AWS_REGION" = "AWS_REGION"
  )
```

</version>
