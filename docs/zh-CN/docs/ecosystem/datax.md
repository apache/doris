---
{
    "title": "DataX Doriswriter",
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

# DataX doriswriter

[DataX](https://github.com/alibaba/DataX) doriswriter 插件，用于通过 DataX 同步其他数据源的数据到 Doris 中。

这个插件是利用Doris的Stream Load 功能进行数据导入的。需要配合 DataX 服务一起使用。

## 关于 DataX

DataX 是阿里云 DataWorks数据集成 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS 等各种异构数据源之间高效的数据同步功能。

更多信息请参阅: `https://github.com/alibaba/DataX/`

## 使用手册

DataX doriswriter 插件代码 [这里](https://github.com/apache/incubator-doris/tree/master/extension/DataX)。

这个目录包含插件代码以及 DataX 项目的开发环境。

doriswriter 插件依赖的 DataX 代码中的一些模块。而这些模块并没有在 Maven 官方仓库中。所以我们在开发 doriswriter 插件时，需要下载完整的 DataX 代码库，才能进行插件的编译和开发。

### 目录结构

1. `doriswriter/`

   这个目录是 doriswriter 插件的代码目录。这个目录中的所有代码，都托管在 Apache Doris 的代码库中。

   doriswriter 插件帮助文档在这里：`doriswriter/doc`

2. `init-env.sh`

   这个脚本主要用于构建 DataX 开发环境，他主要进行了以下操作：

    1. 将 DataX 代码库 clone 到本地。
    2. 将 `doriswriter/` 目录软链到 `DataX/doriswriter` 目录。
    3. 在 `DataX/pom.xml` 文件中添加 `<module>doriswriter</module>` 模块。
    4. 将 `DataX/core/pom.xml` 文件中的 httpclient 版本从 4.5 改为 4.5.13.

       > httpclient v4.5 在处理 307 转发时有bug。

   这个脚本执行后，开发者就可以进入 `DataX/` 目录开始开发或编译了。因为做了软链，所以任何对 `DataX/doriswriter` 目录中文件的修改，都会反映到 `doriswriter/` 目录中，方便开发者提交代码。

### 编译

#### Doris 代码库编译

1. 运行 `init-env.sh`
2. 按需修改 `DataX/doriswriter` 中的代码。
3. 编译 doriswriter：

    1. 单独编译 doriswriter 插件:

       `mvn clean install -pl plugin-rdbms-util,doriswriter -DskipTests`

    2. 编译整个 DataX 项目:

       `mvn package assembly:assembly -Dmaven.test.skip=true`

       产出在 `target/datax/datax/`.

       > hdfsreader, hdfswriter and oscarwriter 这三个插件需要额外的jar包。如果你并不需要这些插件，可以在 `DataX/pom.xml` 中删除这些插件的模块。

    3. 编译错误

       如遇到如下编译错误：

       ```
       Could not find artifact com.alibaba.datax:datax-all:pom:0.0.1-SNAPSHOT ...
       ```

       可尝试以下方式解决：

        1. 下载 [alibaba-datax-maven-m2-20210928.tar.gz](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/alibaba-datax-maven-m2-20210928.tar.gz)
        2. 解压后，将得到的 `alibaba/datax/` 目录，拷贝到所使用的 maven 对应的 `.m2/repository/com/alibaba/` 下。
        3. 再次尝试编译。

4. 按需提交修改。

#### Datax 代码库编译

从datax 代码库拉取代码，执行编译

```
git clone https://github.com/alibaba/DataX.git
cd datax
mvn package assembly:assembly -Dmaven.test.skip=true
```

编译完成后可以在 `datax/target/Datax` 下看到datax.tar.gz 包

### Datax DorisWriter 参数介绍：

* **jdbcUrl**

    - 描述：Doris 的 JDBC 连接串，用户执行 preSql 或 postSQL。
    - 必选：是
    - 默认值：无
* **loadUrl**

  - 描述：作为 Stream Load 的连接目标。格式为 "ip:port"。其中 IP 是 FE 节点 IP，port 是 FE 节点的 http_port。可以填写多个，多个之间使用英文状态的逗号隔开:`,`，doriswriter 将以轮询的方式访问。
  - 必选：是
  - 默认值：无
* **username**

    - 描述：访问Doris数据库的用户名
    - 必选：是
    - 默认值：无
* **password**
  
    - 描述：访问Doris数据库的密码
    - 必选：否
    - 默认值：空
* **connection.selectedDatabase**
    - 描述：需要写入的Doris数据库名称。
    - 必选：是
    - 默认值：无
* **connection.table**
  - 描述：需要写入的Doris表名称。
    - 必选：是
    - 默认值：无
* **flushInterval**
    - 描述：数据写入批次的时间间隔。如果这个时间间隔设置的太小会造成 Doris 写阻塞问题，错误代码 -235，同时如果你这个时间设置太小，`maxBatchRows` 和 `batchSize` 参数设置的有很大，那么很可能达不到你这设置的数据量大小，也会执行导入。
    - 必选：否
    - 默认值：30000（ms）
* **column**
    - 描述：目的表需要写入数据的字段，这些字段将作为生成的 Json 数据的字段名。字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
    - 必选：是
    - 默认值：否
* **preSql**

  - 描述：写入数据到目的表前，会先执行这里的标准语句。
  - 必选：否
  - 默认值：无
* **postSql**

  - 描述：写入数据到目的表后，会执行这里的标准语句。
  - 必选：否
  - 默认值：无


* **maxBatchRows**
  - 描述：每批次导入数据的最大行数。和 **batchSize** 共同控制每批次的导入记录行数。每批次数据达到两个阈值之一，即开始导入这一批次的数据。
  - 必选：否
  - 默认值：500000
  
* **batchSize**
  - 描述：每批次导入数据的最大数据量。和 **maxBatchRows** 共同控制每批次的导入数量。每批次数据达到两个阈值之一，即开始导入这一批次的数据。
  - 必选：否
  - 默认值：104857600
  
* **maxRetries**

  - 描述：每批次导入数据失败后的重试次数。
  - 必选：否
  - 默认值：3


* **labelPrefix**

  - 描述：每批次导入任务的 label 前缀。最终的 label 将有 `labelPrefix + UUID` 组成全局唯一的 label，确保数据不会重复导入
  - 必选：否
  - 默认值：`datax_doris_writer_`

* **loadProps**

  - 描述：StreamLoad 的请求参数，详情参照StreamLoad介绍页面。[Stream load - Apache Doris](https://doris.apache.org/zh-CN/docs/data-operate/import/import-way/stream-load-manual)

    这里包括导入的数据格式：format等，导入数据格式默认我们使用csv，支持JSON，具体可以参照下面类型转换部分，也可以参照上面Stream load 官方信息

  - 必选：否

  - 默认值：无

### 示例

#### 1.Stream读取数据后导入至Doris

该示例插件的使用说明请参阅 [这里](https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md)

#### 2.Mysql读取数据后导入至Doris

1.Mysql表结构

```sql
CREATE TABLE `t_test`(
 `id`bigint(30) NOT NULL,
 `order_code` varchar(30) DEFAULT NULL COMMENT '',
 `line_code` varchar(30) DEFAULT NULL COMMENT '',
 `remark` varchar(30) DEFAULT NULL COMMENT '',
 `unit_no` varchar(30) DEFAULT NULL COMMENT '',
 `unit_name` varchar(30) DEFAULT NULL COMMENT '',
 `price` decimal(12,2) DEFAULT NULL COMMENT '',
 PRIMARY KEY(`id`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='';
```

2.Doris表结构

```sql
CREATE TABLE `ods_t_test` (
 `id` bigint(30) NOT NULL,
 `order_code` varchar(30) DEFAULT NULL COMMENT '',
 `line_code` varchar(30) DEFAULT NULL COMMENT '',
 `remark` varchar(30) DEFAULT NULL COMMENT '',
 `unit_no` varchar(30) DEFAULT NULL COMMENT '',
 `unit_name` varchar(30) DEFAULT NULL COMMENT '',
 `price` decimal(12,2) DEFAULT NULL COMMENT ''
) ENGINE=OLAP
UNIQUE KEY(`id`, `order_code`)
DISTRIBUTED BY HASH(`order_code`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);
```

3.创建datax脚本 

my_import.json

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "column": ["id","order_code","line_code","remark","unit_no","unit_name","price"],
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:mysql://localhost:3306/demo"],
                                "table": ["employees_1"]
                            }
                        ],
                        "username": "root",
                        "password": "xxxxx",
                        "where": ""
                    }
                },
                "writer": {
                    "name": "doriswriter",
                    "parameter": {
                        "loadUrl": ["127.0.0.1:8030"],
                        "loadProps": {
                        },
                        "column": ["id","order_code","line_code","remark","unit_no","unit_name","price"],
                        "username": "root",
                        "password": "xxxxxx",
                        "postSql": ["select count(1) from all_employees_info"],
                        "preSql": [],
                        "flushInterval":30000,
                        "connection": [
                          {
                            "jdbcUrl": "jdbc:mysql://127.0.0.1:9030/demo",
                            "selectedDatabase": "demo",
                            "table": ["all_employees_info"]
                          }
                        ],
                        "loadProps": {
                            "format": "json",
                            "strip_outer_array":"true",
                            "line_delimiter": "\\x02"
                        }
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "1"
            }
        }
    }
}
```

>备注：
>
>```json
>"loadProps": {
>   "format": "json",
>   "strip_outer_array":"true",
>   "line_delimiter": "\\x02"
>}
>```
>
>1. 这里我们使用了 JSON 格式导入数据
>2.  `line_delimiter` 默认是换行符，可能会和数据中的值冲突，我们可以使用一些特殊字符或者不可见字符，避免导入错误
>3. strip_outer_array ：在一批导入数据中表示多行数据，Doris 在解析时会将数组展开，然后依次解析其中的每一个 Object 作为一行数据
>4. 更多 Stream load 参数请参照 [Stream load文档]([Stream load - Apache Doris](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/stream-load-manual))
>5. 如果是 CSV 格式我们可以这样使用
>
>```json
>"loadProps": {
>    "format": "csv",
>    "column_separator": "\\x01",
>    "line_delimiter": "\\x02"
>}
>```
>
>**CSV 格式要特别注意行列分隔符，避免和数据中的特殊字符冲突，这里建议使用隐藏字符，默认列分隔符是：\t，行分隔符：\n**

4.执行datax任务，具体参考 [datax官网](https://github.com/alibaba/DataX/blob/master/userGuid.md)

```
python bin/datax.py my_import.json
```

执行之后我们可以看到下面的信息

```
2022-11-16 14:28:54.012 [job-0] INFO  JobContainer - jobContainer starts to do prepare ...
2022-11-16 14:28:54.012 [job-0] INFO  JobContainer - DataX Reader.Job [mysqlreader] do prepare work .
2022-11-16 14:28:54.013 [job-0] INFO  JobContainer - DataX Writer.Job [doriswriter] do prepare work .
2022-11-16 14:28:54.020 [job-0] INFO  JobContainer - jobContainer starts to do split ...
2022-11-16 14:28:54.020 [job-0] INFO  JobContainer - Job set Channel-Number to 1 channels.
2022-11-16 14:28:54.023 [job-0] INFO  JobContainer - DataX Reader.Job [mysqlreader] splits to [1] tasks.
2022-11-16 14:28:54.023 [job-0] INFO  JobContainer - DataX Writer.Job [doriswriter] splits to [1] tasks.
2022-11-16 14:28:54.033 [job-0] INFO  JobContainer - jobContainer starts to do schedule ...
2022-11-16 14:28:54.036 [job-0] INFO  JobContainer - Scheduler starts [1] taskGroups.
2022-11-16 14:28:54.037 [job-0] INFO  JobContainer - Running by standalone Mode.
2022-11-16 14:28:54.041 [taskGroup-0] INFO  TaskGroupContainer - taskGroupId=[0] start [1] channels for [1] tasks.
2022-11-16 14:28:54.043 [taskGroup-0] INFO  Channel - Channel set byte_speed_limit to -1, No bps activated.
2022-11-16 14:28:54.043 [taskGroup-0] INFO  Channel - Channel set record_speed_limit to -1, No tps activated.
2022-11-16 14:28:54.049 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] taskId[0] attemptCount[1] is started
2022-11-16 14:28:54.052 [0-0-0-reader] INFO  CommonRdbmsReader$Task - Begin to read record by Sql: [select taskid,projectid,taskflowid,templateid,template_name,status_task from dwd_universal_tb_task 
] jdbcUrl:[jdbc:mysql://localhost:3306/demo?yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true].
Wed Nov 16 14:28:54 GMT+08:00 2022 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
2022-11-16 14:28:54.071 [0-0-0-reader] INFO  CommonRdbmsReader$Task - Finished read record by Sql: [select taskid,projectid,taskflowid,templateid,template_name,status_task from dwd_universal_tb_task 
] jdbcUrl:[jdbc:mysql://localhost:3306/demo?yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true].
2022-11-16 14:28:54.104 [Thread-1] INFO  DorisStreamLoadObserver - Start to join batch data: rows[2] bytes[438] label[datax_doris_writer_c4e08cb9-c157-4689-932f-db34acc45b6f].
2022-11-16 14:28:54.104 [Thread-1] INFO  DorisStreamLoadObserver - Executing stream load to: 'http://127.0.0.1:8030/api/demo/dwd_universal_tb_task/_stream_load', size: '441'
2022-11-16 14:28:54.224 [Thread-1] INFO  DorisStreamLoadObserver - StreamLoad response :{"Status":"Success","BeginTxnTimeMs":0,"Message":"OK","NumberUnselectedRows":0,"CommitAndPublishTimeMs":17,"Label":"datax_doris_writer_c4e08cb9-c157-4689-932f-db34acc45b6f","LoadBytes":441,"StreamLoadPutTimeMs":1,"NumberTotalRows":2,"WriteDataTimeMs":11,"TxnId":217056,"LoadTimeMs":31,"TwoPhaseCommit":"false","ReadDataTimeMs":0,"NumberLoadedRows":2,"NumberFilteredRows":0}
2022-11-16 14:28:54.225 [Thread-1] INFO  DorisWriterManager - Async stream load finished: label[datax_doris_writer_c4e08cb9-c157-4689-932f-db34acc45b6f].
2022-11-16 14:28:54.249 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] taskId[0] is successed, used[201]ms
2022-11-16 14:28:54.250 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] completed it's tasks.
2022-11-16 14:29:04.048 [job-0] INFO  StandAloneJobContainerCommunicator - Total 2 records, 214 bytes | Speed 21B/s, 0 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2022-11-16 14:29:04.049 [job-0] INFO  AbstractScheduler - Scheduler accomplished all tasks.
2022-11-16 14:29:04.049 [job-0] INFO  JobContainer - DataX Writer.Job [doriswriter] do post work.
Wed Nov 16 14:29:04 GMT+08:00 2022 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
2022-11-16 14:29:04.187 [job-0] INFO  DorisWriter$Job - Start to execute preSqls:[select count(1) from dwd_universal_tb_task]. context info:jdbc:mysql://172.16.0.13:9030/demo.
2022-11-16 14:29:04.204 [job-0] INFO  JobContainer - DataX Reader.Job [mysqlreader] do post work.
2022-11-16 14:29:04.204 [job-0] INFO  JobContainer - DataX jobId [0] completed successfully.
2022-11-16 14:29:04.204 [job-0] INFO  HookInvoker - No hook invoked, because base dir not exists or is a file: /data/datax/hook
2022-11-16 14:29:04.205 [job-0] INFO  JobContainer - 
         [total cpu info] => 
                averageCpu                     | maxDeltaCpu                    | minDeltaCpu                    
                -1.00%                         | -1.00%                         | -1.00%
                        

         [total gc info] => 
                 NAME                 | totalGCCount       | maxDeltaGCCount    | minDeltaGCCount    | totalGCTime        | maxDeltaGCTime     | minDeltaGCTime     
                 PS MarkSweep         | 1                  | 1                  | 1                  | 0.017s             | 0.017s             | 0.017s             
                 PS Scavenge          | 1                  | 1                  | 1                  | 0.007s             | 0.007s             | 0.007s             

2022-11-16 14:29:04.205 [job-0] INFO  JobContainer - PerfTrace not enable!
2022-11-16 14:29:04.206 [job-0] INFO  StandAloneJobContainerCommunicator - Total 2 records, 214 bytes | Speed 21B/s, 0 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2022-11-16 14:29:04.206 [job-0] INFO  JobContainer - 
任务启动时刻                    : 2022-11-16 14:28:53
任务结束时刻                    : 2022-11-16 14:29:04
任务总计耗时                    :                 10s
任务平均流量                    :               21B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   2
读写失败总数                    :                   0

```

