---
{
    "title": "EXPORT",
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

# EXPORT
## description

    该语句用于将指定表的数据导出到指定位置。
    该功能通过 broker 进程实现。对于不同的目的存储系统，需要部署不同的 broker。可以通过 SHOW BROKER 查看已部署的 broker。
    这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW EXPORT 命令查看进度。

    语法：
        EXPORT TABLE table_name
        [PARTITION (p1[,p2])]
        TO export_path
        [opt_properties]
        broker;

    1. table_name
      当前要导出的表的表名，目前支持engine为olap和mysql的表的导出。

    2. partition
      可以只导出指定表的某些指定分区

    3. export_path
      导出的路径，需为目录。目前不能导出到本地，需要导出到broker。

    4. opt_properties
      用于指定一些特殊参数。
          语法：
          [PROPERTIES ("key"="value", ...)]
        
          可以指定如下参数：
            column_separator: 指定导出的列分隔符，默认为\t。
            line_delimiter: 指定导出的行分隔符，默认为\n。
            exec_mem_limit: 导出在单个 BE 节点的内存使用上限，默认为 2GB，单位为字节。
            timeout：导入作业的超时时间，默认为1天，单位是秒。
            tablet_num_per_task：每个子任务能分配的最大 Tablet 数量。

    5. broker
      用于指定导出使用的broker
          语法：
          WITH BROKER broker_name ("key"="value"[,...])
          这里需要指定具体的broker name, 以及所需的broker属性

      对于不同存储系统对应的 broker，这里需要输入的参数不同。具体参数可以参阅：`help broker load` 中 broker 所需属性。

## example

    1. 将 testTbl 表中的所有数据导出到 hdfs 上
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    2. 将 testTbl 表中的分区p1,p2导出到 hdfs 上

        EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    3. 将 testTbl 表中的所有数据导出到 hdfs 上，以","作为列分隔符

        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

## keyword
    EXPORT

