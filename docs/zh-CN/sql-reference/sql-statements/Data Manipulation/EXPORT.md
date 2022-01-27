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
        [WHERE [expr]]
        TO export_path
        [opt_properties]
        [broker|S3];

    1. table_name
      当前要导出的表的表名，目前支持engine为olap和mysql的表的导出。

    2. partition
      可以只导出指定表的某些指定分区

    3. expr
      导出满足 where 条件的行，选填。不填则默认导出所有行。

    4. export_path
      导出的路径，需为目录。支持导出到本地，hdfs，百度bos，s3协议的其他存储系统。

    5. opt_properties
      用于指定一些特殊参数。
          语法：
          [PROPERTIES ("key"="value", ...)]
        
          可以指定如下参数：
            label: 指定一个自定义作业标识。后续可以使用这个标识查看作业状态。
            column_separator: 指定导出的列分隔符，默认为\t。支持不可见字符，比如 '\x07'。
            column: 指定待导出的列，使用英文逗号隔开，如果不填这个参数默认是导出表的所有列。
            line_delimiter: 指定导出的行分隔符，默认为\n。支持不可见字符，比如 '\x07'。
            exec_mem_limit: 导出在单个 BE 节点的内存使用上限，默认为 2GB，单位为字节。
            timeout：导入作业的超时时间，默认为1天，单位是秒。
            tablet_num_per_task：每个子任务能分配的最大 Tablet 数量。

    6. broker|s3
      指定使用broker导出或者通过S3协议导出
          语法：
          WITH [BROKER broker_name | S3] ("key"="value"[,...])
          这里需要指定具体的broker name, 以及所需的broker属性, 如果使用S3协议则无需指定broker name

      对于不同存储系统对应的 broker，这里需要输入的参数不同。具体参数可以参阅：`help broker load` 中 broker 所需属性。
      导出到本地时，不需要填写这部分。

    7. hdfs
      指定导出到hdfs
          语法：
          WITH HDFS ("key"="value"[,...])

          可以指定如下参数：
            fs.defaultFS: 指定HDFS的fs，格式为：hdfs://ip:port
            hdfs_user：指定写入HDFS的user

## example

    1. 将 testTbl 表中的所有数据导出到 hdfs 上
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    2. 将 testTbl 表中的分区p1,p2导出到 hdfs 上
        EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    
    3. 将 testTbl 表中的所有数据导出到 hdfs 上，以","作为列分隔符，并指定label
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("label" = "mylabel", "column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    
    4. 将 testTbl 表中 k1 = 1 的行导出到 hdfs 上。
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WHERE k1=1 WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    5. 将 testTbl 表中的所有数据导出到本地。
        EXPORT TABLE testTbl TO "file:///home/data/a";

    6. 将 testTbl 表中的所有数据导出到 hdfs 上，以不可见字符 "\x07" 作为列或者行分隔符。
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"="\\x07", "line_delimiter" = "\\x07") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy")
 
    7. 将 testTbl 表的 k1, v1 列导出到本地。
        EXPORT TABLE testTbl TO "file:///home/data/a" PROPERTIES ("columns" = "k1,v1");

    8. 将 testTbl 表中的所有数据导出到 hdfs 上，以不可见字符 "\x07" 作为列或者行分隔符。
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"="\\x07", "line_delimiter" = "\\x07") WITH HDFS ("fs.defaultFS"="hdfs://hdfs_host:port", "hdfs_user"="yyy")

## keyword
    EXPORT

