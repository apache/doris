---
{
    "title": "审计日志插件",
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

# 审计日志插件

Doris 的审计日志插件是在 FE 的插件框架基础上开发的。是一个可选插件。用户可以在运行时安装或卸载这个插件。

该插件可以将 FE 的审计日志定期的导入到指定 Doris 集群中，以方便用户通过 SQL 对审计日志进行查看和分析。

## 编译、配置和部署

### FE 配置

FE的插件框架当前是实验性功能，Doris中默认关闭，在FE的配置文件中，增加`plugin_enable = true`启用plugin框架

### AuditLoader 配置

auditloader plugin的配置位于`${DORIS}/fe_plugins/auditloader/src/main/assembly/`. 

打开 `plugin.conf` 进行配置。配置项说明参见注释。

### 编译

在 Doris 代码目录下执行 `sh build_plugin.sh` 后，会在 `fe_plugins/output` 目录下得到 `auditloader.zip` 文件。

### 部署

您可以将这个文件放置在一个 http 服务器上，或者拷贝`auditloader.zip`(或者解压`auditloader.zip`)到所有 FE 的指定目录下。这里我们使用后者。

### 安装

部署完成后，安装插件前，需要创建之前在 `plugin.conf` 中指定的审计数据库和表。其中建表语句如下：

```
create table doris_audit_tbl__
(
    query_id varchar(48) comment "Unique query id",
    time datetime not null comment "Query start time",
    client_ip varchar(32) comment "Client IP",
    user varchar(64) comment "User name",
    db varchar(96) comment "Database of this query",
    state varchar(8) comment "Query result state. EOF, ERR, OK",
    query_time bigint comment "Query execution time in millisecond",
    scan_bytes bigint comment "Total scan bytes of this query",
    scan_rows bigint comment "Total scan rows of this query",
    return_rows bigint comment "Returned rows of this query",
    stmt_id int comment "An incremental id of statement",
    is_query tinyint comment "Is this statemt a query. 1 or 0",
    frontend_ip varchar(32) comment "Frontend ip of executing this statement",
    stmt varchar(5000) comment "The original statement, trimed if longer than 5000 bytes"
) engine=OLAP
duplicate key(query_id, time, client_ip)
partition by range(time) ()
distributed by hash(query_id) buckets 1
properties(
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1",
    "dynamic_partition.enable" = "true",
    "replication_num" = "3"
);
```

其中 `dynamic_partition` 属性根据自己的需要，选择审计日志安保留的天数。

之后，连接到 Doris 后使用 `INSTALL PLUGIN` 命令完成安装。安装成功后，可以通过 `SHOW PLUGINS` 看到已经安装的插件，并且状态为 `INSTALLED`。

完成后，插件会不断的以指定的时间间隔将审计日志插入到这个表中。



















