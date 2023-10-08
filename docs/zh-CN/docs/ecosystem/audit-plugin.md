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

审计日志插件框架在 Doris 中是默认开启的的，由 FE 的配置 `plugin_enable` 控制

### AuditLoader 配置

1. 下载 Audit Loader 插件

    Audit Loader 插件在 Doris 的发行版中默认提供，通过 [DOWNLOAD](https://doris.apache.org/zh-CN/download) 下载 Doris 安装包解压并进入目录后即可在 extensions/audit_loader 子目录下找到 auditloader.zip 文件。

2. 解压安装包

    ```shell
    unzip auditloader.zip
    ```

    解压生成以下文件：

    * auditloader.jar：插件代码包。
    * plugin.properties：插件属性文件。
    * plugin.conf：插件配置文件。

您可以将这个文件放置在一个 http 服务器上，或者拷贝`auditloader.zip`(或者解压`auditloader.zip`)到所有 FE 的指定目录下。这里我们使用后者。  
该插件的安装可以参阅 [INSTALL](../sql-manual/sql-reference/Database-Administration-Statements/INSTALL-PLUGIN.md)  
执行install后会自动生成AuditLoader目录

3. 修改 plugin.conf 

    以下配置可供修改：

    * frontend_host_port：FE 节点 IP 地址和 HTTP 端口，格式为 <fe_ip>:<fe_http_port>。 默认值为 127.0.0.1:8030。
    * database：审计日志库名。
    * audit_log_table：审计日志表名。
    * slow_log_table：慢查询日志表名。
    * enable_slow_log：是否开启慢查询日志导入功能。默认值为 false。
    * user：集群用户名。该用户必须具有对应表的 INSERT 权限。
    * password：集群用户密码。

4. 重新打包 Audit Loader 插件

    ```shell
    zip -r -q -m auditloader.zip auditloader.jar plugin.properties plugin.conf
    ```

### 创建库表

在 Doris 中，需要创建审计日志的库和表，表结构如下：

若需开启慢查询日志导入功能，还需要额外创建慢表 `doris_slow_log_tbl__`，其表结构与 `doris_audit_log_tbl__` 一致。

其中 `dynamic_partition` 属性根据自己的需要，选择审计日志保留的天数。

```sql
create database doris_audit_db__;

create table doris_audit_db__.doris_audit_log_tbl__
(
    query_id varchar(48) comment "Unique query id",
    `time` datetime not null comment "Query start time",
    client_ip varchar(32) comment "Client IP",
    user varchar(64) comment "User name",
    db varchar(96) comment "Database of this query",
    state varchar(8) comment "Query result state. EOF, ERR, OK",
    error_code int comment "Error code of failing query.",
    error_message string comment "Error message of failing query.",
    query_time bigint comment "Query execution time in millisecond",
    scan_bytes bigint comment "Total scan bytes of this query",
    scan_rows bigint comment "Total scan rows of this query",
    return_rows bigint comment "Returned rows of this query",
    stmt_id int comment "An incremental id of statement",
    is_query tinyint comment "Is this statemt a query. 1 or 0",
    frontend_ip varchar(32) comment "Frontend ip of executing this statement",
    cpu_time_ms bigint comment "Total scan cpu time in millisecond of this query",
    sql_hash varchar(48) comment "Hash value for this query",
    sql_digest varchar(48) comment "Sql digest for this query",
    peak_memory_bytes bigint comment "Peak memory bytes used on all backends of this query",
    stmt string comment "The original statement, trimed if longer than 2G"
) engine=OLAP
duplicate key(query_id, `time`, client_ip)
partition by range(`time`) ()
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

create table doris_audit_db__.doris_slow_log_tbl__
(
    query_id varchar(48) comment "Unique query id",
    `time` datetime not null comment "Query start time",
    client_ip varchar(32) comment "Client IP",
    user varchar(64) comment "User name",
    db varchar(96) comment "Database of this query",
    state varchar(8) comment "Query result state. EOF, ERR, OK",
    error_code int comment "Error code of failing query.",
    error_message string comment "Error message of failing query.",
    query_time bigint comment "Query execution time in millisecond",
    scan_bytes bigint comment "Total scan bytes of this query",
    scan_rows bigint comment "Total scan rows of this query",
    return_rows bigint comment "Returned rows of this query",
    stmt_id int comment "An incremental id of statement",
    is_query tinyint comment "Is this statemt a query. 1 or 0",
    frontend_ip varchar(32) comment "Frontend ip of executing this statement",
    cpu_time_ms bigint comment "Total scan cpu time in millisecond of this query",
    sql_hash varchar(48) comment "Hash value for this query",
    sql_digest varchar(48) comment "Sql digest for this query",
    peak_memory_bytes bigint comment "Peak memory bytes used on all backends of this query",
    stmt string comment "The original statement, trimed if longer than 2G "
) engine=OLAP
duplicate key(query_id, `time`, client_ip)
partition by range(`time`) ()
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

>**注意**
>
> 上面表结构中：stmt string ，这个只能在0.15及之后版本中使用，之前版本，字段类型使用varchar

### 部署

您可以将 打包好的 auditloader.zip 放置在一个 http 服务器上，或者拷贝`auditloader.zip` 到所有 FE 的相同指定目录下。

### 安装

通过以下语句安装 Audit Loader 插件：

```sql
INSTALL PLUGIN FROM [source] [PROPERTIES ("key"="value", ...)]
```

详细命令参考：[INSTALL-PLUGIN.md](../sql-manual/sql-reference/Database-Administration-Statements/INSTALL-PLUGIN)

安装成功后，可以通过 `SHOW PLUGINS` 看到已经安装的插件，并且状态为 `INSTALLED`。

完成后，插件会不断的以指定的时间间隔将审计日志插入到这个表中。
