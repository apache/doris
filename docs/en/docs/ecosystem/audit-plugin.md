---
{
    "title": "Audit Log Plugin",
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

# Audit Log Plugin

Doris's audit log plugin was developed based on FE's plugin framework. Is an optional plugin. Users can install or uninstall this plugin at runtime.

This plugin can periodically import the FE audit log into the specified Doris cluster, so that users can easily view and analyze the audit log through SQL.

## Compile, Configure and Deploy

### FE Configuration

The audit log plug-in framework is enabled by default in Doris and is controlled by the FE configuration `plugin_enable`

### AuditLoader Configuration

1. Download the Audit Loader plugin

   The Audit Loader plug-in is provided by default in the Doris distribution. After downloading the Doris installation package through [DOWNLOAD](https://doris.apache.org/download), decompress it and enter the directory, you can find the auditloader.zip file in the extensionsaudit_loader subdirectory.

2. Unzip the installation package

    ```shell
    unzip auditloader.zip
    ```

    Unzip and generate the following files:

    * auditloader.jar: plug-in code package.
    * plugin.properties: plugin properties file.
    * plugin.conf: plugin configuration file.

You can place this file on an http download server or copy(or unzip) it to the specified directory of all FEs. Here we use the latter.  
The installation of this plugin can be found in [INSTALL](../sql-manual/sql-reference/Database-Administration-Statements/INSTALL-PLUGIN.md)  
After executing install, the AuditLoader directory will be automatically generated.

3. Modify plugin.conf

   The following configurations are available for modification:

    * frontend_host_port: FE node IP address and HTTP port in the format <fe_ip>:<fe_http_port>. The default value is 127.0.0.1:8030.
    * database: Audit log database name.
    * audit_log_table: Audit log table name.
    * slow_log_table: Slow query log table name.
    * enable_slow_log: Whether to enable the slow query log import function. The default value is false.
    * user: Cluster username. The user must have INSERT permission on the corresponding table.
    * password: Cluster user password.

### 创建库表

In Doris, you need to create the library and table of the audit log. The table structure is as follows:

If you need to enable the slow query log import function, you need to create an additional slow table `doris_slow_log_tbl__`, whose table structure is consistent with `doris_audit_log_tbl__`.

Among them, the `dynamic_partition` attribute selects the number of days for audit log retention according to your own needs.

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

>**Notice**
>
> In the above table structure: stmt string, this can only be used in 0.15 and later versions, in previous versions, the field type used varchar

### Deployment

You can place the packaged auditloader.zip on an http server, or copy `auditloader.zip` to the same specified directory in all FEs.

### Installation

通过以下语句安装 Audit Loader 插件：

```sql
INSTALL PLUGIN FROM [source] [PROPERTIES ("key"="value", ...)]
```

Detailed command reference: [INSTALL-PLUGIN.md](../sql-manual/sql-reference/Database-Administration-Statements/INSTALL-PLUGIN)

After successful installation, you can see the installed plug-ins through `SHOW PLUGINS`, and the status is `INSTALLED`.

After completion, the plugin will continuously insert audit logs into this table at specified intervals.
