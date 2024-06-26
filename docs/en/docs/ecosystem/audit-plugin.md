---
{
    "title": "Audit log plugin",
    "language": "en"
}
---

<!-
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
->

# Audit log plugin

Doris's audit log plugin was developed based on FE's plugin framework. Is an optional plugin. Users can install or uninstall this plugin at runtime.

This plugin can periodically import the FE audit log into the specified Doris cluster, so that users can easily view and analyze the audit log through SQL.

## Compile, Configure and Deploy

### FE Configuration

FE's plugin framework is an experimental feature, which is closed by default. In the FE configuration file, add `plugin_enable = true` to enable the plugin framework.

### AuditLoader Configuration

The configuration of the auditloader plugin is located in `$ {DORIS}/fe_plugins/auditloader/src/main/assembly/`.

Open `plugin.conf` for configuration. See the comments of the configuration items.

<version since="1.2.0"></version>
Audit log plugin supports importing slow query logs into a separate slow table since version 1.2, `doris_slow_log_tbl__`,  which is closed by default. In the plugin configuration file, add `enable_slow_log = true` to enable the function. And you could modify 'qe_slow_log_ms' item in FE configuration file to change slow query threshold.

### Compile

After executing `sh build_plugin.sh` in the Doris code directory, you will get the `auditloader.zip` file in the `fe_plugins/output` directory.

### Deployment

You can place this file on an http download server or copy(or unzip) it to the specified directory of all FEs. Here we use the latter.

### Installation

After deployment is complete, and before installing the plugin, you need to create the audit database and tables previously specified in `plugin.conf`. If `enable_slow_log` is set true, the slow table `doris_slow_log_tbl__` needs to be created, with the same schema as `doris_audit_log_tbl__`. The database and table creation statement is as follows:

```
create database doris_audit_db__;

create table doris_audit_db__.doris_audit_log_tbl__
(
    query_id varchar(48) comment "Unique query id",
    `time` datetime not null comment "Query start time",
    client_ip varchar(32) comment "Client IP",
    user varchar(64) comment "User name",
    catalog varchar(128) comment "Catalog of this query",
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

create table doris_audit_db__.doris_slow_log_tbl__
(
    query_id varchar(48) comment "Unique query id",
    `time` datetime not null comment "Query start time",
    client_ip varchar(32) comment "Client IP",
    user varchar(64) comment "User name",
    catalog varchar(128) comment "Catalog of this query",
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
```

>**Notice**
>
> In the above table structure: stmt string, this can only be used in 0.15 and later versions, in previous versions, the field type used varchar

The `dynamic_partition` attribute selects the number of days to keep the audit log based on your needs.

After that, connect to Doris and use the `INSTALL PLUGIN` command to complete the installation. After successful installation, you can see the installed plug-ins through `SHOW PLUGINS`, and the status is `INSTALLED`.

Upon completion, the plug-in will continuously import audit date into this table at specified intervals.
