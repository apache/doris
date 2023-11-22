---
{
    "title": "Hive",
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

# Hive

By connecting to Hive Metastore, or a metadata service compatible with Hive Metatore, Doris can automatically obtain Hive database table information and perform data queries.

In addition to Hive, many other systems also use the Hive Metastore to store metadata. So through Hive Catalog, we can not only access Hive, but also access systems that use Hive Metastore as metadata storage. Such as Iceberg, Hudi, etc.

## Terms and Conditions

1. Need to put core-site.xml, hdfs-site.xml and hive-site.xml in the conf directory of FE and BE. First read the hadoop configuration file in the conf directory, and then read the related to the environment variable `HADOOP_CONF_DIR` configuration file.
2. hive supports version 1/2/3.
3. Support Managed Table and External Table and part of Hive View.
4. Can identify hive, iceberg, hudi metadata stored in Hive Metastore.

## Create Catalog

### Hive On HDFS

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083'
);
```

In addition to the two required parameters of `type` and `hive.metastore.uris`, more parameters can be passed to pass the information required for the connection.

If HDFS HA information is provided, the example is as follows:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:8088',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:8088',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

Provide HDFS HA information and Kerberos authentication information at the same time, examples are as follows:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hive.metastore.sasl.enabled' = 'true',
    'hive.metastore.kerberos.principal' = 'your-hms-principal',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:8088',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:8088',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'hadoop.security.authentication' = 'kerberos',
    'hadoop.kerberos.keytab' = '/your-keytab-filepath/your.keytab',   
    'hadoop.kerberos.principal' = 'your-principal@YOUR.COM',
    'yarn.resourcemanager.principal' = 'your-rm-principal'
);
```

Please place the `krb5.conf` file and `keytab` authentication file under all `BE` and `FE` nodes. The path of the `keytab` authentication file is consistent with the configuration. The `krb5.conf` file is placed in `/etc by default /krb5.conf` path.

The value of `hive.metastore.kerberos.principal` needs to be consistent with the property of the same name of the connected hive metastore, which can be obtained from `hive-site.xml`.

### Hive On VIEWFS

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:8088',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:8088',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'fs.defaultFS' = 'viewfs://your-cluster',
    'fs.viewfs.mounttable.your-cluster.link./ns1' = 'hdfs://your-nameservice/',
    'fs.viewfs.mounttable.your-cluster.homedir' = '/ns1'
);
```

viewfs related parameters can be added to the catalog configuration as above, or added to `conf/core-site.xml`.

How viewfs works and parameter configuration, please refer to relevant hadoop documents, for example, https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html

### Hive On JuiceFS

Data is stored in JuiceFS, examples are as follows:

(Need to put `juicefs-hadoop-x.x.x.jar` under `fe/lib/` and `apache_hdfs_broker/lib/`)

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hadoop.username' = 'root',
    'fs.jfs.impl' = 'io.juicefs.JuiceFileSystem',
    'fs.AbstractFileSystem.jfs.impl' = 'io.juicefs.JuiceFS',
    'juicefs.meta' = 'xxx'
);
```

### Hive On S3

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.uris" = "thrift://172.0.0.1:9083",
    "s3.endpoint" = "s3.us-east-1.amazonaws.com",
    "s3.region" = "us-east-1",
    "s3.access_key" = "ak",
    "s3.secret_key" = "sk"
    "use_path_style" = "true"
);
```

Options:

* s3.connection.maximum: s3 maximum connection number, default 50
* s3.connection.request.timeout: s3 request timeout, default 3000ms
* s3.connection.timeout: s3 connection timeout, default 1000ms

### Hive On OSS

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.uris" = "thrift://172.0.0.1:9083",
    "oss.endpoint" = "oss.oss-cn-beijing.aliyuncs.com",
    "oss.access_key" = "ak",
    "oss.secret_key" = "sk"
);
```

### Hive On OBS

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.uris" = "thrift://172.0.0.1:9083",
    "obs.endpoint" = "obs.cn-north-4.myhuaweicloud.com",
    "obs.access_key" = "ak",
    "obs.secret_key" = "sk"
);
```

### Hive On COS

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.uris" = "thrift://172.0.0.1:9083",
    "cos.endpoint" = "cos.ap-beijing.myqcloud.com",
    "cos.access_key" = "ak",
    "cos.secret_key" = "sk"
);
```

### Hive With Glue

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.type" = "glue",
    "glue.endpoint" = "https://glue.us-east-1.amazonaws.com",
    "glue.access_key" = "ak",
    "glue.secret_key" = "sk"
);
```

## Metadata Cache & Refresh

For Hive Catalog, 4 types of metadata are cached in Doris:

1. Table structure: cache table column information, etc.
2. Partition value: Cache the partition value information of all partitions of a table.
3. Partition information: Cache the information of each partition, such as partition data format, partition storage location, partition value, etc.
4. File information: Cache the file information corresponding to each partition, such as file path location, etc.

The above cache information will not be persisted to Doris, so operations such as restarting Doris's FE node, switching masters, etc. may cause the cache to become invalid. After the cache expires, Doris will directly access the Hive MetaStore to obtain information and refill the cache.

Metadata cache can be updated automatically, manually, or configured with TTL (Time-to-Live) according to user needs.

### Default behavior and TTL

By default, the metadata cache expires 10 minutes after it is first accessed. This time is determined by the configuration parameter `external_cache_expire_time_minutes_after_access` in fe.conf. (Note that in versions 2.0.1 and earlier, the default value for this parameter was 1 day).

For example, if the user accesses the metadata of table A for the first time at 10:00, then the metadata will be cached and will automatically expire after 10:10. If the user accesses the same metadata again at 10:11, Doris will directly access the Hive MetaStore to obtain information and refill the cache.

`external_cache_expire_time_minutes_after_access` affects all 4 caches under Catalog.

For the `INSERT INTO OVERWRITE PARTITION` operation commonly used in Hive, you can also timely update the `File Information Cache` by configuring the TTL of the `File Information Cache`:

```
CREATE CATALOG hive PROPERTIES (
     'type'='hms',
     'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
     'file.meta.cache.ttl-second' = '60'
);
```

In the above example, `file.meta.cache.ttl-second` is set to 60 seconds, and the cache will expire after 60 seconds. This parameter will only affect the `file information cache`.

You can also set this value to 0 to disable file caching, which will fetch file information directly from the Hive MetaStore every time.

### Manual refresh

Users need to manually refresh the metadata through the [REFRESH](../../sql-manual/sql-reference/Utility-Statements/REFRESH.md) command.

1. REFRESH CATALOG: Refresh the specified Catalog.

     ```
     REFRESH CATALOG ctl1 PROPERTIES("invalid_cache" = "true");
     ```

     This command will refresh the database list, table list, and all cache information of the specified Catalog.

     `invalid_cache` indicates whether to flush the cache. Defaults to true. If it is false, only the database and table list of the catalog will be refreshed, but the cache information will not be refreshed. This parameter is applicable when the user only wants to synchronize newly added or deleted database/table information.

2. REFRESH DATABASE: Refresh the specified Database.

     ```
     REFRESH DATABASE [ctl.]db1 PROPERTIES("invalid_cache" = "true");
     ```

     This command will refresh the table list of the specified Database and all cached information under the Database.

     The meaning of the `invalid_cache` attribute is the same as above. Defaults to true. If false, only the Database's table list will be refreshed, not cached information. This parameter is suitable for users who only want to synchronize newly added or deleted table information.

3. REFRESH TABLE: Refresh the specified Table.

     ```
     REFRESH TABLE [ctl.][db.]tbl1;
     ```

     This command will refresh all cache information under the specified Table.

### Regular refresh

Users can set the scheduled refresh of the Catalog when creating the Catalog.

```
CREATE CATALOG hive PROPERTIES (
     'type'='hms',
     'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
     'metadata_refresh_interval_sec' = '600'
);
```

In the above example, `metadata_refresh_interval_sec` means refreshing the Catalog every 600 seconds. Equivalent to automatically executing every 600 seconds:

`REFRESH CATALOG ctl1 PROPERTIES("invalid_cache" = "true");`

The scheduled refresh interval must not be less than 5 seconds.

### Auto Refresh

Currently, Doris only supports automatic update of metadata in Hive Metastore (HMS). It perceives changes in metadata by the FE node which regularly reads the notification events from HMS. The supported events are as follows:

| Event           | Corresponding Update Operation                               |
| :-------------- | :----------------------------------------------------------- |
| CREATE DATABASE | Create a database in the corresponding catalog.              |
| DROP DATABASE   | Delete a database in the corresponding catalog.              |
| ALTER DATABASE  | Such alterations mainly include changes in properties, comments, or storage location of databases. They do not affect Doris' queries in External Catalogs so they will not be synchronized. |
| CREATE TABLE    | Create a table in the corresponding database.                |
| DROP TABLE      | Delete a table in the corresponding database, and invalidate the cache of that table. |
| ALTER TABLE     | If it is a renaming, delete the table of the old name, and then create a new table with the new name; otherwise, invalidate the cache of that table. |
| ADD PARTITION   | Add a partition to the cached partition list of the corresponding table. |
| DROP PARTITION  | Delete a partition from the cached partition list of the corresponding table, and invalidate the cache of that partition. |
| ALTER PARTITION | If it is a renaming, delete the partition of the old name, and then create a new partition with the new name; otherwise, invalidate the cache of that partition. |

> After data ingestion, changes in partition tables will follow the `ALTER PARTITION` logic, while those in non-partition tables will follow the `ALTER TABLE` logic.
>
> If changes are conducted on the file system directly instead of through the HMS, the HMS will not generate an event. As a result, such changes will not be perceived by Doris.

The automatic update feature involves the following parameters in fe.conf:

1. `enable_hms_events_incremental_sync`: This specifies whether to enable automatic incremental synchronization for metadata, which is disabled by default. 
2. `hms_events_polling_interval_ms`: This specifies the interval between two readings, which is set to 10000 by default. (Unit: millisecond) 
3. `hms_events_batch_size_per_rpc`: This specifies the maximum number of events that are read at a time, which is set to 500 by default.

To enable automatic update(Excluding Huawei MRS), you need to modify the hive-site.xml of HMS and then restart HMS and HiveServer2:

```
<property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.dml.events</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>

```

Huawei's MRS needs to change hivemetastore-site.xml and restart HMS and HiveServer2:

```
<property>
    <name>metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>
```

Note: Value is appended with commas separated from the original value, not overwritten.For example, the default configuration for MRS 3.1.0 is

```
<property>
    <name>metastore.transactional.event.listeners</name>
    <value>com.huawei.bigdata.hive.listener.TableKeyFileManagerListener,org.apache.hadoop.hive.metastore.listener.FileAclListener</value>
</property>
```

We need to change to

```
<property>
    <name>metastore.transactional.event.listeners</name>
    <value>com.huawei.bigdata.hive.listener.TableKeyFileManagerListener,org.apache.hadoop.hive.metastore.listener.FileAclListener,org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>
```

> Note: To enable automatic update, whether for existing Catalogs or newly created Catalogs, all you need is to set `enable_hms_events_incremental_sync` to `true`, and then restart the FE node. You don't need to manually update the metadata before or after the restart.

## Hive Version

Doris can correctly access the Hive Metastore in different Hive versions. By default, Doris will access the Hive Metastore with a Hive 2.3 compatible interface. You can also specify the hive version when creating the Catalog. If accessing Hive 1.1.0 version:

```sql 
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hive.version' = '1.1.0'
);
```

## Column type mapping

For Hive/Iceberge/Hudi

| HMS Type | Doris Type | Comment |
|---|---|---|
| boolean| boolean | |
| tinyint|tinyint | |
| smallint| smallint| |
| int| int | |
| bigint| bigint | |
| date| date| |
| timestamp| datetime| |
| float| float| |
| double| double| |
| char| char | |
| varchar| varchar| |
| decimal| decimal | |
| `array<type>` | `array<type>`| support nested type, for example `array<array<int>>` |
| `map<KeyType, ValueType>` | `map<KeyType, ValueType>` | support nested type, for example `map<string, array<int>>` |
| `struct<col1: Type1, col2: Type2, ...>` | `struct<col1: Type1, col2: Type2, ...>` | support nested type, for example `struct<col1: array<int>, col2: map<int, date>>` |
| other | unsupported | |

## Whether to truncate char or varchar columns according to the schema of the hive table

If the variable `truncate_char_or_varchar_columns` is enabled, when the maximum length of the char or varchar column in the schema of the hive table is inconsistent with the schema in the underlying parquet or orc file, it will be truncated according to the maximum length of the hive table column.

The variable default is false.

## Access HMS with broker

Add following setting when creating an HMS catalog, file splitting and scanning for Hive external table will be completed by broker named `test_broker`

```sql
"broker.name" = "test_broker"
```

## Integrate with Apache Ranger

Apache Ranger is a security framework for monitoring, enabling services, and comprehensive data security access management on the Hadoop platform.

Currently doris supports ranger library, table, and column permissions, but does not support encryption, row permissions, etc.

### Settings

To connect to the Hive Metastore with Ranger permission verification enabled, you need to add configuration & configuration environment:

1. When creating a Catalog, add:

```sql
"access_controller.properties.ranger.service.name" = "hive",
"access_controller.class" = "org.apache.doris.catalog.authorizer.RangerHiveAccessControllerFactory",
```

2. Configure all FE environments:

    1. Copy the configuration files ranger-hive-audit.xml, ranger-hive-security.xml, and ranger-policymgr-ssl.xml under the HMS conf directory to the FE conf directory.

    2. Modify the properties of ranger-hive-security.xml, the reference configuration is as follows:

        ```sql
        <?xml version="1.0" encoding="UTF-8"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
            #The directory for caching permission data, needs to be writable
            <property>
                <name>ranger.plugin.hive.policy.cache.dir</name>
                <value>/mnt/datadisk0/zhangdong/rangerdata</value>
            </property>
            #The time interval for periodically pulling permission data
            <property>
                <name>ranger.plugin.hive.policy.pollIntervalMs</name>
                <value>30000</value>
            </property>
        
            <property>
                <name>ranger.plugin.hive.policy.rest.client.connection.timeoutMs</name>
                <value>60000</value>
            </property>
        
            <property>
                <name>ranger.plugin.hive.policy.rest.client.read.timeoutMs</name>
                <value>60000</value>
            </property>
        
            <property>
                <name>ranger.plugin.hive.policy.rest.ssl.config.file</name>
                <value></value>
            </property>
        
            <property>
                <name>ranger.plugin.hive.policy.rest.url</name>
                <value>http://172.21.0.32:6080</value>
            </property>
        
            <property>
                <name>ranger.plugin.hive.policy.source.impl</name>
                <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
            </property>
        
            <property>
                <name>ranger.plugin.hive.service.name</name>
                <value>hive</value>
            </property>
        
            <property>
                <name>xasecure.hive.update.xapolicies.on.grant.revoke</name>
                <value>true</value>
            </property>
        
        </configuration>
        ```

    3. In order to obtain the log of Ranger authentication itself, add the configuration file log4j.properties in the `<doris_home>/conf` directory.

    4. Restart FE.

### Best Practices

1. Create user user1 on the ranger side and authorize the query permission of db1.table1.col1

2. Create role role1 on the ranger side and authorize the query permission of db1.table1.col2

3. Create a user user1 with the same name in doris, user1 will directly have the query authority of db1.table1.col1

4. Create role1 with the same name in doris, and assign role1 to user1, user1 will have the query authority of db1.table1.col1 and col2 at the same time

