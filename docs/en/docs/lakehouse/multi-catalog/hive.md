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

## Metadata cache settings

When creating a Catalog, you can use the parameter `file.meta.cache.ttl-second` to set the automatic expiration time of the Hive partition file cache, or set this value to 0 to disable the partition file cache. The time unit is: second. Examples are as follows:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.0.0.2:8088',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.0.0.3:8088',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'file.meta.cache.ttl-second' = '60'
);
```

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
| `array<type>` | `array<type>`| 支持array嵌套，如 `array<array<int>>` |
| `map<KeyType, ValueType>` | `map<KeyType, ValueType>` | 暂不支持嵌套，KeyType 和 ValueType 需要为基础类型 |
| `struct<col1: Type1, col2: Type2, ...>` | `struct<col1: Type1, col2: Type2, ...>` | 暂不支持嵌套，Type1, Type2, ... 需要为基础类型 |
| other | unsupported | |

## Whether to truncate char or varchar columns according to the schema of the hive table

If the variable `truncate_char_or_varchar_columns` is enabled, when the maximum length of the char or varchar column in the schema of the hive table is inconsistent with the schema in the underlying parquet or orc file, it will be truncated according to the maximum length of the hive table column.

The variable default is false.

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

