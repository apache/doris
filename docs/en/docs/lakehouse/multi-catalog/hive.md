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

Once Doris is connected to Hive Metastore or made compatible with Hive Metastore metadata service, it can access databases and tables in Hive and conduct queries.

Besides Hive, many other systems, such as Iceberg and Hudi, use Hive Metastore to keep their metadata. Thus, Doris can also access these systems via Hive Catalog. 

## Usage

When connnecting to Hive, Doris:

1. Supports Hive version 1/2/3;
2. Supports both Managed Table and External Table;
3. Can identify metadata of Hive, Iceberg, and Hudi stored in Hive Metastore;
4. Supports Hive tables with data stored in JuiceFS, which can be used the same way as normal Hive tables (put `juicefs-hadoop-x.x.x.jar` in `fe/lib/` and `apache_hdfs_broker/lib/`).

## Create Catalog

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

 In addition to `type` and  `hive.metastore.uris` , which are required, you can specify other parameters regarding the connection.

> `specified_database_list`:
> 
> only synchronize the specified databases, split with ','. Default values is '' will synchronize all databases. db name is case sensitive.
> 
	
For example, to specify HDFS HA:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

To specify HDFS HA and Kerberos authentication information:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hive.metastore.sasl.enabled' = 'true',
    'hive.metastore.kerberos.principal' = 'your-hms-principal',
    'dfs.nameservices'='your-nameservice',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'hadoop.security.authentication' = 'kerberos',
    'hadoop.kerberos.keytab' = '/your-keytab-filepath/your.keytab',   
    'hadoop.kerberos.principal' = 'your-principal@YOUR.COM',
    'yarn.resourcemanager.principal' = 'your-rm-principal'
);
```

Remember `krb5.conf` and `keytab` file should be placed at all `BE` nodes and `FE` nodes. The location of `keytab` file should be equal to the value of `hadoop.kerberos.keytab`.
As default, `krb5.conf` should be placed at `/etc/krb5.conf`.

Value of `hive.metastore.kerberos.principal` should be same with the same name property used by HMS you are connecting to, which can be found in `hive-site.xml`.

To provide Hadoop KMS encrypted transmission information:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'dfs.encryption.key.provider.uri' = 'kms://http@kms_host:kms_port/kms'
);
```

Or to connect to Hive data stored on JuiceFS:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'root',
    'fs.jfs.impl' = 'io.juicefs.JuiceFileSystem',
    'fs.AbstractFileSystem.jfs.impl' = 'io.juicefs.JuiceFS',
    'juicefs.meta' = 'xxx'
);
```

Or to connect to Glue and data stored on S3:

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.type" = "glue",
    "aws.region" = "us-east-1",
    "aws.glue.access-key" = "ak",
    "aws.glue.secret-key" = "sk",
    "AWS_ENDPOINT" = "s3.us-east-1.amazonaws.com",
    "AWS_REGION" = "us-east-1",
    "AWS_ACCESS_KEY" = "ak",
    "AWS_SECRET_KEY" = "sk",
    "use_path_style" = "true"
);
```

In Doris 1.2.1 and newer, you can create a Resource that contains all these parameters, and reuse the Resource when creating new Catalogs. Here is an example:

```sql
# 1. Create Resource
CREATE RESOURCE hms_resource PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
	
# 2. Create Catalog and use an existing Resource. The key and value information in the followings will overwrite the corresponding information in the Resource.
CREATE CATALOG hive WITH RESOURCE hms_resource PROPERTIES(
    'key' = 'value'
);
```

<version since="dev"></version> 
You can use the config `file.meta.cache.ttl-second` to set TTL(Time-to-Live) config of File Cache, so that the stale file info will be invalidated automatically after expiring. The unit of time is second.
You can also set file_meta_cache_ttl_second to 0 to disable file cache.Here is an example:
```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'file.meta.cache.ttl-second' = '60'
);
```

You can also put the `hive-site.xml` file in the `conf`  directories of FE and BE. This will enable Doris to automatically read information from `hive-site.xml`. The relevant information will be overwritten based on the following rules :
	

* Information in Resource will overwrite that in  `hive-site.xml`. 
* Information in `CREATE CATALOG PROPERTIES` will overwrite that in Resource.

### Hive Versions

Doris can access Hive Metastore in all Hive versions. By default, Doris uses the interface compatible with Hive 2.3 to access Hive Metastore. You can specify a certain Hive version when creating Catalogs, for example:

```sql 
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hive.version' = '1.1.0'
);
```

## Column Type Mapping

This is applicable for Hive/Iceberge/Hudi.

| HMS Type      | Doris Type    | Comment                                           |
| ------------- | ------------- | ------------------------------------------------- |
| boolean       | boolean       |                                                   |
| tinyint       | tinyint       |                                                   |
| smallint      | smallint      |                                                   |
| int           | int           |                                                   |
| bigint        | bigint        |                                                   |
| date          | date          |                                                   |
| timestamp     | datetime      |                                                   |
| float         | float         |                                                   |
| double        | double        |                                                   |
| char          | char          |                                                   |
| varchar       | varchar       |                                                   |
| decimal       | decimal       |                                                   |
| `array<type>` | `array<type>` | Support nested array, such as `array<array<int>>` |
| `map<KeyType, ValueType>` | `map<KeyType, ValueType>` | Not support nested map. KeyType and ValueType should be primitive types. |
| `struct<col1: Type1, col2: Type2, ...>` | `struct<col1: Type1, col2: Type2, ...>` | Not support nested struct. Type1, Type2, ... should be primitive types. |
| other         | unsupported   |                                                   |

## Use Ranger for permission verification

<version since="dev">

Apache Ranger is a security framework for monitoring, enabling services, and managing comprehensive data security access on the Hadoop platform.

Currently, Doris supports Ranger's library, table, and column permissions, but does not support encryption, row permissions, and so on.

</version>


### Environment configuration

Connecting to Hive Metastore with Ranger permission verification enabled requires additional configuration&configuration environment:
1. When creating a catalog, add:

```sql
"access_controller.properties.ranger.service.name" = "hive",
"access_controller.class" = "org.apache.doris.catalog.authorizer.RangerHiveAccessControllerFactory",
```
2. Configure all FE environments:

    1. Copy the configuration files ranger-live-audit.xml, ranger-live-security.xml, ranger-policymgr-ssl.xml under the HMS conf directory to<doris_ Home>/conf directory.

    2. Modify the properties of ranger-live-security.xml. The reference configuration is as follows:

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
    3. To obtain the log of Ranger authentication itself, you can click<doris_ Add the configuration file log4j.properties under the home>/conf directory.

    4. Restart FE.

### Best Practices

1.Create user user1 on the ranger side and authorize the query permission of db1.table1.col1 

2.Create the role role1 on the ranger side and authorize the query permission of db1.table1.col2

3.Create user user1 with the same name in Doris, and user1 will directly have the query permission of db1.table1.col1

4.Create the role role1 with the same name in Doris and assign role1 to user1. User1 will have query permissions for both db1.table1.col1 and col2

