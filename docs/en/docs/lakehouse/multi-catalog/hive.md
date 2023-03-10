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
    'dfs.nameservices'='your-nameservice',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'hadoop.security.authentication' = 'kerberos',
    'hadoop.kerberos.keytab' = '/your-keytab-filepath/your.keytab',   
    'hadoop.kerberos.principal' = 'your-principal@YOUR.COM',
    'hive.metastore.kerberos.principal' = 'your-hms-principal'
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

<version since="dev">

when connecting to Hive Metastore which is authorized by Ranger, need some properties and update FE runtime environment.

1. add below properties when creating Catalog：

```sql
"access_controller.properties.ranger.service.name" = "<the ranger servive name your hms using>",
"access_controller.class" = "org.apache.doris.catalog.authorizer.RangerHiveAccessControllerFactory",
```

2. update all FEs' runtime environment：
   a. copy all ranger-*.xml files to <doris_home>/conf which are located in HMS/conf directory
   b. update value of `ranger.plugin.hive.policy.cache.dir` in ranger-<ranger_service_name>-security.xml to a writable directory
   c. add a log4j.properties to <doris_home>/conf, thus you can get logs of ranger authorizer
   d. restart FE

</version>

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
