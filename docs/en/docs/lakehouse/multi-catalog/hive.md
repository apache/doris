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
    'yarn.resourcemanager.address' = 'your-rm-address:your-rm-port',    
    'yarn.resourcemanager.principal' = 'your-rm-principal/your-rm-address@YOUR.COM'
);
```

Remember `krb5.conf` and `keytab` file should be placed at all `BE` nodes and `FE` nodes. The location of `keytab` file should be equal to the value of `hadoop.kerberos.keytab`.
As default, `krb5.conf` should be placed at `/etc/krb5.conf`.

To provide Hadoop KMS encrypted transmission information:

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'dfs.encryption.key.provider.uri' = 'kms://http@kms_host:kms_port/kms'
);
```

Or to connect to Hive data stored in JuiceFS:

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
| other         | unsupported   |                                                   |
