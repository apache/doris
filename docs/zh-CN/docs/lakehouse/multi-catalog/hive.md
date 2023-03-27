---
{
    "title": "Hive",
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

# Hive

通过连接 Hive Metastore，或者兼容 Hive Metatore 的元数据服务，Doris 可以自动获取 Hive 的库表信息，并进行数据查询。

除了 Hive 外，很多其他系统也会使用 Hive Metastore 存储元数据。所以通过 Hive Catalog，我们不仅能访问 Hive，也能访问使用 Hive Metastore 作为元数据存储的系统。如 Iceberg、Hudi 等。

## 使用限制

1. hive 支持 1/2/3 版本。
2. 支持 Managed Table 和 External Table。
3. 可以识别 Hive Metastore 中存储的 hive、iceberg、hudi 元数据。
4. 支持数据存储在 Juicefs 上的 hive 表，用法如下（需要把juicefs-hadoop-x.x.x.jar放在 fe/lib/ 和 apache_hdfs_broker/lib/ 下）。

## 创建 Catalog

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

除了 `type` 和 `hive.metastore.uris` 两个必须参数外，还可以通过更多参数来传递连接所需要的信息。
	
如提供 HDFS HA 信息，示例如下：
	
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

同时提供 HDFS HA 信息和 Kerberos 认证信息，示例如下：
	
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

请在所有的 `BE`、`FE` 节点下放置 `krb5.conf` 文件和 `keytab` 认证文件，`keytab` 认证文件路径和配置保持一致，`krb5.conf` 文件默认放置在 `/etc/krb5.conf` 路径。
`hive.metastore.kerberos.principal` 的值需要和所连接的 hive metastore 的同名属性保持一致，可从 `hive-site.xml` 中获取。

提供 Hadoop KMS 加密传输信息，示例如下：
	
```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'dfs.encryption.key.provider.uri' = 'kms://http@kms_host:kms_port/kms'
);
```

hive数据存储在JuiceFS，示例如下：

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

hive元数据存储在Glue，数据存储在S3，示例如下：

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

在 1.2.1 版本之后，我们也可以将这些信息通过创建一个 Resource 统一存储，然后在创建 Catalog 时使用这个 Resource。示例如下：
	
```sql
# 1. 创建 Resource
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
	
# 2. 创建 Catalog 并使用 Resource，这里的 Key Value 信息会覆盖 Resource 中的信息。
CREATE CATALOG hive WITH RESOURCE hms_resource PROPERTIES(
	'key' = 'value'
);
```

我们也可以直接将 hive-site.xml 放到 FE 和 BE 的 conf 目录下，系统也会自动读取 hive-site.xml 中的信息。信息覆盖的规则如下：
	
* Resource 中的信息覆盖 hive-site.xml 中的信息。
* CREATE CATALOG PROPERTIES 中的信息覆盖 Resource 中的信息。

### Hive 版本

Doris 可以正确访问不同 Hive 版本中的 Hive Metastore。在默认情况下，Doris 会以 Hive 2.3 版本的兼容接口访问 Hive Metastore。你也可以在创建 Catalog 时指定 hive 的版本。如访问 Hive 1.1.0 版本：

```sql 
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hive.version' = '1.1.0'
);
```

## 列类型映射

适用于 Hive/Iceberge/Hudi

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

## 使用Ranger进行权限校验

<version since="dev">

Apache Ranger是一个用来在Hadoop平台上进行监控，启用服务，以及全方位数据安全访问管理的安全框架。

目前doris支持ranger的库、表、列权限，不支持加密、行权限等。

</version>

### 环境配置

连接开启 Ranger 权限校验的 Hive Metastore 需要增加配置 & 配置环境：
1. 创建 Catalog 时增加：

```sql
"access_controller.properties.ranger.service.name" = "hive",
"access_controller.class" = "org.apache.doris.catalog.authorizer.RangerHiveAccessControllerFactory",
```
2. 配置所有 FE 环境：
    
   1. 将 HMS conf 目录下的配置文件ranger-hive-audit.xml,ranger-hive-security.xml,ranger-policymgr-ssl.xml复制到 <doris_home>/conf 目录下。

   2. 修改 ranger-hive-security.xml 的属性,参考配置如下：

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
   3. 为获取到 Ranger 鉴权本身的日志，可在 <doris_home>/conf 目录下添加配置文件 log4j.properties。

   4. 重启 FE。

### 最佳实践

1.在ranger端创建用户user1并授权db1.table1.col1的查询权限

2.在ranger端创建角色role1并授权db1.table1.col2的查询权限

3.在doris创建同名用户user1，user1将直接拥有db1.table1.col1的查询权限

4.在doris创建同名角色role1，并将role1分配给user1，user1将同时拥有db1.table1.col1和col2的查询权限




