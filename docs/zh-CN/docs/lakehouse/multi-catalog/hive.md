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

## 使用须知

1. 将 core-site.xml，hdfs-site.xml 和 hive-site.xml  放到 FE 和 BE 的 conf 目录下。优先读取 conf 目录下的 hadoop 配置文件，再读取环境变量 `HADOOP_CONF_DIR` 的相关配置文件。 
2. hive 支持 1/2/3 版本。
3. 支持 Managed Table 和 External Table，支持部分 Hive View。
4. 可以识别 Hive Metastore 中存储的 hive、iceberg、hudi 元数据。

## 创建 Catalog

### Hive On HDFS

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083'
);
```

除了 `type` 和 `hive.metastore.uris` 两个必须参数外，还可以通过更多参数来传递连接所需要的信息。

如提供 HDFS HA 信息，示例如下：

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

同时提供 HDFS HA 信息和 Kerberos 认证信息，示例如下：

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

请在所有的 `BE`、`FE` 节点下放置 `krb5.conf` 文件和 `keytab` 认证文件，`keytab` 认证文件路径和配置保持一致，`krb5.conf` 文件默认放置在 `/etc/krb5.conf` 路径。
`hive.metastore.kerberos.principal` 的值需要和所连接的 hive metastore 的同名属性保持一致，可从 `hive-site.xml` 中获取。

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

viewfs 相关参数可以如上面一样添加到 catalog 配置中，也可以添加到 `conf/core-site.xml` 中。

viewfs 工作原理和参数配置可以参考 hadoop 相关文档，比如 https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html

### Hive On JuiceFS

数据存储在JuiceFS，示例如下：

（需要把 `juicefs-hadoop-x.x.x.jar` 放在 `fe/lib/` 和 `apache_hdfs_broker/lib/` 下）

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

可选属性：

* s3.connection.maximum： s3最大连接数，默认50
* s3.connection.request.timeout：s3请求超时时间，默认3000ms
* s3.connection.timeout： s3连接超时时间，默认1000ms

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

## 元数据缓存设置

创建 Catalog 时可以采用参数 `file.meta.cache.ttl-second` 来设置 Hive 分区文件缓存自动失效时间，也可以将该值设置为 0 来禁用分区文件缓存。时间单位为：秒。示例如下：

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

## Hive 版本

Doris 可以正确访问不同 Hive 版本中的 Hive Metastore。在默认情况下，Doris 会以 Hive 2.3 版本的兼容接口访问 Hive Metastore。你也可以在创建 Catalog 时指定 hive 的版本。如访问 Hive 1.1.0 版本：

```sql 
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
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

## 是否按照 hive 表的 schema 来截断 char 或者 varchar 列

如果变量 `truncate_char_or_varchar_columns` 开启，则当 hive 表的 schema 中 char 或者 varchar 列的最大长度和底层 parquet 或者 orc 文件中的 schema 不一致时会按照 hive 表列的最大长度进行截断。

该变量默认为 false。

## 使用 Ranger 进行权限校验

Apache Ranger是一个用来在Hadoop平台上进行监控，启用服务，以及全方位数据安全访问管理的安全框架。

目前doris支持ranger的库、表、列权限，不支持加密、行权限等。

### 环境配置

连接开启 Ranger 权限校验的 Hive Metastore 需要增加配置 & 配置环境：

1. 创建 Catalog 时增加：

```sql
"access_controller.properties.ranger.service.name" = "hive",
"access_controller.class" = "org.apache.doris.catalog.authorizer.RangerHiveAccessControllerFactory",
```

2. 配置所有 FE 环境：

    1. 将 HMS conf 目录下的配置文件ranger-hive-audit.xml,ranger-hive-security.xml,ranger-policymgr-ssl.xml复制到 FE 的 conf 目录下。

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

    3. 为获取到 Ranger 鉴权本身的日志，可在 `<doris_home>/conf` 目录下添加配置文件 log4j.properties。

    4. 重启 FE。

### 最佳实践

1.在ranger端创建用户user1并授权db1.table1.col1的查询权限

2.在ranger端创建角色role1并授权db1.table1.col2的查询权限

3.在doris创建同名用户user1，user1将直接拥有db1.table1.col1的查询权限

4.在doris创建同名角色role1，并将role1分配给user1，user1将同时拥有db1.table1.col1和col2的查询权限


