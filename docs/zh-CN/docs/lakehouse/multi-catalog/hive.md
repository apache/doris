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
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:8088',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:8088',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
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

> 连接Glue时，如果是在非EC2环境，需要将EC2环境里的 `~/.aws` 目录拷贝到当前环境里。也可以下载[AWS Cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)工具进行配置，这种方式也会在当前用户目录下创建`.aws`目录。

```sql
CREATE CATALOG hive PROPERTIES (
    "type"="hms",
    "hive.metastore.type" = "glue",
    "glue.endpoint" = "https://glue.us-east-1.amazonaws.com",
    "glue.access_key" = "ak",
    "glue.secret_key" = "sk"
);
```

## 元数据缓存与刷新

针对 Hive Catalog，在 Doris 中会缓存 4 种元数据：

1. 表结构：缓存表的列信息等。
2. 分区值：缓存一个表的所有分区的分区值信息。
3. 分区信息：缓存每个分区的信息，如分区数据格式，分区存储位置、分区值等。
4. 文件信息：缓存每个分区所对应的文件信息，如文件路径位置等。

以上缓存信息不会持久化到 Doris 中，所以在 Doris 的 FE 节点重启、切主等操作，都可能导致缓存失效。缓存失效后，Doris 会直接访问 Hive MetaStore 获取信息，并重新填充缓存。

元数据缓可以根据用户的需要，进行自动、手动，或配置 TTL（Time-to-Live） 的方式进行更新。

### 默认行为和 TTL

默认情况下，元数据缓存会在第一次被填充后的 10 分钟后失效。该时间由 fe.conf 的配置参数 `external_cache_expire_time_minutes_after_access` 决定。（注意，在 2.0.1 及以前的版本中，该参数默认值为 1 天）。

例如，用户在 10:00 第一次访问表 A 的元数据，那么这些元数据会被缓存，并且到 10:10 后会自动失效，如果用户在 10:11 再次访问相同的元数据，则会直接访问 Hive MetaStore 获取信息，并重新填充缓存。

`external_cache_expire_time_minutes_after_access` 会影响 Catalog 下的所有 4 种缓存。

针对 Hive 中常用的 `INSERT INTO OVERWRITE PARTITION` 操作，也可以通过配置 `文件信息缓存` 的 TTL，来及时的更新 `文件信息缓存`：

```
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'file.meta.cache.ttl-second' = '60'
);
```

上面的例子中，`file.meta.cache.ttl-second` 设置为 60 秒，则缓存会在 60 秒后失效。这个参数，只会影响 `文件信息缓存`。

也可以将该值设置为 0 来禁用分区文件缓存，每次都会从 Hive MetaStore 直接获取文件信息。

### 手动刷新

用户需要通过 [REFRESH](../../sql-manual/sql-reference/Utility-Statements/REFRESH.md) 命令手动刷新元数据。

1. REFRESH CATALOG：刷新指定 Catalog。

    ```
    REFRESH CATALOG ctl1 PROPERTIES("invalid_cache" = "true");
    ```

    该命令会刷新指定 Catalog 的库列表，表列名以及所有缓存信息等。

    `invalid_cache` 表示是否要刷新缓存。默认为 true。如果为 false，则只会刷新 Catalog 的库、表列表，而不会刷新缓存信息。该参数适用于，用户只想同步新增删的库表信息时。

2. REFRESH DATABASE：刷新指定 Database。

    ```
    REFRESH DATABASE [ctl.]db1 PROPERTIES("invalid_cache" = "true");
    ```

    该命令会刷新指定 Database 的表列名以及 Database 下的所有缓存信息等。

    `invalid_cache` 属性含义同上。默认为 true。如果为 false，则只会刷新 Database 的表列表，而不会刷新缓存信息。该参数适用于，用户只想同步新增删的表信息时。

3. REFRESH TABLE: 刷新指定 Table。

    ```
    REFRESH TABLE [ctl.][db.]tbl1;
    ```

    该命令会刷新指定 Table 下的所有缓存信息等。

### 定时刷新

用户可以在创建 Catalog 时，设置该 Catalog 的定时刷新。

```
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'metadata_refresh_interval_sec' = '600'
);
```

在上例中，`metadata_refresh_interval_sec` 表示每 600 秒刷新一次 Catalog。相当于每隔 600 秒，自动执行一次：

`REFRESH CATALOG ctl1 PROPERTIES("invalid_cache" = "true");`

操作。

定时刷新间隔不得小于 5 秒。

### 自动刷新

自动刷新目前仅支持 Hive Metastore 元数据服务。通过让 FE 节点定时读取 HMS 的 notification event 来感知 Hive 表元数据的变更情况，目前支持处理如下event：

|事件 | 事件行为和对应的动作 |
|---|---|
| CREATE DATABASE | 在对应数据目录下创建数据库。 |
| DROP DATABASE | 在对应数据目录下删除数据库。 |
| ALTER DATABASE  | 此事件的影响主要有更改数据库的属性信息，注释及默认存储位置等，这些改变不影响doris对外部数据目录的查询操作，因此目前会忽略此event。 |
| CREATE TABLE | 在对应数据库下创建表。 |
| DROP TABLE  | 在对应数据库下删除表，并失效表的缓存。 |
| ALTER TABLE | 如果是重命名，先删除旧名字的表，再用新名字创建表，否则失效该表的缓存。 |
| ADD PARTITION | 在对应表缓存的分区列表里添加分区。 |
| DROP PARTITION | 在对应表缓存的分区列表里删除分区，并失效该分区的缓存。 |
| ALTER PARTITION | 如果是重命名，先删除旧名字的分区，再用新名字创建分区，否则失效该分区的缓存。 |

> 当导入数据导致文件变更,分区表会走ALTER PARTITION event逻辑，不分区表会走ALTER TABLE event逻辑。
> 
> 如果绕过HMS直接操作文件系统的话，HMS不会生成对应事件，doris因此也无法感知

该特性在 fe.conf 中有如下参数：

1. `enable_hms_events_incremental_sync`: 是否开启元数据自动增量同步功能,默认关闭。
2. `hms_events_polling_interval_ms`: 读取 event 的间隔时间，默认值为 10000，单位：毫秒。
3. `hms_events_batch_size_per_rpc`: 每次读取 event 的最大数量，默认值为 500。

如果想使用该特性(华为MRS除外)，需要更改HMS的 hive-site.xml 并重启HMS和HiveServer2：

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

华为的MRS需要更改hivemetastore-site.xml 并重启HMS和HiveServer2：

```
<property>
    <name>metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>
```

## Hive 版本

Doris 可以正确访问不同 Hive 版本中的 Hive Metastore。在默认情况下，Doris 会以 Hive 2.3 版本的兼容接口访问 Hive Metastore。

如在查询时遇到如 `Invalid method name: 'get_table_req'` 类似错误，说明 hive 版本不匹配。

你可以在创建 Catalog 时指定 hive 的版本。如访问 Hive 1.1.0 版本：

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
| `array<type>` | `array<type>`| 支持嵌套，如 `array<map<string, int>>` |
| `map<KeyType, ValueType>` | `map<KeyType, ValueType>` | 支持嵌套，如 `map<string, array<int>>` |
| `struct<col1: Type1, col2: Type2, ...>` | `struct<col1: Type1, col2: Type2, ...>` | 支持嵌套，如 `struct<col1: array<int>, col2: map<int, date>>` |
| other | unsupported | |

## 是否按照 hive 表的 schema 来截断 char 或者 varchar 列

如果变量 `truncate_char_or_varchar_columns` 开启，则当 hive 表的 schema 中 char 或者 varchar 列的最大长度和底层 parquet 或者 orc 文件中的 schema 不一致时会按照 hive 表列的最大长度进行截断。

该变量默认为 false。

## 使用 broker 访问 HMS

创建 HMS Catalog 时增加如下配置，Hive 外表文件分片和文件扫描将会由名为 `test_broker` 的 broker 完成

```sql
"broker.name" = "test_broker"
```

Doris 基于 Iceberg `FileIO` 接口实现了 Broker 查询 HMS Catalog Iceberg 的支持。如有需求，可以在创建 HMS Catalog 时增加如下配置。

```sql
"io-impl" = "org.apache.doris.datasource.iceberg.broker.IcebergBrokerIO"
```

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


## 使用 Kerberos 进行认证

Kerberos是一种身份验证协议。它的设计目的是通过使用私钥加密技术为应用程序提供强身份验证。

### 环境配置

1. 当集群中的服务配置了Kerberos认证，配置Hive Catalog时需要获取它们的认证信息。

    `hadoop.kerberos.keytab`: 记录了认证所需的principal，Doris集群中的keytab必须是同一个。

    `hadoop.kerberos.principal`: Doris集群上找对应hostname的principal，如`doris/hostname@HADOOP.COM`，用`klist -kt`检查keytab。

    `yarn.resourcemanager.principal`: 到Yarn Resource Manager节点，从 `yarn-site.xml` 中获取，用`klist -kt`检查Yarn的keytab。

    `hive.metastore.kerberos.principal`: 到Hive元数据服务节点，从 `hive-site.xml` 中获取，用`klist -kt`检查Hive的keytab。

    `hadoop.security.authentication`: 开启Hadoop Kerberos认证。

在所有的 `BE`、`FE` 节点下放置 `krb5.conf` 文件和 `keytab` 认证文件，`keytab` 认证文件路径和配置保持一致，`krb5.conf` 文件默认放置在 `/etc/krb5.conf` 路径。同时需确认JVM参数 `-Djava.security.krb5.conf` 和环境变量`KRB5_CONFIG`指向了正确的 `krb5.conf` 文件的路径。

2. 当配置完成后，如在`FE`、`BE`日志中无法定位到问题，可以开启Kerberos调试。相关错误解决方法课参阅：[常见问题](../faq.md)

 - 在所有的 `FE`、`BE` 节点下，找到部署路径下的`conf/fe.conf`以及`conf/be.conf`。

 - 找到配置文件后，在`JAVA_OPTS`变量中设置JVM参数`-Dsun.security.krb5.debug=true`开启Kerberos调试。

 - `FE`节点的日志路径`log/fe.out`可查看FE Kerberos认证调试信息，`BE`节点的日志路径`log/be.out`可查看BE Kerberos认证调试信息。

### 最佳实践

示例如下：

```sql
CREATE CATALOG hive_krb PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hive.metastore.sasl.enabled' = 'true',
    'hive.metastore.kerberos.principal' = 'your-hms-principal',
    'hadoop.security.authentication' = 'kerberos',
    'hadoop.kerberos.keytab' = '/your-keytab-filepath/your.keytab',   
    'hadoop.kerberos.principal' = 'your-principal@YOUR.COM',
    'yarn.resourcemanager.principal' = 'your-rm-principal'
);
```

同时提供 HDFS HA 信息和 Kerberos 认证信息，示例如下：

```sql
CREATE CATALOG hive_krb_ha PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.0.0.1:9083',
    'hive.metastore.sasl.enabled' = 'true',
    'hive.metastore.kerberos.principal' = 'your-hms-principal',
    'hadoop.security.authentication' = 'kerberos',
    'hadoop.kerberos.keytab' = '/your-keytab-filepath/your.keytab',   
    'hadoop.kerberos.principal' = 'your-principal@YOUR.COM',
    'yarn.resourcemanager.principal' = 'your-rm-principal',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:8088',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:8088',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```
