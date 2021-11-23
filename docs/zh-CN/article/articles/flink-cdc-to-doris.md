---
{
    "title": "使用 Flink CDC 实现 MySQL 数据实时入 Apache Doris",
    "description": "本文通过实例来演示怎么通过Flink CDC 结合Doris的Flink Connector实现从Mysql数据库中监听数据并实时入库到Doris数仓对应的表中.",
    "date": "2021-11-11",
    "metaTitle": "使用 Flink CDC 实现 MySQL 数据实时入 Apache Doris",
    "isArticle": true,
    "language": "zh-CN",
    "author": "张家锋",
    "layout": "Article",
    "sidebar": false
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

本文通过实例来演示怎么通过Flink CDC 结合Doris的Flink Connector实现从Mysql数据库中监听数据并实时入库到Doris数仓对应的表中。

## 1.什么是CDC

CDC 是变更数据捕获（Change Data Capture）技术的缩写，它可以将源数据库（Source）的增量变动记录，同步到一个或多个数据目的（Sink）。在同步过程中，还可以对数据进行一定的处理，例如分组（GROUP BY）、多表的关联（JOIN）等。

例如对于电商平台，用户的订单会实时写入到某个源数据库；A 部门需要将每分钟的实时数据简单聚合处理后保存到 Redis 中以供查询，B 部门需要将当天的数据暂存到 Elasticsearch 一份来做报表展示，C 部门也需要一份数据到 ClickHouse 做实时数仓。随着时间的推移，后续 D 部门、E 部门也会有数据分析的需求，这种场景下，传统的拷贝分发多个副本方法很不灵活，而 CDC 可以实现一份变动记录，实时处理并投递到多个目的地。

### 1.1 CDC的应用场景

- **数据同步：**用于备份，容灾；
- **数据分发：**一个数据源分发给多个下游系统；
- **数据采集：**面向数据仓库 / 数据湖的 ETL 数据集成，是非常重要的数据源。

CDC 的技术方案非常多，目前业界主流的实现机制可以分为两种：

- 基于查询的 CDC：
  - 离线调度查询作业，批处理。把一张表同步到其他系统，每次通过查询去获取表中最新的数据；
  - 无法保障数据一致性，查的过程中有可能数据已经发生了多次变更；
  - 不保障实时性，基于离线调度存在天然的延迟。
- 基于日志的 CDC：
  - 实时消费日志，流处理，例如 MySQL 的 binlog 日志完整记录了数据库中的变更，可以把 binlog 文件当作流的数据源；
  - 保障数据一致性，因为 binlog 文件包含了所有历史变更明细；
  - 保障实时性，因为类似 binlog 的日志文件是可以流式消费的，提供的是实时数据。

## 2.Flink CDC

Flink在1.11版本中新增了CDC的特性，简称 改变数据捕获。名称来看有点乱，我们先从之前的数据架构来看CDC的内容。

以上是之前的`mysq binlog`日志处理流程，例如 canal 监听 binlog 把日志写入到 kafka 中。而 Apache Flink 实时消费 Kakfa 的数据实现 mysql 数据的同步或其他内容等。拆分来说整体上可以分为以下几个阶段。

1. mysql开启binlog
2. canal同步binlog数据写入到kafka
3. flink读取kakfa中的binlog数据进行相关的业务处理。

整体的处理链路较长，需要用到的组件也比较多。Apache Flink CDC可以直接从数据库获取到binlog供下游进行业务计算分析

### 2.1 Flink Connector Mysql CDC 2.0 特性

提供 MySQL CDC 2.0，核心 feature 包括

- 并发读取，全量数据的读取性能可以水平扩展；
- 全程无锁，不对线上业务产生锁的风险；
- 断点续传，支持全量阶段的 checkpoint。

网上有测试文档显示用 TPC-DS 数据集中的 customer 表进行了测试，Flink 版本是 1.13.1，customer 表的数据量是 6500 万条，Source 并发为 8，全量读取阶段:

- MySQL CDC 2.0 用时 **13** 分钟；
- MySQL CDC 1.4 用时 **89** 分钟；
- 读取性能提升 **6.8** 倍。

## 3.什么是Doris  Flink Connector

Flink Doris Connector 是 doris 社区为了方便用户使用 Flink 读写Doris数据表的一个扩展，

目前 doris 支持 Flink 1.11.x ，1.12.x，1.13.x，Scala版本：2.12.x

目前Flink doris connector目前控制入库通过两个参数：

1. sink.batch.size	：每多少条写入一次，默认100条
2. sink.batch.interval ：每个多少秒写入一下，默认1秒

这两参数同时起作用，那个条件先到就触发写doris表操作，

**注意：**

这里**注意**的是要启用 http v2 版本，具体在 fe.conf 中配置 `enable_http_server_v2=true`，同时因为是通过 fe http rest api 获取 be 列表，这俩需要配置的用户有 admin 权限。

## 4. 用法示例

### 4.1 Flink Doris Connector 编译

首先我们要编译Doris的Flink connector，也可以通过下面的地址进行下载：

https://github.com/hf200012/hf200012.github.io/raw/main/lib/doris-flink-1.0-SNAPSHOT.jar

>注意：
>
>这里因为Doris 的Flink Connector 是基于Scala 2.12.x版本进行开发的，所有你在使用Flink 的时候请选择对应scala 2.12的版本，
>
>如果你使用上面地址下载了相应的jar，请忽略下面的编译内容部分

在 doris 的 docker 编译环境 `apache/incubator-doris:build-env-1.2` 下进行编译，因为 1.3 下面的JDK 版本是 11，会存在编译问题。

在 extension/flink-doris-connector/ 源码目录下执行：

```
sh build.sh
```

编译成功后，会在 `output/` 目录下生成文件 `doris-flink-1.0.0-SNAPSHOT.jar`。将此文件复制到 `Flink` 的 `ClassPath` 中即可使用 `Flink-Doris-Connector`。例如，`Local` 模式运行的 `Flink`，将此文件放入 `jars/` 文件夹下。`Yarn`集群模式运行的`Flink`，则将此文件放入预部署包中。

**针对Flink 1.13.x版本适配问题**

```xml
   <properties>
        <scala.version>2.12</scala.version>
        <flink.version>1.11.2</flink.version>
        <libthrift.version>0.9.3</libthrift.version>
        <arrow.version>0.15.1</arrow.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <doris.home>${basedir}/../../</doris.home>
        <doris.thirdparty>${basedir}/../../thirdparty</doris.thirdparty>
    </properties>
```

只需要将这里的 `flink.version` 改成和你 Flink 集群版本一致，重新编辑即可

### 4.2 配置Flink

这里我们是通过Flink Sql Client 方式来进行操作。

这里我们演示使用的软件版本：

1. Mysql 8.x
2. Apache Flink ： 1.13.3
3. Apache Doris ：0.14.13.1

#### 4.2.1 安装Flink

首先下载和安装 Flink ：

https://dlcdn.apache.org/flink/flink-1.13.3/flink-1.13.3-bin-scala_2.12.tgz


下载Flink CDC相关Jar包：

https://repo1.maven.org/maven2/com/ververica/flink-connector-mysql-cdc/2.0.2/flink-connector-mysql-cdc-2.0.2.jar

这里注意Flink CDC 和Flink 的版本对应关系

![/images/cdc/image-20211025170642628.png)

- 将上面下载或者编译好的 Flink  Doris Connector jar包复制到 Flink 根目录下的lib目录下
- Flink cdc的jar包也复制到 Flink 根目录下的lib目录下

这里演示使用的是本地单机模式，

```shell
# wget https://dlcdn.apache.org/flink/flink-1.13.3/flink-1.13.3-bin-scala_2.12.tgz
# tar zxvf flink-1.13.3-bin-scala_2.12.tgz 
# cd flink-1.13.3
# wget https://repo1.maven.org/maven2/com/ververica/flink-connector-mysql-cdc/2.0.2/flink-connector-mysql-cdc-2.0.2.jar -P ./lib/
# wget https://github.com/hf200012/hf200012.github.io/raw/main/lib/doris-flink-1.0-SNAPSHOT.jar -P ./lib/
```

<img src="/images/cdc/image-20211026095513892.png" alt="image-20210903134043421" style="zoom:50%;" />

#### 4.2.2 启动Flink

这里我们使用的是本地单机模式

```
# bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host doris01.
Starting taskexecutor daemon on host doris01.
```

我们通过web访问（默认端口是8081）启动起来Flink 集群，可以看到集群正常启动

<img src="/images/cdc/image-20211025162831632.png" alt="image-20211025162831632" style="zoom:30%;" />

### 4.3 安装Apache Doris

具体安装部署Doris的方法，参照下面的连接：

https://hf200012.github.io/2021/09/Apache-Doris-环境安装部署

### 4.3 安装配置 Mysql 

1. 安装Mysql

   快速使用Docker安装配置Mysql，具体参照下面的连接

   https://segmentfault.com/a/1190000021523570
   
2.  开启Mysql binlog

   进入 Docker 容器修改/etc/my.cnf 文件，在 [mysqld] 下面添加以下内容，

   ```
   log_bin=mysql_bin
   binlog-format=Row
   server-id=1
   ```

   然后重启Mysql

   ```
   systemctl restart mysqld
   ```

3. 创建Mysql数据库表

```sql
 CREATE TABLE `test_cdc` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB 
```

### 4.4 创建doris表

```sql
CREATE TABLE `doris_test` (
  `id` int NULL COMMENT "",
  `name` varchar(100) NULL COMMENT ""
 ) ENGINE=OLAP
 UNIQUE KEY(`id`)
 COMMENT "OLAP"
 DISTRIBUTED BY HASH(`id`) BUCKETS 1
 PROPERTIES (
 "replication_num" = "3",
 "in_memory" = "false",
 "storage_format" = "V2"
 );
```

### 4.5 启动 Flink Sql Client

```shell
./bin/sql-client.sh embedded
> set execution.result-mode=tableau;
```

<img src="/images/cdc/image-20211025165547903.png" alt="image-20211025165547903" style="zoom:30%;" />

#### 4.5.1 创建 Flink CDC Mysql 映射表

```sql
CREATE TABLE test_flink_cdc ( 
  id INT, 
  name STRING,
  primary key(id)  NOT ENFORCED
) WITH ( 
  'connector' = 'mysql-cdc', 
  'hostname' = 'localhost', 
  'port' = '3306', 
  'username' = 'root', 
  'password' = 'password', 
  'database-name' = 'demo', 
  'table-name' = 'test_cdc' 
);
```

执行查询创建的Mysql映射表，显示正常

```
select * from test_flink_cdc;
```

<img src="/images/cdc/image-20211026100505972.png" alt="image-20211025165547903" style="zoom:30%;" />

#### 4.5.2 创建Flink Doris Table 映射表

使用Doris Flink Connector创建 Doris映射表

```sql
CREATE TABLE doris_test_sink (
   id INT,
   name STRING
) 
WITH (
  'connector' = 'doris',
  'fenodes' = 'localhost:8030',
  'table.identifier' = 'db_audit.doris_test',
  'sink.batch.size' = '2',
  'sink.batch.interval'='1',
  'username' = 'root',
  'password' = ''
)
```

在命令行下执行上面的语句，可以看到创建表成功，然后执行查询语句，验证是否正常

```sql
select * from doris_test_sink;
```

![image-20211026100804091](/images/cdc/image-20211026100804091.png)

执行插入操作，将Mysql 里的数据通过 Flink CDC结合Doris Flink Connector方式插入到 Doris中

```sql
INSERT INTO doris_test_sink select id,name from test_flink_cdc
```

<img src="/images/cdc/image-20211026101004547.png" alt="image-20211025165547903" style="zoom:50%;" />

提交成功之后我们在Flink的Web界面可以看到相关的Job任务信息

<img src="/images/cdc/image-20211026100943474.png" alt="image-20211025165547903" style="zoom:30%;" />

#### 4.5.3 向Mysql表中插入数据

```sql
INSERT INTO test_cdc VALUES (123, 'this is a update');
INSERT INTO test_cdc VALUES (1212, '测试flink CDC');
INSERT INTO test_cdc VALUES (1234, '这是测试');
INSERT INTO test_cdc VALUES (11233, 'zhangfeng_1');
INSERT INTO test_cdc VALUES (21233, 'zhangfeng_2');
INSERT INTO test_cdc VALUES (31233, 'zhangfeng_3');
INSERT INTO test_cdc VALUES (41233, 'zhangfeng_4');
INSERT INTO test_cdc VALUES (51233, 'zhangfeng_5');
INSERT INTO test_cdc VALUES (61233, 'zhangfeng_6');
INSERT INTO test_cdc VALUES (71233, 'zhangfeng_7');
INSERT INTO test_cdc VALUES (81233, 'zhangfeng_8');
INSERT INTO test_cdc VALUES (91233, 'zhangfeng_9');
```

#### 4.5.4 观察Doris表的数据

首先停掉Insert into这个任务，因为我是在本地单机模式，只有一个task任务，所以要停掉，然后在命令行执行查询语句才能看到数据

![image-20211026101203629](/images/cdc/image-20211026101203629.png)

#### 4.5.5 修改Mysql的数据

重新启动Insert into任务

<img src="/images/cdc/image-20211025182341086.png" alt="image-20211025182341086" style="zoom:50%;" />

修改Mysql表里的数据

```sql
update test_cdc set name='这个是验证修改的操作' where id =123
```

再去观察Doris表中的数据，你会发现已经修改

注意这里如果要想Mysql表里的数据修改，Doris里的数据也同样修改，Doris数据表的模型要是Unique key模型，其他数据模型（Aggregate Key 和 Duplicate Key）不能进行数据的更新操作。

![image-20211025182435827](/images/cdc/image-20211025182435827.png)

#### 4.5.6 删除数据操作

目前Doris Flink Connector 还不支持删除操作，后面计划会加上这个操作

