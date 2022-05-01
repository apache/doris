---
{
    "title": "快速开始",
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

# 快速开始

## 环境准备

1. CPU：2C（最低）8C（推荐）
2. 内存：4G（最低）48G（推荐）
3. 硬盘：100G（最低）400G（推荐）
4. 平台：MacOS（Inter）、LinuxOS、Windows虚拟机
5. 系统：CentOS（7.1及以上）、Ubuntu（16.04 及以上）
6. 软件：JDK（1.8及以上）、GCC（4.8.2 及以上）

## 单机部署

**在创建之前，请准备好已完成编译的FE/BE文件，此教程不再赘述编译过程。**

1. 设置系统最大打开文件句柄数

   ```shell
   vi /etc/security/limits.conf 
   # 添加以下两行信息
   * soft nofile 65536
   * hard nofile 65536
   # 保存退出并重启服务器
   ```
   
2. 下载二进制包/自主编译 FE / BE 文件

   ```shell
   wget https://dist.apache.org/repos/dist/release/incubator/doris/要部署的版本
   # 例如如下链接
   wget https://dist.apache.org/repos/dist/release/incubator/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-bin.tar.gz
   ```
   
3. 解压缩 tar.gz 文件

   ```shell
   tar -zxvf 下载好的二进制压缩包
   # 例如
   tar -zxvf apache-doris-1.0.0-incubating-bin.tar.gz
   ```

4. 迁移解压缩后的程序文件至指定目录并重命名

   ```shell
   mv 解压后的根目录 目标路径
   cd 目标路径
   # 例如
   mv apache-doris-1.0.0-incubating-bin /opt/doris
   cd /opt/doris
   ```

5. 配置 FE

   ```shell
   # 配置FE-Config
   vi fe/conf/fe.conf
   # 取消priority_networks的注释，修改参数
   priority_networks = 127.0.0.0/24
   # 保存退出
   ```

6. 配置 BE

   ```shell
   # 配置FE-Config
   vi be/conf/be.conf
   # 取消priority_networks的注释，修改参数
   priority_networks = 127.0.0.0/24
   # 保存退出
   ```

7. 配置环境变量

   ```shell
   # 配置环境变量
   vim /etc/profile.d/doris.sh
   export DORIS_HOME=Doris根目录 # 例如/opt/doris
   export PATH=$PATH:$DORIS_HOME/fe/bin:$DORIS_HOME/be/bin
   # 保存并source
   source /etc/profile.d/doris.sh
   ```

8. 启动 FE 和 BE 并注册 BE 至 FE

   ```shell
   start_fe.sh --daemon
   start_be.sh --daemon
   ```

   校验FE启动是否成功

   > 1. 检查是否启动成功, JPS 命令下有没有 PaloFe 进程 
   > 2. FE 进程启动后，会⾸先加载元数据，根据 FE ⻆⾊的不同，在⽇志中会看到 transfer from UNKNOWN to MASTER/FOLLOWER/OBSERVER 。最终会看到 thrift server started ⽇志，并且可以通过 mysql 客户端连接到 FE，则 表示 FE 启动成功。
   > 3. 也可以通过如下连接查看是否启动成功： http://fe_host:fe_http_port/api/bootstrap 如果返回： {"status":"OK","msg":"Success"} 则表示启动成功，其余情况，则可能存在问题。
   > 4. 外⽹环境访问 http://fe_host:fe_http_port 查看是否可以访问WebUI界 ⾯，登录账号默认为root，密码为空
   >
   > **注：如果在 fe.log 中查看不到启动失败的信息，也许在 fe.out 中可以看到。**

   校验 BE 启动是否成功

   > 1. BE 进程启动后，如果之前有数据，则可能有数分钟不等的数据索引加载时 间。 
   > 2. 如果是 BE 的第⼀次启动，或者该 BE 尚未加⼊任何集群，则 BE ⽇志会定期滚 动 waiting to receive first heartbeat from frontend 字样。表示 BE 还未通过 FE 的⼼跳收到 Master 的地址，正在被动等待。这种错误⽇志， 在 FE 中 ADD BACKEND 并发送⼼跳后，就会消失。如果在接到⼼跳后，⼜重 复出现 master client, get client from cache failed.host: , port: 0, code: 7 字样，说明 FE 成功连接了 BE，但 BE ⽆法主动连接 FE。可能需要检查 BE 到 FE 的 rpc_port 的连通性。 
   > 3. 如果 BE 已经被加⼊集群，⽇志中应该每隔 5 秒滚动来⾃ FE 的⼼跳⽇ 志： get heartbeat, host: xx.xx.xx.xx, port: 9020, cluster id: xxxxxx ，表示⼼跳正常。 
   > 4. 其次，⽇志中应该每隔 10 秒滚动 finish report task success. return code: 0 的字样，表示 BE 向 FE 的通信正常。 
   > 5. 同时，如果有数据查询，应该能看到不停滚动的⽇志，并且有 execute time is xxx ⽇志，表示 BE 启动成功，并且查询正常。 
   > 6. 也可以通过如下连接查看是否启动成功： http://be_host:be_http_port/api/health 如果返回： {"status": "OK","msg": "To Be Added"} 则表示启动成功，其余情况，则可能存在问题。 
   >
   > **注：如果在 be.INFO 中查看不到启动失败的信息，也许在 be.out 中可以看到。**

   注册 BE 至 FE（使用MySQL-Client，需自行安装）

   ```shell
   # 登录
   mysql -h 127.0.0.1 -P 9030 -uroot
   # 注册BE
   ALTER SYSTEM ADD BACKEND "127.0.0.1:9050";
   ```

## Apache Doris简单使用

Doris 采用 MySQL 协议进行通信，用户可通过 MySQL Client 或者 MySQL JDBC 连接到 Doris 集群。选择 MySQL client 版本时建议采用5.1 之后的版本，因为 5.1 之前不能支持长度超过 16 个字符的用户名。Doris SQL 语法基本覆盖 MySQL 语法。

### Apache Doris Web UI访问

默认使用 Http 协议进行 WebUI 访问，在浏览器输入以下格式地址访问

```
http://FE_IP:FE_HTTP_PORT(默认8030)
```

如在集群安装时修改了账号密码，则使用新的账号密码登陆

```
默认账号：root
默认密码：空
```

1. WebUI简介

   在 FE-WEB-UI 首页，罗列了 Version 和 Hardware Info

   Tab 页有以下六个模块：

   - Playground（可视化 SQL）

   - System（系统状态）

   - Log（集群日志）

   - QueryProfile（SQL 执行日志）

   - Session（链接列表）

   - Configuration（配置参数）

2. 查看BE列表

   进入 `System` ——> `backends` 即可查看

   需要关注的是 `Alive`列，该列的 `True `和 `False` 代表对应的BE节点的正常和异常状态

3. profile查询

   进入 QueryProfile 可以查看到最近的100条 SQL 执行 Report 信息，点击 QueryID 列的指定 ID 可以查看 Profile 信息

### MySQL命令行/图形化客户端访问

```shell
# 命令行
mysql -h FE-host -P 9030 -u用户名 -p密码
# 客户端（Navicat为例）
主机：FE-host（若是云服务器，则需要公网IP）
端口：9030
用户名：用户名
密码：密码
```

#### Profile设置

FE 将查询计划拆分成为 Fragment 下发到 BE 进行任务执行。BE 在执行 Fragment 时记录了**运行状态时的统计值**，并将 Fragment 执行的统计信息输出到日志之中。 FE 也可以通过开关将各个 Fragment 记录的这些统计值进行搜集，并在 FE 的Web页面上打印结果。

- 打开 FE 的 Report 开关

  ```mysql
  set enable_profile=true; 
  ```

- 执行 SQL 语句后，可在 FE 的 WEB-UI 界面查看对应的 SQL 语句执行 Report 信息

如需获取完整的参数对照表，请至 [Profile 参数解析](../admin-manual/query-profile.html) 查看详情

#### 库表操作

- 查看数据库列表

  ```mysql
  show databases;
  ```

- 创建数据库

  ```mysql
  CREATE DATABASE 数据库名;
  ```

  > 关于 Create-DataBase 使用的更多详细语法及最佳实践，请参阅 [Create-DataBase](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-DATABASE.html) 命令手册。
  >
  > 如果不清楚命令的全名，可以使用 "help 命令某一字段" 进行模糊查询。如键入 'HELP CREATE'，可以匹配到 `CREATE DATABASE`, `CREATE TABLE`, `CREATE USER` 等命令。

  数据库创建完成之后，可以通过 `SHOW DATABASES;` 查看数据库信息。

  ```mysql
  MySQL> SHOW DATABASES;
  +--------------------+
  | Database           |
  +--------------------+
  | example_db         |
  | information_schema |
  +--------------------+
  2 rows in set (0.00 sec)
  ```

  `information_schema` 是为了兼容 MySQL 协议而存在，实际中信息可能不是很准确，所以关于具体数据库的信息建议通过直接查询相应数据库而获得。

- 创建数据表

  > 关于 Create-Table 使用的更多详细语法及最佳实践，请参阅 [Create-Table](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) 命令手册。

  使用 `CREATE TABLE` 命令建立一个表(Table)。更多详细参数可以查看:

  ```mysql
  HELP CREATE TABLE;
  ```

  首先切换数据库:

  ```mysql
  USE example_db;
  ```

  Doris 支持支持单分区和复合分区两种建表方式（详细区别请参阅 [Create-Table](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) 命令手册） 。

  下面以聚合模型为例，演示两种分区的建表语句。

  - 单分区

    建立一个名字为 table1 的逻辑表。分桶列为 siteid，桶数为 10。

    这个表的 schema 如下：

    - siteid：类型是INT（4字节）, 默认值为10
    - citycode：类型是 SMALLINT（2字节）
    - username：类型是 VARCHAR, 最大长度为32, 默认值为空字符串
    - pv：类型是 BIGINT（8字节）, 默认值是0; 这是一个指标列, Doris 内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

    建表语句如下:

    ```mysql
    CREATE TABLE table1
    (
        siteid INT DEFAULT '10',
        citycode SMALLINT,
        username VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(siteid, citycode, username)
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    PROPERTIES("replication_num" = "1");
    ```

  - 复合分区

    建立一个名字为 table2 的逻辑表。

    这个表的 schema 如下：

    - event_day：类型是DATE，无默认值
    - siteid：类型是 INT（4字节）, 默认值为10
    - citycode：类型是 SMALLINT（2字节）
    - username：类型是 VARCHAR, 最大长度为32, 默认值为空字符串
    - pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, Doris 内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

    我们使用 event_day 列作为分区列，建立3个分区: p201706, p201707, p201708

    - p201706：范围为 [最小值, 2017-07-01)

    - p201707：范围为 [2017-07-01, 2017-08-01)

    - p201708：范围为 [2017-08-01, 2017-09-01)

      > 注意区间为左闭右开。

    每个分区使用 siteid 进行哈希分桶，桶数为10

    建表语句如下:

    ```mysql
    CREATE TABLE table2
    (
        event_day DATE,
        siteid INT DEFAULT '10',
        citycode SMALLINT,
        username VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(event_day, siteid, citycode, username)
    PARTITION BY RANGE(event_day)
    (
        PARTITION p201706 VALUES LESS THAN ('2017-07-01'),
        PARTITION p201707 VALUES LESS THAN ('2017-08-01'),
        PARTITION p201708 VALUES LESS THAN ('2017-09-01')
    )
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    PROPERTIES("replication_num" = "1");
    ```

    表建完之后，可以查看 example_db 中表的信息:

    ```mysql
    MySQL> SHOW TABLES;
    +----------------------+
    | Tables_in_example_db |
    +----------------------+
    | table1               |
    | table2               |
    +----------------------+
    2 rows in set (0.01 sec)
    
    MySQL> DESC table1;
    +----------+-------------+------+-------+---------+-------+
    | Field    | Type        | Null | Key   | Default | Extra |
    +----------+-------------+------+-------+---------+-------+
    | siteid   | int(11)     | Yes  | true  | 10      |       |
    | citycode | smallint(6) | Yes  | true  | N/A     |       |
    | username | varchar(32) | Yes  | true  |         |       |
    | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +----------+-------------+------+-------+---------+-------+
    4 rows in set (0.00 sec)
    
    MySQL> DESC table2;
    +-----------+-------------+------+-------+---------+-------+
    | Field     | Type        | Null | Key   | Default | Extra |
    +-----------+-------------+------+-------+---------+-------+
    | event_day | date        | Yes  | true  | N/A     |       |
    | siteid    | int(11)     | Yes  | true  | 10      |       |
    | citycode  | smallint(6) | Yes  | true  | N/A     |       |
    | username  | varchar(32) | Yes  | true  |         |       |
    | pv        | bigint(20)  | Yes  | false | 0       | SUM   |
    +-----------+-------------+------+-------+---------+-------+
    5 rows in set (0.00 sec)
    ```

    > 注意事项：
    >
    > 1. 上述表通过设置 replication_num 建的都是单副本的表，Doris建议用户采用默认的 3 副本设置，以保证高可用。
    > 2. 可以对复合分区表动态的增删分区。详见 `HELP ALTER TABLE` 中 Partition 相关部分。
    > 3. 数据导入可以导入指定的 Partition。详见 `HELP LOAD`。
    > 4. 可以动态修改表的 Schema。
    > 5. 可以对 Table 增加上卷表（Rollup）以提高查询性能，这部分可以参见高级使用指南关于 Rollup 的描述。
    > 6. 表的列的Null属性默认为true，会对查询性能有一定的影响。

#### 插入数据

1. Insert Into 插入

   > 关于 Insert 使用的更多详细语法及最佳实践，请参阅 [Insert](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.html) 命令手册。

   Insert Into 语句的使用方式和 MySQL 等数据库中 Insert Into 语句的使用方式类似。但在 Doris 中，所有的数据写入都是一个独立的导入作业。所以这里将 Insert Into 也作为一种导入方式介绍。

   主要的 Insert Into 命令包含以下两种；

   - INSERT INTO tbl SELECT ...
   - INSERT INTO tbl (col1, col2, ...) VALUES (1, 2, ...), (1,3, ...);

   其中第二种命令仅用于 Demo，不要使用在测试或生产环境中。

   Insert Into 命令需要通过 MySQL 协议提交，创建导入请求会同步返回导入结果。

   语法：

   ```mysql
   INSERT INTO table_name [partition_info] [WITH LABEL label] [col_list] [query_stmt] [VALUES];
   ```

   示例：

   ```mysql
   INSERT INTO tbl2 WITH LABEL label1 SELECT * FROM tbl3;
   INSERT INTO tbl1 VALUES ("qweasdzxcqweasdzxc"),("a");
   ```

   **注意**

   当需要使用 `CTE(Common Table Expressions)` 作为 insert 操作中的查询部分时，必须指定 `WITH LABEL` 和 `column list` 部分。示例

   ```mysql
   INSERT INTO tbl1 WITH LABEL label1
   WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
   SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
   
   INSERT INTO tbl1 (k1)
   WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
   SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
   ```

   Insert Into 本身就是一个 SQL 命令，其返回结果会根据执行结果的不同，分为以下几种：

   - 如果返回结果为 `ERROR 1064 (HY000)`，则表示导入失败。
   - 如果返回结果为`Query OK`，则表示执行成功。
      - 如果 `rows affected` 为 0，表示结果集为空，没有数据被导入。
      - 如果`rows affected`大于 0：
         - 如果 `status` 为 `committed`，表示数据还不可见。需要通过 `show transaction` 语句查看状态直到 `visible`
         - 如果 `status` 为 `visible`，表示数据导入成功。
      - 如果 `warnings` 大于 0，表示有数据被过滤，可以通过 `show load` 语句获取 url 查看被过滤的行。

   更多详细说明，请参阅 [Insert](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.html) 命令手册。

2. 批量导入

   Doris 支持多种数据导入方式。具体可以参阅数据导入文档。这里我们使用流式导入 (Stream-Load) 和 Broker-Load 导入做示例。

   - Stream-Load

     > 关于 Stream-Load 使用的更多详细语法及最佳实践，请参阅 [Stream-Load](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.html) 命令手册。

     流式导入通过 HTTP 协议向 Doris 传输数据，可以不依赖其他系统或组件直接导入本地数据。详细语法帮助可以参阅 `HELP STREAM LOAD;`。

     示例1：以 "table1_20170707" 为 Label，使用本地文件 table1_data 导入 table1 表。

     ```CQL
     curl --location-trusted -u test:test_passwd -H "label:table1_20170707" -H "column_separator:," -T table1_data http://FE_HOST:8030/api/example_db/table1/_stream_load
     ```

     > 1. FE_HOST 是任一 FE 所在节点 IP，8030 为 fe.conf 中的 http_port。
     > 2. 可以使用任一 BE 的 IP，以及 be.conf 中的 webserver_port 进行导入。如：`BE_HOST:8040`

     本地文件 `table1_data` 以 `,` 作为数据之间的分隔，具体内容如下：

     ```text
     1,1,jim,2
     2,1,grace,2
     3,2,tom,2
     4,3,bush,3
     5,3,helen,3
     ```

     示例2: 以 "table2_20170707" 为 Label，使用本地文件 table2_data 导入 table2 表。

     ```CQL
     curl --location-trusted -u test:test -H "label:table2_20170707" -H "column_separator:|" -T table2_data http://127.0.0.1:8030/api/example_db/table2/_stream_load
     ```

     本地文件 `table2_data` 以 `|` 作为数据之间的分隔，具体内容如下：

     ```text
     2017-07-03|1|1|jim|2
     2017-07-05|2|1|grace|2
     2017-07-12|3|2|tom|2
     2017-07-15|4|3|bush|3
     2017-07-12|5|3|helen|3
     ```

     > 注意事项：
     >
     > 1. 采用流式导入建议文件大小限制在 10GB 以内，过大的文件会导致失败重试代价变大。
     > 2. 每一批导入数据都需要取一个 Label，Label 最好是一个和一批数据有关的字符串，方便阅读和管理。Doris 基于 Label 保证在一个Database 内，同一批数据只可导入成功一次。失败任务的 Label 可以重用。
     > 3. 流式导入是同步命令。命令返回成功则表示数据已经导入，返回失败表示这批数据没有导入。

   - Broker-Load

     Broker 导入通过部署的 Broker 进程，读取外部存储上的数据进行导入。

     > 关于 Broker Load 使用的更多详细语法及最佳实践，请参阅 [Broker Load](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.html) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP BROKER LOAD` 获取更多帮助信息。

     示例：以 "table1_20170708" 为 Label，将 HDFS 上的文件导入 table1 表

        ```mysql
        LOAD LABEL table1_20170708
        (
            DATA INFILE("hdfs://your.namenode.host:port/dir/table1_data")
            INTO TABLE table1
        )
        WITH BROKER hdfs 
        (
            "username"="hdfs_user",
            "password"="hdfs_password"
        )
        PROPERTIES
        (
            "timeout"="3600",
            "max_filter_ratio"="0.1"
        );
        ```

     Broker 导入是异步命令。以上命令执行成功只表示提交任务成功。导入是否成功需要通过 `SHOW LOAD;` 查看。如：

        ```mysql
     SHOW LOAD WHERE LABEL = "table1_20170708";
        ```

     返回结果中，`State` 字段为 FINISHED 则表示导入成功。

     关于 `SHOW LOAD` 的更多说明，可以参阅 `HELP SHOW LOAD;`

     异步的导入任务在结束前可以取消：

        ```mysql
     CANCEL LOAD WHERE LABEL = "table1_20170708";
        ```

#### 查询数据

1. 简单查询

   ```mysql
   MySQL> SELECT * FROM table1 LIMIT 3;
   +--------+----------+----------+------+
   | siteid | citycode | username | pv   |
   +--------+----------+----------+------+
   |      2 |        1 | 'grace'  |    2 |
   |      5 |        3 | 'helen'  |    3 |
   |      3 |        2 | 'tom'    |    2 |
   +--------+----------+----------+------+
   3 rows in set (0.01 sec)
   
   MySQL> SELECT * FROM table1 ORDER BY citycode;
   +--------+----------+----------+------+
   | siteid | citycode | username | pv   |
   +--------+----------+----------+------+
   |      2 |        1 | 'grace'  |    2 |
   |      1 |        1 | 'jim'    |    2 |
   |      3 |        2 | 'tom'    |    2 |
   |      4 |        3 | 'bush'   |    3 |
   |      5 |        3 | 'helen'  |    3 |
   +--------+----------+----------+------+
   5 rows in set (0.01 sec)
   ```

2. Join查询

   ```mysql
   MySQL> SELECT SUM(table1.pv) FROM table1 JOIN table2 WHERE table1.siteid = table2.siteid;
   +--------------------+
   | sum(`table1`.`pv`) |
   +--------------------+
   |                 12 |
   +--------------------+
   1 row in set (0.20 sec)
   ```

3. 子查询

   ```mysql
   MySQL> SELECT SUM(pv) FROM table2 WHERE siteid IN (SELECT siteid FROM table1 WHERE siteid > 2);
   +-----------+
   | sum(`pv`) |
   +-----------+
   |         8 |
   +-----------+
   1 row in set (0.13 sec)
   ```

#### 更新数据

> 关于 Update 使用的更多详细语法及最佳实践，请参阅 [Update](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/UPDATE.html) 命令手册。

当前 UPDATE 语句**仅支持**在 Unique 模型上的行更新，存在并发更新导致的数据冲突可能。 目前 Doris 并不处理这类问题，需要用户从业务侧规避这类问题。

1. 语法规则

   ```text
   UPDATE table_name 
       SET assignment_list
       WHERE expression
   
   value:
       {expr | DEFAULT}
   
   assignment:
       col_name = value
   
   assignment_list:
       assignment [, assignment] ...
   ```

   **参数说明**

   - table_name: 待更新数据的目标表。可以是 'db_name.table_name' 形式
   - assignment_list: 待更新的目标列，形如 'col_name = value, col_name = value' 格式
   - where expression: 期望更新的条件，一个返回 true 或者 false 的表达式即可

2. 示例

   `test` 表是一个 unique 模型的表，包含: k1, k2, v1, v2 四个列。其中 k1, k2 是 key，v1, v2 是value，聚合方式是 Replace。

   1. 将 'test' 表中满足条件 k1 =1 , k2 =2 的 v1 列更新为 1

      ```sql
      UPDATE test SET v1 = 1 WHERE k1=1 and k2=2;
      ```

   2. 将 'test' 表中 k1=1 的列的 v1 列自增1

      ```sql
      UPDATE test SET v1 = v1+1 WHERE k1=1;
      ```

#### 删除数据

> 关于 Delete 使用的更多详细语法及最佳实践，请参阅 [Delete](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/DELETE.html) 命令手册。

1. 语法规则

   该语句用于按条件删除指定 table（base index） partition 中的数据。

   该操作会同时删除和此 base index 相关的 rollup index 的数据。
   语法：

   ```sql
   DELETE FROM table_name [PARTITION partition_name | PARTITIONS (p1, p2)]
   WHERE
   column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
   ```

   说明：

   - op 的可选类型包括：=, >, <, >=, <=, !=, in, not in

   - 只能指定 key 列上的条件。

   - 当选定的 key 列不存在于某个 rollup 中时，无法进行 delete。

   - 条件之间只能是“与”的关系。

     若希望达成“或”的关系，需要将条件分写在两个 DELETE 语句中。

   - 如果为分区表，可以指定分区，如不指定，且会话变量 delete_without_partition 为 true，则会应用到所有分区。如果是单分区表，可以不指定。

   注意：

   - 该语句可能会降低执行后一段时间内的查询效率。
   - 影响程度取决于语句中指定的删除条件的数量。
   - 指定的条件越多，影响越大。

2. 示例

   1. 删除 my_table partition p1 中 k1 列值为 3 的数据行
   
      ```sql
      DELETE FROM my_table PARTITION p1 WHERE k1 = 3;
      ```
   
   2. 删除 my_table partition p1 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行
   
      ```sql
      DELETE FROM my_table PARTITION p1 WHERE k1 >= 3 AND k2 = "abc";
      ```
   
   3. 删除 my_table partition p1, p2 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行
   
      ```sql
      DELETE FROM my_table PARTITIONS (p1, p2)  WHERE k1 >= 3 AND k2 = "abc";
      ```
   
      
