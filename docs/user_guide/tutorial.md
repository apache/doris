Palo采用mysql协议进行通信，用户可通过mysql client或者JDBC连接到Palo集群。选择mysql client版本时建议采用5.1之后的版本，因为5.1之前不能支持长度超过16个字符的用户名。本文以mysql client为例，通过一个完整的流程向用户展示Palo的基本使用方法。


# 基础使用指南

## 1. 创建cluster与创建用户

#### 1.1 Root用户登录与密码修改

Palo内置root用户，密码默认为空，启动完Palo程序之后，可以通过root用户连接到Palo集群。
假如mysql客户端和Palo FE程序部署在同一台机器，使用默认端口，下面命令即可登录Palo。
```
mysql -h 127.0.0.1 -P9030 -uroot
```

修改root密码
```
set password for 'root' = PASSWORD('root');
```

#### 1.2 创建cluster
Palo运行在多租户模式下，用户以及相关的数据库都在cluster之下。
修改完root用户密码之后，紧接着需要创建cluster，创建cluster时会为cluster创建一个superuser用户，创建cluster的命令如下:

```
CREATE CLUSTER example_cluster PROPERTIES("instance_num"="1") IDENTIFIED BY 'superuser';
```
上述命令创建了一个example_cluster的cluster，密码为superuser的superuser用户，properties中的instance_num表示这个cluster运行在一个BE节点之上。

此时可使用root用户登录Palo，并进入example_cluster。
```
mysql -h 127.0.0.1 -P9030 -uroot -proot
enter example_cluster;
```

#### 1.3 创建新用户

进入到指定cluster之后，可以在里面创建新的用户。
```
create user 'test' identified by 'test';
```

后续登录时就可以通过下列连接命令登录到指定cluster
```
mysql -h FE_HOST -P QUERY_PORT -uUSERNAME@CLUSTER_NAME -pPASSWORD
```
- FE_HOST: 部署FE的机器。
- QUERY_PORT: 在fe.conf中进行配置,默认配置为9030。
- USERNAME: 用户名。
- CLUSTER_NAME: 创建的cluster名称。
- PASSWORD: 创建用户时指定的密码。

使用root登录Palo集群，并进入example_cluster。
```
mysql -h 127.0.0.1 -P9030 -uroot@example_cluster -proot

```
当然，root账户依然可以采用先登录Palo集群，后enter到指定cluster的方式；而其他用户登录必须显示指名cluster的名称。

使用superuser登录Palo集群，并进入example_cluster。

```
mysql -h 127.0.0.1 -P9030 -usuperuser@example_cluster -psuperuser
```

使用test登录Palo集群，并进入example_cluster。
```
mysql -h 127.0.0.1 -P9030 -utest@example_cluster -ptest
```

## 2 数据表的创建与数据导入

#### 2.1 创建数据库

Palo中只有root账户和superuser账户有权限建立数据库，使用root或superuser用户登录到example_cluster，建立example_db数据库:

    CREATE DATABASE example_db;

-    所有命令都可以使用'HELP your_command'查看到详细的中文帮助
-    如果不清楚命令的全名，可以使用'help 命令某一字段' 进行模糊查询。
     如键入'HELP CREATE'，可以匹配到CREATE DATABASE, CREATE TABLE, CREATE USER三个命令

数据库创建完成之后，可以通过show databases查看数据库信息。
    
    mysql> show databases;
    +--------------------+
    | Database           |
    +--------------------+
    | test               |
    | information_schema |
    +--------------------+
    2 rows in set (0.00 sec)

information_schema是为了兼容mysql协议而存在，实际中信息可能不是很准确，所以关于具体数据库的信息建议通过直接查询相应数据库而获得。

#### 2.2 账户授权
example_db创建完成之后，可以通过root账户或者superuser账户将example_db读写权限授权给test账户, 授权之后采用test账户登录就可以操作example_db数据库了。
```
grant all on example_db to test;
```

#### 2.3 建表

使用CREATE TABLE命令建立一个表(Table)。更多详细参数可以查看:

    HELP CREATE TABLE;

首先切换数据库:

    USE example_db;

Palo支持支持单分区和复合分区两种建表方式。

在复合分区中：

- 第一级称为Partition，即分区。用户可以指定某一维度列作为分区列（当前只支持整型和时间类型的列），并指定每个分区的取值范围。

- 第二级称为Distribution，即分桶。用户可以指定某几个维度列（或不指定，即所有KEY列）以及桶数对数据进行HASH分布。

以下场景推荐使用复合分区

- 有时间维度或类似带有有序值的维度：可以以这类维度列作为分区列。分区粒度可以根据导入频次、分区数据量等进行评估。
- 历史数据删除需求：如有删除历史数据的需求（比如仅保留最近N 天的数据）。使用复合分区，可以通过删除历史分区来达到目的。也可以通过在指定分区内发送DELETE语句进行数据删除。
- 解决数据倾斜问题：每个分区可以单独指定分桶数量。如按天分区，当每天的数据量差异很大时，可以通过指定分区的分桶数，合理划分不同分区的数据,分桶列建议选择区分度大的列。

用户也可以不使用复合分区，即使用单分区。则数据只做HASH分布。

下面以聚合模型为例，分别演示两种分区的建表语句。

#### 单分区

建立一个名字为table1的逻辑表。使用全hash分桶，分桶列为siteid，桶数为10。

这个表的schema如下：

- siteid：类型是INT（4字节）, 默认值为10
- cidy_code：类型是SMALLINT（2字节）
- username：类型是VARCHAR, 最大长度为32, 默认值为空字符串
- pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, Palo内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

建表语句如下:
```
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

#### 复合分区

建立一个名字为table2的逻辑表。

这个表的 schema 如下：

- event_day：类型是DATE，无默认值
- siteid：类型是INT（4字节）, 默认值为10
- cidy_code：类型是SMALLINT（2字节）
- username：类型是VARCHAR, 最大长度为32, 默认值为空字符串
- pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, Palo 内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

我们使用event_day列作为分区列，建立3个分区: p1, p2, p3

- p1：范围为 [最小值,     2017-06-30)
- p2：范围为 [2017-06-30, 2017-07-31)
- p3：范围为 [2017-07-31, 2017-08-31)

每个分区使用siteid进行哈希分桶，桶数为10

建表语句如下:
```
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
    PARTITION p1 VALUES LESS THAN ('2017-06-30'),
    PARTITION p2 VALUES LESS THAN ('2017-07-31'),
    PARTITION p3 VALUES LESS THAN ('2017-08-31')
)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```
表建完之后，可以查看example_db中表的信息:

    mysql> show tables;
    +----------------------+
    | Tables_in_example_db |
    +----------------------+
    | table1               |
    | table2               |
    +----------------------+
    2 rows in set (0.01 sec)
    
    mysql> desc table1;
    +----------+-------------+------+-------+---------+-------+
    | Field    | Type        | Null | Key   | Default | Extra |
    +----------+-------------+------+-------+---------+-------+
    | siteid   | int(11)     | Yes  | true  | 10      |       |
    | citycode | smallint(6) | Yes  | true  | N/A     |       |
    | username | varchar(32) | Yes  | true  |         |       |
    | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +----------+-------------+------+-------+---------+-------+
    4 rows in set (0.00 sec)
    
    mysql> desc table2;
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


**注意事项**：

- 上述表通过设置replication_num建的都是单副本的表，Palo建议用户采用默认的3副本设置,以保证高可用。
- 可以对复合分区表动态的增删分区。详见'HELP ALTER TABLE'中 PARTITION相关部分。
- 数据导入可以导入指定的partition。详见'HELP LOAD'。
- 可以动态修改表的Schema。
- 可以对Table增加上卷表（Rollup）以提高查询性能，这部分可以参见高级使用指南关于Rollup的描述。


#### 2.4 导入数据

Palo 支持两种数据导入方式：

- 小批量导入：针对小批量数据的导入。详见'HELP MINI LOAD'
- 批量导入：支持读取HDFS文件，部署不同broker可以读取不同版本HDFS数据。详见 'HELP LOAD'

我们这里分别提供两种导入方式的操作示例，为快速完成导入建议使用方采用小批量导入进行数据导入的测试。

#### 小批量导入

小批量导入: 主要用于让用户可以不依赖HDFS，导入本地目录文件。

小批量导入是Palo中唯一不使用mysql-client执行的命令，采用http协议完成通信。**小批量导入的端口是fe.conf中配置的http port。**

示例1：以 "table1_20170707"为Label，使用本地文件table1_data导入table1表。

curl --location-trusted -u test@example_cluster:test -T table1_data http://127.0.0.1:8030/api/example_db/table1/_load?label=table1_20170707

本地table1_data以\t作为数据之间的分隔，具体内容如下：
       
    1	1	'jim'	2
    2	1	'grace'	2
    3	2	'tom'	2
    4	3	'bush'	3
    5	3	'helen'	3
        
示例2: 以"table2_20170707"为Label，使用本地文件table2_data导入table2表。

curl --location-trusted -u test@example_cluster:test -T table2_data http://127.0.0.1:8030/api/example_db/table2/_load?label=table2_20170707

本地table2_data以\t作为数据之间的分隔，具体内容如下：
    
    2017-07-03	1	1	'jim'	2
    2017-07-05	2	1	'grace'	2
    2017-07-12	3	2	'tom'	2
    2017-07-15	4	3	'bush'	3
    2017-07-12	5	3	'helen'	3
        
**注意事项**:
- 小批量导入单批次导入的数据量限制为1GB，用户如果要导入大量数据，需要自己手动拆分成多个小于1GB的文件，分多个批次导入，或者采用批量导入。
- 每一批导入数据都需要取一个Label，Label 最好是一个和一批数据有关的字符串，方便阅读和管理。Palo基于Label 保证在一个Database内，同一批数据只可导入成功一次。失败任务的Label可以重用。
- 该方式可以支持用户同时向多个表进行导入，并且多表间原子生效。用法请参阅：'HELP MULTI LOAD'。
- 导入label建议采用表名+时间的方式。

#### 批量导入

示例：以 "table1_20170707"为Label，使用HDFS上的文件导入table1表
```
LOAD LABEL table1_20170707
(
    DATA INFILE("hdfs://your.namenode.host:port/dir/table1_data")
    INTO TABLE table1
)
WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password")
PROPERTIES
(
    "timeout"="3600",
    "max_filter_ratio"="0.1"
);
```

示例：以 "table2_20170707"为Label，使用HDFS上的文件导入table2表
```
LOAD LABEL table1_20170707
(
    DATA INFILE("hdfs://your.namenode.host:port/dir/table2_data")
    INTO TABLE table2
)
WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password")
PROPERTIES
(
    "timeout"="3600",
    "max_filter_ratio"="0.1"
);
```

**注意事项**:

- 该方式导入Palo的源数据文件必须在HDFS上，并且部署过broker。
- Label 的使用同小批量导入。
- timeout表示本次导入的超时时间。
- max_filter_ratio表示本次导入可以过滤的不符合规范的数据比例。
- 更多参数的设置可以参考'HELP LOAD'。


#### 2.5 查询导入任务的状态

导入任务是异步执行的。执行导入命令后，需要通过SHOW LOAD 命令查询导入任务的状态。
更多详细参数可以查看:

    HELP SHOW LOAD;

导入任务的主要信息为:

- State：导入状态
    1.  **pending** 导入任务尚未被调度执行
    2.  **etl** 正在执行 ETL 计算, Palo 内部状态
    3.  **loading** 正在进行加载, Palo 内部状态
    4.  **finished** 导入任务成功完成
    5.  **cancelled** 导入任务被取消或者失败
- Progress：导入进度
- ETL：阶段的作业信息
    1.  **dpp.abnorm.ALL** 输入数据中被过滤掉的非法数据条数
    2.  **dpp.norm.ALL** 输入数据中合法的数据条数
- TaskInfo：本次导入作业的参数信息
- ErrorMsg：导入任务失败原因
- CreateTime：任务创建时间
- EtlStartTime：ETL 开始时间
- EtlFinishTime：ETL 结束时间
- LoadStartTime：加载开始时间
- LoadFinishTime：加载结束时间
- URL: 导入失败后的错误日志地址

示例1：显示当前数据库内以"table1_20170707"为Label 的所有任务的状态的详细信息

    SHOW LOAD WHERE LABEL = "table1_20170707";
    
示例2：显示当前正在做ETL的所有任务的状态信息

    SHOW LOAD WHERE STATE = "ETL";
    
示例3：显示当前数据库内最后20个导入任务的状态

    SHOW LOAD ORDER BY CreateTime DESC LIMIT 20;

**注意事项**:
- 如果任务失败，[常见问题](./FAQ.md)中的导入任务失败原因。

#### 2.6 取消导入任务

使用CANCEL LOAD命令取消一个正在执行的导入任务。
被取消的任务数据不会导入Palo。
已经处于cancelled或finished状态的任务无法被取消。

示例：取消当前数据库中Label为"table1_20170707"的任务

    CANCEL LOAD WHERE LABEL = "table1_20170707";

## 3 数据的查询

#### 3.1 简单查询

示例:

    mysql> select * from table1 limit 3;
    +--------+----------+----------+------+
    | siteid | citycode | username | pv   |
    +--------+----------+----------+------+
    |      2 |        1 | 'grace'  |    2 |
    |      5 |        3 | 'helen'  |    3 |
    |      3 |        2 | 'tom'    |    2 |
    +--------+----------+----------+------+

#### 3.2 order by查询

示例:

    mysql> select * from table1 order by citycode;
    +--------+----------+----------+------+
    | siteid | citycode | username | pv   |
    +--------+----------+----------+------+
    |      2 |        1 | 'grace'  |    2 |
    |      1 |        1 | 'jim'    |    2 |
    |      3 |        2 | 'tom'    |    2 |
    |      4 |        3 | 'bush'   |    3 |
    |      5 |        3 | 'helen'  |    3 |
    +--------+----------+----------+------+
    5 rows in set (0.07 sec)

**注意事项**:
鉴于order by的特殊性，order by后面建议一定要加入limit，如果未加 limit，系统当前默认会自动为你添加limit 65535。

#### 3.3 带有join的查询

示例:

    mysql> select sum(table1.pv) from table1 join table2 where table1.siteid = table2.siteid;
    +--------------------+
    | sum(`table1`.`pv`) |
    +--------------------+
    |                 12 |
    +--------------------+
    1 row in set (0.20 sec)

#### 3.4 带有子查询的查询

示例:

    mysql> select sum(pv) from table2 where siteid in (select siteid from table1 where siteid > 2);
    +-----------+
    | sum(`pv`) |
    +-----------+
    |         8 |
    +-----------+
    1 row in set (0.13 sec)


# 高级使用指南

## 1 数据表的创建和导入相关

#### 1.1 修改Schema

使用ALTER TABLE命令可以修改表的Schema，包括如下修改：

    * 增加列
    * 删除列
    * 修改列类型
    * 改变列顺序

以下举例说明。

原表table1的Schema如下:

    +----------+-------------+------+-------+---------+-------+
    | Field    | Type        | Null | Key   | Default | Extra |
    +----------+-------------+------+-------+---------+-------+
    | siteid   | int(11)     | Yes  | true  | 10      |       |
    | citycode | smallint(6) | Yes  | true  | N/A     |       |
    | username | varchar(32) | Yes  | true  |         |       |
    | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +----------+-------------+------+-------+---------+-------+


我们新增一列uv，类型为BIGINT，聚合类型为SUM，默认值为0:

    ALTER TABLE table1 ADD COLUMN uv BIGINT SUM DEFAULT '0' after pv;

提交成功后，可以通过以下命令查看:

    SHOW ALTER TABLE COLUMN;

当作业状态为FINISHED，则表示作业完成。新的Schema 已生效。

ALTER TABLE完成之后, 可以通过desc table查看最新的schema。

    mysql> desc table1;
    +----------+-------------+------+-------+---------+-------+
    | Field    | Type        | Null | Key   | Default | Extra |
    +----------+-------------+------+-------+---------+-------+
    | siteid   | int(11)     | Yes  | true  | 10      |       |
    | citycode | smallint(6) | Yes  | true  | N/A     |       |
    | username | varchar(32) | Yes  | true  |         |       |
    | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    | uv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +----------+-------------+------+-------+---------+-------+
    5 rows in set (0.00 sec)

可以使用以下命令取消当前正在执行的作业:

    CANCEL ALTER TABLE COLUMN FROM table1;

**注意事项**:

    请使用 HELP ALTER TABLE 查看更多详细信息。

#### 1.2 创建Rollup

Rollup可以理解为Table的一个物化索引结构。**物化**是因为其数据在物理上独立存储，而**索引**的意思是，Rollup可以调整列顺序以增加前缀索引的命中率，也可以减少key列以增加数据的聚合度。

以下举例说明。

原表table1的Schema如下:

    +----------+-------------+------+-------+---------+-------+
    | Field    | Type        | Null | Key   | Default | Extra |
    +----------+-------------+------+-------+---------+-------+
    | siteid   | int(11)     | Yes  | true  | 10      |       |
    | citycode | smallint(6) | Yes  | true  | N/A     |       |
    | username | varchar(32) | Yes  | true  |         |       |
    | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    | uv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +----------+-------------+------+-------+---------+-------+

对于table1明细数据是siteid, citycode, username三者构成一个key，从而对pv字段进行聚合；如果业务方经常有看城市pv总量的需求，可以建立一个只有citycode, pv的rollup。

    ALTER TABLE table1 ADD ROLLUP rollup_city(citycode, pv);

提交成功后，可以通过以下命令查看:

    SHOW ALTER TABLE ROLLUP;

当作业状态为 FINISHED，则表示作业完成。
Rollup建立完成之后可以使用desc table1 all查看表的rollup信息。

    mysql> desc table1 all;
    +-------------+----------+-------------+------+-------+--------+-------+
    | IndexName   | Field    | Type        | Null | Key   | Default | Extra |
    +-------------+----------+-------------+------+-------+---------+-------+
    | table1      | siteid   | int(11)     | Yes  | true  | 10      |       |
    |             | citycode | smallint(6) | Yes  | true  | N/A     |       |
    |             | username | varchar(32) | Yes  | true  |         |       |
    |             | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    |             | uv       | bigint(20)  | Yes  | false | 0       | SUM   |
    |             |          |             |      |       |         |       |
    | rollup_city | citycode | smallint(6) | Yes  | true  | N/A     |       |
    |             | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +-------------+----------+-------------+------+-------+---------+-------+
    8 rows in set (0.01 sec)

可以使用以下命令取消当前正在执行的作业:

    CANCEL ALTER TABLE ROLLUP FROM table1;


**注意事项**:

- 请使用 HELP ALTER TABLE 查看更多详细信息。
- Rollup建立之后查询不需要指定rollup进行查询，还是指定原有表进行查询就行，程序会自动判断是否应该使用ROLLUP。是否命中ROLLUP可以通过EXPLAIN SQL进行查看。


## 2 数据表的查询

#### 2.1 内存限制

* 为了防止用户的一个查询可能因为消耗内存过大，将集群搞挂，所以查询进行了内存控制，默认控制为落在没有节点上的执行计划分片使用不超过 2GB 内存。
* 用户在使用时，如果发现报 memory limit exceeded 错误，一般是超过内存限制了。
* 遇到内存超限时，用户应该尽量通过优化自己的sql语句来解决。
* 如果确切发现2GB内存不能满足，可以手动设置内存参数。

显示查询内存限制:

    mysql> show variables like "%MEM_LIMIT%";
    +---------------+------------+
    | Variable_name | Value      |
    +---------------+------------+
    | exec_mem_limit| 2147483648 |
    +---------------+------------+
    1 row in set (0.00 sec)

exec_mem_limit的单位是byte，可以通过set命令改变exec_mem_limit的值。

    set exec_mem_limit = 10;

    mysql> show variables like "%MEM_LIMIT%";
    +---------------+--------+
    | Variable_name | Value  |
    +---------------+--------+
    | exec_mem_limit| 10     |
    +---------------+--------+
    1 row in set (0.00 sec)

    mysql> select * from table1;
    ERROR:
    Memory limit exceeded

#### 2.2 查询超时

当前默认查询时间设置为最长为300秒，如果一个查询在300 秒内没有完成，则查询会被 Palo系统cancel掉。用户可以通过这个参数来定制自己应用的超时时间，实现类似 wait(timeout) 的阻塞方式。

查看当前超时设置:

    mysql> show variables like "%QUERY_TIMEOUT%";
    +---------------+-------+
    | Variable_name | Value |
    +---------------+-------+
    | QUERY_TIMEOUT | 300   |
    +---------------+-------+
    1 row in set (0.00 sec)

修改超时时间到1分钟:

    set QUERY_TIMEOUT = 60;

**注意事项**:

当前超时的检查间隔为5秒，所以小于5秒的超时不会太准确。这个未来会将精度提高到秒级别。

#### 2.3 broadcast join 和 shuffle join

系统默认实现join的方式，是将小表进行条件过滤后，将其广播到大表所在的各个节点上，形成一个内存hash表，然后流式读出大表的数据进行hash join。但是如果当小表过滤后的数据量无法放入内存的话，此时join 将无法完成，通常的报错应该是首先造成内存超限。

如果遇到上述情况，建议使用shuffle join的方式，也被称作partitioned join。即将小表和大表都按照join的key进行hash，然后进行分布式的 join。这个对内存的消耗就会分摊到集群的所有计算节点上。

使用broadcast join（默认）:

    mysql> select sum(table1.pv) from table1 join table2 where table1.siteid = 2;
    +--------------------+
    | sum(`table1`.`pv`) |
    +--------------------+
    |                 10 |
    +--------------------+
    1 row in set (0.20 sec)

使用 broadcast join（显式指定）:

    mysql> select sum(table1.pv) from table1 join [broadcast] table2 where table1.siteid = 2;
    +--------------------+
    | sum(`table1`.`pv`) |
    +--------------------+
    |                 10 |
    +--------------------+
    1 row in set (0.20 sec)

使用shuffle join:

    mysql> select sum(table1.pv) from table1 join [shuffle] table2 where table1.siteid = 2;
    +--------------------+
    | sum(`table1`.`pv`) |
    +--------------------+
    |                 10 |
    +--------------------+
    1 row in set (0.15 sec)

#### 2.4 failover 和 load balance

**第一种**

自己在应用层代码进行重试和负载均衡。比如发现一个连接挂掉，就自动在其他连接上进行重试。应用层代码重试需要应用自己配置多个palo前端节点地址。


**第二种**

如果使用 mysql jdbc connector 来连接Palo，可以使用 jdbc 的自动重试机制:

    jdbc:mysql://[host:port],[host:port].../[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]...

**第三种**

应用可以连接到和应用部署到同一机器上的mysql proxy，通过配置mysql proxy的failover和loadbalance功能来达到目的。

    http://dev.mysql.com/doc/refman/5.6/en/mysql-proxy-using.html

**注意事项**:

- 无论你是否使用Palo，还是普通的mysql，应用都需要对连接进行错误检测，并且出错后要进行重试。
- 第一种：在有failover时，需要重试其他节点。
- 第二种和第三种：failover 时，也只需要简单重试，不需要在应用层明确地选择重试节点。
