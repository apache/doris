---
{
    "title": "MySql Load",
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

# MySql load
<version since="2.0">

该语句兼容MySQL标准的[LOAD DATA](https://dev.mysql.com/doc/refman/8.0/en/load-data.html)语法，方便用户导入本地数据，并降低学习成本。

MySql load 同步执行导入并返回导入结果。用户可直接通过SQL返回信息判断本次导入是否成功。

MySql load 主要适用于导入客户端本地文件，或通过程序导入数据流中的数据。

</version>

## 基本原理

MySql Load和Stream Load功能相似, 都是导入本地文件到Doris集群中, 因此MySQL Load实现复用了StreamLoad的基础导入能力:

1. FE接收到客户端执行的MySQL Load请求, 完成SQL解析工作

2. FE将Load请求拆解,并封装为StreamLoad的请求.

3. FE选择一个BE节点发送StreamLoad请求

4. 发送请求的同时, FE会异步且流式的从MySQL客户端读取本地文件数据, 并实时的发送到StreamLoad的HTTP请求中.

5. MySQL客户端数据传输完毕, FE等待StreamLoad完成, 并展示导入成功或者失败的信息给客户端.


## 支持数据格式

MySQL Load 支持数据格式：CSV（文本）。

## 基本操作举例

### 客户端连接
```bash
mysql --local-infile  -h 127.0.0.1 -P 9030 -u root -D testdb
```

注意: 执行MySQL Load语句的时候, 客户端命令必须带有`--local-infile`, 否则执行可能会出现错误. 如果是通过JDBC方式连接的话, 需要在URL中需要加入配置`allowLoadLocalInfile=true`


### 创建测试表
```sql
CREATE TABLE testdb.t1 (pk INT, v1 INT SUM) AGGREGATE KEY (pk) DISTRIBUTED BY hash (pk) PROPERTIES ('replication_num' = '1');
```

### 导入客户端文件
假设在客户端本地当前路径上有一个CSV文件, 名为`client_local.csv`, 使用MySQL LOAD语法将表导入到测试表`testdb.t1`中.

```sql
LOAD DATA LOCAL
INFILE 'client_local.csv'
INTO TABLE testdb.t1
PARTITION (partition_a, partition_b, partition_c, partition_d)
COLUMNS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(k1, k2, v2, v10, v11)
set (c1=k1,c2=k2,c3=v10,c4=v11)
PROPERTIES ("strict_mode"="true")
```
1. MySQL Load以语法`LOAD DATA`开头, 指定`LOCAL`表示读取客户端文件.
2. `INFILE`内填写本地文件路径, 可以是相对路径, 也可以是绝对路径.目前只支持单个文件, 不支持多个文件
3. `INTO TABLE`的表名可以指定数据库名, 如案例所示. 也可以省略, 则会使用当前用户所在的数据库.
4. `PARTITION`语法支持指定分区导入
5. `COLUMNS TERMINATED BY`指定列分隔符
6. `LINES TERMINATED BY`指定行分隔符
7. `IGNORE num LINES`用户跳过CSV的num表头.
8. 列映射语法, 具体参数详见[导入的数据转换](../import-scenes/load-data-convert.md) 的列映射章节
9. `PROPERTIES`导入参数, 具体参数详见[MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) 命令手册。

### 导入服务端文件
假设在FE节点上的`/root/server_local.csv`路径为一个CSV文件, 使用MySQL客户端连接对应的FE节点, 然后执行一下命令将数据导入到测试表中.

```sql
LOAD DATA
INFILE '/root/server_local.csv'
INTO TABLE testdb.t1
PARTITION (partition_a, partition_b, partition_c, partition_d)
COLUMNS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(k1, k2, v2, v10, v11)
set (c1=k1,c2=k2,c3=v10,c4=v11)
PROPERTIES ("strict_mode"="true")
```
1. 导入服务端本地文件的语法和导入客户端语法的唯一区别是`LOAD DATA`关键词后面是否加入`LOCAL`关键字.
2. FE为多节点部署, 导入服务端文件功能只能够导入客户端连接的FE节点, 无法导入其他FE节点本地的文件.
3. 服务端导入默认是关闭, 通过设置FE的配置`mysql_load_server_secure_path`开启, 导入文件的必须在该目录下.

### 返回结果

由于 MySQL load 是一种同步的导入方式，所以导入的结果会通过SQL语法返回给用户。
如果导入执行失败, 会展示具体的报错信息. 如果导入成功, 则会显示导入的行数.

```text
Query OK, 1 row affected (0.17 sec)
Records: 1  Deleted: 0  Skipped: 0  Warnings: 0
```

### 异常结果
如果执行出现异常, 会在客户端中出现如下异常显示
```text
ERROR 1105 (HY000): errCode = 2, detailMessage = [INTERNAL_ERROR]too many filtered rows with load id b612907c-ccf4-4ac2-82fe-107ece655f0f
```

当遇到这类异常错误, 可以找到其中的`loadId`, 可以通过`show load warnings`命令在客户端中展示详细的异常信息.
```sql
show load warnings where label='b612907c-ccf4-4ac2-82fe-107ece655f0f';
```

异常信息中的LoadId即为Warning命令中的label字段.


### 配置项
1. `mysql_load_thread_pool`控制单个FE中MySQL Load并发执行线程个数, 默认为4. 线程池的排队队列大小为`mysql_load_thread_pool`的5倍, 因此默认情况下, 可以并发提交的任务为 4 + 4\*5 = 24个. 如果并发个数超过24时, 可以调大该配置项.
2. `mysql_load_server_secure_path`服务端导入的安全路径, 默认为空, 即不允许服务端导入. 如需开启这个功能, 建议在`DORIS_HOME`目录下创建一个`local_import_data`目录, 用于导入数据.
3. `mysql_load_in_memory_record`失败的任务记录个数, 该记录会保留在内存中, 默认只会保留最近的20. 如果有需要可以调大该配置. 在内存中的记录, 有效期为1天, 异步清理线程会固定一天清理一次过期数据.


## 注意事项

1. 如果客户端出现`LOAD DATA LOCAL INFILE file request rejected due to restrictions on access`错误, 需要用`mysql  --local-infile=1`命令来打开客户端的导入功能.

2. MySQL Load的导入会受到StreamLoad的配置项限制, 例如BE支持的StreamLoad最大文件量受`streaming_load_max_mb`控制, 默认为10GB.

## 更多帮助

1. 关于 MySQL Load 使用的更多详细语法及最佳实践，请参阅 [MySQL Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/MYSQL-LOAD.md) 命令手册。

