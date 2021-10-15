---
{
    "title": "Binlog Load",
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


# Binlog Load
Binlog Load提供了一种使Doris增量同步用户在Mysql数据库的对数据更新操作的CDC(Change Data Capture)功能。

## 适用场景

* INSERT/UPDATE/DELETE支持
* 过滤Query
* 暂不兼容DDL语句

## 名词解释
1. Frontend（FE）：Doris 系统的元数据和调度节点。在导入流程中主要负责导入 plan 生成和导入任务的调度工作。
2. Backend（BE）：Doris 系统的计算和存储节点。在导入流程中主要负责数据的 ETL 和存储。
3. Canal：阿里巴巴开源的Mysql Binlog日志解析工具。提供增量数据订阅&消费等功能。
4.  Batch：Canal发送到客户端的一批数据，具有全局唯一自增的ID。
5. SyncJob：用户提交的一个数据同步作业。
6.  Receiver: 负责订阅并接收Canal的数据。
7.  Consumer: 负责分发Receiver接收的数据到各个Channel。
8.  Channel: 接收FE分发的数据的渠道，创建发送数据的子任务，控制单个表事务的开启、提交、终止。
9. Task：Channel向Be发送数据的子任务。

## 基本原理
在第一期的设计中，Binlog Load需要依赖canal作为中间媒介，让canal伪造成一个从节点去获取Mysql主节点上的Binlog并解析，再由Doris去获取Canal上解析好的数据，主要涉及Mysql端、Canal端以及Doris端，总体数据流向如下：

```
+---------------------------------------------+
|                    Mysql                    |
+----------------------+----------------------+
				       | Binlog
+----------------------v----------------------+
|                 Canal Server                |
+-------------------+-----^-------------------+
			   Get  |     |  Ack
+-------------------|-----|-------------------+
| FE                |     |                   |
| +-----------------|-----|----------------+  |
| | Sync Job        |     |                |  |
| |    +------------v-----+-----------+    |  |
| |    | Canal Client                 |    |  |
| |    |   +-----------------------+  |    |  |
| |    |   |       Receiver        |  |    |  |
| |    |   +-----------------------+  |    |  |
| |    |   +-----------------------+  |    |  |
| |    |   |       Consumer        |  |    |  |
| |    |   +-----------------------+  |    |  |
| |    +------------------------------+    |  |
| +----+---------------+--------------+----+  |
|      |               |              |       |
| +----v-----+   +-----v----+   +-----v----+  |
| | Channel1 |   | Channel2 |   | Channel3 |  |
| | [Table1] |   | [Table2] |   | [Table3] |  |
| +----+-----+   +-----+----+   +-----+----+  |
|      |               |              |       |
|   +--|-------+   +---|------+   +---|------+|
|  +---v------+|  +----v-----+|  +----v-----+||
| +----------+|+ +----------+|+ +----------+|+|
| |   Task   |+  |   Task   |+  |   Task   |+ |
| +----------+   +----------+   +----------+  |
+----------------------+----------------------+
     |                 |                  |
+----v-----------------v------------------v---+
|                 Coordinator                 |
|                     BE		              |
+----+-----------------+------------------+---+
     |                 |                  |
+----v---+         +---v----+        +----v---+
|   BE   |         |   BE   |        |   BE   |
+--------+         +--------+        +--------+

```

如上图，用户向FE提交一个数据同步作业。

FE会为每个数据同步作业启动一个canal client，来向canal server端订阅并获取数据。

client中的receiver将负责通过Get命令接收数据，每获取到一个数据batch，都会由consumer根据对应表分发到不同的channel，每个channel都会为此数据batch产生一个发送数据的子任务Task。

在FE上，一个Task是channel向BE发送数据的子任务，里面包含分发到当前channel的同一个batch的数据。

channel控制着单个表事务的开始、提交、终止。一个事务周期内，一般会从consumer获取到多个batch的数据，因此会产生多个向BE发送数据的子任务Task，在提交事务成功前，这些Task不会实际生效。

满足一定条件时（比如超过一定时间、获取到了空的batch），consumer将会阻塞并通知各个channel提交事务。

当且仅当所有channel都提交成功，才会通过Ack命令通知canal并继续获取并消费数据。

如果有任意channel提交失败，将会重新从上一次消费成功的位置获取数据并再次提交（已提交成功的channel不会再次提交以保证幂等性）。

整个数据同步作业中，FE通过以上流程不断的从canal获取数据并提交到BE，来完成数据同步。

## 配置Mysql端

在Mysql Cluster模式的主从同步中，二进制日志文件(Binlog)记录了主节点上的所有数据变化，数据在Cluster的多个节点间同步、备份都要通过Binlog日志进行，从而提高集群的可用性。架构通常由一个主节点(负责写)和一个或多个从节点(负责读)构成，所有在主节点上发生的数据变更将会复制给从节点。

**注意：目前必须要使用Mysql 5.7及以上的版本才能支持Binlog Load功能。**

要打开mysql的二进制binlog日志功能，则需要编辑my.cnf配置文件设置一下。

```
[mysqld]
log-bin = mysql-bin # 开启 binlog
binlog-format=ROW # 选择 ROW 模式
```

### Mysql端说明

在Mysql上，Binlog命名格式为mysql-bin.000001、mysql-bin.000002... ，满足一定条件时mysql会去自动切分Binlog日志：

1. mysql重启了
2. 客户端输入命令flush logs
3. binlog文件大小超过1G

要定位Binlog的最新的消费位置，可以通过binlog文件名和position(偏移量)。

例如，各个从节点上会保存当前消费到的binlog位置，方便随时断开连接、重新连接和继续消费。

```
---------------------                                ---------------------
|       Slave       |              read              |      Master       |
| FileName/Position | <<<--------------------------- |    Binlog Files   |
---------------------                                ---------------------
```

对于主节点来说，它只负责写入Binlog，多个从节点可以同时连接到一台主节点上，消费Binlog日志的不同部分，互相之间不会影响。

Binlog日志支持两种主要格式(此外还有混合模式mixed-based):

```
statement-based格式: Binlog只保存主节点上执行的sql语句，从节点将其复制到本地重新执行
row-based格式:       Binlog会记录主节点的每一行所有列的数据的变更信息，从节点会复制并执行每一行的变更到本地
```

第一种格式只写入了执行的sql语句，虽然日志量会很少，但是有下列缺点
     
 1. 没有保存每一行实际的数据
 2. 在主节点上执行的UDF、随机、时间函数会在从节点上结果不一致
 3. limit语句执行顺序可能不一致

因此我们需要选择第二种格式，才能从Binlog日志中解析出每一行数据。

在row-based格式下，Binlog会记录每一条binlog event的时间戳，server id，偏移量等信息，如下面一条带有两条insert语句的事务:

```
begin;
insert into canal_test.test_tbl values (3, 300);
insert into canal_test.test_tbl values (4, 400);
commit;
```

对应将会有四条binlog event，其中一条begin event，两条insert event，一条commit event：

```
SET TIMESTAMP=1538238301/*!*/; 
BEGIN
/*!*/.
# at 211935643
# at 211935698
#180930 0:25:01 server id 1 end_log_pos 211935698 Table_map: 'canal_test'.'test_tbl' mapped to number 25 
#180930 0:25:01 server id 1 end_log_pos 211935744 Write_rows: table-id 25 flags: STMT_END_F
...
'/*!*/;
### INSERT INTO canal_test.test_tbl
### SET
### @1=1
### @2=100
# at 211935744
#180930 0:25:01 server id 1 end_log_pos 211935771 Xid = 2681726641
...
'/*!*/;
### INSERT INTO canal_test.test_tbl
### SET
### @1=2
### @2=200
# at 211935771
#180930 0:25:01 server id 1 end_log_pos 211939510 Xid = 2681726641 
COMMIT/*!*/;
```

如上图所示，每条Insert event中包含了修改的数据。在进行Delete/Update操作时，一条event还能包含多行数据，使得Binlog日志更加的紧密。



### 开启GTID模式 [可选]
一个全局事务Id(global transaction identifier)标识出了一个曾在主节点上提交过的事务，在全局都是唯一有效的。开启了Binlog后，GTID会被写入到Binlog文件中，与事务一一对应。

要打开mysql的GTID模式，则需要编辑my.cnf配置文件设置一下

```
gtid-mode=on // 开启gtid模式
enforce-gtid-consistency=1 // 强制gtid和事务的一致性
```

在GTID模式下，主服务器可以不需要Binlog的文件名和偏移量，就能很方便的追踪事务、恢复数据、复制副本。

在GTID模式下，由于GTID的全局有效性，从节点将不再需要通过保存文件名和偏移量来定位主节点上的Binlog位置，而通过数据本身就可以定位了。在进行数据同步中，从节点会跳过执行任意被识别为已执行的GTID事务。

GTID的表现形式为一对坐标, `source_id`标识出主节点，`transaction_id`表示此事务在主节点上执行的顺序(最大2<sup>63</sup>-1)。

```
GTID = source_id:transaction_id
```

例如，在同一主节点上执行的第23个事务的gtid为

```
3E11FA47-71CA-11E1-9E33-C80AA9429562:23
```

## 配置Canal端
canal是属于阿里巴巴otter项目下的一个子项目，主要用途是基于 MySQL 数据库增量日志解析，提供增量数据订阅和消费，用于解决跨机房同步的业务场景，建议使用canal 1.1.5及以上版本，[canal deployer下载地址] (https://github.com/alibaba/canal/releases)，下载完成后，请按以下步骤完成部署。

1. 解压canal deployer
2. 在conf文件夹下新建目录并重命名，作为instance的根目录，目录名即后文提到的destination
3. 修改instance配置文件（可拷贝conf/example/instance.properties）

	```
	vim conf/{your destination}/instance.properties
	```
	```
	## canal instance serverId
	canal.instance.mysql.slaveId = 1234
	## mysql adress
	canal.instance.master.address = 127.0.0.1:3306 
	## mysql username/password
	canal.instance.dbUsername = canal
	canal.instance.dbPassword = canal
	```

2. 启动

	```
	sh bin/startup.sh
	```
	
3. 验证启动成功

	```
	cat logs/{your destination}/{your destination}.log
	```
	```
	2013-02-05 22:50:45.636 [main] INFO  c.a.o.c.i.spring.support.PropertyPlaceholderConfigurer - Loading properties file from class path resource [canal.properties]
	2013-02-05 22:50:45.641 [main] INFO  c.a.o.c.i.spring.support.PropertyPlaceholderConfigurer - Loading properties file from class path resource [xxx/instance.properties]
	2013-02-05 22:50:45.803 [main] INFO  c.a.otter.canal.instance.spring.CanalInstanceWithSpring - start CannalInstance for 1-xxx 
	2013-02-05 22:50:45.810 [main] INFO  c.a.otter.canal.instance.spring.CanalInstanceWithSpring - start successful....
	```

### canal端说明
canal通过伪造自己的mysql dump协议，去伪装成一个从节点，获取主节点的Binlog日志并解析。

canal server上可启动多个instance，一个instance可看作一个从节点，每个instance由下面几个部分组成：

```
-------------------------------------------------
|  Server                                       |
|  -------------------------------------------- |
|  |  Instance 1                              | |
|  |  -----------   -----------  -----------  | |
|  |  |  Parser |   |  Sink   |  | Store   |  | |
|  |  -----------   -----------  -----------  | |
|  |  -----------------------------------     | |
|  |  |             MetaManager         |     | |
|  |  -----------------------------------     | |
|  -------------------------------------------- |
-------------------------------------------------
```

* parser：数据源接入，模拟slave协议和master进行交互，协议解析
* sink：parser和store链接器，进行数据过滤，加工，分发的工作
* store：数据存储
* meta manager：元数据管理模块


每个instance都有自己在cluster内的唯一标识，即server Id。

在canal server内，instance用字符串表示，此唯一字符串被记为destination，canal client需要通过destination连接到对应的instance。

**注意：canal client和canal instance是一一对应的**，Binlog Load已限制多个数据同步作业不能连接到同一个destination。

数据在instance内的流向是binlog -> parser -> sink -> store。

instance通过parser模块解析binlog日志，解析出来的数据缓存在store里面，当用户向FE提交一个数据同步作业时，会启动一个canal client订阅并获取对应instance中的store内的数据。

store实际上是一个环形的队列，用户可以自行配置它的长度和存储空间。



![image][store]

store通过三个指针去管理队列内的数据：

1. get指针：get指针代表客户端最后获取到的位置。
2. ack指针：ack指针记录着最后消费成功的位置。
3. put指针：代表sink模块最后写入store成功的位置。

```
canal client异步获取store中数据

       get 0        get 1       get 2                    put
         |            |           |         ......        |
         v            v           v                       v
--------------------------------------------------------------------- store环形队列
         ^            ^
         |            |
       ack 0        ack 1
```

canal client调用get命令时，canal server会产生数据batch发送给client，并右移get指针，client可以获取多个batch，直到get指针赶上put指针为止。

当消费数据成功时，client会返回ack + batch Id通知已消费成功了，并右移ack指针，store会从队列中删除此batch的数据，腾出空间来从上游sink模块获取数据，并右移put指针。

当数据消费失败时，client会返回rollback通知消费失败，store会将get指针重置左移到ack指针位置，使下一次client获取的数据能再次从ack指针处开始。

和Mysql中的从节点一样，canal也需要去保存client最新消费到的位置。canal中所有元数据(如GTID、Binlog位置)都是由MetaManager去管理的，目前元数据默认以json格式持久化在instance根目录下的meta.dat文件内。

## 基本操作
### 配置目标表属性
用户需要先在Doris端创建好与Mysql端对应的目标表

Binlog Load只能支持Unique类型的目标表，且必须激活目标表的Batch Delete功能

示例：

```
-- create target table
CREATE TABLE `test1` (
  `a` int(11) NOT NULL COMMENT "",
  `b` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`a`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`a`) BUCKETS 8;

-- enable batch delete
ALTER TABLE canal_test.test1 ENABLE FEATURE "BATCH_DELETE";
```

### 创建同步作业
创建数据同步作业的的详细语法可以连接到 Doris 后，执行 HELP CREATE SYNC JOB; 查看语法帮助。这里主要详细介绍，创建作业时的注意事项。

* job_name

	`job_name`是数据同步作业在当前数据库内的唯一标识，相同`job_name`的作业只能有一个在运行。
	
* channel_desc
	
	`channel_desc `用来定义任务下的数据通道，可表示mysql源表到doris目标表的映射关系。在设置此项时，如果存在多个映射关系，必须满足mysql源表应该与doris目标表是一一对应关系，其他的任何映射关系（如一对多关系），检查语法时都被视为不合法。
	
* column_mapping

	`column_mapping`主要指mysql源表和doris目标表的列之间的映射关系，如果不指定，FE会默认源表和目标表的列按顺序一一对应。但是我们依然建议显式的指定列的映射关系，这样当目标表的结构发生变化（比如增加一个 nullable 的列），数据同步作业依然可以进行。否则，当发生上述变动后，因为列映射关系不再一一对应，导入将报错。
	
* binlog_desc

	`binlog_desc`中的属性定义了对接远端Binlog地址的一些必要信息，目前可支持的对接类型只有canal方式，所有的配置项前都需要加上canal前缀。
	
	1. `canal.server.ip`: canal server的地址
	2. `canal.server.port`: canal server的端口
	3. `canal.destination`: 前文提到的instance的字符串标识
	4. `canal.batchSize`: 每批从canal server处获取的batch大小的最大值，默认8192
	5. `canal.username`: instance的用户名
	6. `canal.password`: instance的密码
	7. `canal.debug`: 设置为true时，会将batch和每一行数据的详细信息都打印出来，会影响性能。

### 查看作业状态
查看作业状态的具体命令和示例可以通过 HELP SHOW SYNC JOB; 命令查看。

返回结果集中参数意义如下：

* State

	作业当前所处的阶段。作业状态之间的转换如下图所示：
	
	```
	                   +-------------+
           create job  |  PENDING    |    resume job
           +-----------+             <-------------+
           |           +-------------+             |
      +----v-------+                       +-------+----+
      |  RUNNING   |     pause job         |  PAUSED    |
      |            +----------------------->            |
      +----+-------+     run error         +-------+----+
           |           +-------------+             |
           |           | CANCELLED   |             |
           +----------->             <-------------+
          stop job     +-------------+    stop job
          system error
	```
	作业提交之后状态为PENDING，由FE调度执行启动canal client后状态变成RUNNING，用户可以通过 STOP/PAUSE/RESUME 三个命令来控制作业的停止，暂停和恢复，操作后作业状态分别为CANCELLED/PAUSED/RUNNING。

	作业的最终阶段只有一个CANCELLED，当作业状态变为CANCELLED后，将无法再次恢复。当作业发生了错误时，若错误是不可恢复的，状态会变成CANCELLED，否则会变成PAUSED。
	
* Channel

	作业所有源表到目标表的映射关系。
	
* Status

	当前binlog的消费位置(若设置了GTID模式，会显示GTID)，以及doris端执行时间相比mysql端的延迟时间。
	
* JobConfig

	对接的远端服务器信息，如canal server的地址与连接instance的destination
	
### 控制作业
用户可以通过 STOP/PAUSE/RESUME 三个命令来控制作业的停止，暂停和恢复。可以通过`HELP STOP SYNC JOB`; `HELP PAUSE SYNC JOB`; 以及 `HELP RESUME SYNC JOB`; 三个命令查看帮助和示例。
	
## 相关参数

### FE配置

下面配置属于数据同步作业的系统级别配置，主要通过修改 fe.conf 来调整配置值。

* `enable_create_sync_job`

	开启数据同步作业功能。默认为 false，关闭此功能。

* `sync_commit_interval_second`

	提交事务的最大时间间隔。若超过了这个时间channel中还有数据没有提交，consumer会通知channel提交事务。
	
* `max_sync_task_threads_num`

	数据同步作业线程池中的最大线程数量。此线程池整个FE中只有一个，用于处理FE中所有数据同步作业向BE发送数据的任务task，线程池的实现在`SyncTaskPool`类。
	
## 常见问题

1. 修改表结构是否会影响数据同步作业？

	会影响。数据同步作业并不能禁止`alter table`的操作，当表结构发生了变化，如果列的映射无法匹配，可能导致作业发生错误暂停，建议通过在数据同步作业中显式指定列映射关系，或者通过增加 Nullable 列或带 Default 值的列来减少这类问题。
	
2. 删除了数据库后数据同步作业还会继续运行吗？

	不会。删除数据库后的几秒日志中可能会出现找不到元数据的错误，之后该数据同步作业会被FE的定时调度检查时停止。
	
3. 多个数据同步作业可以配置相同的`ip:port + destination`吗？

	不能。创建数据同步作业时会检查`ip:port + destination`与已存在的作业是否重复，防止出现多个作业连接到同一个instance的情况。
	
4. 为什么数据同步时浮点类型的数据精度在Mysql端和Doris端不一样？

	Doris本身浮点类型的精度与Mysql不一样。可以选择用Decimal类型代替。
	
	

[store]:data:image/png;base64,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QgALnt49qFSpkvrss88kgk1qoRC/1kI38syZM6pjx45RXfnFjyrW5DQCZCSdRjiO5SMqQ65cudSoUaPUhg0b4lhzalWFr20o7VesWFF0JSGhxJYZExEgAu5BANur8BW5Zs0aBR+7c+fOVRdccIF7CEwySuA38oMPPpA16Kmnnkqy1rE5kRDIcPDgwQCH5No/ebSYmHbnA5FYjLFIR0p212u1PK/Q99Zbb4n19p133im+DyNhyXvpQwCO4OEWaP369RIhY8aMGekrkE8TASJgGwIPPvigGjNmjCpUqJB4WkDwBibnEKhbt66aM2eOrD+QSCLZxUegLPIHwX3nFv6FOpJJoiOJIQY9STDiV1xxhUQSWLRokWzBBg8/XrELgT///FNdd911avv27aLED8v5aJOnXXWzHCJABIIRwOIKNZ/Ro0fL9up3332nrrrqquCMvGIbAt98842qXbu26OlDNzJjxrObndF0Uc26dpEIcqMfZyO9qUwft7aNIyEJjsFIvvDCC9ISul5wvkNh4IRJFNFv4OiYLkWcx5w1EIFICHTo0EGYyJw5c4qEjExkJLTSfw+M+6OPPioFwbNF5syZ018oS/AUAmQkPdVd1oht3bq1fIH//PPP6uOPP7b2EHOlGYHLLrtMmElYykO14N57701zWXyQCBCBtCOAd+/tt98WrxULFy4Ua+20l8YnrSAwbtw4Bf1IGDMhYANT6iFARjJJ+3zYsGHSsu7du0tovyRtpmuaVaZMGbV48WJ16aWXqo8++kjcjPz111+uoY+EEIFkRuD48eOytYp3D2Fi4ZarQoUKydxkV7Tt5MmTqkePHkLLiBEjXEETiYg/AmQk4495XGq86aabVL169cSXF0KBMTmPAHRTf/zxR1W+fHn19ddfqxtuuEHBIIeJCBAB5xDYu3evqlGjhuwKlCtXTi1dulR8GTpXI0vWCLz22msKeuKNGjUSXXF9nb+phQAZySTu79dff12Unvv166eOHDmSxC11T9PgnBxGTljYEJINhjhQPmciAkTAfgS2bt2qqlatqlasWKFuvPFG2RWAvjKT8wgcPHhQDRo0SPxyvvrqq85XyBpciwAZSdd2TfoJK1WqlIILDDCR/fv3T3+BLMESAlDynz9/vmratKnauHGjqlatmlq9erWlZ5mJCBABawiAeQQTCWYS7s7gJxLvHlN8EEAoRKwt7dq1E08hulZYbyNQA8LJQogB35IwSGRKXgTo/ifJ3P8gwo0x7dmzR5UoUUIi3sBJOXT4mOKDAKwZ27dvr9555x1Z4KZPn66gcsBEBIhA+hAAY4JAANCNxMfyu+++S7db6YM0pqfBvCMU4vnnn6927Nih8ubN638eDD4Mb0IlrE9XX321rENYi6DHmjt3btEpD5XfeC2V3esYcTAeu8V9UiZNiCbOfK6vm3/tzofyEYkgWrnR7ms67c7nVfry58+vevbsqZ599lmFyDfTpk3TEPHXYQTgTxJRhuAIuVevXqpOnTpq4sSJIql0uGoWTwSSFgF8kN11111iRDhw4ECZ25K2sS5tGD6QscZCKoloasb1Fjri//vf/xSsuc0JzOCCBQsCLteqVUsMpKIJOcgfBMAmJ0bcg+8GXnESvwxnzpwJiGyjCbPLkajV8tDkw4cPy9dJYPMDz6yWZ3c+N9KHLz9ghj8cw4IOX4hIK1euFAflOIZkDEYguA+dPVgXM8UXAUglH3nkEekLuCd5+OGH40sAayMCSYAA3GvBVys+0uD8HyEQmeKLwOTJk4WRh3/OZcuWhYxdvmnTJnXllVdGJAw6rRBw4Dcav4GCyB8Ew2mVz3EcP58pgbHEX7Rkdz7Uh3CN0ZLd9Votz430bdmyxXfjjTfiQ8Dy38UXX+w7fvx4NJh53wEEZsyY4Tv//POlr3r16uVADSySCCQvAt27d5d3J2vWrL6ZM2cmb0Nd3LKjR4/6Chcu7MuQIYPvp59+Cksp1tXevXuHXJfy5cvnGz16tDzr5fXX3PhU5l9obBPM5HvmCrYCsE0AFwzQM4mWoAD9xx9/cCsoGlAO3W/QoIEY4WTPnl0NGDBAtWnTRtQ5HKqOxRKBpEAAUpf77rtPDR48WOY5hDyEazOm+CMAv8S7du0SA5sqVapEJKBz584h16UDBw6INBlrEVNyIEBGMgn68fHHH5etbbicCZdg1YgtCSg7w1k5rYjDIeXsdRgH4A8J1ozY1tm9e7ezlbJ0IuBRBHbu3Klq1qypxo8fr4oWLSoqOtdcc41HW+NtsrGNDafj0LsHUx8tYa3B2mRMDRs2VDly5BABCLbGGXnNiI53j8lIerfvAiiHIm2+fPkCrukTSCvff/99YSIRCxVf+LB0hO4kU/wQgNV88+bNpUIYPsGBOcK4YUL98ssv40cIayICHkAA7wQcjCNKDfTt4Gg8mt6dB5rlSRKxZmAHBWvGkCFDZC2x0hAY4yDSkE5wiQb9/erVq4sOP3Rc8QcjHCYPI2De57eqs2B3PtCRyjoG5n7Q59FwPnLkiO+ZZ57x697lzJkzSC9l2rRpujjRf61evbrkGT58uP86D5xF4PDhw75SpUoJ7nfddZdUdujQIV/jxo3lGnSOunXr5jt9+rSzhLB0IuByBPAOPPHEE/557O677/ZBN48pcQi88sor0h/QyceaFC0Z160xY8b4+/Lrr7+WR3F/4MCBvsyZM8s96O7PmzcvYrHkD4LhMeIcfDfwipP44QsjIFklzO58IMLJhgY00nBitR1uo++ff/7xvfPOO75ChQrJi5gxY0Zf69atfbt37/a/tDDCufPOOw2t9ckksHbtWnmBwXTu3Lkz4D5P7EcAfVWnTh3pl6uvvjrI2An9mD17drlfpUoV37Zt2+wngiUSAQ8gAANCvAOYuy644ALfe++95wGqk5vE7du3+7Jly+bLlCmT77fffouZkQQ6FStWlD41r/FLly71lSlTRu7hY7pLly6+v//+OySg5mdDZbK6ntudD7SkMn1kJC1aqbtpoCxYsMD/YmLCxVfiihUr/ANZW3Hnzp3bf02/dPoF6tGjh7y8zZs317f46xACmBzRJ8WKFQvLJK5fv95Xrlw5yYd++/zzzx2ihsUSAXcigJ2THDlyyDuAd2Hjxo3uJDTFqNIfwbDC1utHNAjM+SCJxBxoTsh37NgxX+fOncUSHHnAWP7888/mrEFrWVAG31lBCcqMlsz0hctvNR+eJyNpQNEqcHbnS/WOMHRBwKER502bNvmaNm0qLyReuBIlSvgmT57sz68HMu7hz7ilrTPp8k6cOCGMDfLRlYZGx/7ft956S/oCX/TLly+PWAH65NFHH/X372OPPebDNSYikMwIYIy3a9eO496FnYz1BWvEZZddJnORXj+ikRoqX58+fYIeM+bD1ja2uFEftryff/75AOmnXt+CCjFcMJZnuBx0aHc+VJDK9NEheQwhEhPlEPXo0aPqpZdeUkOHDhWn4ogn26NHD9WlSxe/A3Ko6Wr64OYHBjZTp04N0t41OjBFmDHkLVKkiFq/fr1EKAh6gBfSjMCiRYskJOLp06fVpEmTxImvlcK++OILUUBHf8LYAJE8EI6MiQgkGwK//vqrxGReu3athNlDNJT69esnWzM92Z6DBw+KcdO+ffsUXC4hmIVx/YjUqLTmg9ENHM5ra24Y5cBQFGF+9frmRL3hyrTaDjyfyvTRajvcCHLBdVhijx49WpgJMJJgSB566CGFybdbt24BTKSRXDCReC5agusZ+PqCiw1GWomGVmz3YaHdpEkT6bM+ffpYZiJRCxZSuGfCxI0FFvFowYgyEYFkQmDMmDEKvggxxvVYJxPpnh6+//771d69e9VTTz0l/RMPyuAyCK6e8FewYEH1/fffq8qVK1taz+JBH+sIg4BZ5mu3yNdqeakuGjb3Qzg9SHM+47kWrUNhPVwy98fJkyd9pUuXlu2ECRMmhHuM12NAIJSFdgyP+7OirxABB4ZU2O554IEHwiqi+x/iARFwOQJ//fWXD5bYGNMY21r3zuVkpxR52tL6yiuv9GGN0Mm8fujr5l878sFwtGHDhjJOMFbq1q0rxqTmuozndtSblvLwjF5/jc+bj5OVviDt12RtqLlD9bnV9iJ/PAaKWQ8STB5C61lJaaVv5cqVYpGXK1cu359//mmlKuYJg0A0C+0wj0W8/N133/mKFCkiEyom9jVr1kTMz5tEwK0IYOxefvnlMpYxpjG2mdyFALxGwGI+S5YsvlWrVgUQZ3W9tDMfvFpoI6yCBQv6PvvsswCajCd21otyrZaHvGldf43062Or9VrN5zR9ZCRdYrVt9geJeKRDhw71KzjrARbpNz0DecCAATK5165d2/fvv/9Gqob3IiBgxUI7wuNhbx04cMB3++23Sx8hXjes7iHZYSICXkAAlrlPP/20MCeQLtWvX9+HMc3kLgQw91933XUyzwwePDiIOKuMi935IGCpVq2a0IXx07Zt25C7M3bXa7U8AJWe9dcMtNV6reZzmj4ykglmJM3+IOGrq1OnTr79+/fL2IrXQAEd2n/bq6++ah7XPLeAQCwW2haKC5kFHxdgJDGZXnTRRb4PP/wwZD5eJAJuQACMyfvvv+8rXLiw/yPo9ddfdwNppCEEAmAeMbeAmQwlULC6HtmdD6Tu27cvwIl5KDdBdtdrtTzQR0bSMKCsAmd3vlTsiPnz5wf4g4QOyC+//GLojfiK1uG3LWvWrCI1MNMRQBRPghBYuHChuKyAztekSZOC7tt5YcOGDSLRwYSPP3ypw48oExFwEwIYk/rjFOMUUkiMXSZ3IoBtbAgysK0dLiiC3eu+1fKAmGbU4GNSOzE3uwmyWp7d+Yz0Repdu+u1Wp7T9FEimQCJJMT0TZo0ESYAEyz0IGfNmhVy/MV7oIwYMULoqlChQoCSdUjieFEQ+PXXX30FChQQ3AYNGhQ3VOB3rWTJklIvGNiHH37Yt3fv3rjVz4qIQCgEMAZhGIZIJZjfMEajhb8LVQ6vxQ8BGNRA/xr9BZ3EcMnqemR3PtCjGUkcI/qN0Yk5wv5iXbW7XqvlmelzO35200dGMo6MJHTajHGxtR5kpPjKiRjIOpJB165dw403Xv8PAbsstNMKKMYOtgrz5MkjiwB+sf2NccNEBOKJAMYiYjLDaA8MCcYixmak+S2e9LGu8Ajo2ObQw46UrK5HducDTUZGUtNodGIOg5xRo0ZZmvviRZ+mU//aXa/V8sLhp+nSv1bLM+cjIxkHRhK6JtBlK1q0qEyw2D7o2LGjb8+ePbr/wv6aOyxsxjAvmjm/lfJAV968eUWiMGfOHHMRPP8PAaOFdo0aNYJiaMcTKOjUPvLII77zzjtPxhik3HAhxUQE4oGAUTqOMdihQwe/nnc86mcdaUdg7ty5MteD8Y+2JllZP0CJ3flQZihGUl9v2bKlzHv4gGnQoEHc3QRpOvAbKdmNi9XyQFM4/Iz0Wi3PnI+RbRyObPPjjz+qJ598UuEXqU6dOurVV19VJUuWlPPzzjsvjIfPs5cT5Vl/9uzZqkGDBgoOYtesWaMuvvjiiHSm4s3HH39cog0VK1ZMLVu2TBUoUCDhMMC5c/v27RWi6iA1bdpUDRkyRBUvXjzhtJGA5ENg69atEtTgs88+k8YhyMGbb74pQRSSr7XJ16Jt27ZJwIMjR46oKVOmqEaNGkVspNX1yO58ICpa5BhEw0FUHETHgTPzUaNGyRoWqkGJoA902F2v1fKs4Jce+hjZJtQos+Hajh07VOvWrVXNmjWFiaxYsaJC6Dv8lS5d2oYanC3itttuk+g5eCnvvPNOidDibI3eKn3kyJHCRGbLlk1hEXUDEwkEEVJx4cKFsihceumlEiYT4613797qxIkT3gKZ1LoWgePHj6uePXtKCD2Mf4SwQ0jWBQsWkIl0ba8FEoZIaRAWgEF77rnnFD4CvJxatGihfv75Z3XzzTerPXv2qMaNG8tH9bFjx7zcLG/QbhRr4tgssjTf1+d250O5TopeNd3mX6vtsEof/EEiUkP27NlF1F6oUCHR28A2qDFZrddqPqv0WS0P+U6dOuWDX0lsF2DblOksAvG00E4P5idOnPA9//zzYoWJPoQz3759+4objfSUy2dTFwFsfSLa0oUXXijzAvTSXnjhBfF3m7qoeLPlMM7DvFCrVi1x9ZMM6y/WLejkGp2YlyhRwrdo0aKAToplHUReKylZ8LPSXjN+1JG0SUfSrAcJX3/dunXzgbEMlcwdESoPrlnNh7xODGT47gIzjAnnk08+CUdmylxPlIV2egBGtKL//e9/fitauHjCIrJ+/fr0FMtnUwgBjJUHH3zQ78MUFtn3339/VF20FILIU00dP368zOnwRat9FjuxfkQDxcn1DVbcsObG2gW93Z49e4pwBDRZrddqPpSZbPhF6jszLmQkbWAkf/jhhwCv+40bNxZXBLF0RLi85g4Llw/XnRrI33//vfgXg5R13bp1kUhI6nuJttBOL7hgBsBQwtgLkyuYgTvuuMMHf6ZMRCAUAjC2g39b7coHfvvAQPIjJBRa3rgGH8EQdKAvly1b5ifaqfXDX0GIA6fXN5Q/cOBAaSvmvEqVKskaZrVeq/nQtGTEL0SXySUzLmQk08FIbt++3deqVSv/JFuxYkVZlAFytGTuiHD5rebD804O5Ndee02YjyuuuMJ39OjRcOQm7XU3WWinF+Q//vjDB3cfOn4tJtirr77aN3bsWP8Xe3rr4PPeRQAqLWPGjPGVL19e3nmMj5w5c/qefPJJH8YOk3cRwNytY50PHz48oCFOrh8BFRlO4rW+GZ2YY0cG65kVt1Txos8AiRxarddqPhTqZP+SkUwDIwl/kNA1C6UHabVj7c7n9EBB+c2bN5eFpWHDhjLYU+mfMYZ2NBcZXsHl0KFD8rUO3UkwC/jDVhecquMeU2ohgNjXkN7ocIYYD1BrgZ4tpPFM3kcA0YXQr02bNg1qjJOMRlBl/12wug4ie3rpMzsxv/nmm307duwIR5pcjyd9RkKs1ms1nx34RaKPjGQMjCQmWqM/yFB6kFY71u58Tg8UlA8GGhJJTESpFI87HjG0jS9pvI9hlIM2akkF+hdh0uDrFHpGTMmNAPoYfh/1hzH6X8dznz59enI3PoVa99JLL8ncDR+zmMvNKb2MmrE8N69vRifm8J350UcfGUkPOLbaDjyUKvihrWZcyEhaZCShB3nNNdfIiwh9oXvvvTfkImsGOGBUGk7szhevgQwdSSw40LOD7mSyJ69YaNvVD5MnTxY9IjAT+q9Ro0Z0bm4XwC4qB7qx6Fvdz/itWrWqD2NAq7IgyhWT9xFYvHixGJxg7t64cWPIBqUSIwQj0hYtWvjHPhyah2q/1XUagIZ63gy01fLszuc0fWQkozCSZj3IatWq+cBUhkt2DwCr5Tk9UIzthfU2Fh24AAE+yZq8aKFtV1+AydChMjWjgchM0JNbsmSJXdWwnDgjgLkL+rEXX3yxfxFF/9arV8/31Vdf+ak5fvy4P7oV+9sPiycPfv/9d7+7pilTpoRtQyoyQpBGQiqJdwDvhDkmvBvXX2MHuoU+MpJhGEmzHiQW0bffflv8bRk70nxstWPtzgc64jkRwLURXr4yZcokpf6U1y20zeMyrecrV6703XPPPQFMB/odvtm6d+/uW7FiRVqL5nNxQgCWuc8884zv0ksvDepH9O3q1atDUoJn0NeQ3DB5EwHMY1odqUePHhEbEc/1QxNidR1Efqfog57kLbfcImMdu43Qh4c+JZIb6NNYhfp1C30Z0DlG1+k+39nTDBkyGC8HHdudDxUg8kbWrFmD6jJesLtec3k4nzx5surXr5/6448/VPbs2VWnTp0kDFjGjBkTTp8RC/NxPPEDTm3atJGoLoiIMHfuXJU5c2YzSZ48//fff9Udd9yh5syZo2rUqCFtQwSbVE6IDoEIJhMnThRcTp065YejTJkyqmXLlvJXqlQp/3UeJA6BTZs2qXHjxkl//frrr35CsmTJImFa0V+IWHXBBRf475kPMP8hOhLS77//zjCpZoBcfo7INbfeeqv69ttvJYLN2LFjVaR1PZ7rh4bOvP7q66F+naQPdLz99tuy7qOeK6+8UkIsli9fXkiJhJum1Un6dB3mX7fgR4mkQSJp9Aep9SCNW7dOfRGF+tLQ19zyxaHpMf7CSEPrjUJnNFlSMlpo29k3sOgePXq07/bbbxe9K0it9B/cCA0ePNiH7TSm+CKwdetWiTJToUIFf3+gX6DPjL5Cn8Vqfa31yCCdZPIWArrvrrvuOt/JkyejEs/1zSc+JrUTc/jZHDBggCXsAG4q40dG8swZWfSM/iDD6UGm8kAJNwvt2rXLv2UG1yFeT9pCO1euXL7ly5d7vTmO0793716x+L7xxht9GTNmDGBg8B7BaIO+B53rhp07d4oHBRjJaGYev+gLhDfFeNaRS9JCBfQj8VGdN29eH7xWMHkDgX79+sl4KF68uA/vqJXE9e0sShDeGJ2Yg7G04r0ilfFLaUYSEjVjXGzoQY4bNy6sHmQqD5RwExFeOkRKAOOFBQwWn15NRgvt2bNne7UZCaMbDCMYRzCQRqYGjEjZsmV97dq1833wwQeWJuWENcLlFf/222/iLLxt27ainwxsNdY4vv76632vv/66raELtdEV/IsyuR+BiRMnypgA848IRJijrSSub4EoGZ2YI3hDJDdBeDKV8UtZRnLq1KliMIBJGC4R+vTpE9K3lnFopfJAMeJgPNZb7wsWLJAwVPA/50W3QKlsoW3sT7uOsbWNLW5sdWtGx/gLp9fNmjUTxhNSLytRJuyizSvlABOo28BnKxxI65j3RhxxXLlyZd+LL77omAeFWbNmSR/CqpX95O7RAzc/2JLFH471/GyFaq5vwSgdO3bM16lTJ3/0unBugvBkKuOXcowkrEyxDYcJGNs/Dz30kGV9rlQeKMGv2Nkrxolq/Pjx/i/hLVu2hHvEddeNFtqQ9DDZiwC2Xz/++GOZkBHr9rzzzgtiLvExd9NNN/mee+45H6TBsery2UtxYkpDm2fOnOnr2bOnzFHZsmULwgnYgXHs3Lmzb9KkSbZKHsO1+t9///VdeeWVQks0qUy4MnjdeQQgrYYUEmsbpJJIxvk5GgVcLBAZPAAAIABJREFU34IR0vgZnZiHchOEJ1MZvwwAwGgJ9M8//8jpeeedZ7wcdGx3PlRw6NAhlSdPnqC6jBfSWu+ePXvUs88+q0aPHq1glQtL46FDh6qrrrpKio/WXqfpM7bReGy1vW6hr2/fvmL5VrJkSfXjjz+qvHnzGpvjumOzhfbXX3+dNNbnrgP7P4JgAb548WK1cOFC+fvhhx/U8ePHA8iFleTFF1+sYAWOsWT8u/zyyxWsj72YYO2+ceNG9dtvvwX9wUraNB2LVfV1112natasKR4E4EUAniTind5//331wAMPqKpVq8p7bcWKNd40pnJ9Bw8eVJUqVVJbt26V+bd3794Ch9fWj0h96CR/EK5eI36ov0OHDuIJAeO/c+fOavDgwX5PLommL1wb9HUn6UszI6mJc+uvHgBnzpwRhvH5559XR44cUSVKlFAvvfSSatasmZCu85GRDOxJq7iEyteqVSs1YcIEVaVKFbVgwQKVI0eOwMJddPb444/L+ChWrJhatmyZKlCggIuoSw1SMIZ+/vlnP2O5aNEitXv37rCNhxsu9JdmLo3MJt7vTJkyhX02HjfgdmXz5s1BjCKYx+3bt8uHbDg6ChcuLAwjGEf8gTmwMjeFK8+u65hH4QoIzO6sWbNU3bp17Sqa5aQTAXyY1a5dW+avFi1aCKOjiww1P+t75l8nGQ1zXfrci/TBBRoYSuAF12fjx4+X91S3ya2/TvZvUjOSn376qerWrZtM6jlz5hSJJBiH888/39/XXhzIfuJNB04OFFNV/tNQ+GHRue2224SJhDRl/vz5CZGi+IkMczBy5EiZEHLlyqW++eYbdfXVV4fJycvxRmDXrl0iuYMPREjw1q5dq+Abcd26dVFJwbuOnY3cuXOrfPnyKX2Oa/oc93CeP39++dDR+cHIwR8c3iX8HT58WGGh3r9/v//86NGjco57yIPzAwcOSF59Ho3IcuXKKUhW9S/81oExLlSoULRHE3Yf0pcePXqIH8rZs2cnjA5WfA4BSPJvueUWBan+7bffrr744ouAD6lQ8/O5pwOPMHbxHkRKVsuzOx9ochN9+KC6//77ZW2D/2TsxPXs2TMSdAm/5yR+GaADYGyh1QHghq9kI93m4y+//FImPFx/6KGHVP/+/UNO0lbbi3KwcGABipSslmd3PrfRpye4pUuXqlq1aolTbzdtR0LqddNNNyn0w8yZM/1jJVLf8l7iEcDW77Zt29SGDRtE4odf/Qen2fq9SjSlkIpCgqelpfjVf5CmenFrGNunUDcAo/3LL7+I0+ZE45zK9aMf6tSpIw7HoXIAtRxzQA/9PlhZr7m+BY+maPi9+eab6plnnpF3wqyWElxa4q6gHU72b9IyktBFwnY2ojdUrFgxbA9GGyjGB53sCGM9xmMv04cvIOiiQpqESDHTp08P+Fo2tjOex2A8sG24d+9eNWjQINW9e/d4Vs+6HEIAEznUV/Cemn9DXdN5zPdAHj4YIakO9WvlGp71IrMYrWu0KgikMdCbZEoMAljf6tevr+bNmydSbeyohJImenn9MCPr1vUXH1XY/XzuuefMJLvm3HFGMq3GNq5BKAwhVl8gq/lQjZOi4TDN8EtYrHxRupE+GDldc801ohvWuHFjCT9ppS3h8EjvdTAP+HoHM9m2bVv1zjvvpLdIPk8EUgYBhk1MfFdjzWrUqJHspEC6/dNPP6mCBQuGJIzrW0hYLK+rseAXuqbEXIV6GXTJ8aeTk/zBuVp0ben4PXnypOrSpYt8iTdt2lRhK8TOhO1IfOU/8sgj6u+//7azaJblEAKY4GBwg198tcEQB5bSiUio9+677xYmEtavI0aMSAQZrJMIeBYBbG3DUBEL1fDhwz3bDq8SjjkMcyjUcYxzq1fbk+x0//XXX6K3Co8HpUuXFv4FPMz1118vvBK8V9i5HqIsSKnbtGljO/8Vqa9sZST//PNPcQ2BCtEYWGIyEQFY0n711Vdi9PDxxx/LIE8EKk8++aSaM2eOWPxOmzaNbn4S0Qms0/MIYHsbadSoUfygj2NvQnUDDALmUGxjz507V7yQxJEEVmURATB0kydPFs8lDRo0EDUQGA7qBOOoYcOGqerVq6uHH35Y1Kz0vfT8Qnj3yiuvqH379qWnmJiftZWRBLMA34GdOnUSS8jPPvtMQUrJRARgnfrJJ5+IMvi4ceNkjMQTFVicwm8odNcwLunmJ57os65kQgCeGKpVqyYSj7fffjuZmubqtrRv315h7oRBDVwwYU5lch8CYPjhIujBBx9UEK517NhRrVy5Unzl4h6YTDB6cBsENSv4tu7atavodbuvNdYoso2RBCcM1wMwbLnnnnsUJhswDgCQiQgAASw+kAQivfHGG+qJJ56ICzCQQsIZPfRFJk2aRDc/cUGdlSQzAtpADdIPbHMzOYsAVMa0PjdUhMCAMLkTge+//15cAcHVGPoKkscKFSqobNmyCcHY2obbsXvvvVfNmDFDDILHjh0ru7jubFF0qmxjJLGNje1sMJDwx3fDDTeonTt3qs8//9yv2BqKHHDoYDbBvQNcSIyaNGkifv1wz0pCvqlTp4q7DRh0RHJmbKU85nEOATgyhsgfL9Prr78uWzV26oiYKV+xYoXoRaIOWPHDXQYTESAC6UMA3jDg+xLGN3ifmZxBAGsbtrPBjOBDGFhzDnMGaztKhe0GpMaIMDRw4EBxMYe1LlyC31js4MLXLWxAQu3gwt0ZyqpcubKsm9C1fOqppxTcnRnTgAED1IUXXijqWxCe4Bi8mHFL3Zjf1mNzdEkdW9J8PdL5yZMnfU888YTE+JwyZYpkXbNmja9MmTK+atWq+TZv3hzyccRwRezWfPnyybOIEar/cubMKfeMDy5cuFDut2/f3nf8+HG5hTLmz5/vK168uO/OO+/07dq1S65bbYfVfCg0lWNpGvvBeJxW/NDvmTJlkv5s0KCB79SpU8ZibTnes2ePr1ixYlJHnTp1bCmThRABInAWgbfeekveLczxTPYjcPr0aV/Tpk0FY8yVxjjnVuddq/lAPde34D6MBT88rfmeunXr+vbu3RtcYIgrJ06c8O3fv98HXsacvvnmG3+ce80b6V/wPOC39HP9+/eXsaLv47dixYq+9evXS7FO9q8tEkl8lUKcizBNkEgiwcACjqihMwndyVAJLligu4b03nvviQ4BnKy+++67/muQakZK3377rUgzwXlDX8fNkSEitSPV7rVs2VJUISDuh9QaX9mwcLMr4csQkm2EpIMjaGxpMxEBImAfAvAlmTdvXpnj4ZmByT4EMH/BTyR22hCJDWpjmDOZ3I3AmjVrxFk/1Liww2oloX+xDW6WXCKkKiLzwd/xkCFDJHoWJNSItDVlyhTRlYUBKUL7IvXq1Ut0L7GW4g96mNiRw86B0ymT9pOkKzKf6+vmX6MvQG1kg4ZoRg4MAkI2gbnDSwB3QJh0jAmTD7a1IZJt3bq131n1fffdp1atWiXifHRMkSJFjI/5j+GEFZMZmEh4mNd1I4PVdljNhzKxPRotf7T7mni783mRPoT2QvhEOCtHVAaoQ2AshXKsq3Gz+otxga0CqEpADwW/TESACNiHAOb4zp07q379+olAAIIEpvQjgJCbt956q1qyZIm64IILxNUP3JUZ1wzjcaQareZDGVzfgpGMBT/wRBCOISHcqZkxDC49/BUwjFi3IIjDVjlcPunyMCbAT+XIkUM1b95cjEcrVaqkjDyZuWS0w8n+TbdEUhvZgNmDmbuxMZBOYnIJ5QoIkkcwi9ANQKg6hBTTCRw6LGwBJmI2h0qQgEIBWTOR8G/G5D0Err32WrVw4ULxibZ8+XLxrxVNCh2tlZByw9BLG9dAIslEBIiA/QhgDgZDiZC0cdHFsr8JrioRMd3BNIKJhEQLwhacMyUHAuhf2AmAKTT/GfUZETgDPibhHggCFs1EGlEA8wjJJ/IdOHDAeCvux5mMjJ+x9nDXjXlwrI1sIEo1i1AhIQQIeBngcgVh6cAkIkF0D4VUbIFDKTSWBMYDkiuIfsGVnz59OuhxTb/+DcpgumAlHxgTK/lQdCLyeZU+uLHAlxc+KNavXy8vB/r3iiuuMPVS9FMwkLDQRnr11VepmB4dMuYgAmlGALtM2E3CztNrr72mRo4cmeayUv1BfEBDHWzjxo2yCwe1rWhzoJ3rjFfXj1Djxk5cUL7V8kLRkpZr2M7esmWLbFtfeumlEYuApxwwkpFc2oF+J/s3XRJJxPvEtjVE8bAmw5akkcuGlBHb1khwnooXRCeIWdPqNgKxmyFl6tChg2xfwi1CWsvS9PA3sQjgZQEzWb58edFrhDR79erVMREFfRBsaWNsweeadpwcUyHMTASIQEwIwI0X5n24MIG+PFPsCGBthHQJv3oujMZExl4Ln3AaAQjGkGBpHSpByjx79mzZbcWOK/6gywhBnDFhK9pLPE26GEltZGMEINwxApvDJB3ApTdBCRlfvpA8QYo1ZswYha83Jm8jgJBf6Eds5egtHug5Wkn4gkP8WUi68TzDt1lBjXmIQPoRwE4U9OHx7vG9ix1PfDDjwxmGgXDtArUtxNBm8h4C6D+o+S1dutSWEIXaaEYznaF+42VQE6k30sVIaiMbKFxD5zFUI3FNW2HDG/+uXbuEnuzZs8uX1+bNm4Ujj0Sk+V7RokVFfwR6kdDRgRUTHFyDmWDyNgLQmYW+FRYmSLphkAOr7kgJC5i20MYEzPCHkdDiPSJgPwLaQTnDJsaGLXQh9YezUV88tlKY2y0IgJEE8we7EPA74H/SkiBUAZ8D/sgLUv40M5LayAYLPyRBWvcxFGg333yziO0BLhRDkaCgjS9ZMAuw2DWLceH2ANslPXv2DLBWM5cP6za4RQDzMGHChDR3nLlcnicOAYwl7e4CDlrh/Bghp8Ilo4U2wx+GQ4nXiYBzCMCoEluzWBc++OAD5ypKopKx7gE3rIHYWYMtgR0eK5IIIs81BRbVbdu2FXc+4F30Nna4hkANC+57zFvh8DICB+SwA0FsdTN/hPJgrFy2bFmx4MZ7l9BkdoBp1QHnvHnzfHAa3qRJE9+BAwfMxQSco8xevXqJs8w2bdr4jh07JvfhKBMOM+GQ/L333hMn4//8849v8eLF4si8SJEiPjjk1CmUQ3LcW7lypTg/hwN0HCNZbYfVfCjTSYeeQnSIf6lMHxytduzYUcZNhgwZfMOHDw9CaNCgQXI/Y8aMvtmzZwfd5wUiQATigwAcZsMJ8pVXXumDM22m8AiMHTs2ZEAGq/O93flAKde34P6yirPxSaxb48ePF/4IPFLr1q193333ne/o0aOSDff37dvnmz59ug/BOLQD8Yceekiu67LWrVsn/BHud+3a1bdjxw5xPo4AMF999ZWvRo0a8uyHH36oH5HnEXyjZMmSvqVLl/qv48DJ/oUELyBZAQ55evToIY149913A54Pd6I9voM5/PHHHyUbAIVndnho12AafwcPHhwwIYVjJEHPSy+9JGU8/PDDwqhaaQeIsJoPeZ3siHC4kT6f7/nnn/ePj+eee84P1aRJk3xgIDFmwFAyEQEikDgEwDxefPHF8j4ao7AkjiL31Yz5vEOHDv75rGXLlj4IT3SyOt/bnQ/1c33TvXDu1yrO5544ewTeBhH3qlat6u9rI29jPC5fvrxvwoQJPkS4MSaUMXPmzLD8Ecro2bOnXzCHZxHxD5H/dPngt3766Scp1sn+TRMjiZCHCIsVKfyhERAcA6TOnTtLAyGdRAchAawVK1b4HnjgAZFMgoNHeKG5c+cGvGDIG46RxL2dO3f66tevL18BmMSsDgCr+VCHkx2B8kMl0ncWlVGjRvkglcQL0qJFC5FaZ8uWTc7btm0bCjpeIwJEIM4IvPbaa/JOMmxiMPBYP2644QbBB3MZQtqZk9X53u58oIPrm7k3YhM0BT99lu/5+uuvhfepVKmS9D3WMByDH8I9He451PO4tnXrVt+AAQPkGTyLHdy77rpLpJLGjxD9/IYNG+Q+eCnknTNnjtxysn8zYEAa99a1J/dofpOi3TeWmYhjuCZCikan1fairMOHD6vcuXNHbI7V8uzOlwr0QQ/23nvvFb+hhQsXFsMt+Cb97rvvIvYJbxIBIhAfBGD4BiNI6GxBH16HzI1P7e6tBc7aYYQB38nQo4O/WxgUmpPd64LV8kAH1zdzb5yLkBeNj8CTVvIE1xCfKxgHTvZvmo1t4tN81kIEziEAy2yExdRMJIxyOnbseC4Dj4gAEUgoAjCibNeundCACFNMStzeValSRZjI4sWLS9SaUEwksSICXkUgA2SeRuJj+YIxPue2Y6vtsJoP7Tt06FBUqzqr5dmdL5Xog5unxo0bi781WPb37t1b9enTJ2QYKbeNS9JDBJIdAbgrgVNtzHHwH2yOeJbs7Te2r3///qpv377iTeT666+XuNmRLLPtXheslgeaub4Ze+7scSz4BT/tritO9i8lku7qa1JjAQGEgoLrjKefflom6H79+ql69erJRGjhcWYhAkTAQQSwtd2sWTN5NxE2MRUTtviBAT5wIauBv+MZM2ZEFUSkIlZss/cRICPp/T5MyRZAH2XQoEFq+vTpCr5METUJQe8RPpOJCBCBxCLw1FNP+cMmJtzHXZyh2LFjh6pataqCL+QsWbKID9zXX3/d1Tp0cYaI1SUZAmQkk6xDU605cIa/cuVKVaZMGdFBwgQOB65MRIAIJA4BvIc6bOLQoUMTR0ica/7hhx/8H7TYOUG4wxYtWsSZClZHBOKLQJoZSegOaP2BSCRbzYcysIcfLcVSXrSyeD85ELjsssskOkDz5s0l3i8iHWErKVQ0gORoMVtBBNyPwOOPPy5EDhs2TN5L91OcPgrHjRunbrjhBrV//34F4xp84OKXKTUQsJN/scrnWM3ndA+kmZF0mjCWTwRiQQDWonCpMXz4cJU5c2aFxQuTOuOvx4Ii8xIB+xCAuxsY2iR72EQs5p07d1atW7eWj1dIICGJLFKkiH1gsiQi4GIEyEi6uHNIWuwIwB3QwoULxUUQtpkqVqyo8MtEBIhAfBGAR4Xu3btLpdARNDkIiS8xDtUGv5CwxsYHLPS2X375ZdGJhG4kExFIFQTISKZKT6dQO6+99loJaF+9enW1c+dOVatWLfXGG2+kEAJsKhFwBwL33XefOCiHQ24YxiVTGj16tCpfvrxaunSpypUrl/ryyy9V165dk6mJbAsRsIQAGUlLMDGT1xCAovuiRYtUt27dJBJOp06dxEXQrl27vNYU0ksEPItApkyZ/EEDksVBOXThYOT30EMPqaNHjyp8sK5atUrdfPPNnu0nEk4E0oNABsRfNBagtx+wLREp2Z0PdZ04cUJlzZo1UrX+7RHSFwwT8QvGBON03rx5/kk/X7586p133lFNmzYNzswrRIAI2I4AdCThWxK+Fb0eNnH+/PkKUlZ8kGL7ukePHgofqVbC43F+Dh5aVvkIPEn83ItfmiPbQMEYKdoLZDUfynLS83pwF5y9QvpCI2MVF6v5Et2/27ZtkwUASvBIrVq1UiNGjJAtqdAI8CoRIAJ2IfDII4+ot99+W1zhTJw40a5i41bOqVOnZNsaKjJgfuBuDG7GypYtKzREWweRietbcHd5Zf1g/wb2nbnfuLUdiA/PkhQBuAj67rvv1CuvvKIQo3v8+PGyCEDCwEQEiICzCDzxxBMK29xTpkxR0Jf0Ulq9erX4hoRBDRLasmLFCtGP9FI7SCsRcAoBMpJOIctyXYdAxowZFSJuLF++XFWqVEkhJvBtt90mrjuwbcJEBIiAMwjADRBCBsK3q1fCJkLy+OKLL6prrrlGYoZje37BggVqyJAhsq3tDFIslQh4DwEykt7rM1KcTgSwLbVkyRLVt29fUc2ApKFChQpifZnOovk4ESACYRDQDsrHjh0rviXDZHPFZXxk1q5dW9wXYVv7nnvuUevWrRMPEK4gkEQQARchQEbSRZ1BUuKHALbZ+vTpIwwlGMvffvtNrC/BXDIiTvz6gTWlDgLXXXedqlatmhjdQF/SrQlqL+XKlVPffvutypMnj2zHT5gwgfrUbu0w0pVwBMhIJrwLSEAiEcAWN/Sd4P/t33//Vf369VPwQwnGkokIEAF7EdAOymG0AituNyU4F7/11lvFKO/w4cMiffzll1/o4cFNnURaXIkAGUlXdguJiicCcOOBiBQzZ85UJUqUEB1KOBru3bu3uJyIJy2siwgkMwJ33nmnhE3E1vEHH3zgiqZCP7pXr15CF4zv4ILu1VdfVTguXLiwK2gkEUTAzQiQkXRz75C2uCKAbTdYaHbo0EGdPHlSDRgwQBaXqVOnxpUOVkYEkhUB+P/VupJuCJuId7t06dJq4MCB8s7DqfiaNWtUly5dVDRfxcnaR2wXEYgVgQxnzpwJcEhu9g8UrkC786EebCfkzp07XJVy3e56rZZH+kJ3S7Li9+OPP6p27dqptWvXSsOxwMDvJKxPmYgAEUg7AtjShgU0HJXPmjVL1a1bN+2FpfHJzZs3qwcffFB98803UsIll1wirsF0oAKr85rVfKiE61twZxG/YExwxSouVvM5Pf4okQzdj7ya4ghAOgk3QUOHDhWF+6+++kr8xj3zzDPq+PHjKY4Om08E0o5AtmzZxOUWSoh32ES8u9DThIEdmEioteCdhkW2ZiLT3jI+SQRSEwFGtrEYoQfDg5EJgl+SWL6IvIrfgQMHJGb36NGjJaoFpClwbN6yZctgQHiFCBCBqAgkImwiItHAqG7Hjh1CH3YZRo0apS6//PIgeq3Oa1bzoQKvzn9GcKy21+58xM/YC+eOreLsNH6USJ7rEx4RgZAIID73u+++K34mq1SpIo7M4VeuVq1a/q3vkA/yIhEgAiERyJs3r2rdurXcg66kkwkeGGrWrCkffmAiixYtKuENYUwTiol0khaWTQSSEQEyksnYq2yTIwiAifzhhx9EilGgQAEJuXj11VdLyLQjR444UicLJQLJigBCDcKgBWETYcVtd4LB3EsvvaRKlSqlFi1aJMV369ZNtrHvvvtuu6tjeUQgZREgI5myXc+GpwUBLHxQ0t+0aZPq1KmTbHVDooLFChE7EFaNiQgQgegIwHDt9ttvlwAAcL9lV0JAgZEjR4orL+g/ImH3YP369eqFF15Q2bNnt6sqlkMEiIBSiowkhwERSAMCOXPmVMOGDRNXITVq1FC7d+9W999/v4LUcsaMGWkokY8QgdRDQDsoh64i9CbTk/ARhwg0cOcDF15//vmnxMn+6KOPxLCGHhfSgy6fJQLhESAjGR4b3iECURHAorVw4UJZwGCEA0vvRo0aqapVq4prk6gFMAMRSGEEEM/ajrCJ+HiDmkmrVq1ktwABBaZPny56zTSKS+EBxqbHBQEyknGBmZUkOwIwvoFSP7booD/5008/qTvuuEMhvvCXX36Z7M1n+4hAmhHQDsoRNjHWOPfQWcZHGz7eVq1apa644gqFWNkrV66Ua2kmig8SASJgGQEykpahYkYiEBkB+MeDe5Hff/9ddLHy58+v4Ni8Tp06YjUKX5RMRIAIBCLQvHlzcVAOg5sPP/ww8GaYM0Sgqlevnrr++uvlow2W2NBVRmzse++9l1FpwuDGy0TACQQY2SYGP5KMTBA8BGPxY5Vq+B07dky99dZbasiQIQq+KJHghmTQoEHyG4wmrxCB1EQATCCsuKHHCGYwXHjCjRs3qp49e6rJkyeLYRuk/zCoefTRRxUi5jAyWuD44fwciIc+s4qL1XwoN9XWN7T5vPPOE0gpkdQji79EwGYEcuTIIVE0sPj169dPIuRAn/KGG25Qt9xyi7gSsrlKFkcEPIlA+/btFXxL/vrrr2rOnDlBbYC0sm3btmJI88knn6hcuXLJOwXvCdgaR4QaJiJABBKDQCbNUZqrD3fdyXwZM2b0c7jmesznpM+MiFLELxgTfcXKeHEKPyyQvXv3lgXvtddeU/jDNje25WDxDUlM48aNLY993Sb+EoFkQQBqIYht/+KLL8oWtY6/vWLFCvXqq6+qSZMmqVOnTonrns6dO4sUMk+ePAHNd+r9DagkzEki55cwJAVcJn0BcPhPrOCCzFbypfL4o0TSP6R4QAScRQBSlD59+ogOZa9evUSqAkfJ0BFDhA2EXcT2CBMRSEUE4Jc1U6ZMYpw2YMAAseauVKmSX2+yY8eOasuWLaIaYmYiUxEvtpkIuAUBMpJu6QnSkTIIYBHs37+/2rZtmxo8eLAqVqyY2rp1q3r66afF6OCxxx5T2A5nIgKphEDmzJlFRxL+IPHBtWTJEnXhhReKTiTej+HDh6uCBQumEiRsKxHwBAJkJD3RTSQyGRGAYQCsvME0wmnytddeq/766y81YsQIiZRTv359NXfu3GRsOttEBPwILF68WCytYXm9du1auQ5mEtvcYCDx0VW4cGF/fh4QASLgLgTISLqrP0hNCiIA/Rs4TYarIBjjNG3aVKxWZ86cKSHkypUrJ/G9YZXKRASSAQHoOyKkKHxAVq9eXT6k4EMSvlevueYaaSI8HZx//vnJ0Fy2gQgkNQJkJJO6e9k4ryEA45spU6aozZs3qy5duiiEYly3bp2CVSskNnB9gtBvTETAiwjs2rVLQT8Y6hwIKQrH/Yh9jZCGGzZsUF988YWCfiSSHWETvYgRaSYCXkOAjKTXeoz0pgQCxYsXF+vVHTt2SLQchF+EhAY+KHGMSDrz5s1LCSzYSO8jABUNhC8sUqSIGjhwoNqzZ4+MY4xnfBhBnQNRaZDgwB/+JBF7G8wkExEgAu5GgIyku/uH1KU4ArD0hh4ldMVGjx4tIRcBycSJE9Vtt90mkh1IKWmck+IDxYXNh4SxR48eMkZvv/12iUcPMhE2dMKECSJ17969e5ATcTgjx3Uk+JVkIgJEwN0IMLINI9uEHKFWPfpbzYdKUtnHmlF3AAAJw0lEQVTzf0iQlVJpwe/7779X7733nvr444/ViRMn/EVDt+y+++5TrVu3FufO/hs8IAJxQgBSROg+jhs3Ti1btsxfa9asWVWLFi1kCxvj1Mq4h6/VG2+8Ucqw4seP84sfbv+BFZx1ZuKnkTj3S/zOYWE8MuNCiaQRHR4TAQ8gAOMEMJK7d+8WKWXt2rXFOAf6ZojyAQtXGOx8+umn6vTp0x5oEUn0MgIYY1OnThWn+oUKFZIxCCYSksWbbrpJxijGKsasNqSx0t6bb77ZSjbmIQJEIMEIMLLNfx1g5Ys3lT3XRxunxC80QlZwwZNW8pnHH7a9H3jgAfmDntmYMWNEGoQtxWnTpskfouo0bNhQNWvWTHTPaAUbup94NTYEIAmfPXu2GIZ9/vnn6tChQ/4CoN8IqXibNm3URRdd5L8e6sDKuMdzVvKZ349Q9elrVsqzWq/VfKRPox/8a6U/iF8wbvpKovHLpAnhLxEgAt5FAAv2s88+K7plcCP04Ycfih6l3mrEdiNif8O9CqSV8FGJcyYiYBWBY8eOqRkzZoj0cdasWeLzVD+bL18+cWEFg5pq1apZYvz0s/wlAkTA2wiQkfR2/5F6IhCEABZybH8PHTpUzZ8/XxZ+bHPv3btXYhYjbjEkkzCAgKSyUaNG1KkMQpEXgAA8BWDsYOsaXgJOnjzpBwZRZhAjHh8mt9xyi4Q31LpT/kw8IAJEIOkRICOZ9F3MBqYqAgg5V7duXfkbOXKkODuHj0pse2/fvl2kS5AwIb4x9CzBVIIpYBi6VB0xZ9sNX49gHDFWvvnmG79hDO5ecsklqkmTJjJOatasqbDdyEQEiEBqI0BGMrX7n61PEQSw4NeqVUv+IKn84YcfRNIEphI6lZA24e/RRx9VFSpUEAkTjB1gNcst8OQeJNiyXrBggYKVNCTYq1evVghRqBN0HvGBAXdTMJ5hIgJEgAgYESAjaUSDx0QgRRBAaDr8DR48WOIbawnUypUrlf4bMmSISCuRD0wl/rBlDlcuTN5FAIYyixYtEsYRzCOs/RGe0JgqVarklzwiRCeS0aDGmJfHRIAIpDYCZCRTu//ZeiKgwCjgD6HrYP2NKCSQTOEP54sXL5a/559/XphIMJOasbz22mtpWOHyMQS9xSVLlvgljuhPo/9RkI9oSdBzxB90Z+FCiokIEAEiYAUBMpJWUGIeIpAiCMD6GzGQ8Ye0du1aYSi//vpr2f6EVApSLPwhIU5y5cqVxT+glnKWLFlS7vFfYhCAqgKkjEuXLpW/5cuXq+PHjwcQA7dQ0IvFVjU+CrTUMSATT4gAESACFhAgI2kBJGYhAqmKQOnSpRX+OnfurP7991+JVgJdSkgrsT0KBmXhwoXypzHKkyePn7GEA2owmMWKFdO3+WsjAjCa0gwjmEf8hdqCzpYtm6pRo4Zf6lilShUxlKGVtY2dwaKIQIoikOHgwYPntKqV8itZIypBpKSVse3Kh7qw3RJN/8rueq2WR/pCjwbilz5cvIwfIpqsWbNG/fzzz/KHaCa//fabMJxmVCABg9FGqVKl5BfH+jxLlizm7Dw3IHDq1CmJOQ1JI2JP408fw0+oOcE5MXCGpBi6jvi96qqrFKz4zcnL48/cFq4fZkSsr+d4kvgRP6vzgTlfBp++8h+G+gs1mqd0u/OhenxJQ5oRKdldr9XySF/oXiF+6cMl2fADUwgJGSRj0MvD8ZYtW0KD9N/V4sWLC1OJ7dXLLrtMXX755fIHRjOVEhjETZs2yR8wg1oBrm3dujUiDCVKlBCpr1YtgBRYRzCyax4HAZyfg7sh2d5frr+Bfcz+DcRDn5lx4da2Roa/RIAIpBsB6EzCZRD+dDpy5IgwRWCM9N8vv/yiduzYIVnAKOHvyy+/1I/IL1wWYUscjOUVV1zhZzD1udfcEsHNzsaNG/3MovEYW9RQHYiUihYtqsqWLSt/kDDiGL85c+YMekxP9EE3eIEIEAEiYDMCZCRtBpTFEQEiEIgAYoJff/318qcZHEjKjh49KlvjYC7XrVsnf7///rs4S4fuJRgrzWRq4x5jydD7Q2g+bJuH+8W9/PnzK+SFtBR/2OLVx+Zz5EP6+++/FbaU8YctfH1sPke+/fv3K2wxIwoMfo3H+hp+kTdaAiMO5hlSRuimQkprZBiN+EUri/eJABEgAvFAgIxkPFBmHUSACAQhAEmaZjDNN/ft2ycMJdwPQVq3bds2+cUx/iDNBGP2xx9/yJ/5eTeeg2mFVBGMYqg/MNzY2mciAkSACHgJATKSXuot0koEUgQBSBLxB2ORcAlb5ogfDgkgpJtaMmiUCGoJISScMCaARM8sVdTnKMOYwOiaJZZmaSYkmJpWLRXV55CEogycFyhQQIFRjJRCWVtHys97RIAIEAE3IEBG0g29QBqIABGIGQEwZhdccIG69NJLLTlFd7uxSMwA8AEiQASIgAsQyOgCGkgCESACRIAIEAEiQASIgAcRICPpwU4jyUSACBABIkAEiAARcAMCmbQVoCbGfK6vm3/tzofyYaUZrdxo9zWdducjfRrZwF+rOBO/QNz0GfHTSAT+WsXFaj6UzvklEGOcEb9gTGLBhfgRP41AKs8vlEjqUcBfIkAEiAARIAJEgAgQgZgQyBQu8kG46+bS7cwHB8R2lgda7SyP9Jl7/9y5FZyJ3zm8zEfEz4zI2XMruCCnlXwcf6ExJn7pw4X4ET8gkMrzCyWS4d8B3iECRIAIEAEiQASIABGIgAAZyQjg8BYRIAJEgAgQASJABIhAeATISIbHhneIABEgAkSACBABIkAEIiBARjICOLxFBIgAESACRIAIEAEiEB4BMpLhseEdIkAEiAARIAJEgAgQgQgIkJGMAA5vEQEiQASIABEgAkSACIRHgIxkeGx4hwgQASJABIgAESACRCACAoxs888/EeAJvJXKnusDkTh3xsgO57AwHlnFxWo+lM3xZ0T47DHxC8YEV6ziYjUfyuT4C8aa+AVjwvEXGhMncHHL+KNEMnyf8w4RIAJEgAgQASJABIhABAQY2eY/cBgZI/QosYILnrSSL5U9/4dG99xV4ncOC+ORFVyQ30o+jj8jsoHHxC8QD31mBRfktZKP40+jGvxL/IIxsTqurOZzcvxRIhm6/3iVCBABIkAEiAARIAJEIAoCZCSjAMTbRIAIEAEiQASIABEgAqERICMZGhdeJQJEgAgQASJABIgAEYiCABnJKADxNhEgAkSACBABIkAEiEBoBMhIhsaFV4kAESACRIAIEAEiQASiIEBGMgpAvE0EiAARIAJEgAgQASIQGgEykqFx4VUiQASIABEgAkSACBCBKAj8H/ts1nkSyyo5AAAAAElFTkSuQmCC