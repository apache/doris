# 例行导入使用手册

例行导入（Routine Load）功能为用户提供了一种自动从指定数据源进行数据导入的功能。本文档主要介绍该功能的实现原理、使用方式以及最佳实践。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* RoutineLoadJob：用户提交的一个例行导入作业。
* JobScheduler：例行导入作业调度器，用于调度和拆分一个 RoutineLoadJob 为多个 Task。
* Task：RoutineLoadJob 被 JobScheduler 根据规则拆分的子任务。
* TaskScheduler：任务调度器。用于调度 Task 的执行。

## 原理

```              
         +---------+
         |  Client |
         +----+----+
              |
+-----------------------------+
| FE          |               |
| +-----------v------------+  |
| |                        |  |
| |   Routine Load Job     |  |
| |                        |  |
| +---+--------+--------+--+  |
|     |        |        |     |
| +---v--+ +---v--+ +---v--+  |
| | task | | task | | task |  |
| +--+---+ +---+--+ +---+--+  |
|    |         |        |     |
+-----------------------------+
     |         |        |
     v         v        v
 +---+--+   +--+---+   ++-----+
 |  BE  |   |  BE  |   |  BE  |
 +------+   +------+   +------+

```

如上图，Client 向 FE 提交一个例行导入作业。FE 通过 JobScheduler 将一个导入作业拆分成若干个 Task。每个 Task 负责导入指定的一部分数据。Task 被 TaskScheduler 分配到指定的 BE 上执行。在 BE 上，一个 Task 被视为一个普通的导入任务，通过 Stream Load 的导入机制进行导入。导入完成后，向 FE 汇报。FE 中的 JobScheduler 根据汇报结果，继续生成后续新的 Task，或者对失败的 Task 进行重试。整个例行导入作业通过不断的产生新的 Task，来完成数据不间断的导入。

## Kafka 例行导入

当前我们仅支持从 Kafka 系统进行例行导入。该部分会详细介绍 Kafka 例行导入使用方式和最佳实践。

### 使用限制

1. 仅支持无认证的 Kafka 访问。
2. 支持的消息格式为 csv 文本格式。每一个 message 为一行，且行尾**不包含**换行符。
3. 仅支持 Kafka 0.10.0.0(含) 以上版本。

### 创建例行导入任务

创建例行导入任务的的详细语法可以参照 [这里]()。或者连接到 Doris 后，执行 `HELP ROUTINE LOAD;` 查看语法帮助。这里主要详细介绍，创建作业时的注意事项。

* columns_mapping

    columns_mapping 主要用于指定表结构和 message 中的列映射关系，以及一些列的转换。如果不指定，Doris 会默认 message 中的列和表结构的列按顺序一一对应。虽然在正常情况下，如果源数据正好一一对应，则不指定也可以进行正常的数据导入。但是我们依然强烈建议用户**显示的指定列映射关系**。这样当表结构发生变化（比如增加一个 nullable 的列），或者源文件发生变化（比如增加了一列）时，导入任务依然可以继续进行。否则，当发生上述变动后，因为列映射关系不再一一对应，导入将报错。
    
    在 columns_mapping 中我们同样可以使用一些内置函数进行列的转换。但需要注意函数对应的实际列类型。举例说明：
    
    假设用户需要导入只包含 `k1` 一列的表，列类型为 int。并且需要将源文件中的 null 值转换为 0。该功能可以通过 `ifnull` 函数实现。正确是的使用方式如下：
    
    `COLUMNS (xx, k1=ifnull(xx, "3"))`
    
    注意这里我们使用 `"3"` 而不是 `3`。因为对于导入任务来说，源数据中的列类型都为 `varchar`，所以这里 `xx` 虚拟列的类型也为 `varchar`。所以我们需要使用 `"3"` 来进行对应的匹配，否则 `ifnull` 函数无法找到参数为 `(varchar, int)` 的函数签名，将出现错误。

* desired\_concurrent\_number

    desired_concurrent_number 用于指定一个例行作业期望的并发度。即一个作业，最多有多少 task 同时在执行。对于 Kafka 导入而言，当前的实际并发度计算如下：
    
    `Min(partition num / 3, desired_concurrent_number, alive_backend_num, DEFAULT_TASK_MAX_CONCURRENT_NUM)`
    其中DEFAULT_TASK_MAX_CONCURRENT_NUM是系统的一个默认的最大并发数限制。

    其中 partition 值订阅的 Kafka topic 的 partition数量。`alive_backend_num` 是当前正常的 BE 节点数。
    
* max\_batch\_interval/max\_batch\_rows/max\_batch\_size

    这三个参数用于控制单个任务的执行时间。其中任意一个阈值达到，则任务结束。其中 `max_batch_rows` 用于记录从 Kafka 中读取到的数据行数。`max_batch_size` 用于记录从 Kafka 中读取到的数据量，单位是字节。目前一个任务的消费速率大约为 5-10MB/s。
    
    那么假设一行数据 500B，用户希望每 100MB 或 10 秒为一个 task。100MB 的预期处理时间是 10-20 秒，对应的行数约为 200000 行。则一个合理的配置为：
    
    ```
    "max_batch_interval" = "10",
    "max_batch_rows" = "200000",
    "max_batch_size" = "104857600"
    ```
    
    以上示例中的参数也是这些配置的默认参数。
    
* max\_error\_number

    `max_error_number` 用于控制错误率。在错误率过高的时候，作业会自动暂停。因为整个作业是面向数据流的，因为数据流的无边界性，我们无法像其他导入任务一样，通过一个错误比例来计算错误率。因此这里提供了一种新的计算方式，来计算数据流中的错误比例。
    
    我们设定了一个采样窗口。窗口的大小为 `max_batch_rows * 10`。在一个采样窗口内，如果错误行数超过 `max_error_number`，则作业被暂定。如果没有超过，则下一个窗口重新开始计算错误行数。
    
    我们假设 `max_error_number` 为 200000，则窗口大小为 2000000。设 `max_error_number` 为 20000，即用户预期每 2000000 行的错误行为 20000。即错误率为 1%。但是因为不是每批次任务正好消费 200000 行，所以窗口的实际范围是 [2000000, 2200000]，即有 10% 的统计误差。
    
    错误行不包括通过 where 条件过滤掉的行。但是包括没有对应的 Doris 表中的分区的行。
    
* data\_source\_properties

    `data_source_properties` 中可以指定消费具体的 Kakfa partition。如果不指定，则默认消费所订阅的 topic 的所有 partition。
    
    注意，当显示的指定了 partition，则导入作业不会再动态的检测 Kafka partition 的变化。如果没有指定，则会根据 kafka partition 的变化，动态消费 partition。

### 查看导入作业状态

查看作业状态的具体命令和示例可以通过 `help show routine load;` 命令查看。

查看任务运行状态的具体命令和示例可以通过 `help show routine load task;` 命令查看。

只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

### 作业控制
    
用户可以通过 `STOP/PAUSE/RESUME` 三个命令来控制作业的停止，暂停和重启。可以通过 `help stop routine load;`, `help pause routine load;` 以及 `help resume routine load;` 三个命令查看帮助和示例。

## 其他说明

1. 例行导入作业和 ALTER TABLE 操作的关系

    * 例行导入不会阻塞 SCHEMA CHANGE 和 ROLLUP 操作。但是注意如果 SCHEMA CHANGE 完成后，列映射关系无法匹配，则会导致作业的错误数据激增，最终导致作业暂定。建议通过在例行导入作业中显式指定列映射关系，以及通过增加 Nullable 列或带 Default 值的列来减少这类问题。
    * 删除表的 Partition 可能会导致导入数据无法找到对应的 Partition，作业进入暂定

2. 例行导入作业和其他导入作业的关系（LOAD, DELETE, INSERT）

    * 例行导入和其他 LOAD 作业以及 INSERT 操作没有冲突。
    * 当前 DELETE 操作时，对应表分区不能有任何正在执行的导入任务。所以在执行 DELETE 操作前，可能需要先暂停例行导入作业，并等待已下发的 task 全部完成后，才可以执行 DELETE。

3. 例行导入作业和 DROP DATABASE/TABLE 操作的关系

    当例行导入对应的 database 或 table 被删除后，作业会自动 CANCEL。
 
    
    
    
