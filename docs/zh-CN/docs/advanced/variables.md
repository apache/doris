---
{
    "title": "变量",
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

# 变量

本文档主要介绍当前支持的变量（variables）。

Doris 中的变量参考 MySQL 中的变量设置。但部分变量仅用于兼容一些 MySQL 客户端协议，并不产生其在 MySQL 数据库中的实际意义。

## 变量设置与查看

### 查看

可以通过 `SHOW VARIABLES [LIKE 'xxx'];` 查看所有或指定的变量。如：

```sql
SHOW VARIABLES;
SHOW VARIABLES LIKE '%time_zone%';
```

### 设置

部分变量可以设置全局生效或仅当前会话生效。

注意，在 1.1 版本之前，设置全局生效后，后续新的会话连接中会沿用设置值，但当前会话中的值不变。
而在 1.1 版本（含）之后，设置全局生效后，后续新的会话连接中会沿用设置值，当前会话中的值也会改变。

仅当前会话生效，通过 `SET var_name=xxx;` 语句来设置。如：

```sql
SET exec_mem_limit = 137438953472;
SET forward_to_master = true;
SET time_zone = "Asia/Shanghai";
```

全局生效，通过 `SET GLOBAL var_name=xxx;` 设置。如：

```sql
SET GLOBAL exec_mem_limit = 137438953472
```

> 注1：只有 ADMIN 用户可以设置变量的全局生效。

既支持当前会话生效又支持全局生效的变量包括：

- `time_zone`
- `wait_timeout`
- `sql_mode`
- `enable_profile`
- `query_timeout`
- <version since="dev" type="inline">`insert_timeout`</version>
- `exec_mem_limit`
- `batch_size`
- `allow_partition_column_nullable`
- `insert_visible_timeout_ms`
- `enable_fold_constant_by_be`

只支持全局生效的变量包括：

- `default_rowset_type`
- `default_password_lifetime`
- `password_history`
- `validate_password_policy`

同时，变量设置也支持常量表达式。如：

```sql
SET exec_mem_limit = 10 * 1024 * 1024 * 1024;
SET forward_to_master = concat('tr', 'u', 'e');
```

### 在查询语句中设置变量

在一些场景中，我们可能需要对某些查询有针对性的设置变量。 通过使用SET_VAR提示可以在查询中设置会话变量（在单个语句内生效）。例子：

```sql
SELECT /*+ SET_VAR(exec_mem_limit = 8589934592) */ name FROM people ORDER BY name;
SELECT /*+ SET_VAR(query_timeout = 1, enable_partition_cache=true) */ sleep(3);
```

注意注释必须以/*+ 开头，并且只能跟随在SELECT之后。

## 支持的变量

- `SQL_AUTO_IS_NULL`

  用于兼容 JDBC 连接池 C3P0。 无实际作用。

- `auto_increment_increment`

  用于兼容 MySQL 客户端。无实际作用。虽然 Doris 已经有了 [AUTO_INCREMENT](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE#column_definition_list) 功能，但这个参数并不会对 AUTO_INCREMENT 的行为产生影响。auto_increment_offset 也是如此。

- `autocommit`

  用于兼容 MySQL 客户端。无实际作用。

- `auto_broadcast_join_threshold`

  执行连接时将向所有节点广播的表的最大字节大小，通过将此值设置为 -1 可以禁用广播。

  系统提供了两种 Join 的实现方式，`broadcast join` 和 `shuffle join`。

  `broadcast join` 是指将小表进行条件过滤后，将其广播到大表所在的各个节点上，形成一个内存 Hash 表，然后流式读出大表的数据进行 Hash Join。

  `shuffle join` 是指将小表和大表都按照 Join 的 key 进行 Hash，然后进行分布式的 Join。

  当小表的数据量较小时，`broadcast join` 拥有更好的性能。反之，则shuffle join拥有更好的性能。

  系统会自动尝试进行 Broadcast Join，也可以显式指定每个join算子的实现方式。系统提供了可配置的参数 `auto_broadcast_join_threshold`，指定使用 `broadcast join` 时，hash table 使用的内存占整体执行内存比例的上限，取值范围为0到1，默认值为0.8。当系统计算hash table使用的内存会超过此限制时，会自动转换为使用 `shuffle join`

  这里的整体执行内存是：查询优化器做估算的一个比例

  >注意：
  >
  >不建议用这个参数来调整，如果必须要使用某一种join，建议使用hint，比如 join[shuffle]

- `batch_size`

  用于指定在查询执行过程中，各个节点传输的单个数据包的行数。默认一个数据包的行数为 1024 行，即源端节点每产生 1024 行数据后，打包发给目的节点。

  较大的行数，会在扫描大数据量场景下提升查询的吞吐，但可能会在小查询场景下增加查询延迟。同时，也会增加查询的内存开销。建议设置范围 1024 至 4096。

- `character_set_client`

  用于兼容 MySQL 客户端。无实际作用。

- `character_set_connection`

  用于兼容 MySQL 客户端。无实际作用。

- `character_set_results`

  用于兼容 MySQL 客户端。无实际作用。

- `character_set_server`

  用于兼容 MySQL 客户端。无实际作用。

- `codegen_level`

  用于设置 LLVM codegen 的等级。（当前未生效）。

- `collation_connection`

  用于兼容 MySQL 客户端。无实际作用。

- `collation_database`

  用于兼容 MySQL 客户端。无实际作用。

- `collation_server`

  用于兼容 MySQL 客户端。无实际作用。

- `have_query_cache`

  用于兼容 MySQL 客户端。无实际作用。

- `default_order_by_limit`

  用于控制 OrderBy 以后返回的默认条数。默认值为 -1，默认返回查询后的最大条数，上限为 long 数据类型的 MAX_VALUE 值。

- `delete_without_partition`

  设置为 true 时。当使用 delete 命令删除分区表数据时，可以不指定分区。delete 操作将会自动应用到所有分区。

  但注意，自动应用到所有分区可能到导致 delete 命令耗时触发大量子任务导致耗时较长。如无必要，不建议开启。

- `disable_colocate_join`

  控制是否启用 [Colocation Join](../query-acceleration/join-optimization/colocation-join.md) 功能。默认为 false，表示启用该功能。true 表示禁用该功能。当该功能被禁用后，查询规划将不会尝试执行 Colocation Join。

- `enable_bucket_shuffle_join`

  控制是否启用 [Bucket Shuffle Join](../query-acceleration/join-optimization/bucket-shuffle-join.md) 功能。默认为 true，表示启用该功能。false 表示禁用该功能。当该功能被禁用后，查询规划将不会尝试执行 Bucket Shuffle Join。

- `disable_streaming_preaggregations`

  控制是否开启流式预聚合。默认为 false，即开启。当前不可设置，且默认开启。

- `enable_insert_strict`

  用于设置通过 INSERT 语句进行数据导入时，是否开启 `strict` 模式。默认为 false，即不开启 `strict` 模式。关于该模式的介绍，可以参阅 [这里](../data-operate/import/import-way/insert-into-manual.md)。

- `enable_spilling`

  用于设置是否开启大数据量落盘排序。默认为 false，即关闭该功能。当用户未指定 ORDER BY 子句的 LIMIT 条件，同时设置 `enable_spilling` 为 true 时，才会开启落盘排序。该功能启用后，会使用 BE 数据目录下 `doris-scratch/` 目录存放临时的落盘数据，并在查询结束后，清空临时数据。

  该功能主要用于使用有限的内存进行大数据量的排序操作。

  注意，该功能为实验性质，不保证稳定性，请谨慎开启。

- `exec_mem_limit`

  用于设置单个查询的内存限制。默认为 2GB，单位为B/K/KB/M/MB/G/GB/T/TB/P/PB, 默认为B。

  该参数用于限制一个查询计划中，单个查询计划的实例所能使用的内存。一个查询计划可能有多个实例，一个 BE 节点可能执行一个或多个实例。所以该参数并不能准确限制一个查询在整个集群的内存使用，也不能准确限制一个查询在单一 BE 节点上的内存使用。具体需要根据生成的查询计划判断。

  通常只有在一些阻塞节点（如排序节点、聚合节点、Join 节点）上才会消耗较多的内存，而其他节点（如扫描节点）中，数据为流式通过，并不会占用较多的内存。

  当出现 `Memory Exceed Limit` 错误时，可以尝试指数级增加该参数，如 4G、8G、16G 等。

  需要注意的是，这个值可能有几 MB 的浮动。

- `forward_to_master`

  用户设置是否将一些show 类命令转发到 Master FE 节点执行。默认为 `true`，即转发。Doris 中存在多个 FE 节点，其中一个为 Master 节点。通常用户可以连接任意 FE 节点进行全功能操作。但部分信息查看指令，只有从 Master FE 节点才能获取详细信息。

  如 `SHOW BACKENDS;` 命令，如果不转发到 Master FE 节点，则仅能看到节点是否存活等一些基本信息，而转发到 Master FE 则可以获取包括节点启动时间、最后一次心跳时间等更详细的信息。

  当前受该参数影响的命令如下：

  1. `SHOW FRONTENDS;`

     转发到 Master 可以查看最后一次心跳信息。

  2. `SHOW BACKENDS;`

     转发到 Master 可以查看启动时间、最后一次心跳信息、磁盘容量信息。

  3. `SHOW BROKER;`

     转发到 Master 可以查看启动时间、最后一次心跳信息。

  4. `SHOW TABLET;`/`ADMIN SHOW REPLICA DISTRIBUTION;`/`ADMIN SHOW REPLICA STATUS;`

     转发到 Master 可以查看 Master FE 元数据中存储的 tablet 信息。正常情况下，不同 FE 元数据中 tablet 信息应该是一致的。当出现问题时，可以通过这个方法比较当前 FE 和 Master FE 元数据的差异。

  5. `SHOW PROC;`

     转发到 Master 可以查看 Master FE 元数据中存储的相关 PROC 的信息。主要用于元数据比对。

- `init_connect`

  用于兼容 MySQL 客户端。无实际作用。

- `interactive_timeout`

  用于兼容 MySQL 客户端。无实际作用。

- `enable_profile`

  用于设置是否需要查看查询的 profile。默认为 false，即不需要 profile。

  默认情况下，只有在查询发生错误时，BE 才会发送 profile 给 FE，用于查看错误。正常结束的查询不会发送 profile。发送 profile 会产生一定的网络开销，对高并发查询场景不利。 当用户希望对一个查询的 profile 进行分析时，可以将这个变量设为 true 后，发送查询。查询结束后，可以通过在当前连接的 FE 的 web 页面查看到 profile：

  `fe_host:fe_http_port/query`

  其中会显示最近100条，开启 `enable_profile` 的查询的 profile。

- `language`

  用于兼容 MySQL 客户端。无实际作用。

- `license`

  显示 Doris 的 License。无其他作用。

- `lower_case_table_names`

  用于控制用户表表名大小写是否敏感。

  值为 0 时，表名大小写敏感。默认为0。

  值为 1 时，表名大小写不敏感，doris在存储和查询时会将表名转换为小写。
  优点是在一条语句中可以使用表名的任意大小写形式，下面的sql是正确的：

  ```sql
  mysql> show tables;  
  +------------------+
  | Tables_in_testdb |
  +------------------+
  | cost             |
  +------------------+
  
  mysql> select * from COST where COst.id < 100 order by cost.id;
  ```

  缺点是建表后无法获得建表语句中指定的表名，`show tables` 查看的表名为指定表名的小写。

  值为 2 时，表名大小写不敏感，doris存储建表语句中指定的表名，查询时转换为小写进行比较。 优点是`show tables` 查看的表名为建表语句中指定的表名；
  缺点是同一语句中只能使用表名的一种大小写形式，例如对`cost` 表使用表名 `COST` 进行查询：

  ```sql
  mysql> select * from COST where COST.id < 100 order by COST.id;
  ```

  该变量兼容MySQL。需在集群初始化时通过fe.conf 指定 `lower_case_table_names=`进行配置，集群初始化完成后无法通过`set` 语句修改该变量，也无法通过重启、升级集群修改该变量。

  information_schema中的系统视图表名不区分大小写，当`lower_case_table_names`值为 0 时，表现为 2。

- `max_allowed_packet`

  用于兼容 JDBC 连接池 C3P0。 无实际作用。

- `max_pushdown_conditions_per_column`

  该变量的具体含义请参阅 [BE 配置项](../admin-manual/config/be-config.md) 中 `max_pushdown_conditions_per_column` 的说明。该变量默认置为 -1，表示使用 `be.conf` 中的配置值。如果设置大于 0，则当前会话中的查询会使用该变量值，而忽略 `be.conf` 中的配置值。

- `max_scan_key_num`

  该变量的具体含义请参阅 [BE 配置项](../admin-manual/config/be-config.md) 中 `doris_max_scan_key_num` 的说明。该变量默认置为 -1，表示使用 `be.conf` 中的配置值。如果设置大于 0，则当前会话中的查询会使用该变量值，而忽略 `be.conf` 中的配置值。

- `net_buffer_length`

  用于兼容 MySQL 客户端。无实际作用。

- `net_read_timeout`

  用于兼容 MySQL 客户端。无实际作用。

- `net_write_timeout`

  用于兼容 MySQL 客户端。无实际作用。

- `parallel_exchange_instance_num`

  用于设置执行计划中，一个上层节点接收下层节点数据所使用的 exchange node 数量。默认为 -1，即表示 exchange node 数量等于下层节点执行实例的个数（默认行为）。当设置大于0，并且小于下层节点执行实例的个数，则 exchange node 数量等于设置值。

  在一个分布式的查询执行计划中，上层节点通常有一个或多个 exchange node 用于接收来自下层节点在不同 BE 上的执行实例的数据。通常 exchange node 数量等于下层节点执行实例数量。

  在一些聚合查询场景下，如果底层需要扫描的数据量较大，但聚合之后的数据量很小，则可以尝试修改此变量为一个较小的值，可以降低此类查询的资源开销。如在 DUPLICATE KEY 明细模型上进行聚合查询的场景。

- `parallel_fragment_exec_instance_num`

  针对扫描节点，设置其在每个 BE 节点上，执行实例的个数。默认为 1。

  一个查询计划通常会产生一组 scan range，即需要扫描的数据范围。这些数据分布在多个 BE 节点上。一个 BE 节点会有一个或多个 scan range。默认情况下，每个 BE 节点的一组 scan range 只由一个执行实例处理。当机器资源比较充裕时，可以将增加该变量，让更多的执行实例同时处理一组 scan range，从而提升查询效率。

  而 scan 实例的数量决定了上层其他执行节点，如聚合节点，join 节点的数量。因此相当于增加了整个查询计划执行的并发度。修改该参数会对大查询效率提升有帮助，但较大数值会消耗更多的机器资源，如CPU、内存、磁盘IO。

- `query_cache_size`

  用于兼容 MySQL 客户端。无实际作用。

- `query_cache_type`

  用于兼容 JDBC 连接池 C3P0。 无实际作用。

- `query_timeout`

  用于设置查询超时。该变量会作用于当前连接中所有的查询语句，对于 INSERT 语句推荐使用insert_timeout。默认为 15 分钟，单位为秒。

- `insert_timeout`
  <version since="dev"></version>用于设置针对 INSERT 语句的超时。该变量仅作用于 INSERT 语句，建议在 INSERT 行为易持续较长时间的场景下设置。默认为 4 小时，单位为秒。由于旧版本用户会通过延长 query_timeout 来防止 INSERT 语句超时，insert_timeout 在 query_timeout 大于自身的情况下将会失效, 以兼容旧版本用户的习惯。

- `resource_group`

  暂不使用。

- `send_batch_parallelism`

  用于设置执行 InsertStmt 操作时发送批处理数据的默认并行度，如果并行度的值超过 BE 配置中的 `max_send_batch_parallelism_per_job`，那么作为协调点的 BE 将使用 `max_send_batch_parallelism_per_job` 的值。

- `sql_mode`

  用于指定 SQL 模式，以适应某些 SQL 方言，关于 SQL 模式，可参阅[这里](./sql-mode.md)。

- `sql_safe_updates`

  用于兼容 MySQL 客户端。无实际作用。

- `sql_select_limit`

  用于设置 select 语句的默认返回行数，包括 insert 语句的 select 从句。默认不限制。

- `system_time_zone`

  显示当前系统时区。不可更改。

- `time_zone`

  用于设置当前会话的时区。时区会对某些时间函数的结果产生影响。关于时区，可以参阅 [这里](./time-zone.md)。

- `tx_isolation`

  用于兼容 MySQL 客户端。无实际作用。

- `tx_read_only`

  用于兼容 MySQL 客户端。无实际作用。

- `transaction_read_only`

  用于兼容 MySQL 客户端。无实际作用。

- `transaction_isolation`

  用于兼容 MySQL 客户端。无实际作用。

- `version`

  用于兼容 MySQL 客户端。无实际作用。

- `performance_schema`

  用于兼容 8.0.16及以上版本的MySQL JDBC。无实际作用。

- `version_comment`

  用于显示 Doris 的版本。不可更改。

- `wait_timeout`

  用于设置空闲连接的连接时长。当一个空闲连接在该时长内与 Doris 没有任何交互，则 Doris 会主动断开这个链接。默认为 8 小时，单位为秒。

- `default_rowset_type`

  用于设置计算节点存储引擎默认的存储格式。当前支持的存储格式包括：alpha/beta。

- `use_v2_rollup`

  用于控制查询使用segment v2存储格式的rollup索引获取数据。该变量用于上线segment v2的时候，进行验证使用；其他情况，不建议使用。

- `rewrite_count_distinct_to_bitmap_hll`

  是否将 bitmap 和 hll 类型的 count distinct 查询重写为 bitmap_union_count 和 hll_union_agg 。

- `prefer_join_method`

  在选择join的具体实现方式是broadcast join还是shuffle join时，如果broadcast join cost和shuffle join cost相等时，优先选择哪种join方式。

  目前该变量的可选值为"broadcast" 或者 "shuffle"。

- `allow_partition_column_nullable`

  建表时是否允许分区列为NULL。默认为true，表示允许为NULL。false 表示分区列必须被定义为NOT NULL

- `insert_visible_timeout_ms`

  在执行insert语句时，导入动作(查询和插入)完成后，还需要等待事务提交，使数据可见。此参数控制等待数据可见的超时时间，默认为10000，最小为1000。

- `enable_exchange_node_parallel_merge`

  在一个排序的查询之中，一个上层节点接收下层节点有序数据时，会在exchange node上进行对应的排序来保证最终的数据是有序的。但是单线程进行多路数据归并时，如果数据量过大，会导致exchange node的单点的归并瓶颈。

  Doris在这部分进行了优化处理，如果下层的数据节点过多。exchange node会启动多线程进行并行归并来加速排序过程。该参数默认为False，即表示 exchange node 不采取并行的归并排序，来减少额外的CPU和内存消耗。

- `extract_wide_range_expr`

  用于控制是否开启 「宽泛公因式提取」的优化。取值有两种：true 和 false 。默认情况下开启。

- `enable_fold_constant_by_be`

  用于控制常量折叠的计算方式。默认是 `false`，即在 `FE` 进行计算；若设置为 `true`，则通过 `RPC` 请求经 `BE` 计算。

- `cpu_resource_limit`

  用于限制一个查询的资源开销。这是一个实验性质的功能。目前的实现是限制一个查询在单个节点上的scan线程数量。限制了scan线程数，从底层返回的数据速度变慢，从而限制了查询整体的计算资源开销。假设设置为 2，则一个查询在单节点上最多使用2个scan线程。

  该参数会覆盖 `parallel_fragment_exec_instance_num` 的效果。即假设 `parallel_fragment_exec_instance_num` 设置为4，而该参数设置为2。则单个节点上的4个执行实例会共享最多2个扫描线程。

  该参数会被 user property 中的 `cpu_resource_limit` 配置覆盖。

  默认 -1，即不限制。

- `disable_join_reorder`

  用于关闭所有系统自动的 join reorder 算法。取值有两种：true 和 false。默认行况下关闭，也就是采用系统自动的 join reorder 算法。设置为 true 后，系统会关闭所有自动排序的算法，采用 SQL 原始的表顺序，执行 join

- `return_object_data_as_binary` 用于标识是否在select 结果中返回bitmap/hll 结果。在 select into outfile 语句中，如果导出文件格式为csv 则会将 bimap/hll 数据进行base64编码，如果是parquet 文件格式 将会把数据作为byte array 存储。下面将展示 Java 的例子，更多的示例可查看[samples](https://github.com/apache/doris/tree/master/samples/read_bitmap).

```java
try (Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:9030/test?user=root");
             Statement stmt = conn.createStatement()
) {
    stmt.execute("set return_object_data_as_binary=true"); // IMPORTANT!!!
    ResultSet rs = stmt.executeQuery("select uids from t_bitmap");
    while(rs.next()){
        byte[] bytes = rs.getBytes(1);
        RoaringBitmap bitmap32 = new RoaringBitmap();
        switch(bytes[0]) {
            case 0: // for empty bitmap
                break;
            case 1: // for only 1 element in bitmap32
                bitmap32.add(ByteBuffer.wrap(bytes,1,bytes.length-1)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt());
                break;
            case 2: // for more than 1 elements in bitmap32
                bitmap32.deserialize(ByteBuffer.wrap(bytes,1,bytes.length-1));
                break;
            // for more details, see https://github.com/apache/doris/tree/master/samples/read_bitmap
        }
    }
}
```

- `block_encryption_mode` 可以通过block_encryption_mode参数，控制块加密模式，默认值为：空。当使用AES算法加密时相当于`AES_128_ECB`, 当时用SM3算法加密时相当于`SM3_128_ECB` 可选值：

```text
  AES_128_ECB,
  AES_192_ECB,
  AES_256_ECB,
  AES_128_CBC,
  AES_192_CBC,
  AES_256_CBC,
  AES_128_CFB,
  AES_192_CFB,
  AES_256_CFB,
  AES_128_CFB1,
  AES_192_CFB1,
  AES_256_CFB1,
  AES_128_CFB8,
  AES_192_CFB8,
  AES_256_CFB8,
  AES_128_CFB128,
  AES_192_CFB128,
  AES_256_CFB128,
  AES_128_CTR,
  AES_192_CTR,
  AES_256_CTR,
  AES_128_OFB,
  AES_192_OFB,
  AES_256_OFB,
  SM4_128_ECB,
  SM4_128_CBC,
  SM4_128_CFB128,
  SM4_128_OFB,
  SM4_128_CTR,
```

- `enable_infer_predicate`

    用于控制是否进行谓词推导。取值有两种：true 和 false。默认情况下关闭，系统不在进行谓词推导，采用原始的谓词进行相关操作。设置为 true 后，进行谓词扩展。

- `trim_tailing_spaces_for_external_table_query`

    用于控制查询Hive外表时是否过滤掉字段末尾的空格。默认为false。

* `skip_storage_engine_merge`

    用于调试目的。在向量化执行引擎中，当发现读取Aggregate Key模型或者Unique Key模型的数据结果有问题的时候，把此变量的值设置为`true`，将会把Aggregate Key模型或者Unique Key模型的数据当成Duplicate Key模型读取。

* `skip_delete_predicate`

	用于调试目的。在向量化执行引擎中，当发现读取表的数据结果有误的时候，把此变量的值设置为`true`，将会把被删除的数据当成正常数据读取。

* `skip_delete_bitmap`

    用于调试目的。在Unique Key MoW表中，当发现读取表的数据结果有误的时候，把此变量的值设置为`true`，将会把被delete bitmap标记删除的数据当成正常数据读取。

* `skip_missing_version`

    有些极端场景下，表的 Tablet 下的所有的所有副本都有版本缺失，使得这些 Tablet 没有办法被恢复，导致整张表都不能查询。这个变量可以用来控制查询的行为，打设置为`true`时，查询会忽略 FE partition 中记录的 visibleVersion，使用 replica version。如果 Be 上的 Replica 有缺失的版本，则查询会直接跳过这些缺失的版本，只返回仍存在版本的数据。此外，查询将会总是选择所有存活的 BE 中所有 Replica 里 lastSuccessVersion 最大的那一个，这样可以尽可能的恢复更多的数据。这个变量应该只在上述紧急情况下才被设置为`true`，仅用于临时让表恢复查询。

* `default_password_lifetime`

 	默认的密码过期时间。默认值为 0，即表示不过期。单位为天。该参数只有当用户的密码过期属性为 DEFAULT 值时，才启用。如：
 	
 	```
 	CREATE USER user1 IDENTIFIED BY "12345" PASSWORD_EXPIRE DEFAULT;
 	ALTER USER user1 PASSWORD_EXPIRE DEFAULT;
	```
* `password_history`

	默认的历史密码次数。默认值为0，即不做限制。该参数只有当用户的历史密码次数属性为 DEFAULT 值时，才启用。如：

	```
 	CREATE USER user1 IDENTIFIED BY "12345" PASSWORD_HISTORY DEFAULT;
 	ALTER USER user1 PASSWORD_HISTORY DEFAULT;
	```
	
* `validate_password_policy`

	密码强度校验策略。默认为 `NONE` 或 `0`，即不做校验。可以设置为 `STRONG` 或 `2`。当设置为 `STRONG` 或 `2` 时，通过 `ALTER USER` 或 `SET PASSWORD` 命令设置密码时，密码必须包含“大写字母”，“小写字母”，“数字”和“特殊字符”中的3项，并且长度必须大于等于8。特殊字符包括：`~!@#$%^&*()_+|<>,.?/:;'[]{}"`。

* `group_concat_max_len`

    为了兼容某些BI工具能正确获取和设置该变量，变量值实际并没有作用。

* `rewrite_or_to_in_predicate_threshold`

    默认的改写OR to IN的OR数量阈值。默认值为2，即表示有2个OR的时候，如果可以合并，则会改写成IN。

*   `group_by_and_having_use_alias_first`

    指定group by和having语句是否优先使用列的别名，而非从From语句里寻找列的名字。默认为false。

* `enable_file_cache`

    控制是否启用block file cache，默认 false。该变量只有在be.conf中enable_file_cache=true时才有效，如果be.conf中enable_file_cache=false，该BE节点的block file cache处于禁用状态。

* `file_cache_base_path`

    指定block file cache在BE上的存储路径，默认 'random'，随机选择BE配置的存储路径。

* `enable_inverted_index_query`

    控制是否启用inverted index query，默认 true.

	
* `topn_opt_limit_threshold`

    设置topn优化的limit阈值 (例如：SELECT * FROM t ORDER BY k LIMIT n). 如果limit的n小于等于阈值，topn相关优化（动态过滤下推、两阶段获取结果、按key的顺序读数据）会自动启用，否则会禁用。默认值是1024。

* `drop_table_if_ctas_failed`

    控制create table as select在写入发生错误时是否删除已创建的表，默认为true。

* `show_user_default_role`

    <version since="dev"></version>

    控制是否在 `show roles` 的结果里显示每个用户隐式对应的角色。默认为 false。

* `use_fix_replica`

    <version since="1.2.0"></version>

    使用固定replica进行查询。replica从0开始，如果use_fix_replica为0，则使用最小的，如果use_fix_replica为1，则使用第二个最小的，依此类推。默认值为-1，表示未启用。

* `dry_run_query`

    <version since="dev"></version>

    如果设置为true，对于查询请求，将不再返回实际结果集，而仅返回行数。对于导入和insert，Sink 丢掉了数据，不会有实际的写发生。额默认为 false。

    该参数可以用于测试返回大量数据集时，规避结果集传输的耗时，重点关注底层查询执行的耗时。

    ```
    mysql> select * from bigtable;
    +--------------+
    | ReturnedRows |
    +--------------+
    | 10000000     |
    +--------------+
    ```
  
* `enable_parquet_lazy_materialization`

  控制 parquet reader 是否启用延迟物化技术。默认为 true。

* `enable_orc_lazy_materialization`

  控制 orc reader 是否启用延迟物化技术。默认为 true。

* `enable_strong_consistency_read`

  用以开启强一致读。Doris 默认支持同一个会话内的强一致性，即同一个会话内对数据的变更操作是实时可见的。如需要会话间的强一致读，则需将此变量设置为true。

* `truncate_char_or_varchar_columns`

  是否按照表的 schema 来截断 char 或者 varchar 列。默认为 false。

  因为外表会存在表的 schema 中 char 或者 varchar 列的最大长度和底层 parquet 或者 orc 文件中的 schema 不一致的情况。此时开启改选项，会按照表的 schema 中的最大长度进行截断。

* `jdbc_clickhouse_query_final`

  是否在使用 JDBC Catalog 功能查询 ClickHouse 时增加 final 关键字，默认为 false

  用于 ClickHouse 的 ReplacingMergeTree 表引擎查询去重

* `enable_memtable_on_sink_node`

  <version since="2.1.0">
  是否在数据导入中启用 MemTable 前移，默认为 false
  </version>

  在 DataSink 节点上构建 MemTable，并通过 brpc streaming 发送 segment 到其他 BE。
  该方法减少了多副本之间的重复工作，并且节省了数据序列化和反序列化的时间。

* `enable_unique_key_partial_update`

  <version since="2.0.2">
  是否在对insert into语句启用部分列更新的语义，默认为 false。需要注意的是，控制insert语句是否开启严格模式的会话变量`enable_insert_strict`的默认值为true，即insert语句默认开启严格模式，而在严格模式下进行部分列更新不允许更新不存在的key。所以，在使用insert语句进行部分列更新的时候如果希望能插入不存在的key，需要在`enable_unique_key_partial_update`设置为true的基础上同时将`enable_insert_strict`设置为false。
  </version>

***

#### 关于语句执行超时控制的补充说明

* 控制手段

    目前doris支持通过`variable`和`user property`两种体系来进行超时控制。其中均包含`qeury_timeout`和`insert_timeout`。

* 优先次序

    超时生效的优先级次序是：`session variable` > `user property` > `global variable` > `default value`

    较高优先级的变量未设置时，会自动采用下一个优先级的数值。

* 相关语义

    `query_timeout`用于控制所有语句的超时，`insert_timeout`特定用于控制 INSERT 语句的超时，在执行 INSERT 语句时，超时时间会取
    
    `query_timeout`和`insert_timeout`中的最大值。

    `user property`中的`query_timeout`和`insert_timeout`只能由 ADMIN 用户对目标用户予以指定，其语义在于改变被指定用户的默认超时时间，
    
    并且不具备`quota`语义。

* 注意事项

    `user property`设置的超时时间需要客户端重连后触发。
