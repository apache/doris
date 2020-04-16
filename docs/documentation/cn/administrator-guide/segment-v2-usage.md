# Segment V2 升级手册

## 背景

Doris 0.12版本中实现了新的存储格式：Segment v2，引入词典压缩、bitmap索引、page cache等优化，能够提升系统性能。

0.12 版本会同时支持读写原有的 Segment V1（以下简称V1） 和新的 Segment V2（以下简称V2） 两种格式。如果原有数据想使用 V2 相关特性，需通过命令将 V1 转换成 V2 格式。

本文档主要介绍从 0.11 版本升级至 0.12 版本后，如果转换和使用 V2 格式。


## 集群升级

0.12 版本仅支持从0.11 版本升级，不支持从 0.11 之前的版本升级。请先确保升级的前的 Doris 集群版本为 0.11。

按常规步骤对集群升级后，原有集群数据的存储格式不会变更，即依然为 V1 格式。如果对 V2 格式没有需求，则继续正常使用集群即可，无需做任何额外操作。

## V2 格式转换

### 已有表数据转换成 V2

对于已有表数据的格式转换，Doris 提供两种方式：

1. 创建一个 V2 格式的特殊 Rollup

    该方式会针对指定表，创建一个 V2 格式的特殊 Rollup。创建完成后，新的 V2 格式的 Rollup 会和原有表格式数据并存。用户可以指定对 V2 格式的 Rollup进行查询验证。
    
    该方式主要用于对 V2 格式的验证，因为不会修改原有表数据，因此可以安全的进行 V2 格式的数据验证，而不用担心表数据因格式转换而损坏。
    
    操作步骤如下：
    
    ```
    ## 创建 V2 格式的 ROLLUP
    
    ALTER TABLE table_name ADD ROLLUP table_name (columns) PROPERTIES ("storage_format" = "v2");
    ```

    其中， Rollup 的名称必须为表名。columns 字段可以任意填写，系统不会检查该字段的合法性。该语句会自动生成一个名为 `__v2_table_name` 的 Rollup，并且该 Rollup 列包含表的全部列。
    
    通过以下语句查看创建进度：
    
    ```
    SHOW ALTER TABLE ROLLUP;
    ``
    
    创建完成后，可以通过 `DESC table_name ALL;` 名为 `__v2_table_name` 的 Rollup。
    
    

为了保证上线的稳定性，上线分为三个阶段：
第一个阶段是上线0.12版本，但是不全量开启segment v2的功能，只在验证的时候，创建segment v2的表（或者索引）
第二个阶段是全量开启segment v2的功能，替换现有的segment存储格式，这样对新表会创建segment v2的格式的存储文件，但是对于旧表，需要依赖于compaction和schema change等过程，实现格式的转化
第三个阶段就是转化旧的segment格式到新的segment v2的格式，这个需要在验证segment v2的正确性和性能没有问题之后，可以按照用户意愿逐步完成。

### 上线验证

- 正确性

正确性是segment v2上线最需要保证的指标。 在第一阶段，为了保证线上环境的稳定性，并且验证segment v2的正确性，采用如下的方案：
1. 选择几个需要验证的表，使用以下语句，创建segment v2格式的rollup表，该rollup表与base表的schema相同

	alter table table_name add rollup table_name (columns) properties ("storage_format" = "v2");

	其中，
	rollup后面的index名字直接指定为base table的table name，该语句会自动生成一个__v2_table_name。

	columns可以随便指定一个列名即可，这里一方面是为了兼容现有的语法，一方面是为了方便。

2. 上面的创建出来的rollup index名字格式类似：__v2_table_name

	通过命令 :

	`desc table_name all;`

	查看table中是否存在名字：__v2_table_name的rollup。由于创建segment v2的rollup表是一个异步操作，所以并不会立即成功。如果上面命令中并没有显示新创建的 rollup，可能是还在创建过程中。

    可以通过下面命令查看正在进行的 rollup 表。

	`show alter table rollup;`

	看到会有一个名字为__v2_table_name的rollup表正在创建。

3. 通过查询base表和segment v2格式的rollup表，验证查询结果是否一致（查询可以来自audit日志）

	为了让查询使用segment v2的rollup表，增加了一个session变量，通过在查询中使用如下的语句，来查询segment v2格式的index：

	set use_v2_rollup = true;

	比如说，要当前db中有一个simple的表，直接查询如下：

	```
		mysql> select * from simple;         
		+------+-------+
		| key  | value |
		+------+-------+
		|    2 |     6 |
		|    1 |     6 |
		|    4 |     5 |
		+------+-------+
		3 rows in set (0.01 sec)
	```

	然后使用使用如下的query查询__v2_simpel的rollup表：

	```
		mysql> set use_v2_rollup = true;
		Query OK, 0 rows affected (0.04 sec)

		mysql> select * from simple;
		+------+-------+	
		| key  | value |
		+------+-------+
		|    4 |     5 |
		|    1 |     6 |
		|    2 |     6 |
		+------+-------+
		3 rows in set (0.01 sec)
	```

	期望的结果是两次查询的结果相同。
	如果结果不一致，需要进行排查。第一步需要定位是否是因为由于两次查询之间有新的导入，导致数据不一致。如果是的话，需要进行重试。如果不是，则需要进一步定位原因。

4. 对比同样查询在base表和segment v2的rollup表中的结果是否一致

- 查询延时

	在segment v2中优化了v1中的随机读取，并且增加了bitmap索引、lazy materialization等优化，预计查询延时会有下降，

- 导入延时
- page cache命中率
- string类型字段压缩率
- bitmap索引的性能提升率和空间占用

### 全量开启segment v2

有两个方式：
1. fe中有一个全局变量，通过设置全局变量default_rowset_type，来设置BE中使用segment v2作为默认的存储格式，命令如下：

	set global default_rowset_type = beta;

	使用这个方式，只需要执行上述命令，之后等待10s左右，FE会将配置同步到BE中。不需要在每个BE中进行配置。推荐使用这个配置。不过该方式目前没有简单的方式来验证BE上的default rowset type是否已经变更，一种办法是建一个表，然后查看对应的表的元数据。但是这样子比较麻烦，后续看需要可以在某个地方实现状态查看。

2. 修改BE中的配置文件中的配置default_rowset_type为BETA。这种方式需要在每个BE中添加对应的配置。


### 转化segment v1格式为v2

如果不想等待系统后台自动转换存储格式（比如想要使用bitmap索引、词典压缩），可以手动指定触发某个表的存储格式转换。具体是通过schema change来实现将v1的格式转化为v2的格式：

	alter table table_name set ("storage_format" = "v2");

	将存储格式转化为v2.

如果在没有设置默认的存储格式为v2的时候，想要新建一个v2的表，需要按照以下的步骤进行操作：
1. 使用create table来创建v1的表
2. 使用上面的schema change命令进行格式的转化



