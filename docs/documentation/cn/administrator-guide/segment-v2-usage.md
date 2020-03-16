# Doris Segment V2上线和试用手册

## 背景

Doris 0.12版本中实现了segment v2（新的存储格式），引入词典压缩、bitmap索引、page cache等优化，能够提升系统性能。目前0.12版本已经发布alpha版本，正在内部上线过程中，上线的方案和试用方法记录如下

## 上线

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



