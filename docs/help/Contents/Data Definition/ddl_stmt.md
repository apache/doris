# CREATE DATABASE
## description
    该语句用于新建数据库（database）
    语法：
        CREATE DATABASE [IF NOT EXISTS] db_name;

## example
    1. 新建数据库 db_test
        CREATE DATABASE db_test;
        
## keyword
    CREATE,DATABASE
    
# DROP DATABASE
## description
    该语句用于删除数据库（database）
    语法：
        DROP DATABASE [IF EXISTS] db_name;

    说明：
        执行 DROP DATABASE 一段时间内，可以通过 RECOVER 语句恢复被删除的 database。详见 RECOVER 语句
        
## example
    1. 删除数据库 db_test
        DROP DATABASE db_test;
        
## keyword
    DROP,DATABASE
        
# CREATE TABLE
## description
    该语句用于创建 table。
    语法：
        CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
        (column_definition1[, column_definition2, ...])
        [ENGINE = [olap|mysql|broker]]
        [key_desc]
        [partition_desc]
        [distribution_desc]
        [PROPERTIES ("key"="value", ...)];
        [BROKER PROPERTIES ("key"="value", ...)];
        
    1. column_definition
        语法：
        col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
        
        说明：
        col_name：列名称
        col_type：列类型
                            TINYINT（1字节）
                                范围：-2^7 + 1 ~ 2^7 - 1
                            SMALLINT（2字节）
                                范围：-2^15 + 1 ~ 2^15 - 1
                            INT（4字节）
                                范围：-2^31 + 1 ~ 2^31 - 1
                            BIGINT（8字节）
                                范围：-2^63 + 1 ~ 2^63 - 1
                            LARGEINT（16字节）
                                范围：0 ~ 2^127 - 1
                            FLOAT（4字节）
                            DOUBLE（12字节）
                            DECIMAL[(precision, scale)] (40字节)
                                保证精度的小数类型。默认是 DECIMAL(10, 0)
                                precision: 1 ~ 27
                                scale: 0 ~ 9
                                其中整数部分为 1 ~ 18
                            DATE（3字节）
                                范围：1900-01-01 ~ 9999-12-31
                            DATETIME（8字节）
                                范围：1900-01-01 00:00:00 ~ 9999-12-31 23:59:59
                            CHAR[(length)]
                                定长字符串。长度范围：1 ~ 255。默认为1
                            VARCHAR[(length)]
                                变长字符串。长度范围：1 ~ 65533
                            HLL (1~16385个字节)
                                hll列类型，不需要指定长度和默认值、长度根据数据的聚合
                                程度系统内控制，并且HLL列只能通过配套的hll_union_agg、Hll_cardinality、hll_hash进行查询或使用
                                
        agg_type：聚合类型，如果不指定，则该列为 key 列。否则，该列为 value 列
                            SUM、MAX、MIN、REPLACE、HLL_UNION(仅用于HLL列，为HLL独有的聚合方式)
                            该类型只对聚合模型(key_desc的type为AGGREGATE KEY)有用，其它模型不需要指定这个。
        是否允许为NULL: 默认允许为NULL，导入时用\N来表示

    2. ENGINE 类型
        默认为 olap。可选 mysql, broker
        1) 如果是 mysql，则需要在 properties 提供以下信息：
        
            PROPERTIES (
            "host" = "mysql_server_host",
            "port" = "mysql_server_port",
            "user" = "your_user_name",
            "password" = "your_password",
            "database" = "database_name",
            "table" = "table_name"
            )
        
        注意：
            "table" 条目中的 "table_name" 是 mysql 中的真实表名。
            而 CREATE TABLE 语句中的 table_name 是该 mysql 表在 Palo 中的名字，可以不同。
            
            在 Palo 创建 mysql 表的目的是可以通过 Palo 访问 mysql 数据库。
            而 Palo 本身并不维护、存储任何 mysql 数据。
        2) 如果是 broker，表示表的访问需要通过指定的broker, 需要在 properties 提供以下信息：
            PROPERTIES (
            "broker_name" = "broker_name",
            "paths" = "file_path1[,file_path2]",
            "column_separator" = "value_separator"
            "line_delimiter" = "value_delimiter"
            )
            另外还需要提供Broker需要的Property信息，通过BROKER PROPERTIES来传递，例如HDFS需要传入
            BROKER PROPERTIES(
                "username" = "name", 
                "password" = "password"
            )
            这个根据不同的Broker类型，需要传入的内容也不相同
        注意：
            "paths" 中如果有多个文件，用逗号[,]分割。如果文件名中包含逗号，那么使用 %2c 来替代。如果文件名中包含 %，使用 %25 代替
            现在文件内容格式支持CSV，支持GZ，BZ2，LZ4，LZO(LZOP) 压缩格式。
    
    3. key_desc
        语法：
            key_type(k1[,k2 ...])
        说明：
            数据按照指定的key列进行排序，且根据不同的key_type具有不同特性。
            key_type支持一下类型：
                    AGGREGATE KEY:key列相同的记录，value列按照指定的聚合类型进行聚合，
                                 适合报表、多维分析等业务场景。
                    UNIQUE KEY:key列相同的记录，value列按导入顺序进行覆盖，
                                 适合按key列进行增删改查的点查询业务。
                    DUPLICATE KEY:key列相同的记录，同时存在于Palo中，
                                 适合存储明细数据或者数据无聚合特性的业务场景。
        注意：
            除AGGREGATE KEY外，其他key_type在建表时，value列不需要指定聚合类型。

    4. partition_desc
        1) Range 分区
        语法：
            PARTITION BY RANGE (k1)
            (
            PARTITION partition_name VALUES LESS THAN MAXVALUE|("value1")
            PARTITION partition_name VALUES LESS THAN MAXVALUE|("value2")
            ...
            )
        说明：
            使用指定的 key 列和指定的数值范围进行分区。
            1) 分区名称仅支持字母开头，字母、数字和下划线组成
            2) 目前仅支持以下类型的列作为 Range 分区列，且只能指定一个分区列
                TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME
            3) 分区为左闭右开区间，首个分区的左边界为做最小值
                             
        注意：
            1) 分区一般用于时间维度的数据管理
            2) 有数据回溯需求的，可以考虑首个分区为空分区，以便后续增加分区

    5. distribution_desc
        1) Random 分桶
        语法：
            DISTRIBUTED BY RANDOM [BUCKETS num]
        说明：
            使用所有 key 列进行哈希分桶。默认分区数为10
    
        2) Hash 分桶
        语法：
            DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
        说明：
            使用指定的 key 列进行哈希分桶。默认分区数为10

        建议:建议使用Hash分桶方式

    6. PROPERTIES
        1) 如果 ENGINE 类型为 olap，则可以在 properties 中指定行存或列存

            PROPERTIES (
            "storage_type" = "[row|column]",
            )
        
        2) 如果 ENGINE 类型为 olap
           可以在 properties 设置该表数据的初始存储介质、存储到期时间和副本数。
           
           PROPERTIES (
           "storage_medium" = "[SSD|HDD]",
           ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],
           ["replication_num" = "3"]
           )
           
           storage_medium：        用于指定该分区的初始存储介质，可选择 SSD 或 HDD。默认为 HDD。
           storage_cooldown_time： 当设置存储介质为 SSD 时，指定该分区在 SSD 上的存储到期时间。
                                   默认存放 7 天。
                                   格式为："yyyy-MM-dd HH:mm:ss"
           replication_num:        指定分区的副本数。默认为 3
           
           当表为单分区表时，这些属性为表的属性。
           当表为两级分区时，这些属性为附属于每一个分区。
           如果希望不同分区有不同属性。可以通过 ADD PARTITION 或 MODIFY PARTITION 进行操作

        3) 如果 Engine 类型为 olap, 并且 storage_type 为 column, 可以指定某列使用 bloom filter 索引
           bloom filter 索引仅适用于查询条件为 in 和 equal 的情况，该列的值越分散效果越好
           目前只支持以下情况的列:除了 TINYINT FLOAT DOUBLE 类型以外的 key 列及聚合方法为 REPLACE 的 value 列
           
           PROPERTIES (
           "bloom_filter_columns"="k1,k2,k3"
           )
    
## example
    1. 创建一个 olap 表，使用 Random 分桶，使用列存，相同key的记录进行聚合
        CREATE TABLE example_db.table_random
        (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 CHAR(10) REPLACE,
        v2 INT SUM
        )
        ENGINE=olap
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY RANDOM BUCKETS 32
        PROPERTIES ("storage_type"="column");
        
    2. 创建一个 olap 表，使用 Hash 分桶，使用行存，相同key的记录进行覆盖，
       设置初始存储介质和冷却时间
        CREATE TABLE example_db.table_hash
        (
        k1 BIGINT,
        k2 LARGEINT,
        v1 VARCHAR(2048) REPLACE,
        v2 SMALLINT SUM DEFAULT "10"
        )
        ENGINE=olap
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
        PROPERTIES(
        "storage_type"="row"，
        "storage_medium" = "SSD",
        "storage_cooldown_time" = "2015-06-04 00:00:00"
        );
    
    3. 创建一个 olap 表，使用 Key Range 分区，使用Hash分桶，默认使用列存，
       相同key的记录同时存在，设置初始存储介质和冷却时间
        CREATE TABLE example_db.table_range
        (
        k1 DATE,
        k2 INT,
        k3 SMALLINT,
        v1 VARCHAR(2048),
        v2 DATETIME DEFAULT "2014-02-04 15:36:00"
        )
        ENGINE=olap
        DUPLICATE KEY(k1, k2, k3)
        PARTITION BY RANGE (k1)
        (
        PARTITION p1 VALUES LESS THAN ("2014-01-01"),
        PARTITION p2 VALUES LESS THAN ("2014-06-01"),
        PARTITION p3 VALUES LESS THAN ("2014-12-01")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 32
        PROPERTIES(
        "storage_medium" = "SSD", "storage_cooldown_time" = "2015-06-04 00:00:00"
        );
        
        说明：
        这个语句会将数据划分成如下3个分区：
        ( {    MIN     },   {"2014-01-01"} )
        [ {"2014-01-01"},   {"2014-06-01"} )
        [ {"2014-06-01"},   {"2014-12-01"} )
        
        不在这些分区范围内的数据将视为非法数据被过滤
    
    4. 创建一个 mysql 表
        CREATE TABLE example_db.table_mysql
        (
        k1 DATE,
        k2 INT,
        k3 SMALLINT,
        k4 VARCHAR(2048),
        k5 DATETIME
        )
        ENGINE=mysql
        PROPERTIES
        (
        "host" = "127.0.0.1",
        "port" = "8239",
        "user" = "mysql_user",
        "password" = "mysql_passwd",
        "database" = "mysql_db_test",
        "table" = "mysql_table_test"
        )
        
    5. 创建一个数据文件存储在HDFS上的 broker 外部表, 数据使用 "|" 分割，"\n" 换行
        CREATE EXTERNAL TABLE example_db.table_broker (
        k1 DATE,
        k2 INT,
        k3 SMALLINT,
        k4 VARCHAR(2048),
        k5 DATETIME
        )
        ENGINE=broker
        PROPERTIES (
        "broker_name" = "hdfs",
        "path" = "hdfs://hdfs_host:hdfs_port/data1,hdfs://hdfs_host:hdfs_port/data2,hdfs://hdfs_host:hdfs_port/data3%2c4",
        "column_separator" = "|",
        "line_delimiter" = "\n"
        )
        BROKER PROPERTIES (
        "username" = "hdfs_user",
        "password" = "hdfs_password"
        )

    6. 创建一张含有HLL列的表
        CREATE TABLE example_db.example_table
        (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 HLL HLL_UNION,
        v2 HLL HLL_UNION
        )
        ENGINE=olap
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY RANDOM BUCKETS 32
        PROPERTIES ("storage_type"="column");

## keyword
    CREATE,TABLE
        
# DROP TABLE
## description
    该语句用于删除 table 。
    语法：
        DROP TABLE [IF EXISTS] [db_name.]table_name;
        
    说明：
        执行 DROP TABLE 一段时间内，可以通过 RECOVER 语句恢复被删除的 table。详见 RECOVER 语句

## example
    1. 删除一个 table
        DROP TABLE my_table;
        
    2. 如果存在，删除指定 database 的 table
        DROP TABLE IF EXISTS example_db.my_table;

## keyword
    DROP,TABLE
    
# ALTER TABLE
## description
    该语句用于对已有的 table 进行修改。如果没有指定 rollup index，默认操作 base index。
    该语句分为三种操作类型： schema change 、rollup 、partition
    这三种操作类型不能同时出现在一条 ALTER TABLE 语句中。
    其中 schema change 和 rollup 是异步操作，任务提交成功则返回。之后可使用 SHOW ALTER 命令查看进度。
    partition 是同步操作，命令返回表示执行完毕。

    语法：
        ALTER TABLE [database.]table
        alter_clause1[, alter_clause2, ...];

    alter_clause 分为 partition 、rollup、schema change 和 rename 四种。

    partition 支持如下几种修改方式
    1. 增加分区
        语法：
            ADD PARTITION [IF NOT EXISTS] partition_name VALUES LESS THAN [MAXVALUE|("value1")] ["key"="value"]
            [DISTRIBUTED BY RANDOM [BUCKETS num] | DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
        注意：
            1) 分区为左闭右开区间，用户指定右边界，系统自动确定左边界
            2) 如果没有指定分桶方式，则自动使用建表使用的分桶方式
            3) 如指定分桶方式，只能修改分桶数，不可修改分桶方式或分桶列
            4) ["key"="value"] 部分可以设置分区的一些属性，具体说明见 CREATE TABLE

    2. 删除分区
        语法：
            DROP PARTITION [IF EXISTS] partition_name
        注意：
            1) 使用分区方式的表至少要保留一个分区。
            2) 执行 DROP PARTITION 一段时间内，可以通过 RECOVER 语句恢复被删除的 partition。详见 RECOVER 语句
            
    3. 修改分区属性
        语法：
            MODIFY PARTITION partition_name SET ("key" = "value", ...)
        说明：
            1) 当前支持修改分区的 storage_medium、storage_cooldown_time 和 replication_num 三个属性。
            2) 对于单分区表，partition_name 同表名。
        
    rollup 支持如下几种创建方式：
    1. 创建 rollup index
        语法：
            ADD ROLLUP rollup_name (column_name1, column_name2, ...)
            [FROM from_index_name]
            [PROPERTIES ("key"="value", ...)]
        注意：
            1) 如果没有指定 from_index_name，则默认从 base index 创建
            2) rollup 表中的列必须是 from_index 中已有的列
            3) 在 properties 中，可以指定存储格式。具体请参阅 CREATE TABLE
            
    2. 删除 rollup index
        语法：
            DROP ROLLUP rollup_name
            [PROPERTIES ("key"="value", ...)]
        注意：
            1) 不能删除 base index
            2) 执行 DROP ROLLUP 一段时间内，可以通过 RECOVER 语句恢复被删除的 rollup index。详见 RECOVER 语句
    
            
    schema change 支持如下几种修改方式：
    1. 向指定 index 的指定位置添加一列
        语法：
            ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
            [AFTER column_name|FIRST]
            [TO rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        注意：
            1) 聚合模型如果增加 value 列，需要指定 agg_type
            2) 非聚合模型如果增加key列，需要指定KEY关键字
            3) 不能在 rollup index 中增加 base index 中已经存在的列
                如有需要，可以重新创建一个 rollup index）
            
    2. 向指定 index 添加多列
        语法：
            ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
            [TO rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        注意：
            1) 聚合模型如果增加 value 列，需要指定agg_type
            2) 非聚合模型如果增加key列，需要指定KEY关键字
            3) 不能在 rollup index 中增加 base index 中已经存在的列
            （如有需要，可以重新创建一个 rollup index）
    
    3. 从指定 index 中删除一列
        语法：
            DROP COLUMN column_name
            [FROM rollup_index_name]
        注意：
            1) 不能删除分区列
            2) 如果是从 base index 中删除列，则如果 rollup index 中包含该列，也会被删除
        
    4. 修改指定 index 的列类型以及列位置
        语法：
            MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
            [AFTER column_name|FIRST]
            [FROM rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        注意：
            1) 聚合模型如果修改 value 列，需要指定 agg_type
            2) 非聚合类型如果修改key列，需要指定KEY关键字
            3) 只能修改列的类型，列的其他属性维持原样（即其他属性需在语句中按照原属性显式的写出，参见 example 8）
            4) 分区列不能做任何修改
            5) 目前支持以下类型的转换（精度损失由用户保证）
                TINYINT/SMALLINT/INT/BIGINT 转换成 TINYINT/SMALLINT/INT/BIGINT/DOUBLE。
                LARGEINT 转换成 DOUBLE
                VARCHAR 支持修改最大长度
            6) 不支持从NULL转为NOT NULL
                
    5. 对指定 index 的列进行重新排序
        语法：
            ORDER BY (column_name1, column_name2, ...)
            [FROM rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        注意：
            1) index 中的所有列都要写出来
            2) value 列在 key 列之后
            
    6. 修改table的属性，目前仅支持修改bloom filter列
        语法：
            PROPERTIES ("key"="value")
        注意：
            也可以合并到上面的schema change操作中来修改，见下面例子
     

    rename 支持对以下名称进行修改：
    1. 修改表名
        语法：
            RENAME new_table_name;
            
    2. 修改 rollup index 名称
        语法：
            RENAME ROLLUP old_rollup_name new_rollup_name;
            
    3. 修改 partition 名称
        语法：
            RENAME PARTITION old_partition_name new_partition_name;
      
## example
    [partition]
    1. 增加分区, 现有分区 [MIN, 2013-01-01)，增加分区 [2013-01-01, 2014-01-01)，使用默认分桶方式
        ALTER TABLE example_db.my_table
        ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");

    2. 增加分区，使用新的分桶方式
        ALTER TABLE example_db.my_table
        ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
        DISTRIBUTED BY RANDOM BUCKETS 20;

    3. 删除分区
        ALTER TABLE example_db.my_table
        DROP PARTITION p1;

    [rollup]
    1. 创建 index: example_rollup_index，基于 base index（k1,k2,k3,v1,v2）。列式存储。
        ALTER TABLE example_db.my_table
        ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
        PROPERTIES("storage_type"="column");
        
    2. 创建 index: example_rollup_index2，基于 example_rollup_index（k1,k3,v1,v2）
        ALTER TABLE example_db.my_table
        ADD ROLLUP example_rollup_index2 (k1, v1)
        FROM example_rollup_index;
    
    3. 删除 index: example_rollup_index2
        ALTER TABLE example_db.my_table
        DROP ROLLUP example_rollup_index2;

    [schema change]
    1. 向 example_rollup_index 的 col1 后添加一个key列 new_col(非聚合模型)
        ALTER TABLE example_db.my_table
        ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
        TO example_rollup_index;

    2. 向example_rollup_index的col1后添加一个value列new_col(非聚合模型)
     	  ALTER TABLE example_db.my_table   
     	  ADD COLUMN new_col INT DEFAULT "0" AFTER col1    
     	  TO example_rollup_index;

    3. 向example_rollup_index的col1后添加一个key列new_col(聚合模型)
     	  ALTER TABLE example_db.my_table   
     	  ADD COLUMN new_col INT DEFAULT "0" AFTER col1    
     	  TO example_rollup_index;

    4. 向example_rollup_index的col1后添加一个value列new_col SUM聚合类型(聚合模型)
     	  ALTER TABLE example_db.my_table   
     	  ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1    
     	  TO example_rollup_index;
    
    5. 向 example_rollup_index 添加多列(聚合模型)
        ALTER TABLE example_db.my_table
        ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
        TO example_rollup_index;
    
    6. 从 example_rollup_index 删除一列
        ALTER TABLE example_db.my_table
        DROP COLUMN col2
        FROM example_rollup_index;
        
    7. 修改 base index 的 col1 列的类型为 BIGINT，并移动到 col2 列后面
        ALTER TABLE example_db.my_table
        MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;

    8. 修改 base index 的 val1 列最大长度。原 val1 为 (val1 VARCHAR(32) REPLACE DEFAULT "abc")
        ALTER TABLE example_db.my_table
        MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    
    9. 重新排序 example_rollup_index 中的列（设原列顺序为：k1,k2,k3,v1,v2）
        ALTER TABLE example_db.my_table
        ORDER BY (k3,k1,k2,v2,v1)
        FROM example_rollup_index;
        
    10. 同时执行两种操作
        ALTER TABLE example_db.my_table
        ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
        ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;

    11. 修改表的 bloom filter 列
        ALTER TABLE example_db.my_table
        PROPERTIES ("bloom_filter_columns"="k1,k2,k3");

       也可以合并到上面的 schema change 操作中
        ALTER TABLE example_db.my_table
        DROP COLUMN col2
        PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
        
    [rename]
    1. 将名为 table1 的表修改为 table2
        ALTER TABLE table1 RENAME table2;
        
    2. 将表 example_table 中名为 rollup1 的 rollup index 修改为 rollup2
        ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
        
    3. 将表 example_table 中名为 p1 的 partition 修改为 p2
        ALTER TABLE example_table RENAME PARTITION p1 p2;
        
## keyword
    ALTER,TABLE,ROLLUP,COLUMN,PARTITION,RENAME
    
# CANCEL ALTER
## description
    该语句用于撤销一个 ALTER 操作。
    1. 撤销 ALTER TABLE COLUMN 操作
    语法：
        CANCEL ALTER TABLE COLUMN
        FROM db_name.table_name
    
    2. 撤销 ALTER TABLE ROLLUP 操作
    语法：
        CANCEL ALTER TABLE ROLLUP
        FROM db_name.table_name
        
    2. 撤销 ALTER CLUSTER 操作
    语法：
        （待实现...）

        
## example
    [CANCEL ALTER TABLE COLUMN]
    1. 撤销针对 my_table 的 ALTER COLUMN 操作。
        CANCEL ALTER TABLE COLUMN
        FROM example_db.my_table;
    
    [CANCEL ALTER TABLE ROLLUP]
    1. 撤销 my_table 下的 ADD ROLLUP 操作。
        CANCEL ALTER TABLE ROLLUP
        FROM example_db.my_table;

## keyword
    CANCEL,ALTER,TABLE,COLUMN,ROLLUP
    
# CREATE VIEW
## description
    该语句用于创建一个逻辑视图
    语法：
        CREATE VIEW [IF NOT EXISTS]
        [db_name.]view_name (column1[, column2, ...])
        AS query_stmt
        
    说明：
        1. 视图为逻辑视图，没有物理存储。所有在视图上的查询相当于在视图对应的子查询上进行。
        2. query_stmt 为任意支持的 SQL
        
## example
    1. 在 example_db 上创建视图 example_view
        CREATE VIEW example_db.example_view (k1, k2, k3, v1)
        AS
        SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
        WHERE k1 = 20160112 GROUP BY k1,k2,k3;
    
## keyword
    CREATE,VIEW
    
# DROP VIEW
## description
    该语句用于删除一个逻辑视图 VIEW
    语法：
        DROP VIEW [IF EXISTS]
        [db_name.]view_name;
        
## example
    1. 如果存在，删除 example_db 上的视图 example_view
        DROP VIEW IF EXISTS example_db.example_view;
    
## keyword
    DROP,VIEW
    
# RECOVER
## description
    该语句用于恢复之前删除的 database、table 或者 partition
    语法：
        1) 恢复 database
            RECOVER DATABASE db_name;
        2) 恢复 table
            RECOVER TABLE [db_name.]table_name;
        3) 恢复 partition
            RECOVER PARTITION partition_name FROM [db_name.]table_name;
    
    说明：
        1. 该操作仅能恢复之前一段时间内删除的元信息。默认为 3600 秒。
        2. 如果删除元信息后新建立了同名同类型的元信息，则之前删除的元信息不能被恢复

## example
    1. 恢复名为 example_db 的 database
        RECOVER DATABASE example_db;
        
    2. 恢复名为 example_tbl 的 table
        RECOVER TABLE example_db.example_tbl;
        
    3. 恢复表 example_tbl 中名为 p1 的 partition
        RECOVER PARTITION p1 FROM example_tbl;
        
## keyword
    RECOVER
    
# ALTER DATABASE
## description
    该语句用于设置指定数据库的属性。（仅管理员使用）
    语法：
        1) 设置数据库数据量配额，单位为字节
            ALTER DATABASE db_name SET DATA QUOTA quota;
            
        2) 重命名数据库
            ALTER DATABASE db_name RENAME new_db_name;
            
    说明：
        重命名数据库后，如需要，请使用 REVOKE 和 GRANT 命令修改相应的用户权限。 

## example
    1. 设置指定数据库数据量配额为 1GB
        ALTER DATABASE example_db SET DATA QUOTA 1073741824;
        
    2. 将数据库额 example_db 重命名为 example_db2
        ALTER DATABASE example_db RENAME example_db2;

## keyword
    ALTER,DATABASE,RENAME
    
# BACKUP
## description
    该语句用于备份指定数据库下的数据。该命令为异步操作。提交成功后，需通过 SHOW BACKUP 命令查看进度。
    语法：
        BACKUP LABEL [db_name.label] [backup_objs] INTO "remote_path"
        PROPERTIES ("key"="value", ...)
        
        backup_objs: 需要备份的表名或分区名
        语法：
            (table_name[.partition_name], ...)
            
    说明：
        1. 同一数据库下只能有一个正在执行的 BACKUP 任务。
        2. 如果 backup_objs 中不写 partition_name，则默认备份该 table 下所有分区。
           如果完全不写 backup_objs，则默认备份整个 database。
        3. PROPERTIES 需要填写访问远端备份系统所需信息。
        4. 统一数据库下，BACKUP 任务的 label 不能重复。

## example
    1. 备份 example_db 下的所有数据，备份到 Hdfs 路径：/user/cmy/backup/ 下
        BACKUP LABEL example_db.backup_label1
        INTO "/dir/backup/" 
        PROPERTIES(
        "server_type" = "hadoop",
        "host" = "hdfs://host",
        "port" = "port",
        "user" = "user",
        "password" = "passwd",
        "opt_properties" = ""
        );
        
    2. 备份 example_db 下的表 example_tbl，备份到 Hdfs 路径：/user/cmy/backup/ 下
        BACKUP LABEL example_db.backup_label1
        (example_tbl)
        INTO "/dir/backup/" 
        PROPERTIES (
        "server_type" = "hadoop",
        "host" = "hdfs://host",
        "port" = "port",
        "user" = "user",
        "password" = "passwd",
        "opt_properties" = ""
        );

## keyword
    BACKUP
   
# RESTORE
## description
    1. RESTORE
    该语句用于将之前通过 BACKUP 命令备份的数据，恢复到指定数据库下。该命令为异步操作。提交成功后，需通过 SHOW RESTORE 命令查看进度。
    语法：
        RESTORE LABEL [db_name.label] [restore_objs] FROM "remote_path"
        PROPERTIES ("key"="value", ...)
        
        restore_objs: 需要恢复的表名或分区名
        语法：
            (table_name[.partition_name] [AS new_table_name[.partition_name]], ...)
            
    说明：
        1. 同一数据库下只能有一个正在执行的 RESTORE 任务。
        2. 需恢复的 database 已经创建，并且在备份路径中存在
        3. 如果 restore_objs 中不写 partition_name，则默认恢复该 table 下所有分区。
           如果完全不写 restore_objs，则默认恢复所有备份过的数据。支持重命名需要恢复的表名
        4. remote_path 需指定到之前 BACKUP 任务中，remote_path/backup_label/ 下。
        5. PROPERTIES 需要填写访问远端备份系统所需信息。
        6. 同一数据库下，RESTORE 任务的 label 不能重复。

## example
    1. 恢复 Hdfs 路径：/user/cmy/backup/backup_label1 下所有备份数据，恢复到 example_db 中
        RESTORE LABEL example_db.restore_label1
        FROM "/dir/backup/backup_label1" 
        PROPERTIES(
        "server_type" = "hadoop",
        "host" = "hdfs://host",
        "port" = "port",
        "user" = "user",
        "password" = "passwd",
        "opt_properties" = ""
        );
        
    2. 恢复 Hdfs 路径：/user/cmy/backup/backup_label1 下表 example_tbl 的数据。
        RESTORE LABEL example_db.restore_label1
        (example_tbl)
        FROM "/dir/backup/backup_label1" 
        PROPERTIES (
        "server_type" = "hadoop",
        "host" = "hdfs://host",
        "port" = "port",
        "user" = "user",
        "password" = "passwd",
        "opt_properties" = ""
        );

## keyword
    RESTORE
    
# CANCEL BACKUP
## description
    该语句用于取消一个正在进行的 BACKUP 任务。
    语法：
        CANCEL BACKUP FROM db_name;

## example
    1. 取消 example_db 下的 BACKUP 任务。
        CANCEL BACKUP FROM example_db;

## keyword
    CANCEL, BACKUP
    
# CANCEL RESTORE
## description
    该语句用于取消一个正在进行的 RESTORE 任务。
    语法：
        CANCEL RESTORE FROM db_name;

## example
    1. 取消 example_db 下的 RESTORE 任务。
        CANCEL RESTORE FROM example_db;

## keyword
    CANCEL, RESTORE
    
# HLL
## description
    HLL是基于HyperLogLog算法的工程实现，用于保存HyperLogLog计算过程的中间结果，它只能作为表的value列类型
    通过聚合来不断的减少数据量，以此来实现加快查询的目的，基于它到的是一个估算结果，误差大概在1%左右
    hll列是通过其它列或者导入数据里面的数据生成的，导入的时候通过hll_hash函数来指定数据中哪一列用于生成hll列
    它常用于替代count distinct，通过结合rollup在业务上用于快速计算uv等
	
	  相关函数:
	
	  HLL_UNION_AGG(hll)
	  此函数为聚合函数，用于计算满足条件的所有数据的基数估算。
	
	  HLL_CARDINALITY(hll)
	  此函数用于计算单条hll列的基数估算
	
	  HLL_HASH(column_name)
	  生成HLL列类型，用于insert或导入的时候，导入的使用见相关说明
	
## example
    1. 首先创建一张含有hll列的表
        create table test(
        time date, 
        id int, 
        name char(10), 
        province char(10),
        os char(1),
        set1 hll hll_union, 
        set2 hll hll_union) 
        distributed by hash(id) buckets 32;
		
    2. 导入数据，导入的方式见相关help curl

      a. 使用表中的列生成hll列
        curl --location-trusted -uname:password -T data http://host/api/test_db/test/_load?label=load_1\&hll=set1,id:set2,name

      b. 使用数据中的某一列生成hll列
        curl --location-trusted -uname:password -T data http://host/api/test_db/test/_load?label=load_1\&hll=set1,cuid:set2,os
            \&columns=time,id,name,province,sex,cuid,os

    3. 聚合数据，常用方式3种：（如果不聚合直接对base表查询，速度可能跟直接使用ndv速度差不多）

      a. 创建一个rollup，让hll列产生聚合，
        alter table test add rollup test_rollup(date, set1);
		
      b. 创建另外一张专门计算uv的表，然后insert数据）
	
        create table test_uv(
        time date,
        uv_set hll hll_union)
        distributed by hash(id) buckets 32;

        insert into test_uv select date, set1 from test;
		
      c. 创建另外一张专门计算uv的表，然后insert并通过hll_hash根据test其它非hll列生成hll列
      
        create table test_uv(
        time date,
        id_set hll hll_union)
        distributed by hash(id) buckets 32;
		
        insert into test_uv select date, hll_hash(id) from test;
			
    4. 查询，hll列不允许直接查询它的原始值，可以通过配套的函数进行查询
	
      a. 求总uv
        select HLL_UNION_AGG(uv_set) from test_uv;
			
      b. 求每一天的uv
        select HLL_CARDINALITY(uv_set) from test_uv;

## keyword
	HLL
