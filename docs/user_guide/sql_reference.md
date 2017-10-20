# SQL 手册

Palo提供在线和离线的SQL手册。

在线的SQL手册通过连接Palo后使用help命令查看。如查看如何创建数据库。

	mysql> help create database;
	Name: 'CREATE DATABASE'
	Description:
    	该语句用于新建数据库（database）
    	语法：
        	CREATE DATABASE [IF NOT EXISTS] db_name;
	Examples:
    	1. 新建数据库 db_test
        	CREATE DATABASE db_test;

本文档是离线的SQL手册，详细介绍SQL的语法。

# 数据类型
**TINYINT数据类型**

长度: 长度为1个字节的有符号整型。

范围: [-128, 127]

转换: Palo可以自动将该类型转换成更大的整型或者浮点类型。使用CAST()函数可以将其转换成CHAR。

举例:

	 mysql> select cast(100 as char);
	 +------------------------+
	 | CAST(100 AS CHARACTER) |
	 +------------------------+
	 | 100                    |
	 +------------------------+
	 1 row in set (0.00 sec)


**SMALLINT数据类型**

长度: 长度为2个字节的有符号整型。

范围: [-32768, 32767]

转换: Palo可以自动将该类型转换成更大的整型或者浮点类型。使用CAST()函数可以将其转换成TINYINT，CHAR。

举例:

	 mysql> select cast(10000 as char);
	 +--------------------------+
	 | CAST(10000 AS CHARACTER) |
	 +--------------------------+
	 | 10000                    |
	 +--------------------------+
	 1 row in set (0.01 sec)

	 mysql> select cast(10000 as tinyint);
	 +------------------------+
	 | CAST(10000 AS TINYINT) |
	 +------------------------+
	 |                     16 |
	 +------------------------+
	 1 row in set (0.00 sec)


**INT数据类型**

长度: 长度为4个字节的有符号整型。

范围: [-2147483648, 2147483647]

转换: Palo可以自动将该类型转换成更大的整型或者浮点类型。使用CAST()函数可以将其转换成TINYINT，SMALLINT，CHAR

举例:

	 mysql> select cast(111111111  as char);
	 +------------------------------+
	 | CAST(111111111 AS CHARACTER) |
	 +------------------------------+
	 | 111111111                    |
	 +------------------------------+
	 1 row in set (0.01 sec)


**BIGINT数据类型**

长度: 长度为8个字节的有符号整型。

范围: [-9223372036854775808, 9223372036854775807]

转换: Palo可以自动将该类型转换成更大的整型或者浮点类型。使用CAST()函数可以将其转换成TINYINT，SMALLINT，INT，CHAR

举例:

	 mysql> select cast(9223372036854775807 as char);
	 +----------------------------------------+
	 | CAST(9223372036854775807 AS CHARACTER) |
	 +----------------------------------------+
	 | 9223372036854775807                    |
	 +----------------------------------------+
	 1 row in set (0.01 sec)


**LARGEINT数据类型**

长度: 长度为16个字节的有符号整型。

范围: [-2^127, 2^127-1]

转换: Palo可以自动将该类型转换成浮点类型。使用CAST()函数可以将其转换成TINYINT，SMALLINT，INT，BIGINT，CHAR

举例:

	 mysql> select cast(922337203685477582342342 as double);
	 +------------------------------------------+
	 | CAST(922337203685477582342342 AS DOUBLE) |
	 +------------------------------------------+
	 |                     9.223372036854776e23 |
	 +------------------------------------------+
	 1 row in set (0.05 sec)


**FLOAT数据类型**

长度: 长度为4字节的浮点类型。

范围: -3.40E+38 ~ +3.40E+38。

转换: Palo会自动将FLOAT类型转换成DOUBLE类型。用户可以使用CAST()将其转换成TINYINT, SMALLINT, INT, BIGINT, STRING, TIMESTAMP。

**DOUBLE数据类型**

长度: 长度为8字节的浮点类型。

范围: -1.79E+308 ~ +1.79E+308。

转换: Palo不会自动将DOUBLE类型转换成其他类型。用户可以使用CAST()将其转换成TINYINT, SMALLINT, INT, BIGINT, STRING, TIMESTAMP。用户可以使用指数符号来描述DOUBLE 类型，或通过STRING转换获得。

**DECIMAL数据类型**

DECIMAL[M, D]

保证精度的小数类型。M代表一共有多少个有效数字，D代表小数点后最多有多少数字。M的范围是[1,27]，D的范围是[1,9]，另外，M必须要大于等于D的取值。默认取值为decimal[10,0]。

precision: 1 ~ 27

scale: 0 ~ 9

举例：

1.默认取值是decimal(10, 0)

	 mysql> CREATE TABLE testTable1 (k1 bigint, k2 varchar(100), v decimal SUM) DISTRIBUTED BY RANDOM BUCKETS 8;
	 Query OK, 0 rows affected (0.09 sec)

	 mysql> describe testTable1;
	 +-------+----------------+------+-------+---------+-------+
	 | Field | Type           | Null | Key   | Default | Extra |
	 +-------+----------------+------+-------+---------+-------+
	 | k1    | bigint(20)     | Yes  | true  | N/A     |       |
	 | k2    | varchar(100)   | Yes  | true  | N/A     |       |
	 | v     | decimal(10, 0) | Yes  | false | N/A     | SUM   |
	 +-------+----------------+------+-------+---------+-------+
	 3 rows in set (0.01 sec)


2.显式指定decimal的取值范围

	 CREATE TABLE testTable2 (k1 bigint, k2 varchar(100), v decimal(8,5) SUM) DISTRIBUTED BY RANDOM BUCKETS 8;
	 Query OK, 0 rows affected (0.11 sec)

	 mysql> describe testTable2;
	 +-------+---------------+------+-------+---------+-------+
	 | Field | Type          | Null | Key   | Default | Extra |
	 +-------+---------------+------+-------+---------+-------+
	 | k1    | bigint(20)    | Yes  | true  | N/A     |       |
	 | k2    | varchar(100)  | Yes  | true  | N/A     |       |
	 | v     | decimal(8, 5) | Yes  | false | N/A     | SUM   |
	 +-------+---------------+------+-------+---------+-------+
	 3 rows in set (0.00 sec)


**DATE数据类型**

范围: ['1000-01-01', '9999-12-31']。默认的打印形式是’YYYY-MM-DD’。

**DATETIME数据类型**

范围: ['1000-01-01 00:00:00', '9999-12-31 00:00:00']。默认的打印形式是’YYYY-MM-DD HH:MM:SS’。

**CHAR数据类型**

范围: char[(length)]，定长字符串，长度length范围1~255，默认为1。

转换：用户可以通过CAST函数将CHAR类型转换成TINYINT,，SMALLINT，INT，BIGINT，LARGEINT，DOUBLE，DATE或者DATETIME类型。

示例：

	 mysql> select cast(1234 as bigint);
	 +----------------------+
	 | CAST(1234 AS BIGINT) |
	 +----------------------+
	 |                 1234 |
	 +----------------------+
	 1 row in set (0.01 sec)


**VARCHAR数据类型**

范围: char(length)，变长字符串，长度length范围1~65535。

转换：用户可以通过CAST函数将CHAR类型转换成TINYINT,，SMALLINT，INT，BIGINT，LARGEINT，DOUBLE，DATE或者DATETIME类型。

示例：

	 mysql> select cast('2011-01-01' as date); 
	 +----------------------------+
	 | CAST('2011-01-01' AS DATE) |
	 +----------------------------+
	 | 2011-01-01                 |
	 +----------------------------+
	 1 row in set (0.01 sec)

	 mysql> select cast('2011-01-01' as datetime);
	 +--------------------------------+
	 | CAST('2011-01-01' AS DATETIME) |
	 +--------------------------------+
	 | 2011-01-01 00:00:00            |
	 +--------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select cast(3423 as bigint);
	 +----------------------+
	 | CAST(3423 AS BIGINT) |
	 +----------------------+
	 |                 3423 |
	 +----------------------+
	 1 row in set (0.01 sec)


**HLL数据类型**

范围：char(length),长度length范围1~16385。用户不需要指定长度和默认值、长度根据数据的聚合程度系统内控制，并且HLL列只能通过配套的hll_union_agg、hll_cardinality、hll_hash进行查询或使用


# 字面常量

Palo中的每种数据类型都会对应着一种该类型的Literal。用户可以在SQL语句中指定Literal，比如在select的list中，where从句中以及函数的参数中。

**数字字面常量**

整型类型（TINYINT, SMALLINT, INT, 和BIGINT）的字面常量是一系列数字，这些数字前可以加些0。

浮点类型（DOUBLE）的字面常量是一系列数字，并且可选加上十进制的点（.字符）。

整型类型在需要时可以根据上下文提升到浮点类型。

在描述字面常量时，可以使用指数符号（e 字符）。例如1e+6 表示10 的6 次方（1 百万）。包含指数符号的字面常量会被识别成浮点类型。

**字符串字面常量**

字符串字面常量被单引号或者双引号括起来。字符串字面常量也包含其他形式：字符串字面常量为包含单引号的字符串，外面被双引号括起来；字符串字面常量为包含双引号的字符串，外面被单引号括起来。

为了描述字符串字面常量的特殊字符，需要在特殊字符前加入转义字符（\\字符）。

-   \t 表示tab键

-   \n 表示换行符

-   \r 表示回车符

-   \b 表示回退符

-   \0 表示ASCII码的空字符（和SQL语言的NULL不同）

-   \Z 表示dos 的文本结束符

-   \%和_用来转义传给LIKE操作符的字符串中的通配符

-   \\\\防止反斜线符号被解释成转义字符

-   如果字符串字面常量被单引号或双引号括起来，则反斜线符号可以用来转义该字符串字面常量中出现的单引号或双引号。

-   如果\后面出现的字符不是上面列举的转移字符，则该字符保持不变，不会被转义。

**日期字面常量**

Palo会自动将CHAR类型字面常量转成时间类型字面常量。Palo接受的时间类型字面常量的输入格式为成YYYY-MM-DD HH:MM:SS.ssssss，或者只包含日期。其中上述格式中小数点后面的数字(毫秒数)可带可不带。例如，用户可以指定时间类型为‘2010-01-01’，或者'2010-01-01 10:10:10'。

# SQL操作符

SQL操作符是一系列用于比较的函数，这些操作符广泛的用于select 语句的where从句中。

**算数操作符**

算术操作符通常出现在包含左操作数，操作符，（大部分情况下）右操作数组成的表达式中。

-   +和-：可以作为单元或2元操作符。当其作为单元操作符时，如+1, -2.5 或者-col_name， 表达的意思是该值乘以+1或者-1。因此单元操作符+返回的是未发生变化的值，单元操作符-改变了该值的符号位。用户可以将两个单元操作符叠加起来，比如++5(返回的是正值)，-+2 或者+-2（这两种情况返回的是负值），但是用户不能使用连续的两个-号，因为--被解释为后面的语句是注释（用户在是可以使用两个-号的，此时需要在两个-号之间加上空格或圆括号，如-(-2)或者- -2，这两个数实际表达的结果是+2）。+或者-作为2元操作符时，例如2+2，3+1.5 或者col1 + col2，表达的含义是左值相应的加或者减去右值。左值和右值必须都是数字类型。

-   *和/： 分别代表着乘法和除法。两侧的操作数必须都是数据类型。当两个数相乘时，类型较小的操作数在需要的情况下类型可能会提升（比如SMALLINT提升到INT或者BIGINT 等），表达式的结果被提升到下一个较大的类型，比如TINYINT 乘以INT 产生的结果的类型会是BIGINT）。当两个数相乘时，为了避免精度丢失，操作数和表达式结果都会被解释成DOUBLE 类型。如果用户想把表达式结果转换成其他类型，需要用CAST 函数转换。

-   %：取模操作符。返回左操作数除以右操作数的余数。左操作数和右操作数都必须是整型。

-   &, |和^：按位操作符返回对两个操作数进行按位与，按位或，按位异或操作的结果。两个操作数都要求是一种整型类型。如果按位操作符的两个操作数的类型不一致，则类型小的操作数会被提升到类型较大的操作数，然后再做相应的按位操作。在1个表达式中可以出现多个算术操作符，用户可以用小括号将相应的算术表达式括起来。算术操作符通常没有对应的数学函数来表达和算术操作符相同的功能。比如我们没有MOD()函数来表示%操作符的功能。反过来，数学函数也没有对应的算术操作符。比如幂函数POW()并没有相应的 \*\*求幂操作符。用户可以通过数学函数章节了解我们支持哪些算术函数。

**Between操作符**

在where从句中，表达式可能同时和上界和下界比较。如果表达式大于等于下界，同时小于等于上界的话，比较的结果是true。

语法：

    expression BETWEEN lower_bound AND upper_bound

数据类型：通常expression的计算结果都是数字类型，该操作符也支持其他数据类型。如果必须要确保下界和上界都是可比较的字符，可以使用cast()函数。

使用说明：如果操作数是string类型时使用时，应该小心些。起始部分为上界的长字符串将不会匹配上界，该字符串比上界要大。between 'A' and 'M'不会匹配‘MJ’。如果需要确保表达式能够正常work，可以使用一些函数，如upper(), lower(), substr(), trim()。

举例:

	mysql> select c1 from t1 where month between 1 and 6;


**比较操作符**

比较操作符用来判断列和列是否相等或者对列进行排序。=, !=, &lt;&gt;, &lt;, &lt;=, &gt;, &gt;=可以适用所有数据类型。其中&lt;&gt;符号是不等于的意思,和!=的功能是一样的。IN和BETWEEN操作符提供更简短的表达来描述相等、小于、大小等关系的比较。

**In操作符**

in操作符会和VALUE集合进行比较，如果可以匹配该集合中任何一元素，则返回TRUE。参数和VALUE集合必须是可比较的。所有使用in操作符的表达式都可以写成用OR连接的等值比较，但是IN的语法更简单些，更精准，更容易让Palo进行优化。

举例：

	mysql> select * from small_table where tiny_column in (1,2);

**Like操作符**

该操作符用于和字符串进行比较。_用来匹配单个字符，%用来匹配多个字符。参数必须要匹配完整的字符串。通常，把%放在字符串的尾部更加符合实际用法。

举例：

	 mysql> select varchar_column from small_table where varchar_column like 'm%';
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | milan          |
	 +----------------+
	 1 row in set (0.02 sec)

	 mysql> select varchar_column from small_table where varchar_column like 'm____';
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | milan          |
	 +----------------+
	 1 row in set (0.01 sec)


**逻辑操作符**

逻辑操作符返回一个BOOL值，逻辑操作符包括单元操作符和多元操作符，每个操作符处理的参数都是返回值为BOOL值的表达式。支持的操作符有：

-   AND: 2元操作符，如果左侧和右侧的参数的计算结果都是TRUE，则AND操作符返回TRUE。

-   OR: 2元操作符，如果左侧和右侧的参数的计算结果有一个为TRUE，则OR操作符返回TRUE。如果两个参数都是FALSE，则OR操作符返回FALSE。

-   NOT:单元操作符，反转表达式的结果。如果参数为TRUE，则该操作符返回FALSE；如果参数为FALSE，则该操作符返回TRUE。

举例:

	 mysql> select true and true;
	 +-------------------+
	 | (TRUE) AND (TRUE) |
	 +-------------------+
	 |                 1 |
	 +-------------------+
	 1 row in set (0.00 sec)

	 mysql> select true and false;
	 +--------------------+
	 | (TRUE) AND (FALSE) |
	 +--------------------+
	 |                  0 |
	 +--------------------+
	 1 row in set (0.01 sec)

	 mysql> select true or false;
	 +-------------------+
	 | (TRUE) OR (FALSE) |
	 +-------------------+
	 |                 1 |
	 +-------------------+
	 1 row in set (0.01 sec)

	 mysql> select not true;
	 +----------+
	 | NOT TRUE |
	 +----------+
	 |        0 |
	 +----------+
	 1 row in set (0.01 sec)


**正则表达式操作符**

判断是否匹配正则表达式。使用POSIX标准的正则表达式，^用来匹配字符串的首部,$用来匹配字符串的尾部，.匹配任何一个单字符，\*匹配0个或多个选项，+匹配1个多个选项，?表示分贪婪表示等等。正则表达式需要匹配完整的值，并不是仅仅匹配字符串的部分内容。如果想匹配中间的部分，正则表达式的前面部分可以写成^.\* 或者 .\*。 ^和$通常是可以省略的。RLKIE操作符和REGEXP操作符是同义词。|操作符是个可选操作符，|两侧的正则表达式只需满足1侧条件即可，|操作符和两侧的正则表达式通常需要用()括起来。

举例：

	 mysql> select varchar_column from small_table where varchar_column regexp '(mi|MI).*';
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | milan          |
	 +----------------+
	 1 row in set (0.01 sec)

	 mysql> select varchar_column from small_table where varchar_column regexp 'm.*';
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | milan          |
	 +----------------+
	 1 row in set (0.01 sec)


# 别名

当你在查询中书写表，列，或者包含列的表达式的名字时，你可以同时给他们分配一个别名。当你需要使用表名，列名时，你可以使用别名来访问。别名通常相对原名来说更简短更好记。当需要新建一个别名时，只需在select list或者from list中的表、列、表达式名称后面加上AS alias从句即可。AS关键词是可选的，用户可以直接在原名后面指定别名。如果别名或者其他标志符和内部关键词同名时，需要在该名称加上\`\`符号。别名对大小写是敏感的。

举例:

	 mysql> select tiny_column as name, int_column as sex from big_table;
	 mysql> select sum(tiny_column) as total_count from big_table;
	 mysql> select one.tiny_column, two.int_column from small_table one, big_table two \
    	 -> where one.tiny_column = two.tiny_column;


# SQL语句

## DDL语句

### Create Database

该语句用于新建数据库(database)

语法：

    CREATE DATABASE [IF NOT EXISTS] db_name;

举例：

    新建数据库 db_test
    CREATE DATABASE db_test;

### Create Table

该语句用于创建表(table)

语法：

    CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
     (column_definition[, column_definition, ...]) 
     [ENGINE = [olap|mysql|broker]]
	 [key_desc]   
     [partition_desc]    
     [distribution_desc]    
     [PROPERTIES ("key"="value", ...)]
     [BROKER PROPERTIES ("key"="value", ...)];

**Column_definition**

语法：


    col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]

-	col_name：列名称
-	col_type：列类型，可以是INT，DOUBLE，DATE等，参考数据类型章节。
-	agg_type：聚合类型，目前支持SUM，MAX，MIN、REPLACE和HLL_UNION(仅用于HLL列，为HLL独有的聚合方式)5种。聚合类型是可选选项，如果不指定，说明该列是维度列（key列），否则是事实列（value列）。建表语句中，所有的key列必须在value列之前，一张表可以没有value列，这样的表就是维度表，但不能没有key列。该类型只对聚合模型(key_desc的type为AGGREGATE KEY)有用，其它模型不需要指定这个。
-	是否允许为NULL: 默认允许为NULL，导入时用\N来表示

说明：

在导入的时候，Palo会自动把相同key列对应的value列按照指定的聚合方法合并（针对聚合模型）。比如，Palo中有一张表包含三列：k1，k2和v，其中v是int类型的value列，聚合方法是SUM，k1和k2是key列。假如原本有数据如下

	 | k1  | k2  | v   |
	 |-----|-----|-----|
	 | 1   | 1   | 10  |
	 | 1   | 2   | 20  |
	 | 2   | 2   | 30  |

新导入的数据如下：

	 | k1  | k2  | v   |
	 |-----|-----|-----|
	 | 1   | 1   | 5   |
	 | 2   | 2   | 10  |
	 | 3   | 1   | 5   |

导入以后，Palo中的数据如下

	 | k1  | k2  | v   |
	 |-----|-----|-----|
	 | 1   | 1   | 15  |
	 | 1   | 2   | 20  |
	 | 2   | 2   | 40  |
	 | 3   | 1   | 5   |

可以看到，在k1和k2相同的时候，v列使用SUM聚合方法做了聚合。

**ENGINE类型**

说明：

ENGINE默认为olap，也就是由Palo提供存储支持，也可以选择mysql，broker。

mysql类型用来存储维表，由用户自己维护，方便修改。查询的时候，Palo可以自动实现mysql表和olap表的连接操作。使用mysql类型，需要提供以下properties信息
    
    PROPERTIES (    
     "host" = "mysql_server_host",    
     "port" = "mysql_server_port",   
     "user" = "your_user_name",  
     "password" = "your_password",   
     "database" = "database_name",   
     "table" = "table_name"   
     )

 “table”条目中的“table_name”是mysql中的真实表名。而CREATE TABLE语句中的table_name是该mysql表在Palo中的名字，二者可以不同。

broker类型表示表的访问需要通过指定的broker, 需要在 properties 提供以下信息

	PROPERTIES (
	 "broker_name" = "broker_name",
	 "paths" = "file_path1[,file_path2]",
	 "column_separator" = "value_separator",
	 "line_delimiter" = "value_delimiter"
	）

另外还可以提供Broker需要的Property信息，通过BROKER PROPERTIES来传递，例如HDFS需要传入

	BROKER PROPERTIES(
	 "username" = "name",
	 "password" = "password"
	)

这个根据不同的Broker类型，需要传入的内容也不相同

其中"paths" 中如果有多个文件，用逗号[,]分割。如果文件名中包含逗号，那么使用 %2c 来替代。如果文件名中包含 %，使用 %25 代替。现在文件内容格式支持CSV，支持GZ，BZ2，LZ4，LZO(LZOP) 压缩格式。

**key_desc**

语法：

	key_type(k1[,k2 ...])

说明：

数据按照指定的key列进行排序，且根据不同的key_type具有不同特性。

key_type支持一下类型：

-	AGGREGATE KEY:key列相同的记录，value列按照指定的聚合类型进行聚合，适合报表、多维分析等业务场景。

-	UNIQUE KEY：key列相同的记录，value列按导入顺序进行覆盖，适合按key列进行增删改查的点查询业务。

-	DUPLICATE KEY:key列相同的记录，同时存在于Palo中，适合存储明细数据或者数据无聚合特性的业务场景。


**partition_desc**

 语法：

     PARTITION BY RANGE (k1)   
     (    
     PARTITION partition_name VALUES LESS THAN MAXVALUE|("value1") [("key"="value")],    
     PARTITION partition_name VALUES LESS THAN MAXVALUE|("value2") [("key"="value")],    
     ...   
     )

 Partition使用指定的key列和指定的数据范围对数据进行分区，每个分区在物理上对应不同的数据块，便于快速过滤和按分区删除等操作。目前只支持按Range分区，只能有一个分区列，分区列必须是key列。注意，最后一个PARTITION从句之后没有逗号。

 说明：

-	分区名称仅支持字母开头，并且只能由字母、数字和下划线组成

-	目前只支持以下类型的列作为分区列，且只能指定一个分区列TINYINT, SAMLLINT, INT, BIGINT, LARGEINT, DATE, DATETIME

-	分区为左闭右开区间，首个分区的左边界做为最小值

-	如果指定了分区，无法确定分区范围的导入数据会被过滤掉

-	每个分区后面的key-value键值对可以设置该分区的一些属性，目前支持如下属性：

	-	storage_medium：用于指定该分区的初始存储介质，可选择SSD或HDD。默认为HDD。单节点SSD容量为50G，可以根据性能需求和数据量选择存储介质。

	-	storage_cooldown_time：当设置存储介质为SSD时，指定该分区在SSD上的存储到期时间。默认存放7天。格式为："yyyy-MM-dd HH:mm:ss"。到期后数据会自动迁移到HDD上。

	-	replication_num:指定分区的副本数。默认为3

**distribution_desc**

 distribution用来指定如何分桶，可以选择Random分桶和Hash分桶两种分桶方式。

 Random分桶语法：

     DISTRIBUTED BY RANDOM [BUCKETS num]

 Hash分桶语法：

     DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]

 说明：

 -	Random使用所有key列进行哈希分桶，默认分区数为10。Hash使用指定的key列进行分桶，默认分区数为10。如果ENGINE类型为olap，必须指定分桶方式；如果是mysql则无须指定。
 -	不建议使用Random分桶，建议使用Hash分桶。

**properties**

 如果ENGINE类型为olap，则可以在properties中指定行存或列存

 如果ENGINE类型为olap，且没有指定partition信息，可以在properties设置存储介质、存储到期时间和副本数等属性。如果指定了partition信息，需要为每个partition分别指定属性值，参考partition_desc

     PROPERTIES (    
     "storage_medium" = "[SSD|HDD]",    
     ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],    
     ["replication_num" = "3"]    
     )

 如果ENGINE类型为 olap, 并且 storage_type 为 column, 可以指定某列使用 bloom filter 索引。bloom filter 索引仅适用于查询条件为 in 和 equal 的情况，该列的值越分散效果越好。目前只支持以下情况的列:除了 TINYINT FLOAT DOUBLE 类型以外的 key 列及聚合方法为 REPLACE 的 value 列

	 PROPERTIES (
	 "bloom_filter_columns"="k1,k2,k3"
	 ）

**关于建表的补充说明**

-	Partition和Distribution的说明：

	-	Palo支持复合分区，第一级称为Partition，对应建表语句中的partition_desc从句；第二级称为Distribution，对应建表语句中的distribution_desc从句。Partition是可选的，如果建表时没有指定Partition，系统会自动创建唯一的一个Partition。Distribution必须显式指定。在以下场景中推荐创建Partition：

	-	历史数据删除需求：如有删除历史数据的需求（比如仅保留最近N天的数据）。使用复合分区，可以通过删除历史分区来达到目的。

	-	解决数据倾斜问题：每个分区可以单独指定分桶数量。如按天分区，当每天的数据量差异很大时，可以通过指定分区的分桶数，合理划分不同分区的数据。

	-	如有按时间维度进行数据划分、导入、查询、删除、历史数据回溯等业务需求，推荐使用复合分区功能。


-   合理的表模式：

Palo中使用类似前缀索引的结构来提高查询性能。数据在Palo内部是按照key列排序的，并且组织为一个个Data Block。每个Data Block的第一行的前几列会被用作这个Data Block的索引，在数据导入时创建。该索引可以帮助Palo快速过滤一些Data Block。考虑到索引大小等因素，Palo最多使用一行的前36个字节作为索引，遇到VARCHAR类型则会中断，并且VARCHAR类型最多只使用字符串的前20个字节。下面举例说明。

表1的schema：

![](http://doc.bce.baidu.com/bce-documentation/PALO/image3-17.png?responseContentDisposition=attachment)

前三列的长度和为（4+8+24=）36，正好36字节，所以前三列被用作前缀索引。

表2的schema：

![](http://doc.bce.baidu.com/bce-documentation/PALO/image3-18.png?responseContentDisposition=attachment)

前两列的长度为（4+8=）12，没有达到36，但是第三列为varchar，所以前三列被用作索引，其中k3只去前20字节。

表3的schema：

![](http://doc.bce.baidu.com/bce-documentation/PALO/image3-19.png?responseContentDisposition=attachment)

该表第一列是varchar类型，所以只有k3列的前20字节作为索引。

表4的schema：

![](http://doc.bce.baidu.com/bce-documentation/PALO/image3-20.png?responseContentDisposition=attachment)

前四列的长度和为（8+8+8+8=）32，如果加上第五列（8个字节），就会超过36字节。所以只有前四列被用作索引。

如果对于表2和表3执行同样的语句：

    SELECT * from tbl WHERE k1 = 12345;

表2的性能会明显由于表3，因为在表2中可以用到k1索引，而表3只有k3作为索引，该查询会进行扫全表的操作。因此，在建表时，应该尽量将频繁使用，选择度高的列放在前面，尽量不要将varchar类型放在前几列，尽量使用整型作为索引列。

举例：

1.创建一个olap表，使用Random分桶，使用列存，相同key的记录进行聚合
 
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

 2.创建一个olap表，使用Hash分桶，使用行存，相同key的记录进行覆盖。设置初始存储介质和存放到期时间。
     
	 CREATE TABLE example_db.table_hash    
     (    
     k1 BIGINT,    
     k2 LARGEINT,    
     v1 VARCHAR(2048),   
     v2 SMALLINT DEFAULT "10"   
     )   
     ENGINE=olap
	 UNIQUE KEY(k1, k2)   
     DISTRIBUTED BY HASH (k1, k2) BUCKETS 32   
     PROPERTIES(   
     "storage_type"="row"，    
     "storage_medium" = "SSD",    
     "storage_cooldown_time" = "2015-06-04 00:00:00"    
     );

 3.创建一个olap表，使用Key Range分区，使用Hash分桶。默认使用列存。相同key的记录同时存在。设置初始存储介质和存放到期时间。

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
     PARTITION p1 VALUES LESS THAN ("2014-01-01")   
     ("storage_medium" = "SSD", "storage_cooldown_time" = "2015-06-04 00:00:00"),    
     PARTITION p2 VALUES LESS THAN ("2014-06-01")   
     ("storage_medium" = "SSD", "storage_cooldown_time" = "2015-06-04 00:00:00"),    
     PARTITION p3 VALUES LESS THAN ("2014-12-01")    
     )    
     DISTRIBUTED BY HASH(k2) BUCKETS 32;

 说明：

 这个语句会将数据划分成如下3个分区：
    
     ( { MIN }, {"2014-01-01"} )    
     [ {"2014-01-01"}, {"2014-06-01"} )   
     [ {"2014-06-01"}, {"2014-12-01"} )

 不在这些分区范围内的数据将视为非法数据被过滤

 4.创建一个 mysql 表

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

5.创建一个数据文件存储在HDFS上的 broker 外部表, 数据使用 "|" 分割，"\n" 换行

	CREATE EXTERNAL TABLE example_db.table_broker 
	(
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

6.创建一个含有HLL列的表

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

### Drop Database

该语句用于删除数据库(database)

语法：

    DROP DATABASE [IF EXISTS] db_name;

举例：

 删除数据库 db_test

     DROP DATABASE db_test;

### Drop Table

该语句用于删除表(table)

语法：

     DROP TABLE [IF EXISTS] [db_name.]table_name;

举例：

 1.删除一个 table

     DROP TABLE my_table;

 2.如果存在，删除指定 database 的 table

     DROP TABLE IF EXISTS example_db.my_table;

### Alter Database

该语句用于设置指定数据库的配额。（仅管理员使用）

语法：

     ALTER DATABASE db_name SET DATA QUOTA quota;

举例：

 设置指定数据库数据量配额为1GB

     ALTER DATABASE example_db SET DATA QUOTA 1073741824;

### Alter Table

该语句用于对已有的table进行修改。该语句分为三种操作类型：partition、rollup和schema change。Partition是上文提到的复合分区中的第一级分区；rollup是物化索引相关的操作；schema change用来修改表结构。这三种操作不能同时出现在一条ALTER TABLE语句中。其中schema change和rollup是异步操作，任务提交成功则返回，之后可以使用SHOW ALTER命令查看进度。Partition是同步操作，命令返回表示执行完毕。

语法：

     ALTER TABLE [database.]table alter_clause1[, alter_clause2, ...];

Alter_clause分为partition、rollup、schema change和rename四种。

**partition支持的操作**

***增加分区***

语法：

     ADD PARTITION [IF NOT EXISTS] partition_name VALUES LESS THAN [MAXVALUE|("value1")] ["key"="value"] [DISTRIBUTED BY RANDOM [BUCKETS num] | DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]

注意：

-   分区为左闭右开区间，用户指定右边界，系统自动确定左边界

-	如果没有指定分桶方式，则自动使用建表使用的分桶方式

-	如果已经指定分桶方式，则只能修改分桶数，不可修改分桶方式或分桶列

-	["key"="value"] 部分可以设置分区的一些属性，具体说明见CREATE TABLE

***删除分区***

语法：

     DROP PARTITION [IF EXISTS] partition_name

注意：

-	使用分区方式的表至少要保留一个分区

-	执行 DROP PARTITION 一段时间内，可以通过 RECOVER 语句恢复被删除的 partition。详见 RECOVER 语句

***修改分区属性***

语法：

     MODIFY PARTITION partition_name SET ("key" = "value", ...)

说明：

-	当前支持修改分区的 storage_medium、storage_cooldown_time 和replication_num 三个属性。

-	建表时没有指定partition时，partition_name同表名。

**rollup支持的操作**

rollup index类似于物化视图。建表完成之后，这张表中没有rollup index，只有一个base index，这个index的name和表名相同。用户可以为一张表建立一个或多个rollup index，每个rollup index包含base index中key和value列的一个子集，Palo会为这个子集生成独立的数据，用来提升查询性能。如果一个查询涉及到的列全部包含在一个rollup index中，Palo会选择扫瞄这个rollup index而不是全部的数据。用户可以根据自己应用的特点选择创建rollup index,rollup支持的操作：

***创建 rollup index***

语法：

     ADD ROLLUP rollup_name (column_name1, column_name2, ...) [FROM from_index_name] [PROPERTIES ("key"="value", ...)]

注意：

-	如果没有指定from_index_name，则默认从base index创建

-	rollup表中的列必须是from_index中已有的列

-	在properties中，可以指定存储格式。具体请参阅 CREATE TABLE

***删除 rollup index***

语法：

     DROP ROLLUP rollup_name   
     [PROPERTIES ("key"="value", ...)]

注意：

-	不能删除 base index

-	执行 DROP ROLLUP 一段时间内，可以通过 RECOVER 语句恢复被删除的 rollup index。详见 RECOVER 语句

**schema change**

Schema change操作用来修改表结构，包括添加列、删除列、修改列类型以及调整列顺序等。可以修改base index和rollup index的结构。

Schema change支持的操作：

***向指定index的指定位置添加一列***

语法：

     ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]    
     [AFTER column_name|FIRST]    
     [TO index_name]    
     [PROPERTIES ("key"="value", ...)]

注意：

-	聚合模型如果增加 value 列，需要指定agg_type

-	非聚合模型如果增加key列，需要指定KEY关键字

-	不能在rollup index中增加base index中已经存在的列。如有需要，可以重新创建一个 rollup index

***向指定index添加多列***

语法：
    
     ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)   
     [TO index_name]    
     [PROPERTIES ("key"="value", ...)]

注意：

-	聚合模型如果增加 value 列，需要指定agg_type

-	非聚合模型如果增加key列，需要指定KEY关键字

-	不能在rollup index中增加base index中已经存在的列，如有需要，可以重新创建一个 rollup index。

***从指定 index 中删除一列***

语法：

     DROP COLUMN column_name [FROM index_name]

注意：

-	不能删除分区列

-	如果是从base index中删除列，那么rollup index中如果包含该列，也会被删除

***修改指定index的列类型以及列位置***

语法：

     MODIFY COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]    
     [AFTER column_name|FIRST]    
     [FROM index_name]    
     [PROPERTIES ("key"="value", ...)]

注意：

-	聚合类型如果修改value列，需要指定agg_type

-	非聚合类型如果修改key列，需要指定KEY关键字

-	只能修改列的类型，列的其他属性维持原样（即其他属性需在语句中按照原属性显式的写出，参考schema change举例5）

-	分区列不能做任何修改

-	目前支持以下类型的转换（精度损失由用户保证）。TINYINT, SMALLINT, INT, BIGINT转换成TINYINT, SMALLINT, INT, BIGINT, DOUBLE。LARGEINT转换成DOUBLE 。VARCHAR支持修改最大长度

-	不支持从NULL转为NOT NULL

***对指定index的列进行重新排序***

语法：

     ORDER BY (column_name1, column_name2, ...)    
     [FROM index_name]    
     [PROPERTIES ("key"="value", ...)]

注意：

-	index中的所有列都要写出来

-	value列在key列之后

**rename**

Rename操作用来修改表名、rollup index名称和partition名称

Rename支持的操作：

***修改表名***

语法：
	
	RENAME new_table_name

***修改rollup index名称***

语法：

	RENAME ROLLUP old_rollup_name new_rollup_name

***修改partition名称***

语法：
	
	RENAME PARTITION old_partition_name new_partition_name

举例：

 1.增加分区, 现有分区 [MIN, 2013-01-01)，增加分区[2013-01-01, 2014-01-01)，使用默认分桶方式

     ALTER TABLE example_db.my_table   
     ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");

 2.增加分区，使用新的分桶方式

     ALTER TABLE example_db.my_table   
     ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")    
     DISTRIBUTED BY RANDOM BUCKETS 20;

 3.删除分区

     ALTER TABLE example_db.my_table   
     DROP PARTITION p1;

 4.创建index: example_rollup_index，基于 base index（k1,k2,k3,v1,v2）,列式存储。

     ALTER TABLE example_db.my_table    
     ADD ROLLUP example_rollup_index(k1, k3, v1, v2)    
     PROPERTIES("storage_type"="column");

 5.创建index: example_rollup_index2，基于example_rollup_index（k1,k3,v1,v2）

     ALTER TABLE example_db.my_table    
     ADD ROLLUP example_rollup_index2 (k1, v1)    
     FROM example_rollup_index;

 6.删除index: example_rollup_index2

     ALTER TABLE example_db.my_table    
     DROP ROLLUP example_rollup_index2;

 7.向example_rollup_index的col1后添加一个key列new_col(非聚合模型)

     ALTER TABLE example_db.my_table   
     ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1    
     TO example_rollup_index;

 8.向example_rollup_index的col1后添加一个value列new_col(非聚合模型)

     ALTER TABLE example_db.my_table   
     ADD COLUMN new_col INT DEFAULT "0" AFTER col1    
     TO example_rollup_index;

 9.向example_rollup_index的col1后添加一个key列new_col(聚合模型)

     ALTER TABLE example_db.my_table   
     ADD COLUMN new_col INT DEFAULT "0" AFTER col1    
     TO example_rollup_index;

 10.向example_rollup_index的col1后添加一个value列new_col SUM聚合类型(聚合模型)

     ALTER TABLE example_db.my_table   
     ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1    
     TO example_rollup_index;

 11.向 example_rollup_index 添加多列（聚合模型）

     ALTER TABLE example_db.my_table    
     ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")    
     TO example_rollup_index;

 12.从example_rollup_index删除一列

     ALTER TABLE example_db.my_table    
     DROP COLUMN col2    
     FROM example_rollup_index;

 13.修改base index的col1列的类型为BIGINT，并移动到col2列后面

     ALTER TABLE example_db.my_table    
     MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;

 14.修改base index的val1列最大长度。原val1为(val1 VARCHAR(32) REPLACE DEFAULT "abc")

     ALTER TABLE example_db.my_table    
     MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";

 15.重新排序example_rollup_index中的列（设原列顺序为：k1,k2,k3,v1,v2）

     ALTER TABLE example_db.my_table    
     ORDER BY (k3,k1,k2,v2,v1)    
     FROM example_rollup_index;

 16.同时执行两种操作

     ALTER TABLE example_db.my_table    
     ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,   
     ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;

 17.修改表的 bloom filter 列

	 ALTER TABLE example_db.my_table
	 PROPERTIES ("bloom_filter_columns"="k1,k2,k3");

 18.将名为 table1 的表修改为 table2

	 ALTER TABLE table1 RENAME table2;

 19.将表 example_table 中名为 rollup1 的 rollup index 修改为 rollup2

	 ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;

 20.将表 example_table 中名为 p1 的 partition 修改为 p2

	 ALTER TABLE example_table RENAME PARTITION p1 p2;

### Cancel Alter

该语句用于撤销一个alter操作

撤销alter table column (即schema change)语法：

     CANCEL ALTER TABLE COLUMN FROM db_name.table_name

撤销alter table rollup操作

     CANCEL ALTER TABLE ROLLUP FROM db_name.table_name

举例：

 1.撤销针对 my_table 的 ALTER COLUMN 操作。

     CANCEL ALTER TABLE COLUMN    
     FROM example_db.my_table;

 2.撤销 my_table 下的 ADD ROLLUP 操作。

     CANCEL ALTER TABLE ROLLUP    
     FROM example_db.my_table;

## DML语句

### Load

该语句用于向指定的table导入数据。该操作会同时更新和此table相关的base index和rollup index的数据。这是一个异步操作，任务提交成功则返回。执行后可使用SHOW LOAD命令查看进度。

NULL导入的时候用\N来表示。如果需要将其他字符串转化为NULL，可以使用replace_value进行转化。

语法：

     LOAD LABEL load_label    
     (    
     data_desc1[, data_desc2, ...]    
     )
	 broker    
     [opt_properties];

**load_label**

load_label是当前导入批次的标签，由用户指定，需要保证在一个database是唯一的。也就是说，之前在某个database成功导入的label不能在这个database中再使用。该label用来唯一确定database中的一次导入，便于管理和查询。

语法：

     [database_name.]your_label

**data_desc**

用于具体描述一批导入数据。

语法：

     DATA INFILE    
     (    
     "file_path1 [, file_path2, ...]    
     )   
     [NEGATIVE]   
     INTO TABLE table_name   
     [PARTITION (p1, p2)]   
     [COLUMNS TERMINATED BY "column_separator"]    
     [(column_list)]   
     [SET (k1 = func(k2))]

说明：

-	file_path，broker中的文件路径，可以指定到一个文件，也可以用/*通配符指定某个目录下的所有文件。

-	NEGATIVE：如果指定此参数，则相当于导入一批“负”数据。用于抵消之前导入的同一批数据。该参数仅适用于存在value列，并且value列的聚合类型为SUM的情况。不支持Broker方式导入

-	PARTITION：如果指定此参数，则只会导入指定的分区，导入分区以外的数据会被过滤掉。如果不指定，默认导入table的所有分区。

-	column_separator：用于指定导入文件中的列分隔符。默认为\\t。如果是不可见字符，则需要加\\\\x作为前缀，使用十六进制来表示分隔符。如hive文件的分隔符\\x01，指定为"\\\\x01"

-	column_list：用于指定导入文件中的列和table中的列的对应关系。当需要跳过导入文件中的某一列时，将该列指定为table中不存在的列名即可,语法：


     (col_name1, col_name2, ...)


-	SET: 如果指定此参数，可以将源文件某一列按照函数进行转化，然后将转化后的结果导入到table中。目前支持的函数有：

    -	strftime(fmt, column) 日期转换函数

    	-	fmt: 日期格式，形如%Y%m%d%H%M%S (年月日时分秒)

		-	column: column_list中的列，即输入文件中的列。存储内容应为数字型的时间戳。如果没有column_list，则按照palo表的列顺序默认输入文件的列。

	-	time_format(output_fmt, input_fmt, column) 日期格式转化

 		-	output_fmt: 转化后的日期格式，形如%Y%m%d%H%M%S (年月日时分秒)

 		-	input_fmt: 转化前column列的日期格式，形如%Y%m%d%H%M%S (年月日时分秒)

 		-	column: column_list中的列，即输入文件中的列。存储内容应为input_fmt格式的日期字符串。如果没有column_list，则按照palo表的列顺序默认输入文件的列。

 	-	alignment_timestamp(precision, column) 将时间戳对齐到指定精度

 		-	precision: year|month|day|hour

 		-	column: column_list中的列，即输入文件中的列。存储内容应为数字型的时间戳。 如果没有column_list，则按照palo表的列顺序默认输入文件的列。

 		-	注意：对齐精度为year、month的时候，只支持20050101~20191231范围内的时间戳。

	-	default_value(value) 设置某一列导入的默认值，不指定则使用建表时列的默认值

	-	md5sum(column1, column2, ...) 将指定的导入列的值求md5sum，返回32位16进制字符串

	-	replace_value(old_value[, new_value]) 导入文件中指定的old_value替换为new_value。new_value如不指定则使用建表时列的默认值

	-	hll_hash(column) 用于将表或数据里面的某一列转化成HLL列的数据结构

**broker**

用于指定导入使用的Broker

语法：

	 WITH BROKER broker_name ("key"="value"[,...])

这里需要指定具体的Broker name, 以及所需的Broker属性

**opt_properties**

用于指定一些特殊参数。

语法：

     [PROPERTIES ("key"="value", ...)]

可以指定如下参数：

-	timeout：指定导入操作的超时时间。默认不超时。单位秒

-	max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。

-	load_delete_flag：指定该导入是否通过导入key列的方式删除数据，仅适用于UNIQUE KEY，导入时可不指定value列。默认为false (不支持Broker方式导入)

-	exe_mem_limit：在Broker Load方式时生效，指定导入执行时，后端可使用的最大内存。

举例：

 1.导入一批数据，指定个别参数

     LOAD LABEL example_db.label1    
     (    
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")   
     INTO TABLE my_table   
     )
	 WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password")   
     PROPERTIES    
     (   
     "timeout"="3600",    
     "max_filter_ratio"="0.1",    
     );

 2.导入一批数据，包含多个文件。导入不同的 table，指定分隔符，指定列对应关系

     LOAD LABEL example_db.label2
     (   
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file1")    
     INTO TABLE my_table_1    
     COLUMNS TERMINATED BY ","    
     (k1, k3, k2, v1, v2),    
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file2")    
     INTO TABLE my_table_2    
     COLUMNS TERMINATED BY "\t"   
     (k1, k2, k3, v2, v1)    
     )    
	 WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

 3.导入一批数据，指定hive的默认分隔符\x01，并使用通配符*指定目录下的所有文件

     LOAD LABEL example_db.label3   
     (   
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")    
     NEGATIVE    
     INTO TABLE my_table    
     COLUMNS TERMINATED BY "\\x01"    
     )
	 WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

 4.导入一批“负”数据

     LOAD LABEL example_db.label4   
     (    
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file")    
     NEGATIVE    
     INTO TABLE my_table    
     COLUMNS TERMINATED BY "\t"   
     )
	 WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

 5.导入一批数据，指定分区

     LOAD LABEL example_db.label5    
     (   
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")    
     INTO TABLE my_table    
     PARTITION (p1, p2)    
     COLUMNS TERMINATED BY ","    
     (k1, k3, k2, v1, v2)   
     )
	 WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

 6.导入一批数据，指定分区, 并对导入文件的列做一些转化，如下：

 -	k1将tmp_k1时间戳列转化为datetime类型的数据

 -	k2将tmp_k2 date类型的数据转化为datetime的数据

 -	k3将tmp_k3时间戳列转化为天级别时间戳

 -	k4指定导入默认值为1

 -	k5将tmp_k1、tmp_k2、tmp_k3列计算md5串
 
导入语句为：

	 LOAD LABEL example_db.label6
     (
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
     INTO TABLE my_table
     PARTITION (p1, p2)
     COLUMNS TERMINATED BY ","
	 (tmp_k1, tmp_k2, tmp_k3, v1, v2)
     SET (
	   k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)),	    
	   k2 = time_format("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", tmp_k2)),	    
	   k3 = alignment_timestamp("day", tmp_k3),	    
	   k4 = default_value("1"),	    
	   k5 = md5sum(tmp_k1, tmp_k2, tmp_k3)	
     )
     )
     WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");	

7.导入数据到含有HLL列的表，可以是表中的列或者数据里面的列

	 LOAD LABEL example_db.label7
     (
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
     INTO TABLE my_table
     PARTITION (p1, p2)
     COLUMNS TERMINATED BY ","
     SET (
       v1 = hll_hash(k1),
       v2 = hll_hash(k2)
     )
     )
     WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");	

     LOAD LABEL example_db.label8
     (
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
     INTO TABLE `my_table`
     PARTITION (p1, p2)
     COLUMNS TERMINATED BY ","
     (k1, k2, tmp_k3, tmp_k4, v1, v2)
     SET (
       v1 = hll_hash(tmp_k3),
       v2 = hll_hash(tmp_k4)
     )
     )
     WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

### 小批量导入


小批量导入是Palo新提供的一种导入方式，这种导入方式可以使用户不依赖 Hadoop，从而完成导入。此种导入方式提交任务并不是通过MySQL客户端，而是通过http协议来完成的。用户通过http协议将导入描述和数据一同发送给Palo，Palo在接收任务成功后，会立即返回给用户成功信息，但是此时，数据并未真正导入。用户需要通过 'SHOW LOAD' 命令来查看具体的导入结果。

语法：

     curl --location-trusted -u user:passwd -T data.file http://fe.host:port/api/{db}/{table}/_load?label=xxx

参数说明：

-	user:用户如果是在default_cluster中的，user即为user_name。否则为user_name@cluster_name。

-	label:用于指定这一批次导入的label，用于后期进行作业状态查询等。 这个参数是必须传入的。

-	columns: 用于描述导入文件中对应的列名字。如果不传入，那么认为文件中的列顺序与建表的顺序一致，指定的方式为逗号分隔，例如：columns=k1,k2,k3,k4

-	column_separator: 用于指定列与列之间的分隔符，默认的为'\\t'。需要注意的是，这里应使用url编码，例如需要指定'\\t'为分隔符，那么应该传入'column_separator=%09'；需要指定'\\x01'为分隔符，那么应该传入'column_separator=%01'

-	max_filter_ratio: 用于指定允许过滤不规范数据的最大比例，默认是0，不允许过滤。自定义指定应该如下：'max_filter_ratio=0.2'，含义是允许20%的错误率。

-	hll:用于指定数据里面和表里面的HLL列的对应关系，表中的列和数据里面指定的列（如果不指定columns，则数据列里面的列也可以是表里面的其它非HLL列）通过","分割，指定多个hll列使用“:”分割，例如: 'hll1,cuid:hll2,device'

举例：

 1.将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表（用户是defalut_cluster中的）

     curl --location-trusted -u root:root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123

 2.将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表（用户是test_cluster中的）
	
	 curl --location-trusted -u root@test_cluster:root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123

 3.将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率（用户是defalut_cluster中的）

     curl --location-trusted -u root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123\$amp;max_filter_ratio=0.2

 4.将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率，并且指定文件的列名（用户是defalut_cluster中的）

     curl --location-trusted -u root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123\$amp;max_filter_ratio=0.2\$amp;columns=k1,k2,k3

 5.使用streaming方式导入（用户是defalut_cluster中的）

     seq 1 10 | awk '{OFS="\\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123

 6.导入含有HLL列的表，可以是表中的列或者数据中的列用于生成HLL列（用户是defalut_cluster中的）
	
	 curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&hll=hll_column1,k1:hll_column2,k2
            
     curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&hll=hll_column1,tmp_k4:hll_column2,tmp_k5\&columns=k1,k2,k3,tmp_k4,tmp_k5

### Cancel Load

Cancel load用于撤销指定load label的导入作业。这是一个异步操作，任务提交成功就返回。提交后可以使用show load命令查看进度。

语法：

     CANCEL LOAD [FROM db_name] WHERE LABEL = "load_label";

举例：

 撤销数据库 example_db 上， label 为 example_db_test_load_label 的导入作业

     CANCEL LOAD FROM example_db WHERE LABEL = "example_db_test_load_label";

### Export

该语句用于将指定表的数据导出到指定位置。这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW EXPORT 命令查看进度。

语法：

	 EXPORT TABLE table_name
	 [PARTITION (p1[,p2])]
	 TO export_path
	 [opt_properties]
	 broker;

**table_name**

当前要导出的表的表名，目前支持engine为olap和mysql的表的导出。

**partition**

可以只导出指定表的某些指定分区

**export_path**

导出的路径，需为目录。目前不能导出到本地，需要导出到broker。

**opt_properties**

用于指定一些特殊参数。

语法：

	 [PROPERTIES ("key"="value", ...)]

可以指定如下参数：

-	column_separator：指定导出的列分隔符，默认为\t。
-	line_delimiter:指定导出的行分隔符，默认为\n。

**broker**

用于指定导出使用的broker

语法：

	 WITH BROKER broker_name ("key"="value"[,...])

这里需要指定具体的broker name, 以及所需的broker属性

举例：

 1.将testTbl表中的所有数据导出到hdfs上

	 EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

 2.将testTbl表中的分区p1,p2导出到hdfs上

	 EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

 3.将testTbl表中的所有数据导出到hdfs上，以","作为列分隔符

	 EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

### Delete

该语句用于按条件删除指定table（base index） partition中的数据。该操作会同时删除和此相关的rollup index的数据。

语法：

     DELETE FROM table_name PARTITION partition_name WHERE   
     column_name1 op value[ AND column_name2 op value ...];

说明：

-	op的可选类型包括：=, &lt;, &gt;, &lt;=, &gt;=, !=

-	只能指定key列上的条件。

-	条件之间只能是“与”的关系。若希望达成“或”的关系，需要将条件分写在两个 DELETE语句中。

-	如果没有创建partition，partition_name 同 table_name。

注意：

-	该语句可能会降低执行后一段时间内的查询效率，影响程度取决于语句中指定的删除条件的数量，指定的条件越多，影响越大。

举例：

 1.删除 my_table partition p1 中 k1 列值为 3 的数据行

     DELETE FROM my_table PARTITION p1 WHERE k1 = 3;

 2.删除 my_table partition p1 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行

     DELETE FROM my_table PARTITION p1 WHERE k1 >= 3 AND k2 = "abc";

## SELECT语句

Select语句由select，from，where，group by，having，order by，union等部分组成，Palo的查询语句基本符合SQL92标准，下面详细介绍支持的select用法。

### 连接(Join)

连接操作是合并2个或多个表的数据，然后返回其中某些表中的某些列的结果集。目前Palo支持inner join，outer join，semi join，anti join, cross join。在inner join条件里除了支持等值join，还支持不等值join，为了性能考虑，推荐使用等值join。其它join只支持等值join。

语法：

	 SELECT select_list FROM
		 table_or_subquery1 [INNER] JOIN table_or_subquery2 |
		 table_or_subquery1 {LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]} JOIN table_or_subquery2 |
		 table_or_subquery1 {LEFT | RIGHT} SEMI JOIN table_or_subquery2 |
		 table_or_subquery1 {LEFT | RIGHT} ANTI JOIN table_or_subquery2 |
    		 [ ON col1 = col2 [AND col3 = col4 ...] |
      			 USING (col1 [, col2 ...]) ]
		 [other_join_clause ...]
		 [ WHERE where_clauses ]

	 SELECT select_list FROM
		 table_or_subquery1, table_or_subquery2 [, table_or_subquery3 ...]
		 [other_join_clause ...]
	 WHERE
    	 col1 = col2 [AND col3 = col4 ...]

	 SELECT select_list FROM
		 table_or_subquery1 CROSS JOIN table_or_subquery2
		 [other_join_clause ...]
	 [ WHERE where_clauses ]

**Self-Join**

Palo支持self-joins，即自己和自己join。例如同一张表的不同列进行join。实际上没有特殊的语法标识self-join。self-join中join两边的条件都来自同一张表，我们需要给他们分配不同的别名。

举例：

	 SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;

**笛卡尔积(Cross Join)**

Cross join会产生大量的结果，须慎用cross join，即使需要使用cross join时也需要使用过滤条件并且确保返回结果数较少。

举例：

	 SELECT * FROM t1, t2;
	 SELECT * FROM t1 CROSS JOIN t2;

**Inner join**

inner join 是大家最熟知，最常用的join。返回的结果来自相近的2张表所请求的列，join 的条件为两个表的列包含有相同的值。如果两个表的某个列名相同，我们需要使用全名（table_name.column_name形式）或者给列名起别名。

举例：

	 -- The following 3 forms are all equivalent.
	 SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;
	 SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;
	 SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

**Outer join**

outer join返回左表或者右表或者两者所有的行。如果在另一张表中没有匹配的数据，则将其设置为NULL。

举例：

	 SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;
	 SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;
	 SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;

**等值和不等值join**

通常情况下，用户使用等值join居多，等值join要求join条件的操作符是等号。不等值join 在join条件上可以使用!,，&lt;, &gt;等符号。不等值join会产生大量的结果，在计算过程中可能超过内存限额，因此需要谨慎使用。不等值join只支持inner join。

举例：

	 SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
	 SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;

**Semi join**

left semi join只返回左表中能匹配右表数据的行，不管能匹配右表多少行数据，左表的该行最多只返回一次。right semi join原理相似，只是返回的数据是右表的。

举例：

	 SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;

**Anti join**

left anti join只返回左表中不能匹配右表的行。right anti join反转了这个比较，只返回右表中不能匹配左表的行。

举例：

	 SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;

### Order by

order by通过比较1列或者多列的大小来对结果集进行排序。order by是比较耗时耗资源的操作，因为所有数据都需要发送到1个节点后才能排序，排序操作相比不排序操作需要更多的内存。如果需要返回前N个排序结果，需要使用LIMIT从句；为了限制内存的使用，如果用户没有指定LIMIT从句，则默认返回前65535个排序结果。

语法：

    ORDER BY col [ASC | DESC]

默认的排序是ASC

举例：

	mysql> select * from big_table order by tiny_column, short_column desc;


### Group by

group by从句通常和聚合函数（例如COUNT(), SUM(), AVG(), MIN()和MAX()）一起使用。group by指定的列不会参加聚合操作。group by从句可以加入having从句来过滤聚合函数产出的结果。

举例：

	 mysql> select tiny_column, sum(short_column) from small_table group by tiny_column;
	 +-------------+---------------------+
	 | tiny_column | sum(`short_column`) |
	 +-------------+---------------------+
	 |           1 |                   2 |
	 |           2 |                   1 |
	 +-------------+---------------------+
	 2 rows in set (0.07 sec)


### Having

having从句不是过滤表中的行数据，而是过滤聚合函数产出的结果。通常来说having要和聚合函数（例如COUNT(), SUM(), AVG(), MIN(), MAX()）以及group by从句一起使用。

举例：

	 mysql> select tiny_column, sum(short_column) from small_table group by tiny_column having sum(short_column) = 1;
	 +-------------+---------------------+
	 | tiny_column | sum(`short_column`) |
	 +-------------+---------------------+
	 |           2 |                   1 |
	 +-------------+---------------------+
	 1 row in set (0.07 sec)

	 mysql> select tiny_column, sum(short_column) from small_table group by tiny_column having tiny_column > 1;
	 +-------------+---------------------+
	 | tiny_column | sum(`short_column`) |
	 +-------------+---------------------+
	 |           2 |                   1 |
	 +-------------+---------------------+
	 1 row in set (0.07 sec)


### Limit

Limit从句用于限制返回结果的最大行数。设置返回结果的最大行数可以帮助Palo优化内存的使用。该从句主要应用如下场景：

-	返回top-N的查询结果。

-	想简单看下表中包含的内容。

-	如果表中数据足够大，或者where从句没有过滤太多的数据，需要使用

使用说明：

limit从句的值必须是数字型字面常量。

举例：

	 mysql> select tiny_column from small_table limit 1;
	 +-------------+
	 | tiny_column |
	 +-------------+
	 |           1 |
	 +-------------+
	 1 row in set (0.02 sec)

	 mysql> select tiny_column from small_table limit 10000;
	 +-------------+
	 | tiny_column |
	 +-------------+
	 |           1 |
	 |           2 |
	 +-------------+
	 2 rows in set (0.01 sec)


### Offset

offset从句使得结果集跳过前若干行结果后直接返回后续的结果。结果集默认起始行为第0行，因此offset 0和不带offset返回相同的结果。通常来说，offset从句需要与order by从句和limit从句一起使用才有效。

举例：

	 mysql> select varchar_column from big_table order by varchar_column limit 3;
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | beijing        |
	 | chongqing      |
	 | tianjin        |
	 +----------------+
	 3 rows in set (0.02 sec)

	 mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | beijing        |
	 +----------------+
	 1 row in set (0.01 sec)

	 mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | chongqing      |
	 +----------------+
	 1 row in set (0.01 sec)

	 mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;
	 +----------------+
	 | varchar_column |
	 +----------------+
	 | tianjin        |
	 +----------------+
	 1 row in set (0.02 sec)


注意：

在没有order by的情况下使用offset语法是允许的，但是此时offset无意义，这种情况只取limit的值，忽略掉offset的值。因此在没有order by的情况下，offset超过结果集的最大行数依然是有结果的。建议用户使用offset时一定要带上order by。

### Union

Union从句用于合并多个查询的结果集。

语法：

    query_1 UNION [DISTINCT | ALL] query_2

使用说明：

只使用union关键词和使用union disitnct的效果是相同的。由于去重工作是比较耗费内存的，因此使用union all操作查询速度会快些，耗费内存会少些。如果用户想对返回结果集进行order by和limit操作，需要将union操作放在子查询中，然后select from subquery，最后把subgquery和order by放在子查询外面。

举例：

	 mysql> (select tiny_column from small_table) union all (select tiny_column from small_table);
	 +-------------+
	 | tiny_column |
	 +-------------+
	 |           1 |
	 |           2 |
	 |           1 |
	 |           2 |
	 +-------------+
	 4 rows in set (0.10 sec)

	 mysql> (select tiny_column from small_table) union (select tiny_column from small_table);    
	 +-------------+
	 | tiny_column |
	 +-------------+
	 |           2 |
	 |           1 |
	 +-------------+
	 2 rows in set (0.11 sec)

	 mysql> select * from (select tiny_column from small_table union all\
    	 -> select tiny_column from small_table) as t1 \
    	 -> order by tiny_column limit 4;
	 +-------------+
	 | tiny_column |
	 +-------------+
	 |           1 |
	 |           1 |
	 |           2 |
	 |           2 |
	 +-------------+
	 4 rows in set (0.11 sec)


### Distinct

Distinct操作符对结果集进行去重。

举例：

	 mysql> -- Returns the unique values from one column.
	 mysql> select distinct tiny_column from big_table limit 2;
	 mysql> -- Returns the unique combinations of values from multiple columns.
	 mysql> select distinct tiny_column, int_column from big_table limit 2;

distinct可以和聚合函数(通常是count函数)一同使用，count(disitnct)用于计算出一个列或多个列上包含多少不同的组合。

	 mysql> -- Counts the unique values from one column.
	 mysql> select count(distinct tiny_column) from small_table;
	 +-------------------------------+
	 | count(DISTINCT `tiny_column`) |
	 +-------------------------------+
	 |                             2 |
	 +-------------------------------+
	 1 row in set (0.06 sec)
	 mysql> -- Counts the unique combinations of values from multiple columns.
	 mysql> select count(distinct tiny_column, int_column) from big_table limit 2;

Palo支持多个聚合函数同时使用distinct

	 mysql> -- Count the unique value from multiple aggregation function separately.
	 mysql> select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;

### 子查询

子查询按相关性分为不相关子查询和相关子查询。

***不相关子查询***

不相关子查询支持[NOT] IN和EXISTS。

举例：

	SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);
	SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);

***相关子查询***

相关子查询支持[NOT] IN和[NOT] EXISTS。

举例：

	SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);
	SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);

子查询还支持标量子查询。分为不相关标量子查询、相关标量子查询和标量子查询作为普通函数的参数。

举例：

	1、不相关标量子查询，谓词为=号。例如输出最大工资的人的信息
	SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
	2、不相关标量子查询，谓词为>,<等。例如输出比平均工资高的人的信息
	SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
	3、相关标量子查询。例如输出各个部门工资最高的信息
	SELECT name FROM table a WHERE salary = （SELECT MAX(salary) FROM table b WHERE b.部门= a.部门）;
	4、标量子查询作为普通函数的参数
	SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));

### with子句

可以在SELECT语句之前添加的子句，用于定义在SELECT内部多次引用的复杂表达式的别名。与CREATE VIEW类似，除了在子句中定义的表和列名在查询结束后不会持久以及不会与实际表或VIEW中的名称冲突。

用WITH子句的好处有：

1.方便和易于维护，减少查询内部的重复。

2.通过将查询中最复杂的部分抽象成单独的块，更易于阅读和理解SQL代码。

举例：

	 -- Define one subquery at the outer level, and another at the inner level as part of the
	 -- initial stage of the UNION ALL query.
	 with t1 as (select 1) (with t2 as (select 2) select * from t2) union all select * from t1;

## SHOW语句

### Show alter

该语句用于展示当前正在进行的各类修改任务的执行情况.

语法：

     SHOW ALTER TABLE [COLUMN | ROLLUP] [FROM db_name];

说明：

-	TABLE COLUMN：展示修改列的ALTER任务

-	TABLE ROLLUP：展示创建或删除ROLLUP index的任务

-	如果不指定 db_name，使用当前默认 db

举例：

 1.展示默认 db 的所有修改列的任务执行情况

     SHOW ALTER TABLE COLUMN;

 2.展示指定 db 的创建或删除 ROLLUP index 的任务执行情况

     SHOW ALTER TABLE ROLLUP FROM example_db;

### Show data

该语句用于展示数据量

语法:

     SHOW DATA [FROM db_name[.table_name]];

说明：

如果不指定FROM子句，使用展示当前db下细分到各个 table的数据量。如果指定 FROM子句，则展示table下细分到各个index的数据量

举例：

 1.展示默认db的各个table的数据量及汇总数据量
    
     SHOW DATA;

 2.展示指定db的下指定表的细分数据量

     SHOW DATA FROM example_db.table_name;

### Show databases

该语句用于展示当前可见的database

语法：

     SHOW DATABASES;

### Show load

该语句用于展示指定的导入任务的执行情况

语法：

     SHOW LOAD    
     [FROM db_name]    
     [   
     WHERE    
     [LABEL [ = "your_label" | LIKE "label_matcher"]]    
     [STATUS = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]    
     ]    
     [ORDER BY ...]    
     [LIMIT limit];

说明：

-	如果不指定 db_name，使用当前默认db

-	如果使用 LABEL LIKE，则会匹配导入任务的 label 包含 label_matcher 的导入任务

-	如果使用 LABEL = ，则精确匹配指定的 label

-	如果指定了 STATUS，则匹配 LOAD 状态

-	可以使用 ORDER BY 对任意列组合进行排序

-	如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示

举例：

 1.展示默认 db 的所有导入任务
    
     SHOW LOAD;

 2.展示指定 db 的导入任务，label 中包含字符串 "2014_01_02"，展示最老的10个

     SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;

 3.展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" 并按 LoadStartTime 降序排序

     SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;

 4.展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" ，state 为 "loading", 并按 LoadStartTime 降序排序

     SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;

### Show export

该语句用于展示指定的导出任务的执行情况

语法：

	 SHOW EXPORT
     [FROM db_name]
     [
     WHERE
	 [EXPORT_JOB_ID = your_job_id]
     [STATE = ["PENDING"|"EXPORTING"|"FINISHED"|"CANCELLED"]]
     ]
     [ORDER BY ...]
     [LIMIT limit];

说明：

-	如果不指定 db_name，使用当前默认db

-	如果指定了 STATE，则匹配 EXPORT 状态

-	可以使用 ORDER BY 对任意列组合进行排序

-	如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示

举例：

 1.展示默认 db 的所有导出任务

	SHOW EXPORT;

 2.展示指定 db 的导出任务，按 StartTime 降序排序

	SHOW EXPORT FROM example_db ORDER BY StartTime DESC;

 3.展示指定 db 的导出任务，state 为 "exporting", 并按 StartTime 降序排序

	SHOW EXPORT FROM example_db WHERE STATE = "exporting" ORDER BY StartTime DESC;

 4.展示指定db，指定job_id的导出任务

	SHOW EXPORT FROM example_db WHERE EXPORT_JOB_ID = job_id;

### Show partitions

该语句用于展示分区信息

语法：

     SHOW PARTITIONS FROM [db_name.]table_name [PARTITION partition_name];

举例：

 1.展示指定 db 下指定表的分区信息

     SHOW PARTITIONS FROM example_db.table_name;

 2.展示指定 db 下指定表的指定分区的信息

     SHOW PARTITIONS FROM example_db.table_name PARTITION p1;

### Show quota

该语句用于显示一个用户不同组的资源分配情况

语法：

     SHOW QUOTA FOR [user]

举例：

 显示system用户的资源在各个组的分配情况

     SHOW QUOTA FOR system;

### Show resource

该语句用于显示一个用户在不同资源上的权重

语法：

     SHOW RESOURCE [LIKE user_name]

举例：

显示system用户在不同资源上的权重

     SHOW RESOURCE LIKE "system";

### Show tables

该语句用于展示当前db下所有的table

语法：

     SHOW TABLES;

### Show tablet

该语句用于显示tablet相关的信息（仅管理员使用）

语法：

     SHOW TABLET [FROM [db_name.]table_name | tablet_id]

举例：

 1.显示指定 db 的下指定表所有 tablet 信息

     SHOW TABLET FROM example_db.table_name;

 2.显示指定 tablet id 为 10000 的 tablet 的父层级 id 信息

     SHOW TABLET 10000;

## 账户管理

### Create user

该语句用来创建一个用户，需要管理员权限。如果在非default_cluster下create user，用户在登录连接palo和mini load等使用到用户名时，用户名将为user_name@cluster_name。如果在default_cluster下create user，用户在登录连接palo和mini load等使用到用户名时，用户名中不需要添加@cluster_name，即直接为user_name。

语法：

     CREATE USER user_specification [SUPERUSER]    
     user_specification:    
     'user_name' [IDENTIFIED BY [PASSWORD] 'password']

说明：

CREATE USER命令可用于创建一个palo用户，使用这个命令需要使用者必须有管理员权限。SUPERUSER用于指定需要创建的用户是个超级用户

举例：

 1.创建一个没有密码的用户，用户名为 jack

     CREATE USER 'jack'

 2.创建一个带有密码的用户，用户名为 jack，并且密码被指定为 123456

     CREATE USER 'jack' IDENTIFIED BY '123456'

 3.为了避免传递明文，用例2也可以使用下面的方式来创建
    
     CREATE USER 'jack' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

 后面加密的内容可以通过PASSWORD()获得到,例如：
    
     SELECT PASSWORD('123456')

 4.创建一个超级用户'jack'

     CREATE USER 'jack' SUPERUSER

### Drop user

该语句用于删除一个用户，需要管理员权限

语法：

     DROP USER 'user_name'

举例：

删除用户 jack

    DROP USER 'jack'

### Alter user

该语句用于修改用户的相关属性以及分配给用户的资源

语法：
    
     ALTER USER user alter_user_clause_list    
     alter_user_clause_list:    
     alter_user_clause [, alter_user_clause] ...    
     alter_user_clause:   
     MODIFY RESOURCE resource value | MODIFY PROPERTY property value   
     resource:    
     CPU_SHARE   
     property:   
     MAX_USER_CONNECTIONS

举例：

 1.修改用户jack的CPU_SHARE为1000

     ALTER USER jack MODIFY RESOURCE CPU_SHARE 1000

 2.修改用户 jack 最大连接数为1000

     ALTER USER jack MODIFY PROPERTY MAX_USER_CONNECTIONS 1000

### Alter quota

该语句用于修改某用户不同组资源的分配

语法：

     ALTER QUOTA FOR user_name MODIFY group_name value

举例：

 修改system用户的normal组的权重

     ALTER QUOTA FOR system MODIFY normal 400;

### Grant

该语句用于将一个数据库的具体权限授权给具体用户。调用者必须是管理员身份。权限当前只包括只读 (READ_ONLY)，读写 (READ_WRITE) 两种权限，如果指定为ALL，那么就是将全部权限授予该用户。

语法：

     GRANT privilege_list ON db_name TO 'user_name'    
     privilege_list:   
     privilege [, privilege] ...    
     privilege:    
     READ_ONLY | READ_WRITE | ALL

举例：

 1.授予用户 jack 数据库 testDb 的写权限
    
     GRANT READ_ONLY ON testDb to 'jack';

 2.授予用户 jack 数据库 testDb 全部权限

     GRANT ALL ON testDb to 'jack';

### Set password

该语句用于修改一个用户的登录密码。如果 [FOR 'user_name'] 字段不存在，那么修改当前用户的密码。PASSWORD() 方式输入的是明文密码; 而直接使用字符串，需要传递的是已加密的密码。如果修改其他用户的密码，需要具有管理员权限。

语法：

     SET PASSWORD [FOR 'user_name'] = [PASSWORD('plain password')]|['hashed password']

举例：

 1.修改当前用户的密码为 123456

     SET PASSWORD = PASSWORD('123456')    
     SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

 2.修改用户 jack 的密码为 123456
    
     SET PASSWORD FOR 'jack' = PASSWORD('123456')    
     SET PASSWORD FOR 'jack' = '\*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'

## 集群管理

### Alter system

该语句用于操作一个集群内的节点。（仅管理员使用！）

语法：

	 1.增加节点
	     ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
	 2.删除节点
	     ALTER SYSTEM DROP BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
	 3.节点下线
	     ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
	 4.增加Broker
		 ALTER SYSTEM ADD BROKER broker_name "host:port"[,"host:port"...];
	 5.减少Broker
		 ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
	 6.删除所有Broker
		 ALTER SYSTEM DROP ALL BROKER broker_name

说明：
	
-	host 可以是主机名或者ip地址
-	heartbeat_port 为该节点的心跳端口
-	加和删除节点为同步操作。这两种操作不考虑节点上已有的数据，节点直接从元数据中删除，请谨慎使用。
-	点下线操作用于安全下线节点。该操作为异步操作。如果成功，节点最终会从元数据中删除。如果失败，则不会完成下线。
-	可以手动取消节点下线操作。详见 CANCEL ALTER SYSTEM

举例：

 1.增加一个节点

	 ALTER SYSTEM ADD BACKEND "host:9850";

 2.删除两个节点

	 ALTER SYSTEM DROP BACKEND "host1:9850", "host2:9850";

 3.下线两个节点

	 ALTER SYSTEM DECOMMISSION BACKEND "host1:9850", "host2:9850";

 4.增加两个Hdfs Broker
	
	 ALTER SYSTEM ADD BROKER hdfs "host1:9850", "host2:9850";

### Cancel alter system

该语句用于撤销一个节点下线操作。（仅管理员使用！）

语法：
	
	 CANCEL ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];

举例：

 1.取消两个节点的下线操作

	 CANCEL ALTER SYSTEM DECOMMISSION BACKEND "host1:9850", "host2:9850";

### Create cluster

该语句用于新建逻辑集群 (cluster), 需要管理员权限。如果不使用多租户，直接创建一个名称为default_cluster的cluster。否则创建一个自定义名称的cluster。

语法：

	 CREATE CLUSTER [IF NOT EXISTS] cluster_name
	 PROPERTIES ("key"="value", ...)
	 IDENTIFIED BY 'password'

**PROPERTIES**

指定逻辑集群的属性。PROERTIES ("instance_num" = "3")。其中instance_num是逻辑集群节点数。

**identified by ‘password'**

每个逻辑集群含有一个superuser，创建逻辑集群时必须指定其密码

举例：

 1.新建一个含有3个be节点逻辑集群 test_cluster, 并指定其superuser用户密码

	 CREATE CLUSTER test_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';

 2.新建一个含有3个be节点逻辑集群 default_cluster(不使用多租户), 并指定其superuser用户密码

	 CREATE CLUSTER default_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';

### Alter cluster

该语句用于更新逻辑集群。需要有管理员权限

语法：

	 ALTER CLUSTER cluster_name PROPERTIES ("key"="value", ...);

**PROPERTIES**

缩容，扩容 （根据集群现有的be数目，大则为扩容，小则为缩容), 扩容为同步操作，缩容为异步操作，通过backend的状态可以得知是否缩容完成。PROERTIES ("instance_num" = "3")。其中instance_num是逻辑集群节点数。

举例：

 1.缩容，减少含有3个be的逻辑集群test_cluster的be数为2

	 ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="2");

 2.扩容，增加含有3个be的逻辑集群test_cluster的be数为4

	 ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="4");

### Drop cluster

该语句用于删除逻辑集群,成功删除逻辑集群需要首先删除集群内的db，需要管理员权限

语法：

	 DROP CLUSTER [IF EXISTS] cluster_name;

# 白名单管理
**白名单格式**  
白名单有3种格式：  
1、ip： 127.0.0.1  
2、ip 和\*的组合(相当于掩码)： 127.1.\*.\* 或者 127.0.1.*  
3、hostName：hostname.beijing

**添加白名单**  
多个白名单之间用逗号隔开 
    
    ALTER USER user_name ADD WHITELIST  "ip1, ip2"; 

**删除白名单**

    ALTER USER jack DELETE WHITELIST  "ip1, ip2, ip3"

**显示白名单**

    SHOW WHITELIST

**权限**  
1、不添加白名单，默认所有ip可以访问   
一旦添加白名单，只能白名单允许的ip才能访问   
2、superuser可以设置普通用户的白名单  
普通用户（有写权限）只能修改自己的白名单

# 内置函数

## 数学函数

**abs(double a)**

功能： 返回参数的绝对值

返回类型：double类型

使用说明：使用该函数需要确保函数的返回值是整数。

**acos(double a)**

功能： 返回参数的反余弦值

返回类型：double类型

**asin(double a)**

功能： 返回参数的反正弦值

返回类型：double类型

**atan(double a)**

功能： 返回参数的反正切值

返回类型：double类型

**bin(bigint a)**

功能： 返回整型的二进制表示形式（即0 和1 序列）

返回类型：string类型

	 mysql> select bin(10);
	 +---------+
	 | bin(10) |
	 +---------+
	 | 1010    |
	 +---------+
	 1 row in set (0.01 sec)

**ceil(double a)**

**ceiling(double a)**

**dceil(double a)**

功能： 返回大于等于该参数的最小整数

返回类型：int类型

**conv(bigint num, int from_base, int to_base)**

**conv(string num,int from_base, int to_base)**

功能： 进制转换函数，返回某个整数在特定进制下的的字符串形式。输入参数可以是整型的字符串形式。如果想要将函数的返回值转换成整数，可以使用CAST函数。

返回类型：string类型

举例:

	 mysql> select conv(64,10,8);
	 +-----------------+
	 | conv(64, 10, 8) |
	 +-----------------+
	 | 100             |
	 +-----------------+
	 1 row in set (0.01 sec)

	 mysql> select cast(conv('fe', 16, 10) as int) as "transform_string_to_int";
	 +-------------------------+
	 | transform_string_to_int |
	 +-------------------------+
	 |                     254 |
	 +-------------------------+
	 1 row in set (0.00 sec)

**cos(double a)**

功能：返回参数的余弦值

返回类型：double类型

**degrees(double a)**

功能：将弧度转成角度

返回类型：double类型

**e()**

功能：返回数学上的常量e

返回类型：double类型

**exp(double a)**

**dexp(double a)**

功能： 返回e 的a 次幂（即ea）

返回类型： double 类型

**floor(double a)**

**dfloor(double a)**

功能：返回小于等于该参数的最大整数

返回类型：int类型

**fmod(double a, double b)**

**fmod(float a, float b)**

功能：返回a除以b的余数。等价于%算术符

返回类型：float或者double类型

举例：

	 mysql> select fmod(10,3);
	 +-----------------+
	 | fmod(10.0, 3.0) |
	 +-----------------+
	 |               1 |
	 +-----------------+
	 1 row in set (0.01 sec)

	 mysql> select fmod(5.5,2);
	 +----------------+
	 | fmod(5.5, 2.0) |
	 +----------------+
	 |            1.5 |
	 +----------------+
	 1 row in set (0.01 sec)

**greatest(bigint a[, bigint b ...])**

**greatest(double a[, double b ...])**

**greatest(decimal(p,s) a[, decimal(p,s) b ...])**

**greatest(string a[, string b ...])**

**greatest(timestamp a[, timestamp b ...])**

功能：返回列表里的最大值

返回类型：和参数类型相同

**hex(bigint a)**

**hex(string a)**

功能：返回整型或字符串中各个字符的16进制表示形式。

返回类型：string类型

举例：

	 mysql> select hex('abc');
	 +------------+
	 | hex('abc') |
	 +------------+
	 | 616263     |
	 +------------+
	 1 row in set (0.01 sec)

	 mysql> select unhex(616263);
	 +---------------+
	 | unhex(616263) |
	 +---------------+
	 | abc           |
	 +---------------+
	 1 row in set (0.01 sec)

**least(bigint a[, bigint b ...])**

**least(double a[, double b ...])**

**least(decimal(p,s) a[, decimal(p,s) b ...])**

**least(string a[, string b ...])**

**least(timestamp a[, timestamp b ...])**

功能：返回列表里的最小值

返回类型：和参数类型相同

**ln(double a)**

**dlog1(double a)**

功能：返回参数的自然对数形式

返回类型：double类型

**log(double base, double a)**

功能：返回log以base为底数，以a为指数的对数值。

返回类型：double类型

**log10(double a)**

**dlog10(double a)**

功能：返回log以10为底数，以a为指数的对数值。

返回类型：double类型

**log2(double a)**

功能：返回log以2为底数，以a为指数的对数值。

返回类型：double类型

**mod(numeric_type a, same_type b)**

功能：返回a除以b的余数。等价于%算术符。

返回类型：和输入类型相同

举例：

	 mysql> select mod(10,3);
	 +------------+
	 | mod(10, 3) |
	 +------------+
	 |          1 |
	 +------------+
	 1 row in set (0.01 sec)

	 mysql> select mod(5.5,2);
	 +-------------+
	 | mod(5.5, 2) |
	 +-------------+
	 |         1.5 |
	 +-------------+
	 1 row in set (0.01 sec)

**negative(int a)**

**negative(double a)**

功能：将参数a的符号位取反，如果参数是负值，则返回正值

返回类型：根据输入参数类型返回int类型或double类型

使用说明：如果你需要确保所有返回值都是负值，可以使用-abs(a)函数。

**pi()**

功能：返回常量Pi

返回类型： double类型

**pmod(int a, int b)**

**pmod(double a, double b)**

功能：正取余函数

返回类型：int类型或者double类型（由输入参数决定）

**positive(int a)**

功能：返回参数的原值，即使参数是负的，仍然返回原值。

返回类型：int类型

使用说明：如果你需要确保所有返回值都是正值，可以使用abs()函数。

**pow(double a, double p)**

**power(double a, double p)**

功能：返回a的p次幂

返回类型：double类型

**radians(double a)**

功能：将弧度转换成角度

返回类型：double类型

**rand()**

**rand(int seed)**

**random()**

**random(int seed)**

功能：返回0～1之间的随机值。参数为随机种子。

返回类型：double

使用说明：每次查询的随机序列都会重置，多次调用rand 函数会产生相同的结果。如果每次查询想产生不同的结果，可以在每次查询时使用不同的随机种子。例如select rand(unix_timestamp()) from ...

**round(double a)**

**round(double a, int d)**

功能： 取整函数。如果只带一个参数，该函数会返回距离该值最近的整数。如果带2个参数，第二个参数为小数点后面保留的位数。

返回类型：如果参数是浮点类型则返回bigint。如果第二个参数大于1，则返回double类型。

举例:

	 mysql> select round(100.456, 2);
	 +-------------------+
	 | round(100.456, 2) |
	 +-------------------+
	 |            100.46 |
	 +-------------------+
	 1 row in set (0.02 sec)

**sign(double a)**

功能：如果a是整数或者0，返回1；如果a是负数，则返回-1

返回类型：int类型

**sin(double a)**

功能：返回a的正弦值

返回类型：double类型

**sqrt(double a)**

功能：返回a的平方根

返回类型：double类型

**tan(double a)**

功能：返回a的正切值

返回类型：double类型

**unhex(string a)**

功能：把十六进制格式的字符串转化为原来的格式

返回类型：string类型

举例：

	 mysql> select hex('abc');
	 +------------+
	 | hex('abc') |
	 +------------+
	 | 616263     |
	 +------------+
	 1 row in set (0.01 sec)

	 mysql> select unhex(616263);
	 +---------------+
	 | unhex(616263) |
	 +---------------+
	 | abc           |
	 +---------------+
	 1 row in set (0.01 sec)

## 位操作函数

**bitand(integer_type a, same_type b)**

功能：按位与运算

返回类型： 和输入类型相同

举例：

	 mysql> select bitand(255, 32767); /* 0000000011111111 & 0111111111111111 */
	 +--------------------+
	 | bitand(255, 32767) |
	 +--------------------+
	 |                255 |
	 +--------------------+
	 1 row in set (0.01 sec)

	 mysql> select bitand(32767, 1); /* 0111111111111111 & 0000000000000001 */
	 +------------------+
	 | bitand(32767, 1) |
	 +------------------+
	 |                1 |
	 +------------------+
	 1 row in set (0.01 sec)

	 mysql> select bitand(32, 16); /* 00010000 & 00001000 */
	 +----------------+
	 | bitand(32, 16) |
	 +----------------+
	 |              0 |
	 +----------------+
	 1 row in set (0.01 sec)

	 mysql> select bitand(12,5); /* 00001100 & 00000101 */
	 +---------------+
	 | bitand(12, 5) |
	 +---------------+
	 |             4 |
	 +---------------+
	 1 row in set (0.01 sec)

	 mysql> select bitand(-1,15); /* 11111111 & 00001111 */
	 +----------------+
	 | bitand(-1, 15) |
	 +----------------+
	 |             15 |
	 +----------------+
	 1 row in set (0.01 sec)

**bitnot(integer_type a)**

功能：按位非运算

返回类型：和输入类型相同

举例：

	 mysql> select bitnot(127); /* 01111111 -> 10000000 */
	 +-------------+
	 | bitnot(127) |
	 +-------------+
	 |        -128 |
	 +-------------+
	 1 row in set (0.01 sec)

	 mysql> select bitnot(16); /* 00010000 -> 11101111 */
	 +------------+
	 | bitnot(16) |
	 +------------+
	 |        -17 |
	 +------------+
	 1 row in set (0.01 sec)

	 mysql> select bitnot(0); /* 00000000 -> 11111111 */
	 +-----------+
	 | bitnot(0) |
	 +-----------+
	 |        -1 |
	 +-----------+
	 1 row in set (0.01 sec)

	 mysql> select bitnot(-128); /* 10000000 -> 01111111 */
	 +--------------+
	 | bitnot(-128) |
	 +--------------+
	 |          127 |
	 +--------------+
	 1 row in set (0.01 sec)

**bitor(integer_type a, same_type b)**

功能：按位或运算

返回类型：和输入类型相同

举例：

	 mysql> select bitor(1,4); /* 00000001 | 00000100 */
	 +-------------+
	 | bitor(1, 4) |
	 +-------------+
	 |           5 |
	 +-------------+
	 1 row in set (0.01 sec)

	 mysql> select bitor(16,48); /* 00001000 | 00011000 */
	 +---------------+
	 | bitor(16, 48) |
	 +---------------+
	 |            48 |
	 +---------------+
	 1 row in set (0.01 sec)

	 mysql> select bitor(0,7); /* 00000000 | 00000111 */
	 +-------------+
	 | bitor(0, 7) |
	 +-------------+
	 |           7 |
	 +-------------+
	 1 row in set (0.01 sec)

**bitxor(integer_type a, same_type b)**

功能：按位异或运算

返回类型：和输入类型相同

举例：

	 mysql> select bitxor(0,15); /* 00000000 ^ 00001111 */
	 +---------------+
	 | bitxor(0, 15) |
	 +---------------+
	 |            15 |
	 +---------------+
	 1 row in set (0.01 sec)

	 mysql> select bitxor(7,7); /* 00000111 ^ 00000111 */
	 +--------------+
	 | bitxor(7, 7) |
	 +--------------+
	 |            0 |
	 +--------------+
	 1 row in set (0.01 sec)

	 mysql> select bitxor(8,4); /* 00001000 ^ 00000100 */
	 +--------------+
	 | bitxor(8, 4) |
	 +--------------+
	 |           12 |
	 +--------------+
	 1 row in set (0.01 sec)

	 mysql> select bitxor(3,7); /* 00000011 ^ 00000111 */
	 +--------------+
	 | bitxor(3, 7) |
	 +--------------+
	 |            4 |
	 +--------------+
	 1 row in set (0.01 sec)

## 类型转换函数

**cast(expr as type)**

转换函数通常会和其他函数一同使用，显示的将expression转换成指定的参数类型。Palo对于函数的参数类型有严格的数据类型定义。例如Palo不会自动将bigtint转换成int类型，或者其余可能会损失精度或者产生溢出的转换。用户使用cast函数可以把列值或者字面常量转换成函数参数需要的其他类型。

举例：

	 mysql> select concat('Here are the first ', cast(10 as string), ' results.');
	 +-------------------------------------------------------------------+
	 | concat('Here are the first ', CAST(10 AS CHARACTER), ' results.') |
	 +-------------------------------------------------------------------+
	 | Here are the first 10 results.                                    |
	 +-------------------------------------------------------------------+
	 1 row in set (0.01 sec)

## 日期和时间函数

Palo支持的时间类型是TIMESTAMP，包括DATE和DATETIME两种类型。TIMESTAMP包含date和time两部分，日期和时间函数可以抽取出单个字段，如hour(), minute()。通常这些函数的返回值是整型。格式化日期的函数(如date_add())的返回值是字符串类型。用户可以通过加上或减去时间间隔来改变时间类型的值。时间间隔通常作为date_add()和date_sub()的第二个参数。Palo支持如下的日期和时间函数。

**add_months(timestamp date, int months)**

**add_months(timestamp date, bigint months)**

功能：返回指定date加上months个月的新date。和months_add()相同

返回类型：timestamp类型

举例：

如果这个月的这一日并不存在于目标月中，那么结果将是那个月的最后一天；如果参数中的months是负数，则是求先前的月。

	 mysql> select now(), add_months(now(), 2);
	 +---------------------+---------------------+
	 | now()               | add_months(now(), 2)|
	 +---------------------+---------------------+
	 | 2016-05-31 10:47:00 | 2016-07-31 10:47:00 |
	 +---------------------+---------------------+
	 1 row in set (0.01 sec)

	 mysql> select now(), add_months(now(), 1);
	 +---------------------+---------------------+
	 | now()               | add_months(now(), 1)|
	 +---------------------+---------------------+
	 | 2016-05-31 10:47:14 | 2016-06-30 10:47:14 |
	 +---------------------+---------------------+
	 1 row in set (0.01 sec)

	 mysql> select now(), add_months(now(), -1);
	 +---------------------+----------------------+
	 | now()               | add_months(now(), -1)|
	 +---------------------+----------------------+
	 | 2016-05-31 10:47:31 | 2016-04-30 10:47:31  |
	 +---------------------+----------------------+
	 1 row in set (0.01 sec)

**adddate(timestamp startdate, int days)**

**adddate(timestamp startdate, bigint days)**

功能：给startdate加上指定的天数

返回类型：timestamp类型

举例:

	 mysql> select adddate(date_column, 10) from big_table limit 1;
	 +-------------------------------+
	 |   adddate(date_column, 10)    |
	 +-------------------------------+
	 |          2014-01-11 00:00:00  |
	 +-------------------------------+

**current_timestamp()**

功能：和now()函数功能相同，获取当前的时间

返回类型：timestamp类型

**date_add(timestamp startdate, int days)**

功能：给TIMESTAMP值加上指定的天数。第一个参数可以是字符串，如果字符串符合TIMESTAMP数据类型的格式，该字符串会自动转成TIMESTAMP类型。第二个参数是时间间隔。

返回类型：timestamp类型

**date_format(timestamp day, string fmt)**

功能：将日期类型按照format的类型转化为字符串，当前支持最大128字节的字符串，如果返回长度超过128，则返回NULL。

返回类型：string类型

format的含义如下：

     %a Abbreviated weekday name (Sun..Sat)   
     %b Abbreviated month name (Jan..Dec)    
     %c Month, numeric (0..12)    
     %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)   
     %d Day of the month, numeric (00..31)   
     %e Day of the month, numeric (0..31)    
     %f Microseconds (000000..999999)   
     %H Hour (00..23)   
     %h Hour (01..12)    
     %I Hour (01..12)    
     %i Minutes, numeric (00..59)   
     %j Day of year (001..366)   
     %k Hour (0..23)   
     %l Hour (1..12)    
     %M Month name (January..December)   
     %m Month, numeric (00..12)   
     %p AM or PM    
     %r Time, 12-hour (hh:mm:ss followed by AM or PM)    
     %S Seconds (00..59)    
     %s Seconds (00..59)    
     %T Time, 24-hour (hh:mm:ss)    
     %U Week (00..53), where Sunday is the first day of the week; WEEK() mode 0    
     %u Week (00..53), where Monday is the first day of the week; WEEK() mode 1    
     %V Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X    
     %v Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x   
     %W Weekday name (Sunday..Saturday)    
     %w Day of the week (0=Sunday..6=Saturday)    
     %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V    
     %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v    
     %Y Year, numeric, four digits    
     %y Year, numeric (two digits)   
     %% A literal “%” character   
     %x x, for any “x” not listed above

举例：

	 mysql> select date_format('2009-10-04 22:23:00', '%W %M %Y');
	 +------------------------------------------------+
	 | date_format('2009-10-04 22:23:00', '%W %M %Y') |
	 +------------------------------------------------+
	 | Sunday October 2009                            |
	 +------------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select date_format('2007-10-04 22:23:00', '%H:%i:%s');
	 +------------------------------------------------+
	 | date_format('2007-10-04 22:23:00', '%H:%i:%s') |
	 +------------------------------------------------+
	 | 22:23:00                                       |
	 +------------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
	 +------------------------------------------------------------+
	 | date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j') |
	 +------------------------------------------------------------+
	 | 4th 00 Thu 04 10 Oct 277                                   |
	 +------------------------------------------------------------+
	 1 row in set (0.03 sec)

	 mysql> select date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
	 +------------------------------------------------------------+
	 | date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w') |
	 +------------------------------------------------------------+
	 | 22 22 10 10:23:00 PM 22:23:00 00 6                         |
	 +------------------------------------------------------------+
	 1 row in set (0.00 sec)

**date_sub(timestamp startdate, int days)**

功能：给TIMESTAMP值减去指定的天数。第一个参数可以是字符串，如果字符串符合TIMESTAMP数据类型的格式，该字符串会自动转成成TIMESTAMP类型。第二个参数是时间间隔。

返回类型：timestamp类型

**datediff(string enddate, string startdate)**

功能：返回两个日期的天数差值

返回类型：int类型

**day(string date)**

**dayofmonth(string date)**

功能：返回日期中的天字段

返回类型：int类型

举例:

	 mysql> select dayofmonth('2013-01-21');
	 +-----------------------------------+
	 | dayofmonth('2013-01-21 00:00:00') |
	 +-----------------------------------+
	 |                                21 |
	 +-----------------------------------+
	 1 row in set (0.01 sec)

**days_add(timestamp startdate, int days)**

**days_add(timestamp startdate, bigint days)**

功能：给startdate加上指定的天数，和date_add函数相似，差别在于本函数的参数是TIMESTAMP类型而不是string类型。

返回类型：timestamp类型

**days_sub(timestamp startdate, int days)**

**days_sub(timestamp startdate, bigint days)**

功能：给startdate减去指定的天数，和date_dub函数相似，差别在于本函数的参数是TIMESTAMP类型而不是string类型。

返回类型：timestamp类型

**extract(unit FROM timestamp)**

功能：提取timestamp某个指定单位的值。单位可以为year, month, day, hour, minute或者second

返回类型：int类型

举例：

	 mysql> select now() as right_now,
     ->   extract(year from now()) as this_year,
     ->   extract(month from now()) as this_month;
	 +---------------------+-----------+------------+
	 | right_now           | this_year | this_month |
	 +---------------------+-----------+------------+
	 | 2017-10-16 20:47:28 |      2017 |         10 |
	 +---------------------+-----------+------------+
	 1 row in set (0.01 sec)

	 mysql> select now() as right_now,
    	 ->   extract(day from now()) as this_day,
    	 ->   extract(hour from now()) as this_hour;
	 +---------------------+----------+-----------+
	 | right_now           | this_day | this_hour |
	 +---------------------+----------+-----------+
	 | 2017-10-16 20:47:34 |       16 |        20 |
	 +---------------------+----------+-----------+
	 1 row in set (0.01 sec)



**from_unixtime(bigint unixtime[, string format])**

功能：将unix时间（自1970年1月1日起经过的秒数）转换成相应格式的日期类型

返回类型：字符串类型

使用说明：当前日期格式是大小写敏感的，用户尤其要区分小写m（表达分钟）和大写M（表达月份）。日期字符串的完整型式是"yyyy-MM-dd HH:mm:ss.SSSSSS"，也可以只包含其中部分字段。

举例:

	 mysql> select from_unixtime(100000);
	 +-----------------------+
	 | from_unixtime(100000) |
	 +-----------------------+
	 | 1970-01-02 11:46:40   |
	 +-----------------------+
	 1 row in set (0.01 sec)

	 mysql> select from_unixtime(100000, 'yyyy-MM-dd');
	 +-------------------------------------+
	 | from_unixtime(100000, 'yyyy-MM-dd') |
	 +-------------------------------------+
	 | 1970-01-02                          |
	 +-------------------------------------+
	 1 row in set (0.00 sec)

	 mysql> select from_unixtime(1392394861, 'yyyy-MM-dd');      
	 +-----------------------------------------+
	 | from_unixtime(1392394861, 'yyyy-MM-dd') |
	 +-----------------------------------------+
	 | 2014-02-15                              |
	 +-----------------------------------------+
	 1 row in set (0.00 sec)

unix_timestamp()和from_unixtime()经常结合使用，将时间戳类型转换成指定格式的字符串。

	 mysql> select from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd');
	 +----------------------------------------------------+
	 | from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd') |
	 +----------------------------------------------------+
	 | 2014-01-01                                         |
	 +----------------------------------------------------+

**hour(string date)**

功能：返回字符串所表达日期的小时字段

返回类型：int类型

**hours_add(timestamp date, int hours)**

**hours_add(timestamp date, bigint hours)**

功能：返回指定的日期加上若干小时后的时间

返回类型：timestamp

**hours_sub(timestamp date, int hours)**

**hours_sub(timestamp date, bigint hours)**

功能：返回指定的日期减去若干小时后的时间

返回类型：timestamp

**microseconds_add(timestamp date, int microseconds)**

**microseconds_add(timestamp date, bigint microseconds)**

功能：返回指定的日期加上若干微秒后的时间

返回类型：timestamp

**microseconds_sub(timestamp date, int microseconds)**

**microseconds_sub(timestamp date, bigint microseconds)**

功能：返回指定的日期减去若干微秒后的时间

返回类型：timestamp

**minute(string date)**

功能：返回字符串所表达日期的分钟字段

返回类型：int类型

**minutes_add(timestamp date, int minutes)**

**minutes_add(timestamp date, bigint minutes)**

功能：返回指定的日期加上若干分钟后的时间

返回类型：timestamp

**minutes_sub(timestamp date, int minutes)**

**minutes_sub(timestamp date, bigint minutes)**

功能：返回指定的日期减去若干分钟后的时间

返回类型：timestamp

**month(string date)**

功能：返回字符串所表达的日期的月份字段

返回类型：int类型

**months_add(timestamp date, int months)**

**months_add(timestamp date, bigint months)**

功能：返回指定的日期加上若干月份后的时间

返回类型：timestamp

**months_sub(timestamp date, int months)**

**months_sub(timestamp date, bigint months)**

功能：返回指定的日期减去若干月份后的时间

返回类型：timestamp

**now()**

功能：返回当前的日期和时间（东八区的时区）

返回类型：timestamp

**second(string date)**

功能：返回字符串所表达的日期的秒字段

返回类型：int 类型

**seconds_add(timestamp date, int seconds)**

**seconds_add(timestamp date, bigint seconds)**

功能：返回指定的日期加上若干秒后的时间

返回类型：timestamp

**seconds_sub(timestamp date, int seconds)**

**seconds_sub(timestamp date, bigint seconds)**

功能：返回指定的日期减去若干秒后的时间

返回类型：timestamp

**subdate(timestamp startdate, int days)**

**subdate(timestamp startdate, bigint days)**

功能：从startdate的时间减去若干天后的时间。和date_sub()函数相似，但是本函数的第一个参数是确切的TIMESTAMP，而非可以转成TIMESTAMP类型的字符串。

返回类型：timestamp

**str_to_date(string str, string format)**

功能：通过format指定的方式将str转化为timestamp类型，如果转化结果不对返回NULL。支持的format格式与date_format一致。

返回类型：timestamp

**to_date(timestamp)**

功能：返回timestamp的date域

返回类型：string类型

举例：

	 mysql> select now() as right_now,
     ->   concat('The date today is ',to_date(now()),'.') as date_announcement;
	 +---------------------+-------------------------------+
	 | right_now           | date_announcement             |
	 +---------------------+-------------------------------+
	 | 2017-10-16 21:10:24 | The date today is 2017-10-16. |
	 +---------------------+-------------------------------+
	 1 row in set (0.01 sec)

**unix_timestamp()**

**unix_timestamp(string datetime)**

**unix_timestamp(string datetime, string format)**

**unix_timestamp(timestamp datetime)**

功能：返回当前时间的时间戳（相对1970年1月1日的秒数）或者从一个指定的日期和时间转换成时间戳。返回的时间戳是相对于格林尼治时区的时间戳。

返回类型：bigint类型

**weeks_add(timestamp date, int weeks)**

**weeks_add(timestamp date, bigint weeks)**

功能：返回指定的日期加上若干周后的时间

返回类型：timestamp

**weeksofyear(timestamp date)**

功能：获得一年中的第几周

返回类型：int

**weeks_add(timestamp date, int weeks)**

**weeks_add(timestamp date, bigint weeks)**

功能：返回指定的日期加上若干周后的时间

返回类型：timestamp

**weeks_sub(timestamp date, int weeks)**

**weeks_sub(timestamp date, bigint weeks)**

功能：返回指定的日期减去若干周后的时间

返回类型：timestamp

**year(string date)**

功能：返回字符串所表达的日期的年字段

返回类型：int类型

**years_add(timestamp date, int years)**

**years_add(timestamp date, bigint years)**

功能：返回指定的日期加上若干年后的时间

返回类型：timestamp

**years_sub(timestamp date, int years)**

**years_sub(timestamp date, bigint years)**

功能：返回指定的日期减去若干年的时间

返回类型：timestamp

## 条件函数

**CASE a WHEN b THEN c [WHEN d THEN e]... [ELSE f] END**

功能：将表达式和多个可能的值进行比较，当匹配时返回相应的结果

返回类型：匹配后返回结果的类型

举例:

	 mysql> select case tiny_column when 1 then "tiny_column=1" when 2 then "tiny_column=2" end from small_table limit 2;
	 +-------------------------------------------------------------------------------+
	 | CASE`tiny_column` WHEN 1 THEN 'tiny_column=1' WHEN 2 THEN 'tiny_column=2' END |
	 +-------------------------------------------------------------------------------+
	 | tiny_column=1                                                                 |
	 | tiny_column=2                                                                 |
	 +-------------------------------------------------------------------------------+
	 2 rows in set (0.02 sec)

**if(boolean condition, type ifTrue, type ifFalseOrNull)**

功能：测试一个表达式，根据结果是true还是false返回相应的结果

返回类型：ifTrue表达式结果的类型

举例

	 mysql> select if(tiny_column = 1, "true", "false") from small_table limit 1;
	 +----------------------------------------+
	 | if(`tiny_column` = 1, 'true', 'false') |
	 +----------------------------------------+
	 | true                                   |
	 +----------------------------------------+
	 1 row in set (0.03 sec)

**ifnull(type a, type isNull)**

功能：测试一个表达式，如果表达式是NULL，则返回第二个参数，否则返回第一个参数。

返回类型：第一个参数的类型

举例

	 mysql> select ifnull(1,0);
	 +--------------+
	 | ifnull(1, 0) |
	 +--------------+
	 |            1 |
	 +--------------+
	 1 row in set (0.01 sec)

	 mysql> select ifnull(null,10);
	 +------------------+
	 | ifnull(NULL, 10) |
	 +------------------+
	 |               10 |
	 +------------------+
	 1 row in set (0.01 sec)

**nullif(expr1,expr2)**

功能：如果两个参数相等，则返回NULL。否则返回第一个参数的值。它和以下的CASE WHEN效果一样。

	CASE
		WHEN expr1 = expr2 THEN NULL
		ELSE expr1
	END

返回类型：expr1的类型或者NULL

举例

	 mysql> select nullif(1,1);
	 +--------------+
	 | nullif(1, 1) |
	 +--------------+
	 |         NULL |
	 +--------------+
	 1 row in set (0.00 sec)

	 mysql> select nullif(1,0);
	 +--------------+
	 | nullif(1, 0) |
	 +--------------+
	 |            1 |
	 +--------------+
	 1 row in set (0.01 sec)

## 字符串函数

**ascii(string str)**

功能：返回字符串第一个字符串对应的ascii 码

返回类型：int类型

**concat(string a, string b...)**

功能：将多个字符串连接起来

返回类型：string类型

使用说明：concat()和concat_ws()都是将一行中的多个列合成1个新的列，group_concat()是聚合函数，将不同行的结果合成1个新的列

**concat_ws(string sep, string a, string b...)**

功能：将第二个参数以及后面的参数连接起来，连接符为第一个参数。

返回类型：string类型

举例:

	 mysql> select concat_ws('a', 'b', 'c', 'd');
	 +-------------------------------+
	 | concat_ws('a', 'b', 'c', 'd') |
	 +-------------------------------+
	 | bacad                         |
	 +-------------------------------+
	 1 row in set (0.01 sec)

**find_in_set(string str, string strList)**

功能：返回strlist中出现第一次str的位置（从1开始计数）。strList用逗号分隔多个字符串。如果在strList中没有找到str，则返回0。

返回类型：int类型

举例:

	 mysql> select find_in_set("beijing", "tianji,beijing,shanghai");
	 +---------------------------------------------------+
	 | find_in_set('beijing', 'tianji,beijing,shanghai') |
	 +---------------------------------------------------+
	 |                                                 2 |
	 +---------------------------------------------------+
	 1 row in set (0.00 sec)

**group_concat(string s [, string sep])**

功能：该函数是类似于sum()的聚合函数，group_concat将结果集中的多行结果连接成一个字符串。第二个参数为字符串之间的连接符号，该参数可以省略。该函数通常需要和group by 语句一起使用。

返回类型：string类型

**instr(string str, string substr)**

功能：返回substr在str中第一次出现的位置（从1开始计数）。如果substr不在str中出现，则返回0。

返回类型：int类型

举例：

	 mysql> select instr('foo bar bletch', 'b');
	 +------------------------------+
	 | instr('foo bar bletch', 'b') |
	 +------------------------------+
	 |                            5 |
	 +------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select instr('foo bar bletch', 'z');
	 +------------------------------+
	 | instr('foo bar bletch', 'z') |
	 +------------------------------+
	 |                            0 |
	 +------------------------------+
	 1 row in set (0.01 sec)


**length(string a)**

功能：返回字符串的长度

返回类型：int类型

**locate(string substr, string str[, int pos])**

功能：返回substr在str中出现的位置（从1开始计数）。如果指定第3个参数，则从str以pos下标开始的字符串处开始查找substr出现的位置。

返回类型：int类型

举例:

	 mysql> select locate('bj', 'where is bj', 10);
	 +---------------------------------+
	 | locate('bj', 'where is bj', 10) |
	 +---------------------------------+
	 |                              10 |
	 +---------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select locate('bj', 'where is bj', 11);
	 +---------------------------------+
	 | locate('bj', 'where is bj', 11) |
	 +---------------------------------+
	 |                               0 |
	 +---------------------------------+
	 1 row in set (0.01 sec)

**lower(string a)**

**lcase(string a)**

功能：将参数中所有的字符串都转换成小写

返回类型：string类型

**lpad(string str, int len, string pad)**

功能：返回str中长度为len（从首字母开始算起）的字符串。如果len大于str的长度，则在str的前面不断补充pad字符，直到该字符串的长度达到len为止。如果len小于str的长度，该函数相当于截断str字符串，只返回长度为len的字符串。

返回类型：string类型

举例:

	 mysql> select lpad("hello", 10, 'xy');
	 +-------------------------+
	 | lpad('hello', 10, 'xy') |
	 +-------------------------+
	 | xyxyxhello              |
	 +-------------------------+
	 1 row in set (0.01 sec)

**ltrim(string a)**

功能：将参数中从开始部分连续出现的空格去掉。

返回类型：string类型

**regexp_extract(string subject, string pattern, int index)**

功能：字符串正则匹配。index为0返回整个匹配的字符串，index为1，2，……，返回第一，第二，……部分。

返回类型：string类型

举例：

	 mysql> select regexp_extract('AbcdBCdefGHI','.*?([[:lower:]]+)',1);
	 +--------------------------------------------------------+
	 | regexp_extract('AbcdBCdefGHI', '.*?([[:lower:]]+)', 1) |
	 +--------------------------------------------------------+
	 | def                                                    |
	 +--------------------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select regexp_extract('AbcdBCdefGHI','.*?([[:lower:]]+).*?',1);
	 +-----------------------------------------------------------+
	 | regexp_extract('AbcdBCdefGHI', '.*?([[:lower:]]+).*?', 1) |
	 +-----------------------------------------------------------+
	 | bcd                                                       |
	 +-----------------------------------------------------------+
	 1 row in set (0.01 sec)

**regexp_replace(string initial, string pattern, string replacement)**

功能：用replacement替换initial字符串中匹配pattern的部分。

返回类型：string类型

举例：

	 mysql> select regexp_replace('aaabbbaaa','b+','xyz');
	 +------------------------------------------+
	 | regexp_replace('aaabbbaaa', 'b+', 'xyz') |
	 +------------------------------------------+
	 | aaaxyzaaa                                |
	 +------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select regexp_replace('aaabbbaaa','(b+)','<\\1>');
	 +---------------------------------------------+
	 | regexp_replace('aaabbbaaa', '(b+)', '<\1>') |
	 +---------------------------------------------+
	 | aaa<bbb>aaa                                 |
	 +---------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select regexp_replace('123-456-789','[^[:digit:]]','');
	 +---------------------------------------------------+
	 | regexp_replace('123-456-789', '[^[:digit:]]', '') |
	 +---------------------------------------------------+
	 | 123456789                                         |
	 +---------------------------------------------------+
	 1 row in set (0.01 sec) 

**repeat(string str, int n)**

功能：返回字符串str重复n次的结果

返回类型：string类型

举例：

	 mysql> select repeat("abc", 3);
	 +------------------+
	 | repeat('abc', 3) |
	 +------------------+
	 | abcabcabc        |
	 +------------------+
	 1 row in set (0.01 sec)

**reverse(string a)**

功能：将字符串反转

返回类型：string类型

**rpad(string str, int len, string pad)**

功能：返回str中长度为len（从首字母开始算起）的字符串。如果len大于str的长度，则在str 的后面不断补充pad字符，直到该字符串的长度达到len 为止。如果len小于str的长度，该函数相当于截断str字符串，只返回长度为len的字符串。

返回类型：string类型

举例:

	 mysql> select rpad("hello", 10, 'xy');  
	 +-------------------------+
	 | rpad('hello', 10, 'xy') |
	 +-------------------------+
	 | helloxyxyx              |
	 +-------------------------+
	 1 row in set (0.00 sec)

**rtrim(string a)**

功能：将参数中从右侧部分部分连续出现的空格去掉。

返回类型：string类型

**space(int n)**

功能：返回n个空格的字符串

返回类型：string类型

**strleft(string a, int num_chars)**

功能：返回字符串中最左边的num_chars个字符。

返回类型：string类型

**strright(string a, int num_chars)**

功能：返回字符串中最右边的num_chars个字符。

返回类型：string类型

**substr(string a, int start [, int len])**

**substring(string a, int start[, int len])**

功能：求子串函数，返回第一个参数描述的字符串中从start开始长度为len的部分字符串。首字母的下标为1。

返回类型：string类型

**trim(string a)**

功能：将参数中右侧部分连续出现的空格和左侧部分连续出现的空格都去掉。该函数的效果和同时使用ltrim()和rtrim()的效果是一样的。

返回类型：string类型

**upper(string a)**

**ucase(string a)**

功能：将字符串所有字母都转换成大写。

返回类型：string类型

## 聚合函数

**AVG函数**

功能：该聚合函数返回集合中的平均数。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL值的行将被忽略。如果该表是空的或者AVG 的参数都是NULL，则该函数返回NULL。当查询指定使用GROUP BY从句时，则每个group by的值都会返回1条结果。

返回类型： double类型

**COUNT函数**

功能： 该聚合函数返回满足要求的行的数目，或者非NULL行的数目。COUNT(\*) 会计算包含NULL 值的行。COUNT(column_name)仅会计算非NULL值的行。用户可以同时使用COUNT函数和DISTINCT操作符，count(distinct col_name...)会先对数据去重，然后再计算多个列的组合出现的次数。

返回类型：int类型

举例:

	 mysql> select count(distinct tiny_column, short_column) from small_table;
	 +-----------------------------------------------+
	 | count(DISTINCT `tiny_column`, `short_column`) |
	 +-----------------------------------------------+
	 |                                             2 |
	 +-----------------------------------------------+
	 1 row in set (0.08 sec)

**MAX函数**

功能：该聚合函数返回集合中的最大值。该函数和min函数的功能相反。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL值的行将被忽略。如果该表是空的或者MAX的参数都是NULL，则该函数返回NULL。当查询指定使用GROUP BY从句时，则每个group by的值都会返回1条结果。

返回类型：和输入参数相同的类型。

**MIN函数**

功能：该聚合函数返回集合中的最小值。该函数和max函数的功能相反。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL 值的行将被忽略。如果该表是空的或者MIN 的参数都是NULL，则该函数返回NULL。当查询指定使用GROUP BY从句时，则每个group by的值都会返回1条结果。

返回类型：和输入参数相同的类型。

**SUM函数**

功能：该聚合函数返回集合中所有值的和。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL值的行将被忽略。如果该表是空的或者MIN的参数都是NULL，则该函数返回NULL。当查询指定使用GROUP BY从句时，则每个group by的值都会返回1条结果。

返回类型：如果参数整型，则返回BIGINT，如果参数是浮点型则返回double类型

**GROUP_CONCAT函数**

功能：该聚合函数会返回1个字符串，该字符串是集合中所有字符串连接起来形成的新字符串。如果用户指定分隔符，则分隔符用来连接两个相邻行的字符串。

返回类型：string类型

使用说明：默认情况下，该函数返回1个覆盖所有结果集的字符串。当查询指定使用group by 从句时，则每个group by的值都会返回1条结果。concat()和concat_ws()都是将一行中的多个列合成1个新的列，group_concat()是聚合函数，将不同行的结果合成1个新的列。

**方差函数**

语法：

	 VARIANCE | VAR[IANCE]_SAMP | VAR[IANCE]_POP

功能：该类聚合函数返回一组数的方差。这是一个数学属性，它表示值与平均值之间的距离。它作用于数值类型。VARIANCE_SAMP()和VARIANCE_POP()分别计算样本方差和总体方差，VARIANCE()是VARIANCE_SAMP()的别名。VAR_SAMP()和VAR_POP()分别是VARIANCE_SAMP()和VARIANCE_POP()是别名。

返回类型：double类型

**标准差函数**

语法：

	 STDDEV | STDDEV_SAMP | STDDEV_POP

功能：该类聚合函数返回一组数的标准差。它作用于数值类型。STDDEV_POP()和STDDEV_SAMP()分别计算总体标准差和样本标准差。STDDEV()是STDDEV_SAMP()的别名。

返回类型：double类型


## json解析函数 ##

palo目前支持3个json解析函数

-	get_json_int（string，string）
	
-	get_json_string（string，string）
	
-	get_json_double（string，string）

其中第一个参数为json字符串，第二个参数为json内的路径

举例：

	 mysql> select get_json_int('{"col1":100, "col2":"string", "col3":1.5}', "$.col1");
	 +---------------------------------------------------------------------+
	 | get_json_int('{"col1":100, "col2":"string", "col3":1.5}', '$.col1') |
	 +---------------------------------------------------------------------+
	 |                                                                 100 |
	 +---------------------------------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select get_json_string('{"col1":100, "col2":"string", "col3":1.5}', "$.col2");   
	 +------------------------------------------------------------------------+
	 | get_json_string('{"col1":100, "col2":"string", "col3":1.5}', '$.col2') |
	 +------------------------------------------------------------------------+
	 | string                                                                 |
	 +------------------------------------------------------------------------+
	 1 row in set (0.01 sec)

	 mysql> select get_json_double('{"col1":100, "col2":"string", "col3":1.5}', "$.col3");      
	 +------------------------------------------------------------------------+
	 | get_json_double('{"col1":100, "col2":"string", "col3":1.5}', '$.col3') |
	 +------------------------------------------------------------------------+
	 |                                                                    1.5 |
	 +------------------------------------------------------------------------+
	 1 row in set (0.01 sec)

## HLL函数 ##

HLL是基于HyperLogLog算法的工程实现，用于保存HyperLogLog计算过程的中间结果，它只能作为表的value列类型、通过聚合来不断的减少数据量，以此来实现加快查询的目的，基于它得到的是一个估算结果，误差大概在1%左右，hll列是通过其它列或者导入数据里面的数据生成的，导入的时候通过hll_hash函数来指定数据中哪一列用于生成hll列，它常用于替代count distinct，通过结合rollup在业务上用于快速计算uv等
		
**HLL_UNION_AGG(hll)**
	
此函数为聚合函数，用于计算满足条件的所有数据的基数估算。
	
**HLL_CARDINALITY(hll)**
	
此函数用于计算单条hll列的基数估算
	
**HLL_HASH(column_name)**
	
生成HLL列类型，用于insert或导入的时候，导入的使用见相关说明

example:(仅为说明使用方式)
	
1、首先创建一张含有hll列的表：
	
		create table test(		
		time date, 		
		id int, 		
		name char(10), 		
		province char(10), 		
		os char(1), 		
		set1 hll hll_union, 		
		set2 hll hll_union) 		
		distributed by hash(id) buckets 32;
		
2、导入数据，导入的方式见相关help mini load
	
	（1）使用表中的列生成hll列	
	    curl --location-trusted -uname:password -T data http://host/api/test_db/test/_load?label=load_1\&hll=set1,id:set2,name	    
	（2）使用数据中的某一列生成hll列	    
	    curl --location-trusted -uname:password -T data http://host/api/test_db/test/_load?label=load_1\&hll=set1,cuid:set2,os\&columns=time,id,name,province,sex,cuid,os
	    
3、聚合数据，常用方式3种：（如果不聚合直接对base表查询，速度可能跟直接使用ndv速度差不多）
	
	（1）创建一个rollup，让hll列产生聚合，	
		alter table test add rollup test_rollup(date, set1);		
	（2）创建另外一张专门计算uv的表，然后insert数据）	
		create table test_uv(		
		time date,		
		uv_set hll hll_union)		
		distributed by hash(id) buckets 32;		
		insert into test_uv select date, set1 from test;		
	（3）创建另外一张专门计算uv的表，然后insert并通过hll_hash根据test其它非hll列生成hll列						
		create table test_uv(		
		time date,		
		id_set hll hll_union)		
		distributed by hash(id) buckets 32;		
		insert into test_uv select date, hll_hash(id) from test;
			
4、查询，hll列不允许直接查询它的原始值，可以通过配套的函数进行查询
	
	（1）求总uv		
		select HLL_UNION_AGG(uv_set) from test_uv;			
	（2）求每一天的uv		
		select HLL_CARDINALITY(uv_set) from test_uv;

## 分析函数（窗口函数）

### 分析函数介绍

分析函数是一类特殊的内置函数。和聚合函数类似，分析函数也是对于多个输入行做计算得到一个数据值。不同的是，分析函数是在一个特定的窗口内对输入数据做处理，而不是按照group by来分组计算。每个窗口内的数据可以用over()从句进行排序和分组。分析函数会对结果集的每一行计算出一个单独的值，而不是每个group by分组计算一个值。这种灵活的方式允许用户在select从句中增加额外的列，给用户提供了更多的机会来对结果集进行重新组织和过滤。分析函数只能出现在select列表和最外层的order by从句中。在查询过程中，分析函数会在最后生效，就是说，在执行完join，where和group by等操作之后再执行。分析函数在金融和科学计算领域经常被使用到，用来分析趋势、计算离群值以及对大量数据进行分桶分析等。

分析函数的语法：

    function(args) OVER(partition_by_clause order_by_clause [window_clause])    
    partition_by_clause ::= PARTITION BY expr [, expr ...]    
    order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]

window_clause: 见后面[Window Clause](http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_analytic_functions.html#window_clause_unique_1)

**Function**

目前支持的Function包括AVG(), COUNT(), DENSE_RANK(), FIRST_VALUE(), LAG(), LAST_VALUE(), LEAD(), MAX(), MIN(), RANK(), ROW_NUMBER()和SUM()。

**PARTITION BY从句**

Partition By从句和Group By类似。它把输入行按照指定的一列或多列分组，相同值的行会被分到一组。

**ORDER BY从句**

Order By从句和外层的Order By基本一致。它定义了输入行的排列顺序，如果指定了Partition By，则Order By定义了每个Partition分组内的顺序。与外层Order By的唯一不同点是，OVER从句中的Order By n（n是正整数）相当于不做任何操作，而外层的Order By n表示按照第n列排序。

举例:

这个例子展示了在select列表中增加一个id列，它的值是1，2，3等等，顺序按照events表中的date_and_time列排序。

    SELECT   
    row_number() OVER (ORDER BY date_and_time) AS id,   
    c1, c2, c3, c4   
    FROM events;

**Window从句**

Window从句用来为分析函数指定一个运算范围，以当前行为准，前后若干行作为分析函数运算的对象。Window从句支持的方法有：AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE()和SUM()。对于 MAX()和MIN(), window从句可以指定开始范围UNBOUNDED PRECEDING

语法:

    ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]

举例：

假设我们有如下的股票数据，股票代码是JDR，closing price是每天的收盘价。

    create table stock_ticker (stock_symbol string, closing_price decimal(8,2), closing_date timestamp);    
    ...load some data...    
    select * from stock_ticker order by stock_symbol, closing_date
	 | stock_symbol | closing_price | closing_date |
	 |--------------|---------------|---------------------|
	 | JDR | 12.86 | 2014-10-02 00:00:00 |
	 | JDR | 12.89 | 2014-10-03 00:00:00 |
	 | JDR | 12.94 | 2014-10-04 00:00:00 |
	 | JDR | 12.55 | 2014-10-05 00:00:00 |
	 | JDR | 14.03 | 2014-10-06 00:00:00 |
	 | JDR | 14.75 | 2014-10-07 00:00:00 |
	 | JDR | 13.98 | 2014-10-08 00:00:00 |

这个查询使用分析函数产生moving_average这一列，它的值是3天的股票均价，即前一天、当前以及后一天三天的均价。第一天没有前一天的值，最后一天没有后一天的值，所以这两行只计算了两天的均值。这里Partition By没有起到作用，因为所有的数据都是JDR的数据，但如果还有其他股票信息，Partition By会保证分析函数值作用在本Partition之内。

    select stock_symbol, closing_date, closing_price,    
    avg(closing_price) over (partition by stock_symbol order by closing_date    
    rows between 1 preceding and 1 following) as moving_average    
    from stock_ticker;
	 | stock_symbol | closing_date | closing_price | moving_average |
	 |--------------|---------------------|---------------|----------------|
	 | JDR | 2014-10-02 00:00:00 | 12.86 | 12.87 |
	 | JDR | 2014-10-03 00:00:00 | 12.89 | 12.89 |
	 | JDR | 2014-10-04 00:00:00 | 12.94 | 12.79 |
	 | JDR | 2014-10-05 00:00:00 | 12.55 | 13.17 |
	 | JDR | 2014-10-06 00:00:00 | 14.03 | 13.77 |
	 | JDR | 2014-10-07 00:00:00 | 14.75 | 14.25 |
	 | JDR | 2014-10-08 00:00:00 | 13.98 | 14.36 |

### Function使用举例

本节介绍Palo中可以用作分析函数的方法。

#### AVG()

语法：

AVG([DISTINCT | ALL] *expression*) [OVER (*analytic_clause*)]

举例：

计算当前行和它前后各一行数据的x平均值

    select x, property,    
    avg(x) over    
    (   
    partition by property    
    order by x    
    rows between 1 preceding and 1 following    
    ) as 'moving average'    
    from int_t where property in ('odd','even');
	 | x | property | moving average |
	 |----|----------|----------------|
	 | 2 | even | 3 |
	 | 4 | even | 4 |
	 | 6 | even | 6 |
	 | 8 | even | 8 |
	 | 10 | even | 9 |
	 | 1 | odd | 2 |
	 | 3 | odd | 3 |
	 | 5 | odd | 5 |
	 | 7 | odd | 7 |
	 | 9 | odd | 8 |

#### COUNT()

语法：


    COUNT([DISTINCT | ALL] expression) [OVER (analytic_clause)]

举例：

计算从当前行到第一行x出现的次数。

    select x, property,   
    count(x) over   
    (   
    partition by property    
    order by x    
    rows between unbounded preceding and current row    
    ) as 'cumulative total'    
    from int_t where property in ('odd','even');
	 | x | property | cumulative count |
	 |----|----------|------------------|
	 | 2 | even | 1 |
	 | 4 | even | 2 |
	 | 6 | even | 3 |
	 | 8 | even | 4 |
	 | 10 | even | 5 |
	 | 1 | odd | 1 |
	 | 3 | odd | 2 |
	 | 5 | odd | 3 |
	 | 7 | odd | 4 |
	 | 9 | odd | 5 |

#### DENSE_RANK()

DENSE_RANK()函数用来表示排名，与RANK()不同的是，DENSE_RANK()不会出现空缺数字。比如，如果出现了两个并列的1，DENSE_RANK()的第三个数仍然是2，而RANK()的第三个数是3。

语法：

    DENSE_RANK() OVER(partition_by_clause order_by_clause)

举例：

下例展示了按照property列分组对x列排名：

     select x, y, dense_rank() over(partition by x order by y) as rank from int_t;
	 | x | y | rank |
	 |----|------|----------|
	 | 1 | 1 | 1 |
	 | 1 | 2 | 2 |
	 | 1 | 2 | 2 |
	 | 2 | 1 | 1 |
	 | 2 | 2 | 2 |
	 | 2 | 3 | 3 |
	 | 3 | 1 | 1 |
	 | 3 | 1 | 1 |
	 | 3 | 2 | 2 |

#### FIRST_VALUE()

FIRST_VALUE()返回窗口范围内的第一个值。

语法：

    FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])

举例：

我们有如下数据

     select name, country, greeting from mail_merge;
	 | name | country | greeting |
	 |---------|---------|--------------|
	 | Pete | USA | Hello |
	 | John | USA | Hi |
	 | Boris | Germany | Guten tag |
	 | Michael | Germany | Guten morgen |
	 | Bjorn | Sweden | Hej |
	 | Mats | Sweden | Tja |

使用FIRST_VALUE()，根据country分组，返回每个分组中第一个greeting的值：

    select country, name,    
    first_value(greeting)    
    over (partition by country order by name, greeting) as greeting from mail_merge;
	| country | name | greeting |
	|---------|---------|-----------|
	| Germany | Boris | Guten tag |
	| Germany | Michael | Guten tag |
	| Sweden | Bjorn | Hej |
	| Sweden | Mats | Hej |
	| USA | John | Hi |
	| USA | Pete | Hi |

#### LAG()

LAG()方法用来计算当前行向前数若干行的值。

语法：

    LAG (expr, offset, default) OVER (partition_by_clause order_by_clause)

举例：

计算前一天的收盘价

    select stock_symbol, closing_date, closing_price,    
    lag(closing_price,1, 0) over (partition by stock_symbol order by closing_date) as "yesterday closing"   
    from stock_ticker   
    order by closing_date;
	| stock_symbol | closing_date | closing_price | yesterday closing |
	|--------------|---------------------|---------------|-------------------|
	| JDR | 2014-09-13 00:00:00 | 12.86 | 0 |
	| JDR | 2014-09-14 00:00:00 | 12.89 | 12.86 |
	| JDR | 2014-09-15 00:00:00 | 12.94 | 12.89 |
	| JDR | 2014-09-16 00:00:00 | 12.55 | 12.94 |
	| JDR | 2014-09-17 00:00:00 | 14.03 | 12.55 |
	| JDR | 2014-09-18 00:00:00 | 14.75 | 14.03 |
	| JDR | 2014-09-19 00:00:00 | 13.98 | 14.75

#### LAST_VALUE()

LAST_VALUE()返回窗口范围内的最后一个值。与FIRST_VALUE()相反。

语法：

    LAST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])

使用FIRST_VALUE()举例中的数据：

    select country, name,    
    last_value(greeting)   
    over (partition by country order by name, greeting) as greeting   
    from mail_merge;
	| country | name | greeting |
	|---------|---------|--------------|
	| Germany | Boris | Guten morgen |
	| Germany | Michael | Guten morgen |
	| Sweden | Bjorn | Tja |
	| Sweden | Mats | Tja |
	| USA | John | Hello |
	| USA | Pete | Hello

#### LEAD()

LEAD()方法用来计算当前行向后数若干行的值。

语法：

    LEAD (expr, offset, default]) OVER (partition_by_clause order_by_clause)

举例：

计算第二天的收盘价对比当天收盘价的走势，即第二天收盘价比当天高还是低。
    
    select stock_symbol, closing_date, closing_price,    
    case   
    (lead(closing_price,1, 0)   
    over (partition by stock_symbol order by closing_date)-closing_price) > 0   
    when true then "higher"   
    when false then "flat or lower"    
    end as "trending"   
    from stock_ticker    
    order by closing_date;
	| stock_symbol | closing_date | closing_price | trending |
	|--------------|---------------------|---------------|---------------|
	| JDR | 2014-09-13 00:00:00 | 12.86 | higher |
	| JDR | 2014-09-14 00:00:00 | 12.89 | higher |
	| JDR | 2014-09-15 00:00:00 | 12.94 | flat or lower |
	| JDR | 2014-09-16 00:00:00 | 12.55 | higher |
	| JDR | 2014-09-17 00:00:00 | 14.03 | higher |
	| JDR | 2014-09-18 00:00:00 | 14.75 | flat or lower |
	| JDR | 2014-09-19 00:00:00 | 13.98 | flat or lower |

#### MAX()

语法：

    MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]

举例：

计算从第一行到当前行之后一行的最大值
    
    select x, property,   
    max(x) over    
    (   
    order by property, x    
    rows between unbounded preceding and 1 following    
    ) as 'local maximum'    
    from int_t where property in ('prime','square');
	| x | property | local maximum |
	|---|----------|---------------|
	| 2 | prime | 3 |
	| 3 | prime | 5 |
	| 5 | prime | 7 |
	| 7 | prime | 7 |
	| 1 | square | 7 |
	| 4 | square | 9 |
	| 9 | square | 9 |

#### MIN()

语法：

    MIN([DISTINCT | ALL] expression) [OVER (analytic_clause)]

举例：

计算从第一行到当前行之后一行的最小值

    select x, property,   
    min(x) over    
    (    
    order by property, x desc    
    rows between unbounded preceding and 1 following   
    ) as 'local minimum'   
    from int_t where property in ('prime','square');
	| x | property | local minimum |
	|---|----------|---------------|
	| 7 | prime | 5 |
	| 5 | prime | 3 |
	| 3 | prime | 2 |
	| 2 | prime | 2 |
	| 9 | square | 2 |
	| 4 | square | 1 |
	| 1 | square | 1 |

#### RANK()

RANK()函数用来表示排名，与DENSE_RANK()不同的是，RANK()会出现空缺数字。比如，如果出现了两个并列的1， RANK()的第三个数就是3，而不是2。

语法：

    RANK() OVER(partition_by_clause order_by_clause)

举例：

根据x列进行排名

    select x, y, rank() over(partition by x order by y) as rank from int_t;
	| x | y | rank |
	|----|------|----------|
	| 1 | 1 | 1 |
	| 1 | 2 | 2 |
	| 1 | 2 | 2 |
	| 2 | 1 | 1 |
	| 2 | 2 | 2 |
	| 2 | 3 | 3 |
	| 3 | 1 | 1 |
	| 3 | 1 | 1 |
	| 3 | 2 | 3 |

#### ROW_NUMBER()

为每个Partition的每一行返回一个从1开始连续递增的整数。与RANK()和DENSE_RANK()不同的是，ROW_NUMBER()返回的值不会重复也不会出现空缺，是连续递增的。

语法：

    ROW_NUMBER() OVER(partition_by_clause order_by_clause)

举例：

    select x, y, row_number() over(partition by x order by y) as rank from int_t;
	| x | y | rank |
	|---|------|----------|
	| 1 | 1 | 1 |
	| 1 | 2 | 2 |
	| 1 | 2 | 3 |
	| 2 | 1 | 1 |
	| 2 | 2 | 2 |
	| 2 | 3 | 3 |
	| 3 | 1 | 1 |
	| 3 | 1 | 2 |
	| 3 | 2 | 3 |

#### SUM()

语法：

    SUM([DISTINCT | ALL] expression) [OVER (analytic_clause)]

举例：

按照property进行分组，在组内计算当前行以及前后各一行的x列的和。

    select x, property,   
    sum(x) over    
    (   
    partition by property   
    order by x   
    rows between 1 preceding and 1 following    
    ) as 'moving total'    
    from int_t where property in ('odd','even');
	| x | property | moving total |
	|----|----------|--------------|
	| 2 | even | 6 |
	| 4 | even | 12 |
	| 6 | even | 18 |
	| 8 | even | 24 |
	| 10 | even | 18 |
	| 1 | odd | 4 |
	| 3 | odd | 9 |
	| 5 | odd | 15 |
	| 7 | odd | 21 |
	| 9 | odd | 16 |

# 注释
Palo支持SQL注释

-   单行注释： 以--开头的语句会被识别成注释并且被忽略掉。单行注释可以独立成行或者出现在其他语句的部分语句或者完整语句之后。

-   多行注释：'/\*' 和'\*/'之间的文本会被识别成注释并且被忽略掉。多行注释可以单独占据单行或多行，或者出现在其他语句的中间，前面或者后面。

举例：

	 mysql> -- This line is a comment about a query
	 mysql> select ...
	 mysql> /*
	    /*> This is a multi-line comment about a query.
	    /*> */
	 mysql> select ...
	 mysql> select * from t /* This is an embedded comment about a query. */ limit 1;
	 mysql> select * from t -- This is an embedded comment about a multi-line command.
    	 -> limit 1;
