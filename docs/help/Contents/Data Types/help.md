# BOOLEAN
## description
    BOOL, BOOLEN
    与TINYINT一样，0代表false，1代表true

# TINYINT
## description
    TINYINT
    1字节有符号整数，范围[-128, 127]

# SMALLINT
## description
    SMALLINT
    2字节有符号整数，范围[-32768, 32767]

# INT
## description
    INT
    4字节有符号整数，范围[-2147483648, 2147483647]

# BIGINT
## description
    BIGINT
    8字节有符号整数，范围[-9223372036854775808, 9223372036854775807]

# FLOAT
## description
    FLOAT
    4字节浮点数

# DOUBLE
## description
    DOUBLE
    8字节浮点数

# DECIMAL
## description
    DECIMAL(M[,D])
    高精度定点数，M代表一共有多少个有效数字(precision)，D代表小数点后最多有多少数字(scale)
    M的范围是[1,27], D的范围[1, 9], 另外，M必须要大于等于D的取值。默认的D取值为0

# CHAR
## description
    CHAR(M)
    定长字符串，M代表的是定长字符串的长度。M的范围是1-255

# VARCHAR
## description
    VARCHAR(M)
    变长字符串，M代表的是变长字符串的长度。M的范围是1-65535

# DATE
## description
    DATE函数
        Syntax:
            DATE(expr) 
        将输入的类型转化为DATE类型
    DATE类型
        日期类型，目前的取值范围是['1900-01-01', '9999-12-31'], 默认的打印形式是'YYYY-MM-DD'

## example
    mysql> SELECT DATE('2003-12-31 01:02:03');
        -> '2003-12-31'

# DATETIME
## description
    DATETIME
    日期时间类型，取值范围是['1000-01-01 00:00:00', '9999-12-31 23:59:59'].
    打印的形式是'YYYY-MM-DD HH:MM:SS'

# HLL(HyperLogLog)
## description
    VARCHAR(M)
    变长字符串，M代表的是变长字符串的长度。M的范围是1-16385
    用户不需要指定长度和默认值。长度根据数据的聚合程度系统内控制
    并且HLL列只能通过配套的hll_union_agg、hll_raw_agg、hll_cardinality、hll_hash进行查询或使用

