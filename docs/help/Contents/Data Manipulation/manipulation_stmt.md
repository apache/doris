# LOAD
## description

    Palo 目前支持以下4种导入方式：

    1. Hadoop Load：基于 MR 进行 ETL 的导入。
    2. Broker Load：使用 broker 进行进行数据导入。
    3. Mini Load：通过 http 协议上传文件进行批量数据导入。
    4. Stream Load：通过 http 协议进行流式数据导入。

    本帮助主要描述第一种导入方式，即 Hadoop Load 相关帮助信息。其余导入方式可以使用以下命令查看帮助：

    !!!该导入方式可能在后续某个版本即不再支持，建议使用其他导入方式进行数据导入。!!!
    
    1. help broker load;
    2. help mini load;
    3. help stream load;

    Hadoop Load 仅适用于百度内部环境。公有云、私有云以及开源环境无法使用这种导入方式。
    该导入方式必须设置用于 ETL 的 Hadoop 计算队列，设置方式可以通过 help set property 命令查看帮助。

    Stream load 暂时只支持百度内部用户使用。开源社区和公有云用户将在后续版本更新中支持。

语法：

    LOAD LABEL load_label
    (
    data_desc1[, data_desc2, ...]
    )
    [opt_properties];

    1. load_label

        当前导入批次的标签。在一个 database 内唯一。
        语法：
        [database_name.]your_label
     
    2. data_desc

        用于描述一批导入数据。
        语法：
            DATA INFILE
            (
            "file_path1"[, file_path2, ...]
            )
            [NEGATIVE]
            INTO TABLE `table_name`
            [PARTITION (p1, p2)]
            [COLUMNS TERMINATED BY "column_separator"]
            [FORMAT AS "file_type"]
            [(column_list)]
            [SET (k1 = func(k2))]
    
        说明：
            file_path: 

            文件路径，可以指定到一个文件，也可以用 * 通配符指定某个目录下的所有文件。通配符必须匹配到文件，而不能是目录。

            PARTITION:

            如果指定此参数，则只会导入指定的分区，导入分区以外的数据会被过滤掉。
            如果不指定，默认导入table的所有分区。
        
            NEGATIVE：
            如果指定此参数，则相当于导入一批“负”数据。用于抵消之前导入的同一批数据。
            该参数仅适用于存在 value 列，并且 value 列的聚合类型仅为 SUM 的情况。
            
            column_separator：

            用于指定导入文件中的列分隔符。默认为 \t
            如果是不可见字符，则需要加\\x作为前缀，使用十六进制来表示分隔符。
            如hive文件的分隔符\x01，指定为"\\x01"
            
            file_type：

            用于指定导入文件的类型，例如：parquet、csv。默认值通过文件后缀名判断。 
 
            column_list：

            用于指定导入文件中的列和 table 中的列的对应关系。
            当需要跳过导入文件中的某一列时，将该列指定为 table 中不存在的列名即可。
            语法：
            (col_name1, col_name2, ...)
            
            SET:

            如果指定此参数，可以将源文件某一列按照函数进行转化，然后将转化后的结果导入到table中。
            目前支持的函数有：

                strftime(fmt, column) 日期转换函数
                    fmt: 日期格式，形如%Y%m%d%H%M%S (年月日时分秒)
                    column: column_list中的列，即输入文件中的列。存储内容应为数字型的时间戳。
                        如果没有column_list，则按照palo表的列顺序默认输入文件的列。

                time_format(output_fmt, input_fmt, column) 日期格式转化
                    output_fmt: 转化后的日期格式，形如%Y%m%d%H%M%S (年月日时分秒)
                    input_fmt: 转化前column列的日期格式，形如%Y%m%d%H%M%S (年月日时分秒)
                    column: column_list中的列，即输入文件中的列。存储内容应为input_fmt格式的日期字符串。
                        如果没有column_list，则按照palo表的列顺序默认输入文件的列。

                alignment_timestamp(precision, column) 将时间戳对齐到指定精度
                    precision: year|month|day|hour
                    column: column_list中的列，即输入文件中的列。存储内容应为数字型的时间戳。
                        如果没有column_list，则按照palo表的列顺序默认输入文件的列。
                        注意：对齐精度为year、month的时候，只支持20050101~20191231范围内的时间戳。

                default_value(value) 设置某一列导入的默认值
                    不指定则使用建表时列的默认值

                md5sum(column1, column2, ...) 将指定的导入列的值求md5sum，返回32位16进制字符串                                

                replace_value(old_value[, new_value]) 将导入文件中指定的old_value替换为new_value
                    new_value如不指定则使用建表时列的默认值
                    
                hll_hash(column) 用于将表或数据里面的某一列转化成HLL列的数据结构
            
    3. opt_properties

        用于指定一些特殊参数。
        语法：
        [PROPERTIES ("key"="value", ...)]
        
        可以指定如下参数：
        cluster:          导入所使用的 Hadoop 计算队列。
        timeout：         指定导入操作的超时时间。默认超时为3天。单位秒。
        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。
        load_delete_flag：指定该导入是否通过导入key列的方式删除数据，仅适用于UNIQUE KEY，
                          导入时可不指定value列。默认为false。

    5. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）
        字符串类（CHAR/VARCHAR）："I am a student", "a"
        NULL值：\N

## example

    1. 导入一批数据，指定超时时间和过滤比例。指定导入队列为 my_cluster。

        LOAD LABEL example_db.label1
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        )
        PROPERTIES
        (
        "cluster" = "my_cluster",
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
        );

        其中 hdfs_host 为 namenode 的 host，hdfs_port 为 fs.defaultFS 端口（默认9000）
    
    2. 导入一批数据，包含多个文件。导入不同的 table，指定分隔符，指定列对应关系

        LOAD LABEL example_db.label2
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file1")
        INTO TABLE `my_table_1`
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2),
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file2")
        INTO TABLE `my_table_2`
        COLUMNS TERMINATED BY "\t"
        (k1, k2, k3, v2, v1)
        );

    3. 导入一批数据，指定hive的默认分隔符\x01，并使用通配符*指定目录下的所有文件

        LOAD LABEL example_db.label3
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
        NEGATIVE
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\\x01"
        );
    
    4. 导入一批“负”数据

        LOAD LABEL example_db.label4
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file)
        NEGATIVE
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\t"
        );

    5. 导入一批数据，指定分区

        LOAD LABEL example_db.label5
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2)
        );

    6. 导入一批数据，指定分区, 并对导入文件的列做一些转化，如下：
       表结构为：
        k1 datetime
        k2 date
        k3 bigint
        k4 varchar(20)
        k5 varchar(64)
        k6 int

        假设数据文件只有一行数据，5列，逗号分隔：

        1537002087,2018-08-09 11:12:13,1537002087,-,1

        数据文件中各列，对应导入语句中指定的各列：
        tmp_k1, tmp_k2, tmp_k3, k6, v1

        转换如下：

        1) k1：将 tmp_k1 时间戳列转化为 datetime 类型的数据
        2) k2：将 tmp_k2 datetime 类型的数据转化为 date 的数据
        3) k3：将 tmp_k3 时间戳列转化为天级别时间戳
        4) k4：指定导入默认值为1
        5) k5：将 tmp_k1、tmp_k2、tmp_k3 列计算 md5 值
        6) k6：将导入文件中的 - 值替换为 10

        LOAD LABEL example_db.label6
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (tmp_k1, tmp_k2, tmp_k3, k6, v1)
        SET (
          k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1),
          k2 = time_format("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", tmp_k2),
          k3 = alignment_timestamp("day", tmp_k3), 
          k4 = default_value("1"), 
          k5 = md5sum(tmp_k1, tmp_k2, tmp_k3),
          k6 = replace_value("-", "10")
        )
        );
    
    7. 导入数据到含有HLL列的表，可以是表中的列或者数据里面的列

        LOAD LABEL example_db.label7
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        SET (
          v1 = hll_hash(k1),
          v2 = hll_hash(k2)
        )
        );

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

    8. 导入Parquet文件中数据  指定FORMAT 为parquet， 默认是通过文件后缀判断
        LOAD LABEL example_db.label9
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        FORMAT AS "parquet"
        (k1, k2, k3)
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password"); 

## keyword
    LOAD
    
# CANCEL LOAD
## description

    该语句用于撤销指定 load label 的批次的导入作业。
    这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW LOAD 命令查看进度。
    语法：
        CANCEL LOAD
        [FROM db_name]
        WHERE LABEL = "load_label";
        
## example

    1. 撤销数据库 example_db 上， label 为 example_db_test_load_label 的导入作业
        CANCEL LOAD
        FROM example_db
        WHERE LABEL = "example_db_test_load_label";
        
## keyword
    CANCEL,LOAD

# DELETE
## description

    该语句用于按条件删除指定 table（base index） partition 中的数据。
    该操作会同时删除和此 base index 相关的 rollup index 的数据。
    语法：
        DELETE FROM table_name [PARTITION partition_name]
        WHERE 
        column_name1 op value[ AND column_name2 op value ...];
        
    说明：
        1) op 的可选类型包括：=, >, <, >=, <=, !=
        2) 只能指定 key 列上的条件。
        2) 当选定的 key 列不存在于某个 rollup 中时，无法进行 delete。
        3) 条件之间只能是“与”的关系。
           若希望达成“或”的关系，需要将条件分写在两个 DELETE 语句中。
        4) 如果为RANGE分区表，则必须指定 PARTITION。如果是单分区表，可以不指定。
           
    注意：
        该语句可能会降低执行后一段时间内的查询效率。
        影响程度取决于语句中指定的删除条件的数量。
        指定的条件越多，影响越大。

## example

    1. 删除 my_table partition p1 中 k1 列值为 3 的数据行
        DELETE FROM my_table PARTITION p1
        WHERE k1 = 3;
        
    2. 删除 my_table partition p1 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行
        DELETE FROM my_table PARTITION p1
        WHERE k1 >= 3 AND k2 = "abc";
        
## keyword
    DELETE
    
# CANCEL DELETE
## description

    该语句用于撤销一个 DELETE 操作。（仅管理员使用！）（待实现）
        
## example

## keyword
    CANCEL,DELETE
    
# MINI LOAD
## description

    MINI LOAD 和 STREAM LOAD 的导入实现方式完全一致。在导入功能支持上，MINI LOAD 的功能是 STREAM LOAD 的子集。
	后续的导入新功能只会在 STREAM LOAD 中支持，MINI LOAD 将不再新增功能。建议改用 STREAM LOAD，具体使用方式请 HELP STREAM LOAD。

	MINI LOAD 是 通过 http 协议完成的导入方式。用户可以不依赖 Hadoop，也无需通过 Mysql 客户端，即可完成导入。
	用户通过 http 协议描述导入，数据在接受 http 请求的过程中被流式的导入 Doris , **导入作业完成后** 返回给用户导入的结果。

    * 注：为兼容旧版本 mini load 使用习惯，用户依旧可以通过 'SHOW LOAD' 命令来查看导入结果。

    语法：
    导入：

        curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table}/_load?label=xxx

    查看导入信息
    
        curl -u user:passwd http://host:port/api/{db}/_load_info?label=xxx

    HTTP协议相关说明

        权限认证            当前 Doris 使用 http 的 Basic 方式权限认证。所以在导入的时候需要指定用户名密码
                            这种方式是明文传递密码的，暂不支持加密传输。

        Expect              Doris 需要发送过来的 http 请求带有 'Expect' 头部信息，内容为 '100-continue'。
                            为什么呢？因为我们需要将请求进行 redirect，那么必须在传输数据内容之前，
                            这样可以避免造成数据的多次传输，从而提高效率。

        Content-Length      Doris 需要在发送请求时带有 'Content-Length' 这个头部信息。如果发送的内容比
                            'Content-Length' 要少，那么 Doris 认为传输出现问题，则提交此次任务失败。
                            NOTE: 如果，发送的数据比 'Content-Length' 要多，那么 Doris 只读取 'Content-Length'
                            长度的内容，并进行导入


    参数说明：

        user:               用户如果是在default_cluster中的，user即为user_name。否则为user_name@cluster_name。

        label:              用于指定这一批次导入的 label，用于后期进行作业查询等。
                            这个参数是必须传入的。

        columns:            用于描述导入文件中对应的列名字。
                            如果不传入，那么认为文件中的列顺序与建表的顺序一致，
                            指定的方式为逗号分隔，例如：columns=k1,k2,k3,k4

        column_separator:   用于指定列与列之间的分隔符，默认的为'\t'
                            NOTE: 需要进行url编码，譬如
                            需要指定'\t'为分隔符，那么应该传入'column_separator=%09'
                            需要指定'\x01'为分隔符，那么应该传入'column_separator=%01'
                            需要指定','为分隔符，那么应该传入'column_separator=%2c'


        max_filter_ratio:   用于指定允许过滤不规范数据的最大比例，默认是0，不允许过滤
                            自定义指定应该如下：'max_filter_ratio=0.2'，含义是允许20%的错误率

        timeout:            指定 load 作业的超时时间，单位是秒。当load执行时间超过该阈值时，会自动取消。默认超时时间是 86400 秒。
                            建议指定 timeout 时间小于 86400 秒。
                            
        hll:                用于指定数据里面和表里面的HLL列的对应关系，表中的列和数据里面指定的列
                            （如果不指定columns，则数据列面的列也可以是表里面的其它非HLL列）通过","分割
                            指定多个hll列使用“:”分割，例如: 'hll1,cuid:hll2,device'
    
    NOTE: 
        1. 此种导入方式当前是在一台机器上完成导入工作，因而不宜进行数据量较大的导入工作。
        建议导入数据量不要超过 1 GB

        2. 当前无法使用 `curl -T "{file1, file2}"` 这样的方式提交多个文件，因为curl是将其拆成多个
        请求发送的，多个请求不能共用一个label号，所以无法使用

        3. mini load 的导入方式和 streaming 完全一致，都是在流式的完成导入后，同步的返回结果给用户。
		后续查询虽可以查到 mini load 的信息，但不能对其进行操作，查询只为兼容旧的使用方式。

        4. 当使用 curl 命令行导入时，需要在 & 前加入 \ 转义，否则参数信息会丢失。

## example

    1. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表（用户是defalut_cluster中的）
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123

    2. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表（用户是test_cluster中的）。超时时间是 3600 秒
        curl --location-trusted -u root@test_cluster:root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123&timeout=3600

    3. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率（用户是defalut_cluster中的）
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2

    4. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率，并且指定文件的列名（用户是defalut_cluster中的）
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&columns=k1,k2,k3

    5. 使用streaming方式导入（用户是defalut_cluster中的）
        seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_load?label=123

    6. 导入含有HLL列的表，可以是表中的列或者数据中的列用于生成HLL列（用户是defalut_cluster中的

        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2
              \&hll=hll_column1,tmp_k4:hll_column2,tmp_k5\&columns=k1,k2,k3,tmp_k4,tmp_k5

    7. 查看提交后的导入情况

        curl -u root http://host:port/api/testDb/_load_info?label=123

## keyword
    MINI, LOAD

# MULTI LOAD
## description

    Syntax:
        curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_start?label=xxx
        curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table1}/_load?label=xxx\&sub_label=yyy
        curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table2}/_load?label=xxx\&sub_label=zzz
        curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_commit?label=xxx
        curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_desc?label=xxx

    'MULTI LOAD'在'MINI LOAD'的基础上，可以支持用户同时向多个表进行导入，具体的命令如上面所示
    '/api/{db}/_multi_start'    开始一个多表导入任务
    '/api/{db}/{table}/_load'   向一个导入任务添加一个要导入的表，与'MINI LOAD'的主要区别是，需要传入'sub_label'参数
    '/api/{db}/_multi_commit'   提交整个多表导入任务，后台开始进行处理
    '/api/{db}/_multi_abort'    放弃一个多表导入任务
    '/api/{db}/_multi_desc'     可以展示某个多表导入任务已经提交的作业数

    HTTP协议相关说明
        权限认证            当前 Doris 使用http的Basic方式权限认证。所以在导入的时候需要指定用户名密码
                            这种方式是明文传递密码的，鉴于我们当前都是内网环境。。。

        Expect              Doris 需要发送过来的http请求，需要有'Expect'头部信息，内容为'100-continue'
                            为什么呢？因为我们需要将请求进行redirect，那么必须在传输数据内容之前，
                            这样可以避免造成数据的多次传输，从而提高效率。

        Content-Length      Doris 需要在发送请求是带有'Content-Length'这个头部信息。如果发送的内容比
                            'Content-Length'要少，那么Palo认为传输出现问题，则提交此次任务失败。
                            NOTE: 如果，发送的数据比'Content-Length'要多，那么 Doris 只读取'Content-Length'
                            长度的内容，并进行导入

    参数说明：
        user:               用户如果是在default_cluster中的，user即为user_name。否则为user_name@cluster_name。

        label:              用于指定这一批次导入的label号，用于后期进行作业状态查询等。
                            这个参数是必须传入的。

        sub_label:          用于指定一个多表导入任务内部的子版本号。对于多表导入的load， 这个参数是必须传入的。

        columns:            用于描述导入文件中对应的列名字。
                            如果不传入，那么认为文件中的列顺序与建表的顺序一致，
                            指定的方式为逗号分隔，例如：columns=k1,k2,k3,k4

        column_separator:   用于指定列与列之间的分隔符，默认的为'\t'
                            NOTE: 需要进行url编码，譬如需要指定'\t'为分隔符，
                            那么应该传入'column_separator=%09'

        max_filter_ratio:   用于指定允许过滤不规范数据的最大比例，默认是0，不允许过滤
                            自定义指定应该如下：'max_filter_ratio=0.2'，含义是允许20%的错误率
                            在'_multi_start'时传入有效果
    
    NOTE: 
        1. 此种导入方式当前是在一台机器上完成导入工作，因而不宜进行数据量较大的导入工作。
        建议导入数据量不要超过1GB

        2. 当前无法使用`curl -T "{file1, file2}"`这样的方式提交多个文件，因为curl是将其拆成多个
        请求发送的，多个请求不能共用一个label号，所以无法使用

        3. 支持类似streaming的方式使用curl来向 Doris 中导入数据，但是，只有等这个streaming结束后 Doris
        才会发生真实的导入行为，这中方式数据量也不能过大。

## example

    1. 将本地文件'testData1'中的数据导入到数据库'testDb'中'testTbl1'的表，并且
    把'testData2'的数据导入到'testDb'中的表'testTbl2'(用户是defalut_cluster中的)
        curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
        curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
        curl --location-trusted -u root -T testData2 http://host:port/api/testDb/testTbl2/_load?label=123\&sub_label=2
        curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_commit?label=123

    2. 多表导入中途放弃(用户是defalut_cluster中的)
        curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
        curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
        curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_abort?label=123

    3. 多表导入查看已经提交多少内容(用户是defalut_cluster中的)
        curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
        curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
        curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_desc?label=123

## keyword
    MULTI, MINI, LOAD

# EXPORT
## description

    该语句用于将指定表的数据导出到指定位置。
    该功能通过 broker 进程实现。对于不同的目的存储系统，需要部署不同的 broker。可以通过 SHOW BROKER 查看已部署的 broker。
    这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW EXPORT 命令查看进度。

    语法：
        EXPORT TABLE table_name
        [PARTITION (p1[,p2])]
        TO export_path
        [opt_properties]
        broker;

    1. table_name
      当前要导出的表的表名，目前支持engine为olap和mysql的表的导出。

    2. partition
      可以只导出指定表的某些指定分区

    3. export_path
      导出的路径，需为目录。目前不能导出到本地，需要导出到broker。

    4. opt_properties
      用于指定一些特殊参数。
          语法：
          [PROPERTIES ("key"="value", ...)]
        
          可以指定如下参数：
            column_separator: 指定导出的列分隔符，默认为\t。
            line_delimiter: 指定导出的行分隔符，默认为\n。
            exec_mem_limit: 导出在单个 BE 节点的内存使用上限，默认为 2GB，单位为字节。
            timeout：导入作业的超时时间，默认为1天，单位是秒。
            tablet_num_per_task：每个子任务能分配的最大 Tablet 数量。

    5. broker
      用于指定导出使用的broker
          语法：
          WITH BROKER broker_name ("key"="value"[,...])
          这里需要指定具体的broker name, 以及所需的broker属性

      对于不同存储系统对应的 broker，这里需要输入的参数不同。具体参数可以参阅：`help broker load` 中 broker 所需属性。

## example

    1. 将 testTbl 表中的所有数据导出到 hdfs 上
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    2. 将 testTbl 表中的分区p1,p2导出到 hdfs 上

        EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    3. 将 testTbl 表中的所有数据导出到 hdfs 上，以","作为列分隔符

        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

## keyword
    EXPORT

# SHOW DATABASES
## description
    该语句用于展示当前可见的 db
    语法：
        SHOW DATABASES;
        
## keyword
    SHOW,DATABASES
        
# SHOW TABLES
## description
    该语句用于展示当前 db 下所有的 table
    语法：
        SHOW TABLES;
        
## keyword
    SHOW,TABLES

# SHOW LOAD
## description
    该语句用于展示指定的导入任务的执行情况
    语法：
        SHOW LOAD
        [FROM db_name]
        [
            WHERE 
            [LABEL [ = "your_label" | LIKE "label_matcher"]]
            [STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
        ]
        [ORDER BY ...]
        [LIMIT limit];
        
    说明：
        1) 如果不指定 db_name，使用当前默认db
        2) 如果使用 LABEL LIKE，则会匹配导入任务的 label 包含 label_matcher 的导入任务
        3) 如果使用 LABEL = ，则精确匹配指定的 label
        4) 如果指定了 STATE，则匹配 LOAD 状态
        5) 可以使用 ORDER BY 对任意列组合进行排序
        6) 如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示
        7) 如果是使用 broker/mini load，则 URL 列中的连接可以使用以下命令查看：

            SHOW LOAD WARNINGS ON 'url'

## example
    1. 展示默认 db 的所有导入任务
        SHOW LOAD;
    
    2. 展示指定 db 的导入任务，label 中包含字符串 "2014_01_02"，展示最老的10个
        SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
        
    3. 展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" 并按 LoadStartTime 降序排序
        SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;
        
    4. 展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" ，state 为 "loading", 并按 LoadStartTime 降序排序
        SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;

    5. 小批量导入是查看导入状态的命令
        curl --location-trusted -u {user}:{passwd} http://{hostname}:{port}/api/{database}/_load_info?label={labelname}
        
## keyword
    SHOW,LOAD

# SHOW EXPORT
## description
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
        1) 如果不指定 db_name，使用当前默认db
        2) 如果指定了 STATE，则匹配 EXPORT 状态
        3) 可以使用 ORDER BY 对任意列组合进行排序
        4) 如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示

## example
    1. 展示默认 db 的所有导出任务
        SHOW EXPORT;
        
    2. 展示指定 db 的导出任务，按 StartTime 降序排序
        SHOW EXPORT FROM example_db ORDER BY StartTime DESC;

    3. 展示指定 db 的导出任务，state 为 "exporting", 并按 StartTime 降序排序
        SHOW EXPORT FROM example_db WHERE STATE = "exporting" ORDER BY StartTime DESC;
    
    4. 展示指定db，指定job_id的导出任务
            SHOW EXPORT FROM example_db WHERE EXPORT_JOB_ID = job_id;

## keyword
    SHOW,EXPORT
    
# SHOW DELETE
## description
    该语句用于展示已执行成功的历史 delete 任务
    语法：
        SHOW DELETE [FROM db_name]

## example
    1. 展示数据库 database 的所有历史 delete 任务
        SHOW DELETE FROM database;
        
## keyword
    SHOW,DELETE
    
# SHOW ALTER
## description
    该语句用于展示当前正在进行的各类修改任务的执行情况
    语法：
        SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];
        
    说明：
        TABLE COLUMN：展示修改列的 ALTER 任务
        TABLE ROLLUP：展示创建或删除 ROLLUP index 的任务
        如果不指定 db_name，使用当前默认 db
        CLUSTER: 展示集群操作相关任务情况（仅管理员使用！待实现...）
        
## example
    1. 展示默认 db 的所有修改列的任务执行情况
        SHOW ALTER TABLE COLUMN;
        
    2. 展示指定 db 的创建或删除 ROLLUP index 的任务执行情况
        SHOW ALTER TABLE ROLLUP FROM example_db;
        
    3. 展示集群操作相关任务（仅管理员使用！待实现...）
        SHOW ALTER CLUSTER;
        
## keyword
    SHOW,ALTER
    
# SHOW DATA
## description
    该语句用于展示数据量
    语法：
        SHOW DATA [FROM db_name[.table_name]];
        
    说明：
        1. 如果不指定 FROM 子句，使用展示当前 db 下细分到各个 table 的数据量
        2. 如果指定 FROM 子句，则展示 table 下细分到各个 index 的数据量
        3. 如果想查看各个 Partition 的大小，请参阅 help show partitions

## example
    1. 展示默认 db 的各个 table 的数据量及汇总数据量
        SHOW DATA;
        
    2. 展示指定 db 的下指定表的细分数据量
        SHOW DATA FROM example_db.table_name;

## keyword
    SHOW,DATA

# SHOW PARTITIONS
## description
    该语句用于展示分区信息
    语法：
        SHOW PARTITIONS FROM [db_name.]table_name [PARTITION partition_name];

## example
    1. 展示指定 db 的下指定表的分区信息
        SHOW PARTITIONS FROM example_db.table_name;
        
    1. 展示指定 db 的下指定表的指定分区的信息
        SHOW PARTITIONS FROM example_db.table_name PARTITION p1;

## keyword
    SHOW,PARTITIONS
    
# SHOW TABLET
## description
    该语句用于显示 tablet 相关的信息（仅管理员使用）
    语法：
        SHOW TABLET
        [FROM [db_name.]table_name | tablet_id]

## example
    1. 显示指定 db 的下指定表所有 tablet 信息
        SHOW TABLET FROM example_db.table_name;
        
    2. 显示指定 tablet id 为 10000 的 tablet 的父层级 id 信息
        SHOW TABLET 10000;

## keyword
    SHOW,TABLET

# SHOW PROPERTY
## description
    该语句用于查看用户的属性
    语法：
        SHOW PROPERTY [FOR user] [LIKE key]

## example
    1. 查看 jack 用户的属性
        SHOW PROPERTY FOR 'jack'

    2. 查看 jack 用户导入cluster相关属性
        SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'

## keyword
    SHOW, PROPERTY
    
# SHOW BACKUP
## description
    该语句用于查看 BACKUP 任务
    语法：
        SHOW BACKUP [FROM db_name]
        
    说明：
        1. Palo 中仅保存最近一次 BACKUP 任务。
        2. 各列含义如下：
            JobId：                  唯一作业id
            SnapshotName：           备份的名称
            DbName：                 所属数据库
            State：                  当前阶段
                PENDING：        提交作业后的初始状态
                SNAPSHOTING：    执行快照中
                UPLOAD_SNAPSHOT：快照完成，准备上传
                UPLOADING：      快照上传中
                SAVE_META：      将作业元信息保存为本地文件
                UPLOAD_INFO：    上传作业元信息
                FINISHED：       作业成功
                CANCELLED：      作业失败
            BackupObjs：             备份的表和分区
            CreateTime：             任务提交时间
            SnapshotFinishedTime：   快照完成时间
            UploadFinishedTime：     快照上传完成时间
            FinishedTime：           作业结束时间
            UnfinishedTasks：        在 SNAPSHOTING 和 UPLOADING 阶段会显示还未完成的子任务id
            Status：                 如果作业失败，显示失败信息
            Timeout：                作业超时时间，单位秒

## example
    1. 查看 example_db 下最后一次 BACKUP 任务。
        SHOW BACKUP FROM example_db;

## keyword
    SHOW, BACKUP

# SHOW RESTORE
## description
    该语句用于查看 RESTORE 任务
    语法：
        SHOW RESTORE [FROM db_name]
        
    说明：
        1. Palo 中仅保存最近一次 RESTORE 任务。
        2. 各列含义如下：
            JobId：                  唯一作业id
            Label：                  要恢复的备份的名称
            Timestamp：              要恢复的备份的时间版本
            DbName：                 所属数据库
            State：                  当前阶段
                PENDING：        提交作业后的初始状态
                SNAPSHOTING：    执行快照中
                DOWNLOAD：       快照完成，准备下载仓库中的快照
                DOWNLOADING：    快照下载中
                COMMIT：         快照下载完成，准备生效
                COMMITING：      生效中
                FINISHED：       作业成功
                CANCELLED：      作业失败
            AllowLoad：              恢复时是否允许导入（当前不支持）
            ReplicationNum：         指定恢复的副本数
            RestoreJobs：            要恢复的表和分区
            CreateTime：             任务提交时间
            MetaPreparedTime：       元数据准备完成时间
            SnapshotFinishedTime：   快照完成时间
            DownloadFinishedTime：   快照下载完成时间
            FinishedTime：           作业结束时间
            UnfinishedTasks：        在 SNAPSHOTING、DOWNLOADING 和 COMMITING 阶段会显示还未完成的子任务id
            Status：                 如果作业失败，显示失败信息
            Timeout：                作业超时时间，单位秒

## example
    1. 查看 example_db 下最近一次 RESTORE 任务。
        SHOW RESTORE FROM example_db;

## keyword
    SHOW, RESTORE
    
# SHOW REPOSITORIES
## description
    该语句用于查看当前已创建的仓库。
    语法：
        SHOW REPOSITORIES;
        
    说明：
        1. 各列含义如下：
            RepoId：     唯一的仓库ID
            RepoName：   仓库名称
            CreateTime： 第一次创建该仓库的时间
            IsReadOnly： 是否为只读仓库
            Location：   仓库中用于备份数据的根目录
            Broker：     依赖的 Broker
            ErrMsg：     Palo 会定期检查仓库的连通性，如果出现问题，这里会显示错误信息
    
## example
    1. 查看已创建的仓库：
        SHOW REPOSITORIES;
        
## keyword
    SHOW, REPOSITORY, REPOSITORIES
    
# SHOW SNAPSHOT
## description
    该语句用于查看仓库中已存在的备份。
    语法：
        SHOW SNAPSHOT ON `repo_name`
        [WHERE SNAPSHOT = "snapshot" [AND TIMESTAMP = "backup_timestamp"]];
        
    说明：
        1. 各列含义如下：
            Snapshot：   备份的名称
            Timestamp：  对应备份的时间版本
            Status：     如果备份正常，则显示 OK，否则显示错误信息
            
        2. 如果指定了 TIMESTAMP，则会额外显示如下信息：
            Database：   备份数据原属的数据库名称
            Details：    以 Json 的形式，展示整个备份的数据目录及文件结构
    
## example
    1. 查看仓库 example_repo 中已有的备份：
        SHOW SNAPSHOT ON example_repo;
        
    2. 仅查看仓库 example_repo 中名称为 backup1 的备份：
        SHOW SNAPSHOT ON example_repo WHERE SNAPSHOT = "backup1";
        
    2. 查看仓库 example_repo 中名称为 backup1 的备份，时间版本为 "2018-05-05-15-34-26" 的详细信息：
        SHOW SNAPSHOT ON example_repo
        WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
        
## keyword
    SHOW, SNAPSHOT

# RESTORE TABLET
## description
   
    该功能用于恢复trash目录中被误删的tablet数据。

    说明：这个功能暂时只在be服务中提供一个http接口。如果要使用，
    需要向要进行数据恢复的那台be机器的http端口发送restore tablet api请求。api格式如下：
    METHOD: POST
    URI: http://be_host:be_http_port/api/restore_tablet?tablet_id=xxx&schema_hash=xxx

## example

    curl -X POST "http://hostname:8088/api/restore_tablet?tablet_id=123456&schema_hash=1111111"
