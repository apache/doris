# LOAD
## description

    该语句用于向指定的 table（base table） 导入数据。
    该操作会同时更新和此 base table 相关的 rollup table 的数据。
    这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW LOAD 命令查看进度。

    NULL导入的时候用\N来表示。如果需要将其他字符串转化为NULL，可以使用replace_value进行转化。
    语法：
        LOAD LABEL load_label
        (
        data_desc1[, data_desc2, ...]
        )
        broker
        [opt_properties];

    1. load_label
        当前导入批次的标签。在一个 database 唯一。
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
            [(column_list)]
            [SET (k1 = func(k2))]
    
        说明：
            file_path: 文件路径，可以指定到一个文件，也可以用 * 通配符指定某个目录下的所有文件。

            PARTITION:
            如果指定此参数，则只会导入指定的分区，导入分区以外的数据会被过滤掉。
            如果不指定默认导入table的所有分区。
        
            NEGATIVE：
            如果指定此参数，则相当于导入一批“负”数据。用于抵消之前导入的同一批数据。
            该参数仅适用于存在 value 列，并且 value 列的聚合类型仅为 SUM 的情况。
            
            column_separator：
            用于指定导入文件中的列分隔符。默认为 \t
            如果是不可见字符，则需要加\\x作为前缀，使用十六进制来表示分隔符。
            如hive文件的分隔符\x01，指定为"\\x01"
            
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
            
    3. broker
        用于指定导入使用的Broker
        语法：
        WITH BROKER broker_name ("key"="value"[,...])
        这里需要指定具体的Broker name, 以及所需的Broker属性.
        开源hdfs Broker支持的属性如下:
        -   fs.defaultFS:默认文件系统
        -   username: 访问hdfs的用户名
        -   password: 访问hdfs的用户密码
        -   dfs.nameservices: ha模式的hdfs中必须配置这个，指定hdfs服务的名字.以下参数都是在ha hdfs中需要指定.例子: "dfs.nameservices" = "palo"
        -   dfs.ha.namenodes.xxx：ha模式中指定namenode的名字,多个名字以逗号分隔,ha模式中必须配置。其中xxx表示dfs.nameservices配置的value.例子: "dfs.ha.namenodes.palo" = "nn1,nn2"
        -   dfs.namenode.rpc-address.xxx.nn: ha模式中指定namenode的rpc地址信息,ha模式中必须配置。其中nn表示dfs.ha.namenodes.xxx中配置的一个namenode的名字。例子: "dfs.namenode.rpc-address.palo.nn1" = "host:port"
        -   dfs.client.failover.proxy.provider: ha模式中指定client连接namenode的provider,默认为:org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider


    4. opt_properties
        用于指定一些特殊参数。
        语法：
        [PROPERTIES ("key"="value", ...)]
        
        可以指定如下参数：
        timeout：         指定导入操作的超时时间。默认不超时。单位秒。
        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。
        load_delete_flag：指定该导入是否通过导入key列的方式删除数据，仅适用于UNIQUE KEY，
                          导入时可不指定value列。默认为false。
        exec_mem_limit:   当使用 broker 方式导入时，可以指定导入作业在单个 BE 上的内存大小限制。
                          单位是字节。（默认是 2147483648，即 2G）

    5. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）
        字符串类（CHAR/VARCHAR）："I am a student", "a"
        NULL值：\N

## example

    1. 导入一批数据，指定超时时间和过滤比例
        LOAD LABEL example_db.label1
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        )
		WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password")
        PROPERTIES
        (
        "timeout"="3600",
        "max_filter_ratio"="0.1"
        );
    
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
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    3. 导入一批数据，指定hive的默认分隔符\x01，并使用通配符*指定目录下的所有文件
        LOAD LABEL example_db.label3
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
        NEGATIVE
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\\x01"
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
    
    4. 导入一批“负”数据
        LOAD LABEL example_db.label4
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file)
        NEGATIVE
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\t"
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    5. 导入一批数据，指定分区
        LOAD LABEL example_db.label5
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2)
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    6. 导入一批数据，指定分区, 并对导入文件的列做一些转化，如下：
       1) k1将tmp_k1时间戳列转化为datetime类型的数据
       2) k2将tmp_k2 date类型的数据转化为datetime的数据
       3) k3将tmp_k3时间戳列转化为天级别时间戳
       4) k4指定导入默认值为1
       5) k5将tmp_k1、tmp_k2、tmp_k3列计算md5串
       6) k6将导入文件中的-值替换为10
        LOAD LABEL example_db.label6
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (tmp_k1, tmp_k2, tmp_k3, k6, v1, v2)
        SET (
          k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)),
          k2 = time_format("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", tmp_k2)),
          k3 = alignment_timestamp("day", tmp_k3), 
          k4 = default_value("1"), 
          k5 = md5sum(tmp_k1, tmp_k2, tmp_k3),
          k6 = replace_value("-", "10")
        )
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
    
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
 
    8. 从ha模式的hdfs的路径中导入数据
         LOAD LABEL table1_20170707 (
             DATA INFILE("hdfs://bdos/palo/table1_data")
             INTO TABLE table1
         )
         WITH BROKER hdfs (
             "fs.defaultFS"="hdfs://bdos",
             "username"="hdfs_user",
             "password"="hdfs_password",
             "dfs.nameservices"="bdos",
             "dfs.ha.namenodes.bdos"="nn1,nn2",
             "dfs.namenode.rpc-address.bdos.nn1"="host1:port1",
             "dfs.namenode.rpc-address.bdos.nn2"="host2:port2")
         PROPERTIES (
             "timeout"="3600",
             "max_filter_ratio"="0.1"
         );

## keyword
    LOAD,TABLE
    
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
        DELETE FROM table_name PARTITION partition_name
        WHERE 
        column_name1 op value[ AND column_name2 op value ...];
        
    说明：
        1) op 的可选类型包括：=, >, <, >=, <=, !=
        2) 只能指定 key 列上的条件。
        3) 条件之间只能是“与”的关系。
           若希望达成“或”的关系，需要将条件分写在两个 DELETE语句中。
        4) 如果为单分区表，partition_name 同 table_name。
           
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

    Syntax:
        curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table}/_load?label=xxx

    MINI LOAD 是 Palo 新提供的一种导入方式，这种导入方式可以使用户不依赖 Hadoop，从而完成导入方式。
    此种导入方式提交任务并不是通过 MySQL 客户端，而是通过 http 协议来完成的。用户通过 http 协议将导入描述，
    数据一同发送给 Palo，Palo 在接收任务成功后，会立即返回给用户成功信息，但是此时，数据并未真正导入。
    用户需要通过 'SHOW LOAD' 命令来查看具体的导入结果。

    HTTP协议相关说明
        权限认证            当前 Palo 使用 http 的 Basic 方式权限认证。所以在导入的时候需要指定用户名密码
                            这种方式是明文传递密码的，鉴于我们当前都是内网环境。。。

        Expect              Palo需要发送过来的 http 请求带有 'Expect' 头部信息，内容为 '100-continue'。
                            为什么呢？因为我们需要将请求进行 redirect，那么必须在传输数据内容之前，
                            这样可以避免造成数据的多次传输，从而提高效率。

        Content-Length      Palo 需要在发送请求时带有 'Content-Length' 这个头部信息。如果发送的内容比
                            'Content-Length' 要少，那么 Palo 认为传输出现问题，则提交此次任务失败。
                            NOTE: 如果，发送的数据比 'Content-Length' 要多，那么 Palo 只读取 'Content-Length'
                            长度的内容，并进行导入

    参数说明：
        user:               用户如果是在default_cluster中的，user即为user_name。否则为user_name@cluster_name。

        label:              用于指定这一批次导入的 label，用于后期进行作业状态查询等。
                            这个参数是必须传入的。

        columns:            用于描述导入文件中对应的列名字。
                            如果不传入，那么认为文件中的列顺序与建表的顺序一致，
                            指定的方式为逗号分隔，例如：columns=k1,k2,k3,k4

        column_separator:   用于指定列与列之间的分隔符，默认的为'\t'
                            NOTE: 需要进行url编码，譬如
                            需要指定'\t'为分隔符，那么应该传入'column_separator=%09'
                            需要指定'\x01'为分隔符，那么应该传入'column_separator=%01'


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

        3. 支持类似 streaming 的方式使用 curl 来向 palo 中导入数据，但是，只有等这个 streaming 结束后 palo
        才会发生真实的导入行为，这中方式数据量也不能过大。

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

        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&hll=hll_column1,k1:hll_column2,k2
     
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2
              \&hll=hll_column1,tmp_k4:hll_column2,tmp_k5\&columns=k1,k2,k3,tmp_k4,tmp_k5

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
        权限认证            当前Palo使用http的Basic方式权限认证。所以在导入的时候需要指定用户名密码
                            这种方式是明文传递密码的，鉴于我们当前都是内网环境。。。

        Expect              Palo需要发送过来的http请求，需要有'Expect'头部信息，内容为'100-continue'
                            为什么呢？因为我们需要将请求进行redirect，那么必须在传输数据内容之前，
                            这样可以避免造成数据的多次传输，从而提高效率。

        Content-Length      Palo需要在发送请求是带有'Content-Length'这个头部信息。如果发送的内容比
                            'Content-Length'要少，那么Palo认为传输出现问题，则提交此次任务失败。
                            NOTE: 如果，发送的数据比'Content-Length'要多，那么Palo只读取'Content-Length'
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

        3. 支持类似streaming的方式使用curl来向palo中导入数据，但是，只有等这个streaming结束后palo
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
            column_separator：指定导出的列分隔符，默认为\t。
            line_delimiter:指定导出的行分隔符，默认为\n。
    5. broker
      用于指定导出使用的broker
          语法：
          WITH BROKER broker_name ("key"="value"[,...])
          这里需要指定具体的broker name, 以及所需的broker属性

## example

    1. 将testTbl表中的所有数据导出到hdfs上
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    2. 将testTbl表中的分区p1,p2导出到hdfs上
        EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    3. 将testTbl表中的所有数据导出到hdfs上，以","作为列分隔符
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
        如果不指定 FROM 子句，使用展示当前 db 下细分到各个 table 的数据量
        如果指定 FROM 子句，则展示 table 下细分到各个 index 的数据量

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
    SHOW,PARTITION
    
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
        [WHERE LABEL = "backup_label" | LABEL LIKE "pattern"];
        
    说明：
        BACKUP 任务的状态（State）有以下几种：
        PENDING：刚提交的
        SNAPSHOT：正在执行快照
        UPLOAD：准备上传数据
        CHECK_UPLOAD：正在上传数据
        FINISHING：即将结束
        FINISHED：任务完成
        CANCELLED：任务失败
        
        失败的任务可以通过 ErrMsg 列失败查看原因。

## example
    1. 查看 example_db 下的所有 BACKUP 任务。
        SHOW BACKUP FROM example_db;

    2. 查看 example_db 下 LABEL 为 "backup_label" 的任务。
        SHOW BACKUP FROM example_db WHERE LABEL = "backup_label";
        
    3. 查看 example_db 下 LABEL 前缀为 "backup" 的任务。
        SHOW BACKUP FROM example_db WHERE LABEL LIKE "backup%";

## keyword
    SHOW, BACKUP

# SHOW RESTORE
## description
    该语句用于查看 RESTORE 任务
    语法：
        SHOW RESTORE [FROM db_name]
        [WHERE LABEL = "restore_label" | LABEL LIKE "pattern"];
        
    说明：
        BACKUP 任务的状态（State）有以下几种：
        PENDING：刚提交的
        RESTORE_META：正在恢复元数据
        DOWNLOAD_OBJS：准备恢复数据
        DOWNLOADING：正在恢复数据
        FINISHING：恢复完成，等待确认
        FINISHED：任务完成
        CANCELLED：任务失败
        
        FINISHING 状态的任务需要通过 RESTORE COMMIT 语句进行确认生效，详见 RESTORE 语句
        失败的任务可以通过 ErrMsg 列失败查看原因。

## example
    1. 查看 example_db 下的所有 RESTORE 任务。
        SHOW RESTORE FROM example_db;

    2. 查看 example_db 下 LABEL 为 "restore_label" 的任务。
        SHOW RESTORE FROM example_db WHERE LABEL = "restore_label";
        
    3. 查看 example_db 下 LABEL 前缀为 "restore" 的任务。
        SHOW RESTORE FROM example_db WHERE LABEL LIKE "restore%";

## keyword
    SHOW, RESTORE

# SHOW BACKENDS
## description
    该语句用于查看cluster内的节点
    语法：
        SHOW BACKENDS
        
## keyword
    SHOW, BACKENDS
