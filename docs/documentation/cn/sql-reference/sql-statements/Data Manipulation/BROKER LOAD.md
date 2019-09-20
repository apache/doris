# BROKER LOAD
## description

    Broker load 通过随 Doris 集群一同部署的 broker 进行，访问对应数据源的数据，进行数据导入。
    可以通过 show broker 命令查看已经部署的 broker。
    目前支持以下4种数据源：

    1. Baidu HDFS：百度内部的 hdfs，仅限于百度内部使用。
    2. Baidu AFS：百度内部的 afs，仅限于百度内部使用。
    3. Baidu Object Storage(BOS)：百度对象存储。仅限百度内部用户、公有云用户或其他可以访问 BOS 的用户使用。
    4. Apache HDFS：社区版本 hdfs。

语法：

    LOAD LABEL load_label
    (
    data_desc1[, data_desc2, ...]
    )
    WITH BROKER broker_name
    [broker_properties]
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
            [WHERE predicate]    

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

            如果指定此参数，可以将源文件某一列按照函数进行转化，然后将转化后的结果导入到table中。语法为 `column_name` = expression。举几个例子帮助理解。
            例1: 表中有3个列“c1, c2, c3", 源文件中前两列依次对应(c1,c2)，后两列之和对应c3；那么需要指定 columns (c1,c2,tmp_c3,tmp_c4) SET (c3=tmp_c3+tmp_c4); 
            例2: 表中有3个列“year, month, day"三个列，源文件中只有一个时间列，为”2018-06-01 01:02:03“格式。
            那么可以指定 columns(tmp_time) set (year = year(tmp_time), month=month(tmp_time), day=day(tmp_time)) 完成导入。

            WHERE:
          
            对做完 transform 的数据进行过滤，符合 where 条件的数据才能被导入。WHERE 语句中只可引用表中列名。
    3. broker_name

        所使用的 broker 名称，可以通过 show broker 命令查看。

    4. broker_properties

        用于提供通过 broker 访问数据源的信息。不同的 broker，以及不同的访问方式，需要提供的信息不同。

        1. Baidu HDFS/AFS

            访问百度内部的 hdfs/afs 目前仅支持简单认证，需提供：
            username：hdfs 用户名
            password：hdfs 密码

        2. BOS

            需提供：
            bos_endpoint：BOS 的endpoint
            bos_accesskey：公有云用户的 accesskey
            bos_secret_accesskey：公有云用户的 secret_accesskey
        
        3. Apache HDFS

            社区版本的 hdfs，支持简单认证、kerberos 认证。以及支持 HA 配置。
            简单认证：
            hadoop.security.authentication = simple (默认)
            username：hdfs 用户名
            password：hdfs 密码

            kerberos 认证：
            hadoop.security.authentication = kerberos
            kerberos_principal：指定 kerberos 的 principal
            kerberos_keytab：指定 kerberos 的 keytab 文件路径。该文件必须为 broker 进程所在服务器上的文件。
            kerberos_keytab_content：指定 kerberos 中 keytab 文件内容经过 base64 编码之后的内容。这个跟 kerberos_keytab 配置二选一就可以。

            namenode HA：
            通过配置 namenode HA，可以在 namenode 切换时，自动识别到新的 namenode
            dfs.nameservices: 指定 hdfs 服务的名字，自定义，如："dfs.nameservices" = "my_ha"
            dfs.ha.namenodes.xxx：自定义 namenode 的名字,多个名字以逗号分隔。其中 xxx 为 dfs.nameservices 中自定义的名字，如 "dfs.ha.namenodes.my_ha" = "my_nn"
            dfs.namenode.rpc-address.xxx.nn：指定 namenode 的rpc地址信息。其中 nn 表示 dfs.ha.namenodes.xxx 中配置的 namenode 的名字，如："dfs.namenode.rpc-address.my_ha.my_nn" = "host:port"
            dfs.client.failover.proxy.provider：指定 client 连接 namenode 的 provider，默认为：org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

    4. opt_properties

        用于指定一些特殊参数。
        语法：
        [PROPERTIES ("key"="value", ...)]
        
        可以指定如下参数：
        timeout：         指定导入操作的超时时间。默认超时为4小时。单位秒。
        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。
        exec_mem_limit:   设置导入使用的内存上限。默认为2G，单位字节。这里是指单个 BE 节点的内存上限。
                          一个导入可能分布于多个BE。我们假设 1GB 数据在单个节点处理需要最大5GB内存。那么假设1GB文件分布在2个节点处理，那么理论上，每个节点需要内存为2.5GB。则该参数可以设置为 2684354560，即2.5GB
        strict mode：     是否对数据进行严格限制。默认为true。
        timezone:         指定某些受时区影响的函数的时区，如 strftime/alignment_timestamp/from_unixtime 等等，具体请查阅 [时区] 文档。如果不指定，则使用 "Asia/Shanghai" 时区。

    5. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）
        字符串类（CHAR/VARCHAR）："I am a student", "a"
        NULL值：\N

## example

    1. 从 HDFS 导入一批数据，指定超时时间和过滤比例。使用铭文 my_hdfs_broker 的 broker。简单认证。

        LOAD LABEL example_db.label1
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        )
        WITH BROKER my_hdfs_broker
        (
        "username" = "hdfs_user",
        "password" = "hdfs_passwd"
        )
        PROPERTIES
        (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
        );
    
        其中 hdfs_host 为 namenode 的 host，hdfs_port 为 fs.defaultFS 端口（默认9000）

    2. 从 AFS 一批数据，包含多个文件。导入不同的 table，指定分隔符，指定列对应关系。

        LOAD LABEL example_db.label2
        (
        DATA INFILE("afs://afs_host:hdfs_port/user/palo/data/input/file1")
        INTO TABLE `my_table_1`
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2),
        DATA INFILE("afs://afs_host:hdfs_port/user/palo/data/input/file2")
        INTO TABLE `my_table_2`
        COLUMNS TERMINATED BY "\t"
        (k1, k2, k3, v2, v1)
        )
        WITH BROKER my_afs_broker
        (
        "username" = "afs_user",
        "password" = "afs_passwd"
        )
        PROPERTIES
        (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
        );
        

    3. 从 HDFS 导入一批数据，指定hive的默认分隔符\x01，并使用通配符*指定目录下的所有文件。
       使用简单认证，同时配置 namenode HA

        LOAD LABEL example_db.label3
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\\x01"
        )
        WITH BROKER my_hdfs_broker
        (
        "username" = "hdfs_user",
        "password" = "hdfs_passwd",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        )
    
    4. 从 HDFS 导入一批“负”数据。同时使用 kerberos 认证方式。提供 keytab 文件路径。

        LOAD LABEL example_db.label4
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file)
        NEGATIVE
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\t"
        )
        WITH BROKER my_hdfs_broker
        (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal"="doris@YOUR.COM",
        "kerberos_keytab"="/home/palo/palo.keytab"
        )

    5. 从 HDFS 导入一批数据，指定分区。同时使用 kerberos 认证方式。提供 base64 编码后的 keytab 文件内容。

        LOAD LABEL example_db.label5
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2)
        )
        WITH BROKER my_hdfs_broker
        (
        "hadoop.security.authentication"="kerberos",
        "kerberos_principal"="doris@YOUR.COM",
        "kerberos_keytab_content"="BQIAAABEAAEACUJBSURVLkNPTQAEcGFsbw"
        )

    6. 从 BOS 导入一批数据，指定分区, 并对导入文件的列做一些转化，如下：
       表结构为：
        k1 varchar(20)
        k2 int

        假设数据文件只有一行数据：

        Adele,1,1

        数据文件中各列，对应导入语句中指定的各列：
        k1,tmp_k2,tmp_k3

        转换如下：

        1) k1: 不变换
        2) k2：是 tmp_k2 和 tmp_k3 数据之和

        LOAD LABEL example_db.label6
        (
        DATA INFILE("bos://my_bucket/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, tmp_k2, tmp_k3)
        SET (
          k2 = tmp_k2 + tmp_k3
        )
        )
        WITH BROKER my_bos_broker
        (
        "bos_endpoint" = "http://bj.bcebos.com",
        "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyy"
        )
    
    7. 导入数据到含有HLL列的表，可以是表中的列或者数据里面的列

        如果表中有三列分别是（id,v1,v2）。其中v1和v2列是hll列。导入的源文件有3列。则（column_list）中声明第一列为id，第二三列为一个临时命名的k1,k2。
        在SET中必须给表中的hll列特殊声明 hll_hash。表中的v1列等于原始数据中的hll_hash(k1)列。
        LOAD LABEL example_db.label7
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (id, k1, k2)
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

    8. 导入Parquet文件中数据  指定FORMAT 为parquet， 默认是通过文件后缀判断

        LOAD LABEL example_db.label9
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        FORMAT AS "parquet"
        (k1, k2, k3)
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    9. 提取文件路径中的分区字段

        如果需要，则会根据表中定义的字段类型解析文件路径中的分区字段（partitioned fields），类似Spark中Partition Discovery的功能

        LOAD LABEL example_db.label10
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/*/*")
        INTO TABLE `my_table`
        FORMAT AS "csv"
        (k1, k2, k3)
        COLUMNS FROM PATH AS (city, utc_date)
        SET (uniq_id = md5sum(k1, city))
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

        hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing目录下包括如下文件：

        [hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]

        则提取文件路径的中的city和utc_date字段

    10. 对待导入数据进行过滤，k1 值大于 k2 值的列才能被导入

        LOAD LABEL example_db.label10
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        where k1 > k2
        )
     
## keyword
    BROKER,LOAD
