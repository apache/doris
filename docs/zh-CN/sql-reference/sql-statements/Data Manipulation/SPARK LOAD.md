---
{
    "title": "SPARK LOAD",
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

# SPARK LOAD
## description

    Spark load 通过外部的 Spark 资源实现对导入数据的预处理，提高 Doris 大数据量的导入性能并且节省 Doris 集群的计算资源。主要用于初次迁移，大数据量导入 Doris 的场景。

    Spark load 是一种异步导入方式，用户需要通过 MySQL 协议创建 Spark 类型导入任务，并通过 `SHOW LOAD` 查看导入结果。

语法：

    LOAD LABEL load_label
    (
    data_desc1[, data_desc2, ...]
    )
    WITH RESOURCE resource_name
    [resource_properties]
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
            [COLUMNS FROM PATH AS (col2, ...)]
            [SET (k1 = func(k2))]
            [WHERE predicate]    

            DATA FROM TABLE hive_external_tbl
            [NEGATIVE]
            INTO TABLE tbl_name
            [PARTITION (p1, p2)]
            [SET (k1=f1(xx), k2=f2(xx))]
            [WHERE predicate]

        说明：
            file_path: 

            文件路径，可以指定到一个文件，也可以用 * 通配符指定某个目录下的所有文件。通配符必须匹配到文件，而不能是目录。

            hive_external_tbl:

            hive 外部表名。
            要求导入的 doris 表中的列必须在 hive 外部表中存在。
            每个导入任务只支持从一个 hive 外部表导入。
            不能与 file_path 方式同时使用。

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

            用于指定导入文件的类型，目前仅支持csv。 
 
            column_list：

            用于指定导入文件中的列和 table 中的列的对应关系。
            当需要跳过导入文件中的某一列时，将该列指定为 table 中不存在的列名即可。
            语法：
            (col_name1, col_name2, ...)
            
            SET:

            如果指定此参数，可以将源文件某一列按照函数进行转化，然后将转化后的结果导入到table中。语法为 `column_name` = expression。
            仅支持Spark SQL built-in functions，具体可参考 https://spark.apache.org/docs/2.4.6/api/sql/index.html。
            举几个例子帮助理解。
            例1: 表中有3个列“c1, c2, c3", 源文件中前两列依次对应(c1,c2)，后两列之和对应c3；那么需要指定 columns (c1,c2,tmp_c3,tmp_c4) SET (c3=tmp_c3+tmp_c4); 
            例2: 表中有3个列“year, month, day"，源文件中只有一个时间列，为”2018-06-01 01:02:03“格式。
            那么可以指定 columns(tmp_time) set (year = year(tmp_time), month=month(tmp_time), day=day(tmp_time)) 完成导入。

            WHERE:
          
            对做完 transform 的数据进行过滤，符合 where 条件的数据才能被导入。WHERE 语句中只可引用表中列名。
    3. resource_name

        所使用的 spark 资源名称，可以通过 `SHOW RESOURCES` 命令查看。

    4. resource_properties

        当用户有临时性的需求，比如增加任务使用的资源而修改 Spark configs，可以在这里设置，设置仅对本次任务生效，并不影响 Doris 集群中已有的配置。
        另外不同的 broker，以及不同的访问方式，需要提供的信息不同。可以查看 broker 使用文档。

    4. opt_properties

        用于指定一些特殊参数。
        语法：
        [PROPERTIES ("key"="value", ...)]
        
        可以指定如下参数：
        timeout：         指定导入操作的超时时间。默认超时为4小时。单位秒。
        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。
        strict mode：     是否对数据进行严格限制。默认为 false。
        timezone:         指定某些受时区影响的函数的时区，如 strftime/alignment_timestamp/from_unixtime 等等，具体请查阅 [时区] 文档。如果不指定，则使用 "Asia/Shanghai" 时区。

    5. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）
        字符串类（CHAR/VARCHAR）："I am a student", "a"
        NULL值：\N

## example

    1. 从 HDFS 导入一批数据，指定超时时间和过滤比例。使用名为 my_spark 的 spark 资源。

        LOAD LABEL example_db.label1
        (
            DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
            INTO TABLE `my_table`
        )
        WITH RESOURCE 'my_spark'
        PROPERTIES
        (
            "timeout" = "3600",
            "max_filter_ratio" = "0.1"
        );
    
        其中 hdfs_host 为 namenode 的 host，hdfs_port 为 fs.defaultFS 端口（默认9000）

    2. 从 HDFS 导入一批"负"数据，指定分隔符为逗号，使用通配符*指定目录下的所有文件，并指定 spark 资源的临时参数。

        LOAD LABEL example_db.label3
        (
            DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
            NEGATIVE
            INTO TABLE `my_table`
            COLUMNS TERMINATED BY ","
        )
        WITH RESOURCE 'my_spark'
        (
            "spark.executor.memory" = "3g",
            "broker.username" = "hdfs_user",
            "broker.password" = "hdfs_passwd"
        );
    
    3. 从 HDFS 导入一批数据，指定分区, 并对导入文件的列做一些转化，如下：
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
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, tmp_k2, tmp_k3)
        SET (
          k2 = tmp_k2 + tmp_k3
        )
        )
        WITH RESOURCE 'my_spark';
    
    4. 提取文件路径中的分区字段

        如果需要，则会根据表中定义的字段类型解析文件路径中的分区字段（partitioned fields），类似Spark中Partition Discovery的功能

        LOAD LABEL example_db.label10
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/*/*")
        INTO TABLE `my_table`
        (k1, k2, k3)
        COLUMNS FROM PATH AS (city, utc_date)
        SET (uniq_id = md5sum(k1, city))
        )
        WITH RESOURCE 'my_spark';

        hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing目录下包括如下文件：

        [hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]

        则提取文件路径的中的city和utc_date字段

    5. 对待导入数据进行过滤，k1 值大于 10 的列才能被导入。

        LOAD LABEL example_db.label10
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        WHERE k1 > 10
        )
        WITH RESOURCE 'my_spark';

    6. 从 hive 外部表导入，并将源表中的 uuid 列通过全局字典转化为 bitmap 类型。

        LOAD LABEL db1.label1
        (
            DATA FROM TABLE hive_t1
            INTO TABLE tbl1
            SET
            (
                uuid=bitmap_dict(uuid)
            )
        )
        WITH RESOURCE 'my_spark';

## keyword

    SPARK,LOAD
