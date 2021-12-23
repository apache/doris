---
{
    "title": "HDFS LOAD",
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

# HDFS LOAD
## description

    Hdfs load 通过BE直接访问社区版本HDFS集群的数据，进行数据导入。不支持Baidu HDFS和Baidu AFS

语法：

    LOAD LABEL load_label
    (
    data_desc1[, data_desc2, ...]
    )
    WITH HDFS
    [load_properties]
    [opt_properties];

    1. load_label

        当前导入批次的标签。在一个 database 内唯一。
        语法：
        [database_name.]your_label
     
    2. data_desc

        用于描述一批导入数据。
        语法：
            [MERGE|APPEND|DELETE]
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
            [PRECEDING FILTER predicate]
            [SET (k1 = func(k2))]
            [WHERE predicate]
            [DELETE ON label=true]
            [ORDER BY source_sequence]
            [read_properties]

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

            用于指定导入文件的类型，例如：parquet、orc、csv。默认值通过文件后缀名判断。 
 
            column_list：

            用于指定导入文件中的列和 table 中的列的对应关系。
            当需要跳过导入文件中的某一列时，将该列指定为 table 中不存在的列名即可。
            语法：
            (col_name1, col_name2, ...)

            PRECEDING FILTER predicate:

            用于过滤原始数据。原始数据是未经列映射、转换的数据。用户可以在对转换前的数据前进行一次过滤，选取期望的数据，再进行转换。
            
            SET:

            如果指定此参数，可以将源文件某一列按照函数进行转化，然后将转化后的结果导入到table中。语法为 `column_name` = expression。举几个例子帮助理解。
            例1: 表中有3个列“c1, c2, c3", 源文件中前两列依次对应(c1,c2)，后两列之和对应c3；那么需要指定 columns (c1,c2,tmp_c3,tmp_c4) SET (c3=tmp_c3+tmp_c4); 
            例2: 表中有3个列“year, month, day"三个列，源文件中只有一个时间列，为”2018-06-01 01:02:03“格式。
            那么可以指定 columns(tmp_time) set (year = year(tmp_time), month=month(tmp_time), day=day(tmp_time)) 完成导入。

            WHERE:
          
            对做完 transform 的数据进行过滤，符合 where 条件的数据才能被导入。WHERE 语句中只可引用表中列名。

            merge_type:

            数据的合并类型，一共支持三种类型APPEND、DELETE、MERGE 其中，APPEND是默认值，表示这批数据全部需要追加到现有数据中，DELETE 表示删除与这批数据key相同的所有行，MERGE 语义 需要与delete on条件联合使用，表示满足delete 条件的数据按照DELETE 语义处理其余的按照APPEND 语义处理,

            delete_on_predicates:

            表示删除条件，仅在 merge type 为MERGE 时有意义，语法与where 相同
            
            ORDER BY:
            
            只适用于UNIQUE_KEYS,相同key列下，保证value列按照source_sequence进行REPLACE, source_sequence可以是数据源中的列，也可以是表结构中的一列。

            read_properties:

            用于指定一些特殊参数。
            语法：
            [PROPERTIES ("key"="value", ...)]

            可以指定如下参数：

            line_delimiter： 用于指定导入文件中的换行符，默认为\n。可以使用做多个字符的组合作为换行符。

            fuzzy_parse： 布尔类型，为true表示json将以第一行为schema 进行解析，开启这个选项可以提高json 导入效率，但是要求所有json 对象的key的顺序和第一行一致， 默认为false，仅用于json格式。

            jsonpaths: 导入json方式分为：简单模式和匹配模式。
            简单模式：没有设置jsonpaths参数即为简单模式，这种模式下要求json数据是对象类型，例如：
            {"k1":1, "k2":2, "k3":"hello"}，其中k1，k2，k3是列名字。
            匹配模式：用于json数据相对复杂，需要通过jsonpaths参数匹配对应的value。

            strip_outer_array: 布尔类型，为true表示json数据以数组对象开始且将数组对象中进行展平，默认值是false。例如：
            [
             {"k1" : 1, "v1" : 2},
             {"k1" : 3, "v1" : 4}
             ]
             当strip_outer_array为true，最后导入到doris中会生成两行数据。

             json_root: json_root为合法的jsonpath字符串，用于指定json document的根节点，默认值为""。

             num_as_string： 布尔类型，为true表示在解析json数据时会将数字类型转为字符串，然后在确保不会出现精度丢失的情况下进行导入。

    3. load_properties
        语法：
            使用HDFS协议直接连接远程存储时需要指定如下属性
            (
                "fs.defaultFS" = "",
                "hdfs_user"="",
                "dfs.nameservices"="my_ha",
                "dfs.ha.namenodes.xxx"="my_nn1,my_nn2",
                "dfs.namenode.rpc-address.xxx.my_nn1"="host1:port",
                "dfs.namenode.rpc-address.xxx.my_nn2"="host2:port",
                "dfs.client.failover.proxy.provider.xxx"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            )
        说明：
            fs.defaultFS: hdfs集群defaultFS
            hdfs_user: 连接hdfs集群时使用的用户名
            
            高可用配置：namenode HA(可选)。通过配置 namenode HA，可以在 namenode 切换时，自动识别到新的 namenode
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
        exec_mem_limit：  导入内存限制。默认为 2GB。单位为字节。
        strict mode：     是否对数据进行严格限制。默认为 false。
        timezone:         指定某些受时区影响的函数的时区，如 strftime/alignment_timestamp/from_unixtime 等等，具体请查阅 [时区] 文档。如果不指定，则使用 "Asia/Shanghai" 时区。
        send_batch_parallelism: 用于设置发送批处理数据的并行度，如果并行度的值超过 BE 配置中的 `max_send_batch_parallelism_per_job`，那么作为协调点的 BE 将使用 `max_send_batch_parallelism_per_job` 的值。

    5. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）
        字符串类（CHAR/VARCHAR）："I am a student", "a"
        NULL值：\N

## example
     1. LOAD WITH HDFS, 普通HDFS集群
        LOAD LABEL example_db.label_filter
        (
            DATA INFILE("hdfs://host:port/user/data/*/test.txt")
            INTO TABLE `tbl1`
            COLUMNS TERMINATED BY ","
            (k1,k2,v1,v2)
        ) 
        with HDFS (
            "fs.defaultFS"="hdfs://testFs",
            "hdfs_user"="user"
        );
     2. LOAD WITH HDFS, 高可用HDFS集群(ha)
        LOAD LABEL example_db.label_filter
        (
            DATA INFILE("hdfs://host:port/user/data/*/test.txt")
            INTO TABLE `tbl1`
            COLUMNS TERMINATED BY ","
            (k1,k2,v1,v2)
        ) 
        with HDFS (
            "fs.defaultFS"="hdfs://testFs",
            "hdfs_user"="user"
            "dfs.nameservices"="my_ha",
            "dfs.ha.namenodes.xxx"="my_nn1,my_nn2",
            "dfs.namenode.rpc-address.xxx.my_nn1"="host1:port",
            "dfs.namenode.rpc-address.xxx.my_nn2"="host2:port",
            "dfs.client.failover.proxy.provider.xxx"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );

## keyword

    HDFS,LOAD