---
{
    "title": "ROUTINE LOAD",
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

# ROUTINE LOAD
## description

    例行导入（Routine Load）功能，支持用户提交一个常驻的导入任务，通过不断的从指定的数据源读取数据，将数据导入到 Doris 中。
    目前仅支持通过无认证或者 SSL 认证方式，从 Kakfa 导入文本格式（CSV）的数据。

语法：

    CREATE ROUTINE LOAD [db.]job_name ON tbl_name
    [merge_type]
    [load_properties]
    [job_properties]
    FROM data_source
    [data_source_properties]

    1. [db.]job_name

        导入作业的名称，在同一个 database 内，相同名称只能有一个 job 在运行。

    2. tbl_name

        指定需要导入的表的名称。
        
    3. merge_type
        数据的合并类型，一共支持三种类型APPEND、DELETE、MERGE 其中，APPEND是默认值，表示这批数据全部需要追加到现有数据中，DELETE 表示删除与这批数据key相同的所有行，MERGE 语义 需要与delete on条件联合使用，表示满足delete 条件的数据按照DELETE 语义处理其余的按照APPEND 语义处理, 语法为[WITH MERGE|APPEND|DELETE]

    4. load_properties

        用于描述导入数据。语法：
        [column_separator],
        [columns_mapping],
        [where_predicates],
        [delete_on_predicates],
        [source_sequence],
        [partitions],
        [preceding_predicates]

        1. column_separator:

            指定列分隔符，如：

                COLUMNS TERMINATED BY ","

            默认为：\t

        2. columns_mapping:

            指定源数据中列的映射关系，以及定义衍生列的生成方式。

            1. 映射列：

                按顺序指定，源数据中各个列，对应目的表中的哪些列。对于希望跳过的列，可以指定一个不存在的列名。
                假设目的表有三列 k1, k2, v1。源数据有4列，其中第1、2、4列分别对应 k2, k1, v1。则书写如下：

                COLUMNS (k2, k1, xxx, v1)

                其中 xxx 为不存在的一列，用于跳过源数据中的第三列。

            2. 衍生列：

                以 col_name = expr 的形式表示的列，我们称为衍生列。即支持通过 expr 计算得出目的表中对应列的值。
                衍生列通常排列在映射列之后，虽然这不是强制的规定，但是 Doris 总是先解析映射列，再解析衍生列。
                接上一个示例，假设目的表还有第4列 v2，v2 由 k1 和 k2 的和产生。则可以书写如下：

                COLUMNS (k2, k1, xxx, v1, v2 = k1 + k2);

        3. where_predicates

            用于指定过滤条件，以过滤掉不需要的列。过滤列可以是映射列或衍生列。
            例如我们只希望导入 k1 大于 100 并且 k2 等于 1000 的列，则书写如下：

            WHERE k1 > 100 and k2 = 1000

        4. partitions

            指定导入目的表的哪些 partition 中。如果不指定，则会自动导入到对应的 partition 中。
            示例：

            PARTITION(p1, p2, p3)

        5. delete_on_predicates
            表示删除条件，仅在 merge type 为MERGE 时有意义，语法与where 相同

        6. source_sequence:
        
            只适用于UNIQUE_KEYS,相同key列下，保证value列按照source_sequence列进行REPLACE, source_sequence可以是数据源中的列，也可以是表结构中的一列。

        7. preceding_predicates

            PRECEDING FILTER predicate

            用于过滤原始数据。原始数据是未经列映射、转换的数据。用户可以在对转换前的数据前进行一次过滤，选取期望的数据，再进行转换。

    5. job_properties

        用于指定例行导入作业的通用参数。
        语法：

        PROPERTIES (
            "key1" = "val1",
            "key2" = "val2"
        )

        目前我们支持以下参数：

        1. desired_concurrent_number

            期望的并发度。一个例行导入作业会被分成多个子任务执行。这个参数指定一个作业最多有多少任务可以同时执行。必须大于0。默认为3。
            这个并发度并不是实际的并发度，实际的并发度，会通过集群的节点数、负载情况，以及数据源的情况综合考虑。
            例：

                "desired_concurrent_number" = "3"

        2. max_batch_interval/max_batch_rows/max_batch_size

            这三个参数分别表示：
            1）每个子任务最大执行时间，单位是秒。范围为 5 到 60。默认为10。
            2）每个子任务最多读取的行数。必须大于等于200000。默认是200000。
            3）每个子任务最多读取的字节数。单位是字节，范围是 100MB 到 1GB。默认是 100MB。

            这三个参数，用于控制一个子任务的执行时间和处理量。当任意一个达到阈值，则任务结束。
            例：

                "max_batch_interval" = "20",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200"

        3. max_error_number

            采样窗口内，允许的最大错误行数。必须大于等于0。默认是 0，即不允许有错误行。
            采样窗口为 max_batch_rows * 10。即如果在采样窗口内，错误行数大于 max_error_number，则会导致例行作业被暂停，需要人工介入检查数据质量问题。
            被 where 条件过滤掉的行不算错误行。

        4. strict_mode

            是否开启严格模式，默认为关闭。如果开启后，非空原始数据的列类型变换如果结果为 NULL，则会被过滤。指定方式为 "strict_mode" = "true"

        5. timezone

            指定导入作业所使用的时区。默认为使用 Session 的 timezone 参数。该参数会影响所有导入涉及的和时区有关的函数结果。

        6. format

            指定导入数据格式，默认是csv，支持json格式。

        7. jsonpaths

            jsonpaths: 导入json方式分为：简单模式和匹配模式。如果设置了jsonpath则为匹配模式导入，否则为简单模式导入，具体可参考示例。

        8. strip_outer_array

            布尔类型，为true表示json数据以数组对象开始且将数组对象中进行展平，默认值是false。

        9. json_root

            json_root为合法的jsonpath字符串，用于指定json document的根节点，默认值为""。

        10. send_batch_parallelism
            
            整型，用于设置发送批处理数据的并行度，如果并行度的值超过 BE 配置中的 `max_send_batch_parallelism_per_job`，那么作为协调点的 BE 将使用 `max_send_batch_parallelism_per_job` 的值。 
               
        11. load_to_single_tablet
         
            布尔类型，为true表示支持一个任务只导入数据到对应分区的一个tablet，默认值为false，该参数只允许在对带有random分区的olap表导数的时候设置。

    6. data_source

        数据源的类型。当前支持：

            KAFKA

    7. data_source_properties

        指定数据源相关的信息。
        语法：

        (
            "key1" = "val1",
            "key2" = "val2"
        )

        1. KAFKA 数据源

            1. kafka_broker_list

                Kafka 的 broker 连接信息。格式为 ip:host。多个broker之间以逗号分隔。
                示例：

                    "kafka_broker_list" = "broker1:9092,broker2:9092"

            2. kafka_topic

                指定要订阅的 Kafka 的 topic。
                示例：

                    "kafka_topic" = "my_topic"

            3. kafka_partitions/kafka_offsets

                指定需要订阅的 kafka partition，以及对应的每个 partition 的起始 offset。

                offset 可以指定从大于等于 0 的具体 offset，或者：
                1) OFFSET_BEGINNING: 从有数据的位置开始订阅。
                2) OFFSET_END: 从末尾开始订阅。
                3) 时间戳，格式必须如："2021-05-11 10:00:00"，系统会自动定位到大于等于该时间戳的第一个消息的offset。注意，时间戳格式的offset不能和数字类型混用，只能选其一。

                如果没有指定，则默认从 OFFSET_END 开始订阅 topic 下的所有 partition。
                示例：

                    "kafka_partitions" = "0,1,2,3",
                    "kafka_offsets" = "101,0,OFFSET_BEGINNING,OFFSET_END"

                    "kafka_partitions" = "0,1",
                    "kafka_offsets" = "2021-05-11 10:00:00, 2021-05-11 11:00:00"
                    
            4. property

                指定自定义kafka参数。
                功能等同于kafka shell中 "--property" 参数。
                当参数的 value 为一个文件时，需要在 value 前加上关键词："FILE:"。
                关于如何创建文件，请参阅 "HELP CREATE FILE;"
                更多支持的自定义参数，请参阅 librdkafka 的官方 CONFIGURATION 文档中，client 端的配置项。

                示例:
                    "property.client.id" = "12345",
                    "property.ssl.ca.location" = "FILE:ca.pem"

                1.使用 SSL 连接 Kafka 时，需要指定以下参数：

                "property.security.protocol" = "ssl",
                "property.ssl.ca.location" = "FILE:ca.pem",
                "property.ssl.certificate.location" = "FILE:client.pem",
                "property.ssl.key.location" = "FILE:client.key",
                "property.ssl.key.password" = "abcdefg"

                其中：
                "property.security.protocol" 和 "property.ssl.ca.location" 为必须，用于指明连接方式为 SSL，以及 CA 证书的位置。

                如果 Kafka server 端开启了 client 认证，则还需设置：

                "property.ssl.certificate.location"
                "property.ssl.key.location"
                "property.ssl.key.password"

                分别用于指定 client 的 public key，private key 以及 private key 的密码。


                2.指定kafka partition的默认起始offset
                如果没有指定kafka_partitions/kafka_offsets,默认消费所有分区,此时可以指定kafka_default_offsets指定起始 offset。默认为 OFFSET_END，即从末尾开始订阅。
                值为
                    1) OFFSET_BEGINNING: 从有数据的位置开始订阅。
                    2) OFFSET_END: 从末尾开始订阅。
                    3) 时间戳，格式同 kafka_offsets

                    示例：
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    "property.kafka_default_offsets" = "2021-05-11 10:00:00"

    8. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        字符串类（CHAR/VARCHAR）（无引号）：I am a student, a
        NULL值：\N

## example
    1. 为 example_db 的 example_tbl 创建一个名为 test1 的 Kafka 例行导入任务。指定列分隔符和 group.id 和 client.id，并且自动默认消费所有分区，且从有数据的位置（OFFSET_BEGINNING）开始订阅

        CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS TERMINATED BY ",",
        COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100)
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "property.group.id" = "xxx",
            "property.client.id" = "xxx",
            "property.kafka_default_offsets" = "OFFSET_BEGINNING"
        );

    2. 为 example_db 的 example_tbl 创建一个名为 test1 的 Kafka 例行导入任务。导入任务为严格模式。

        CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
        WHERE k1 > 100 and k2 like "%doris%"
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "kafka_partitions" = "0,1,2,3",
            "kafka_offsets" = "101,0,0,200"
        );

    3. 通过 SSL 认证方式，从 Kafka 集群导入数据。同时设置 client.id 参数。导入任务为非严格模式，时区为 Africa/Abidjan

        CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
        WHERE k1 > 100 and k2 like "%doris%"
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false",
            "timezone" = "Africa/Abidjan"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "property.security.protocol" = "ssl",
            "property.ssl.ca.location" = "FILE:ca.pem",
            "property.ssl.certificate.location" = "FILE:client.pem",
            "property.ssl.key.location" = "FILE:client.key",
            "property.ssl.key.password" = "abcdefg",
            "property.client.id" = "my_client_id"
        );
    4. 简单模式导入json
        CREATE ROUTINE LOAD example_db.test_json_label_1 ON table1
        COLUMNS(category,price,author)
        PROPERTIES
        (
        "desired_concurrent_number"="3",
        "max_batch_interval" = "20",
        "max_batch_rows" = "300000",
        "max_batch_size" = "209715200",
        "strict_mode" = "false",
        "format" = "json"
        )
        FROM KAFKA
        (
        "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
        "kafka_topic" = "my_topic",
        "kafka_partitions" = "0,1,2",
        "kafka_offsets" = "0,0,0"
        );
        支持两种json数据格式：
      1）{"category":"a9jadhx","author":"test","price":895}
        2）[
                {"category":"a9jadhx","author":"test","price":895},
                {"category":"axdfa1","author":"EvelynWaugh","price":1299}
            ]

    5. 精准导入json数据格式
        CREATE TABLE `example_tbl` (
        `category` varchar(24) NULL COMMENT "",
        `author` varchar(24) NULL COMMENT "",
        `timestamp` bigint(20) NULL COMMENT "",
        `dt` int(11) NULL COMMENT "",
        `price` double REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`category`,`author`,`timestamp`,`dt`)
        COMMENT "OLAP"
        PARTITION BY RANGE(`dt`)
        (PARTITION p0 VALUES [("-2147483648"), ("20200509")),
        PARTITION p20200509 VALUES [("20200509"), ("20200510")),
        PARTITION p20200510 VALUES [("20200510"), ("20200511")),
        PARTITION p20200511 VALUES [("20200511"), ("20200512")))
        DISTRIBUTED BY HASH(`category`,`author`,`timestamp`) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
          );

        CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false",
            "format" = "json",
            "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
            "strip_outer_array" = "true"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "kafka_partitions" = "0,1,2",
            "kafka_offsets" = "0,0,0"
        );
   json数据格式:
        [
          {"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
          {"category":"22","author":"2avc","price":895,"timestamp":1589191487},
          {"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
        ]
        说明：
           1）如果json数据是以数组开始，并且数组中每个对象是一条记录，则需要将strip_outer_array设置成true，表示展平数组。
           2）如果json数据是以数组开始，并且数组中每个对象是一条记录，在设置jsonpath时，我们的ROOT节点实际上是数组中对象。

    6. 用户指定根节点json_root
        CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false",
            "format" = "json",
            "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
            "strip_outer_array" = "true",
            "json_root" = "$.RECORDS"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "kafka_partitions" = "0,1,2",
            "kafka_offsets" = "0,0,0"
        );
   json数据格式:
        {
        "RECORDS":[
            {"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
            {"category":"22","author":"2avc","price":895,"timestamp":1589191487},
            {"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
            ]
        }

    7. 为 example_db 的 example_tbl 创建一个名为 test1 的 Kafka 例行导入任务。并且删除与v3 >100 行相匹配的key列的行

        CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        WITH MERGE
        COLUMNS(k1, k2, k3, v1, v2, v3),
        WHERE k1 > 100 and k2 like "%doris%",
        DELETE ON v3 >100
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false"
        )
        FROM KAFKA
        
    8. 导入数据到含有sequence列的UNIQUE_KEYS表中
        CREATE ROUTINE LOAD example_db.test_job ON example_tbl
        COLUMNS TERMINATED BY ",",
        COLUMNS(k1,k2,source_sequence,v1,v2),
        ORDER BY source_sequence
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "30",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200"
        ) FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "kafka_partitions" = "0,1,2,3",
            "kafka_offsets" = "101,0,0,200"
        );

    8. 过滤原始数据

        CREATE ROUTINE LOAD example_db.test_job ON example_tbl
        COLUMNS TERMINATED BY ",",
        COLUMNS(k1,k2,source_sequence,v1,v2),
        PRECEDING FILTER k1 > 2
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "30",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200"
        ) FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "kafka_partitions" = "0,1,2,3",
            "kafka_offsets" = "101,0,0,200"
        );

    9. 从指定的时间点开始消费

        CREATE ROUTINE LOAD example_db.test_job ON example_tbl
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "30",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200"
        ) FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "property.kafka_default_offsets" = "2021-10-10 11:00:00"
        );

## keyword

    CREATE,ROUTINE,LOAD

