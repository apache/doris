# ROUTINE LOAD
## description

    例行导入（Routine Load）功能，支持用户提交一个常驻的导入任务，通过不断的从指定的数据源读取数据，将数据导入到 Doris 中。
    目前仅支持通过无认证或者 SSL 认证方式，从 Kakfa 导入文本格式（CSV）的数据。

语法：

    CREATE ROUTINE LOAD [db.]job_name ON tbl_name
    [load_properties]
    [job_properties]
    FROM data_source
    [data_source_properties]

    1. [db.]job_name

        导入作业的名称，在同一个 database 内，相同名称只能有一个 job 在运行。
     
    2. tbl_name

        指定需要导入的表的名称。

    3. load_properties

        用于描述导入数据。语法：

        [column_separator],
        [columns_mapping],
        [where_predicates],
        [partitions]

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

    4. job_properties    

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

            是否开启严格模式，默认为开启。如果开启后，非空原始数据的列类型变换如果结果为 NULL，则会被过滤。指定方式为 "strict_mode" = "true"

        5. timezone
            
            指定导入作业所使用的时区。默认为使用 Session 的 timezone 参数。该参数会影响所有导入涉及的和时区有关的函数结果。

    5. data_source

        数据源的类型。当前支持：

            KAFKA

    6. data_source_properties

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

                如果没有指定，则默认从 OFFSET_END 开始订阅 topic 下的所有 partition。
                示例：

                    "kafka_partitions" = "0,1,2,3",
                    "kafka_offsets" = "101,0,OFFSET_BEGINNING,OFFSET_END" 
                    
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
                    示例：
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"

    7. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234
        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356
        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        字符串类（CHAR/VARCHAR）（无引号）：I am a student, a
        NULL值：\N

## example

    1. 为 example_db 的 example_tbl 创建一个名为 test1 的 Kafka 例行导入任务。导入任务为严格模式。

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

    2. 通过 SSL 认证方式，从 Kafka 集群导入数据。同时设置 client.id 参数。导入任务为非严格模式，时区为 Africa/Abidjan

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

## keyword

    CREATE,ROUTINE,LOAD

