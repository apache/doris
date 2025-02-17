// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_doc_case","p0") {
    def kafkaCsvTopics = [
                  "test_rl_csv",
                  "test_rl_max_filter_ratio",
                  "test_rl_partition",
                  "test_rl_delete",
                  "test_rl_column_mapping",
                  "test_rl_hll"
                ]

    def kafkaJsonTopics = [
                  "test_rl_json",
                  "test_rl_json_path",
                  "test_rl_json_root",
                  "test_rl_array",
                  "test_rl_map",
                  "test_rl_bitmap"
                ]

    def jsonpaths = [
                '[\"$.id\",\"$.name\",\"$.age\"]',
                '[\"$.name\",\"$.id\",\"$.num\",\"$.age\"]',
                ]

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // define kafka 
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        // Create kafka producer
        def producer = new KafkaProducer<>(props)

        for (String kafkaCsvTopic in kafkaCsvTopics) {
            def txt = new File("""${context.config.dataPath}/doc/data-operate/import/import-way/${kafkaCsvTopic}.csv""").text
            def lines = txt.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }
        for (String kafkaJsonTopic in kafkaJsonTopics) {
            def kafkaJson = new File("""${context.config.dataPath}/doc/data-operate/import/import-way/${kafkaJsonTopic}.json""").text
            def lines = kafkaJson.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaJsonTopic, null, line)
                producer.send(record)
            }
        }     

        // case1: load csv
        def tableName = "test_routine_load_doc_case"
        def jobName = "test_routine_load_doc_case_job"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                user_id            BIGINT       NOT NULL COMMENT "用户 ID",
                name               VARCHAR(20)           COMMENT "用户姓名",
                age                INT                   COMMENT "用户年龄"
            )
            UNIQUE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(user_id, name, age)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql1 "select * from ${tableName} order by user_id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case2: load json
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS(user_id,name,age)
                PROPERTIES(
                    "format"="json",
                    "jsonpaths"='${jsonpaths[0]}'
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case3: alter routine load
        sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(user_id, name, age)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
        """
        sql "pause routine load for ${jobName}"
        sql """
        ALTER ROUTINE LOAD FOR ${jobName}
        PROPERTIES(
            "desired_concurrent_number" = "3"
        )
        FROM KAFKA(
            "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
            "kafka_topic" = "test-topic"
        );
        """
        sql "stop routine load for ${jobName}"

        //case4: max_filter_ratio
        def tableName1 = "test_routine_load_doc_case1"
        sql """ DROP TABLE IF EXISTS ${tableName1} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
                id       INT             NOT NULL   COMMENT "User ID",
                name     VARCHAR(30)     NOT NULL   COMMENT "Name",
                age      INT                        COMMENT "Age"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName1}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_filter_ratio"="0.5",
                    "max_error_number" = "100",
                    "strict_mode" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[1]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName1}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                log.info("url: ${state[0][18].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql4 "select * from ${tableName1} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName1} """
        }

        //case5: kafka_offsets
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "kafka_partitions" = "0",
                    "kafka_offsets" = "3"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql5 "select * from ${tableName} order by user_id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case6: group.id and client.id
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.group.id" = "kafka_job03",
                    "property.client.id" = "kafka_client_03",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql6 "select * from ${tableName} order by user_id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case7: filter
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                WHERE user_id >= 3
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql7 "select * from ${tableName} order by user_id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        // case8: Loading specified partition data
        def tableName2 = "test_routine_load_doc_case2"
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                id       INT             NOT NULL   COMMENT "User ID",
                name     VARCHAR(30)     NOT NULL   COMMENT "Name",
                age      INT                        COMMENT "Age",
                date    DATETIME                 COMMENT "Date"
            )
            DUPLICATE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (PARTITION partition_a VALUES [("0"), ("1")),
            PARTITION partition_b VALUES [("1"), ("2")),
            PARTITION partition_c VALUES [("2"), ("3")))
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName2}
                COLUMNS TERMINATED BY ",",
                PARTITION(partition_b)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[2]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName2}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql8 "select * from ${tableName2} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName2} """
        }

        // case9: timezone
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName2}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "timezone" = "Asia/Shanghai"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[2]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName2}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql9 "select * from ${tableName2} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName2} """
        }

        // case10: merge delete
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(user_id, name, age)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            sql "stop routine load for ${jobName}"
            sql "sync"

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                WITH DELETE
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[3]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql10 "select * from ${tableName} order by user_id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case11: delete on
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                WITH MERGE
                COLUMNS TERMINATED BY ",",
                DELETE ON user_id = 2
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql11 "select * from ${tableName} order by user_id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        // case12: Load with column mapping and derived column calculation
        tableName = "test_routine_load_doc_case3"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id      INT            NOT NULL  COMMENT "id",
                name    VARCHAR(30)    NOT NULL  COMMENT "name",
                age     INT                      COMMENT "age",
                num     INT                      COMMENT "number"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(id, name, age, num=age*10)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[4]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql12 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case13: test json
        tableName = "routine_test12"
        jobName = "kafka_job12"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id      INT            NOT NULL  COMMENT "id",
                name    VARCHAR(30)    NOT NULL  COMMENT "name",
                age     INT                      COMMENT "age"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                PROPERTIES
                (
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql13 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case14: json path
        tableName = "routine_test13"
        jobName = "kafka_job13"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id      INT            NOT NULL  COMMENT "id",
                name    VARCHAR(30)    NOT NULL  COMMENT "name",
                age     INT                      COMMENT "age",
                num     INT                      COMMENT "num"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS(name, id, num, age)
                PROPERTIES
                (
                    "format" = "json",
                    "jsonpaths"='${jsonpaths[1]}'
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[1]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql14 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case15: json root
        tableName = "routine_test14"
        jobName = "kafka_job14"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id      INT            NOT NULL  COMMENT "id",
                name    VARCHAR(30)    NOT NULL  COMMENT "name",
                age     INT                      COMMENT "age"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                PROPERTIES
                (
                    "format" = "json",
                    "json_root" = "\$.source"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[2]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql15 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        // case16: array
        tableName = "routine_test16"
        jobName = "kafka_job16"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id      INT             NOT NULL  COMMENT "id",
                name    VARCHAR(30)     NOT NULL  COMMENT "name",
                age     INT                       COMMENT "age",
                array   ARRAY<int(11)>  NULL      COMMENT "test array column"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                PROPERTIES
                (
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[3]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql16 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        // case17: map
        tableName = "routine_test17"
        jobName = "kafka_job17"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id      INT                 NOT NULL  COMMENT "id",
                name    VARCHAR(30)         NOT NULL  COMMENT "name",
                age     INT                           COMMENT "age",
                map     Map<STRING, INT>    NULL      COMMENT "test column"
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                PROPERTIES
                (
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[4]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql17 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        // case18: bitmap
        tableName = "routine_test18"
        jobName = "kafka_job18"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id        INT            NOT NULL      COMMENT "id",
                name      VARCHAR(30)    NOT NULL      COMMENT "name",
                age       INT                          COMMENT "age",
                bitmap_id INT                          COMMENT "test",
                device_id BITMAP         BITMAP_UNION  COMMENT "test column"
            )
            AGGREGATE KEY (`id`,`name`,`age`,`bitmap_id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS(id, name, age, bitmap_id, device_id=to_bitmap(bitmap_id))
                PROPERTIES
                (
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaJsonTopics[4]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql18 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        // case19: hll
        tableName = "routine_test19"
        jobName = "kafka_job19"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                dt        DATE,
                id        INT,
                name      VARCHAR(10),
                province  VARCHAR(10),
                os        VARCHAR(10),
                pv        hll hll_union
            )
            Aggregate KEY (dt,id,name,province,os)
            distributed by hash(id) buckets 10
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(dt, id, name, province, os, pv=hll_hash(id))
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[5]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                log.info("url: ${state[0][18].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql19 "select * from ${tableName} order by id"
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ truncate table ${tableName} """
        }

        //case 19: Single-task Loading to Multiple Tables
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName}
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[5]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
        } finally {
            sql "stop routine load for ${jobName}"
        }

        //case20: strict mode
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName}
                PROPERTIES
                (
                    "strict_mode" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTopics[5]}"
                );
            """
        } finally {
            sql "stop routine load for ${jobName}"
        }
    }
}