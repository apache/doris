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

suite("test_routine_load_alter","p0") {
    def kafkaCsvTpoics = [
                  "test_routine_load_alter",
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
        // add timeout config
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")  
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

        // check conenction
        def verifyKafkaConnection = { prod ->
            try {
                logger.info("=====try to connect Kafka========")
                def partitions = prod.partitionsFor("__connection_verification_topic")
                return partitions != null
            } catch (Exception e) {
                throw new Exception("Kafka connect fail: ${e.message}".toString())
            }
        }
        // Create kafka producer
        def producer = new KafkaProducer<>(props)
        try {
            logger.info("Kafka connecting: ${kafka_broker}")
            if (!verifyKafkaConnection(producer)) {
                throw new Exception("can't get any kafka info")
            }
        } catch (Exception e) {
            logger.error("FATAL: " + e.getMessage())
            producer.close()
            throw e  
        }
        logger.info("Kafka connect success")

        for (String kafkaCsvTopic in kafkaCsvTpoics) {
            def txt = new File("""${context.file.parent}/data/${kafkaCsvTopic}.csv""").text
            def lines = txt.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }
    }

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def tableName = "test_routine_load_alter"
        def jobName = "test_alter"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(20) NULL,
                `k2` string NULL,
                `v1` date  NULL,
                `v2` string  NULL,
                `v3` datetime  NULL,
                `v4` string  NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
                    "kafka_partitions" = "0",
                    "kafka_offsets" = "2"
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
            qt_sql_before "select * from ${tableName} order by k1"

            // test alter offset
            sql "pause routine load for ${jobName}"
            sql "ALTER ROUTINE LOAD FOR ${jobName} FROM KAFKA(\"kafka_partitions\" = \"0\", \"kafka_offsets\" = \"1\");"
            sql "resume routine load for ${jobName}"

            def props = new Properties()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            // Create kafka producer
            def producer = new KafkaProducer<>(props)

            for (String kafkaCsvTopic in kafkaCsvTpoics) {
                def txt = new File("""${context.file.parent}/data/${kafkaCsvTopic}.csv""").text
                def lines = txt.readLines()
                lines.each { line ->
                    logger.info("=====${line}========")
                    def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                    producer.send(record)
                }
            }

            count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                log.info("count: ${res[0][0]}".toString())
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] >= 6) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(1000)
                count++
            }
            qt_sql_alter_after "select * from ${tableName} order by k1" 
        } finally {
            sql "stop routine load for ${jobName}"
            sql "truncate table ${tableName}"
        }

        // test alter columns
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY "|",
                COLUMNS(k1, k2)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
                    "kafka_partitions" = "0",
                    "kafka_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            sql "pause routine load for ${jobName}"
            sql "ALTER ROUTINE LOAD FOR ${jobName} COLUMNS(k1, k2, v1, v2, v3, v4);"
            sql "ALTER ROUTINE LOAD FOR ${jobName} COLUMNS TERMINATED BY ',';"
            sql "resume routine load for ${jobName}"
            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                log.info("count: ${res[0][0]}".toString())
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load properties: ${state[0][11].toString()}".toString())
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
                sleep(1000)
                count++
            }
            def res = sql "select * from ${tableName} order by k1"
            log.info("res: ${res.size()}".toString())
            assertEquals(3, res.size())
        } finally {
            sql "stop routine load for ${jobName}"
            sql "truncate table ${tableName}"
        }

        // test show after alter
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(k1, k2)
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
                    "kafka_partitions" = "0",
                    "kafka_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"
            sql "pause routine load for ${jobName}"
            def res = sql "show routine load for ${jobName}"
            log.info("routine load job properties: ${res[0][11].toString()}".toString())
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"desired_concurrent_number\" = \"1\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"max_error_number\" = \"1\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"max_batch_rows\" = \"300001\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"max_batch_size\" = \"209715201\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"max_batch_interval\" = \"6\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"max_filter_ratio\" = \"0.5\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"jsonpaths\" = \"jsonpaths\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"json_root\" = \"json_root\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"strip_outer_array\" = \"true\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"strict_mode\" = \"true\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"timezone\" = \"Asia/Shanghai\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"num_as_string\" = \"true\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"fuzzy_parse\" = \"true\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"workload_group\" = \"alter_routine_load_group\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES(\"max_filter_ratio\" = \"0.5\");"
            sql "ALTER ROUTINE LOAD FOR ${jobName} COLUMNS(k00, k01, k02, k03, k04, k05);"
            sql "ALTER ROUTINE LOAD FOR ${jobName} COLUMNS TERMINATED BY ',';"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PRECEDING FILTER k00 = 8;"
            sql "ALTER ROUTINE LOAD FOR ${jobName} WHERE k00 = 8;"
            sql "ALTER ROUTINE LOAD FOR ${jobName} DELETE ON k00 = 8;"
            sql "ALTER ROUTINE LOAD FOR ${jobName} PARTITION(p1);"
            sql "ALTER ROUTINE LOAD FOR ${jobName} ORDER BY k00;"
            res = sql "show routine load for ${jobName}"
            log.info("routine load job properties: ${res[0][11].toString()}".toString())

            res = sql "show routine load for ${jobName}"
            def json = parseJson(res[0][11])
            assertEquals("1", json.desired_concurrent_number.toString())
            assertEquals("1", json.max_error_number.toString())
            assertEquals("300001", json.max_batch_rows.toString())
            assertEquals("209715201", json.max_batch_size.toString())
            assertEquals("6", json.max_batch_interval.toString())
            assertEquals("0.5", json.max_filter_ratio.toString())
            assertEquals("jsonpaths", json.jsonpaths.toString())
            assertEquals("json_root", json.json_root.toString())
            assertEquals("true", json.strict_mode.toString())
            assertEquals("true", json.strip_outer_array.toString())
            assertEquals("Asia/Shanghai", json.timezone.toString())
            assertEquals("true", json.num_as_string.toString())
            assertEquals("k00,k01,k02,k03,k04,k05", json.columnToColumnExpr.toString())
            assertEquals("','", json.column_separator.toString())
            assertEquals("(CAST(k00 AS double) = CAST(8 AS double))", json.precedingFilter.toString())
            assertEquals("(CAST(k00 AS double) = CAST(8 AS double))", json.whereExpr.toString())
            assertEquals("p1", json.partitions.toString())
            assertEquals("k00", json.sequence_col.toString())
        } finally {
            sql "stop routine load for ${jobName}"
            sql "truncate table ${tableName}"
        }
    }
}
