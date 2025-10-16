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


suite("test_routine_load_strict_mode_and_filter_ratio","p0") {
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"
    def kafkaCsvTpoics = [
        "test_decimal_overflow",
        "test_not_number",
        "test_no_partition"
    ]
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
    sleep(10000)

    def create_job_and_produce_data = {jobName, tableName, columns, kafkaTopic, strictMode, maxFilterRatio, expectErrorRows ->
        sql """
            CREATE ROUTINE LOAD ${jobName} on ${tableName}
            COLUMNS(${columns}),
            COLUMNS TERMINATED BY "|"
            PROPERTIES
            (
                "strict_mode" = "${strictMode}",
                "max_filter_ratio" = "${maxFilterRatio}",
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "${kafkaTopic}",
                "property.kafka_default_offsets" = "OFFSET_END"
            );
        """
        sql "sync"

        def count = 0
        while (true) {
            sleep(1000)
            def res = sql "show routine load for ${jobName}"
            def state = res[0][8].toString()
            if (state != "RUNNING") {
                count++
                if (count > 300) {
                    log.info("routine load job state: ${res}".toString())
                    assertEquals(1, 2)
                } 
                continue;
            }
            break;
        }
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        // Create kafka producer
        def producer = new KafkaProducer<>(props)

        def txt = new File("""${context.file.parent}/data/${kafkaTopic}.csv""").text
        def lines = txt.readLines()
        lines.each { line ->
            logger.info("=====${line}========")
            def record = new ProducerRecord<>(kafkaTopic, null, line)
            producer.send(record)
        }

        while (true) {
            sleep(1000)
            def res = sql "show routine load for ${jobName}"
            log.info("routine load statistic: ${res[0][14].toString()}".toString())
            log.info("progress: ${res[0][15].toString()}".toString())
            log.info("lag: ${res[0][16].toString()}".toString())
            res = sql "select count(*) from ${tableName}"
            if (res[0][0] > 0) {
                break;
            }
            count++
            if (count > 60) {
                assertEquals(1, 2)
            } 
            continue;
        }
        def res = sql "show routine load for ${jobName}"
        // log.info("routine load job state: ${res}".toString())
        def state = res[0][8].toString()
        log.info("job state: ${state}".toString())
        def statisticJson = parseJson(res[0][14])
        assertEquals(expectErrorRows, statisticJson.errorRows)
        // assertEquals(statisticJson.loadedRows, expectLoadedRows)
    }

/*
    // number overflow
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // strict_mode=false, and max_filter_ratio = 0, load SUCCESS
        def jobName = "test_routine_load_number_overflow_non_strict"
        def tableName = "test_routine_load_number_overflow_non_strict"
        def kafkaTopic = "test_decimal_overflow"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE ${tableName}
            (
                k00 DECIMALV3(10,0)
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"
            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "false", "0", 0)

            qt_sql_number_overflow0 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
        // strict_mode=true, and max_filter_ratio = 0.3, load SUCCESS
        jobName = "test_routine_load_number_overflow_strict_1"
        tableName = "test_routine_load_number_overflow_strict_1"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 DECIMALV3(10,0)
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"
            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "true", "0.3", 3)

            qt_sql_number_overflow1 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
        // strict_mode=true, and max_filter_ratio = 0.2, load FAIL
        jobName = "test_routine_load_number_overflow_strict_2"
        tableName = "test_routine_load_number_overflow_strict_2"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 DECIMALV3(10,0)
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"
            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "true", "0.2", 3)

            qt_sql_number_overflow2 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }

    // non-number to number
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // strict_mode=false, and max_filter_ratio = 0, load SUCCESS
        def jobName = "test_routine_load_not_number_non_strict"
        def tableName = "test_routine_load_not_number_non_strict"
        def kafkaTopic = "test_not_number"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 DECIMALV3(10,0)
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "false", "0", 0)
            qt_sql_not_number_to_number_non_strict0 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
        // strict_mode=true, and max_filter_ratio = 0, load SUCCESS
        jobName = "test_routine_load_not_number_strict1"
        tableName = "test_routine_load_not_number_strict1"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 DECIMALV3(10,0)
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "true", "0", 3)
            qt_sql_not_number_to_number_strict0 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
    // null value to not null column
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // strict_mode=false, and max_filter_ratio = 0, load SUCCESS
        def jobName = "test_routine_load_not_null_column_non_strict"
        def tableName = "test_routine_load_not_null_column_non_strict"
        def kafkaTopic = "test_not_number"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 DECIMALV3(10,0) NOT NULL
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "false", "0", 3)
            qt_sql_null_to_not_null_column_non_strict0 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
        // strict_mode=true, and max_filter_ratio = 0, load SUCCESS
        jobName = "test_routine_load_not_null_column_strict1"
        tableName = "test_routine_load_not_null_column_strict1"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 DECIMALV3(10,0) NOT NULL
            )
            PROPERTIES ("replication_num" = "1");
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "k00", kafkaTopic, "true", "0", 3)
            qt_sql_null_to_not_null_column_strict0 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
    */
    // no partition
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // strict_mode=false, and max_filter_ratio = 0, load SUCCESS
        def jobName = "test_routine_load_no_partition_non_strict"
        def tableName = "test_routine_load_no_partition_non_strict"
        def kafkaTopic = "test_no_partition"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
                create table ${tableName} (
                  id int,
                  name string
                ) PARTITION BY RANGE(`id`)
                  (
                      PARTITION `p0` VALUES LESS THAN ("60"),
                      PARTITION `p1` VALUES LESS THAN ("80")
                  )
                properties (
                  'replication_num' = '1'
                );
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "id,name", kafkaTopic, "false", "0", 3)
            qt_sql_no_partition_non_strict "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
        // strict_mode=true, and max_filter_ratio = 0, load SUCCESS
        jobName = "test_routine_load_no_partition_strict"
        tableName = "test_routine_load_no_partition_strict"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
                create table ${tableName} (
                  id int,
                  name string
                ) PARTITION BY RANGE(`id`)
                  (
                      PARTITION `p0` VALUES LESS THAN ("60"),
                      PARTITION `p1` VALUES LESS THAN ("80")
                  )
                properties (
                  'replication_num' = '1'
                );
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "id,name", kafkaTopic, "true", "0", 3)
            qt_sql_no_partition_strict "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }

    // string exceed schema length
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // strict_mode=false, and max_filter_ratio = 0, load SUCCESS
        def jobName = "test_routine_string_exceed_len_non_strict"
        def tableName = "test_routine_string_exceed_len_non_strict"
        def kafkaTopic = "test_no_partition"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
                create table ${tableName} (
                  id int,
                  name char(10)
                ) properties ('replication_num' = '1');
            """
            sql "sync"

            create_job_and_produce_data(jobName, tableName, "id,name", kafkaTopic, "false", "0", 0)
            qt_sql_string_exceed_len_non_strict "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
        // strict_mode=true, and max_filter_ratio = 0.2, load success
        jobName = "test_routine_string_exceed_len_strict0"
        tableName = "test_routine_string_exceed_len_strict0"
        try {
            sql """ drop table if exists ${tableName} """
            sql """
                create table ${tableName} (
                  id int,
                  name char(10)
                ) properties ('replication_num' = '1');
            """

            create_job_and_produce_data(jobName, tableName, "id,name", kafkaTopic, "true", "0.2", 3)
            qt_sql_string_exceed_len_strict0 "select * from ${tableName} order by 1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}