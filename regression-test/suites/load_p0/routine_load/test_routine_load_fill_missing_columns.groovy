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

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

// End-to-end coverage for the json `fill_missing_columns` routine load option.
// It verifies that a job which declares only a derived column in COLUMNS can still load
// into a table that has a sequence column (the sequence column and other base-schema
// columns are auto-filled), and that the same job with `fill_missing_columns` = false
// keeps the original behavior and fails with "need to specify the sequence column".
suite("test_routine_load_fill_missing_columns", "p0") {
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    def dataFile = "test_routine_load_fill_missing_columns.json"
    // Use a per-run topic suffix so OFFSET_BEGINNING jobs only see the records produced in this run.
    // Otherwise a topic retained from a previous run would let the exact row-count assertions read
    // stale messages and fail even when fill_missing_columns works correctly.
    def topicSuffix = System.currentTimeMillis()
    def topic = "test_routine_load_fill_missing_columns_${topicSuffix}"

    // produce one json object per kafka message
    def props = new Properties()
    props.put("bootstrap.servers", "${externalEnvIp}:${kafka_port}".toString())
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    def producer = new KafkaProducer<>(props)
    try {
        def lines = new File("""${context.file.parent}/data/${dataFile}""").text.readLines()
        lines.each { line ->
            if (line.trim().isEmpty()) {
                return
            }
            logger.info("=====${line}========")
            producer.send(new ProducerRecord<>(topic, null, line))
        }
    } finally {
        producer.close()
    }

    // Build a unique-key table whose sequence column maps to a value column that is NOT
    // listed in COLUMNS. Without fill_missing_columns the sequence column cannot be
    // resolved and the job must fail.
    def createTable = { tableName ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT NOT NULL,
                name VARCHAR(50) NULL,
                score INT NULL,
                score_x2 INT NULL,
                update_time BIGINT NULL
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.sequence_col" = "update_time"
            );
        """
    }

    // ---------------------------------------------------------------------------------
    // Positive case: fill_missing_columns = true.
    // COLUMNS only declares the derived column `score_x2`; id/name/score/update_time and
    // the sequence column are auto-filled from the base schema.
    // ---------------------------------------------------------------------------------
    def posTable = "test_routine_load_fill_missing_columns_pos"
    def posJob = "test_routine_load_fill_missing_columns_pos_job"
    try {
        createTable(posTable)
        sql "sync"

        sql """
            CREATE ROUTINE LOAD ${posJob} ON ${posTable}
            COLUMNS(score_x2 = score * 2)
            PROPERTIES
            (
                "format" = "json",
                "fill_missing_columns" = "true",
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200",
                "strict_mode" = "false"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "${topic}",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """
        sql "sync"

        // (a) the job must reach RUNNING and must NOT pause with the sequence-column error
        def count = 0
        while (true) {
            sleep(1000)
            def res = sql "show routine load for ${posJob}"
            def state = res[0][8].toString()
            def reason = res[0][17].toString()
            log.info("positive job state: ${state}, reason: ${reason}".toString())
            assertFalse(reason.contains("need to specify the sequence column"),
                    "fill_missing_columns=true must not fail with sequence column error, reason: ${reason}")
            if (state == "RUNNING") {
                break
            }
            count++
            if (count >= 60) {
                assertEquals("RUNNING", state)
                break
            }
        }

        // (b) the unspecified columns are auto-filled from the base schema
        count = 0
        while (true) {
            def res = sql "select count(*) from ${posTable}"
            def state = sql "show routine load for ${posJob}"
            log.info("positive routine load state: ${state[0][8].toString()}".toString())
            log.info("positive routine load statistic: ${state[0][14].toString()}".toString())
            if (res[0][0] >= 3) {
                break
            }
            if (count >= 60) {
                log.error("positive routine load can not load data for long time")
                assertEquals(3, res[0][0])
                break
            }
            sleep(5000)
            count++
        }
        sql "sync"

        def rows = sql "select id, name, score, score_x2, update_time from ${posTable} order by id"
        assertEquals(3, rows.size())
        // id=1: name/score/update_time auto-filled from json, score_x2 derived as score*2
        assertEquals(1, rows[0][0])
        assertEquals("alice", rows[0][1].toString())
        assertEquals(10, rows[0][2])
        assertEquals(20, rows[0][3])
        assertEquals(100L, rows[0][4])
        // id=2
        assertEquals(2, rows[1][0])
        assertEquals("bob", rows[1][1].toString())
        assertEquals(20, rows[1][2])
        assertEquals(40, rows[1][3])
        assertEquals(200L, rows[1][4])
        // id=3
        assertEquals(3, rows[2][0])
        assertEquals("carol", rows[2][1].toString())
        assertEquals(30, rows[2][2])
        assertEquals(60, rows[2][3])
        assertEquals(300L, rows[2][4])
    } finally {
        try {
            sql "stop routine load for ${posJob}"
        } catch (Exception e) {
            log.info("stop positive routine load failed: ${e.getMessage()}".toString())
        }
        sql "DROP TABLE IF EXISTS ${posTable}"
    }

    // ---------------------------------------------------------------------------------
    // Contrast case: fill_missing_columns = false.
    // The identical all-expression COLUMNS must keep the original behavior and pause the
    // job with the sequence-column error.
    // ---------------------------------------------------------------------------------
    def negTable = "test_routine_load_fill_missing_columns_neg"
    def negJob = "test_routine_load_fill_missing_columns_neg_job"
    try {
        createTable(negTable)
        sql "sync"

        sql """
            CREATE ROUTINE LOAD ${negJob} ON ${negTable}
            COLUMNS(score_x2 = score * 2)
            PROPERTIES
            (
                "format" = "json",
                "fill_missing_columns" = "false",
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200",
                "strict_mode" = "false"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "${topic}",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """
        sql "sync"

        def count = 0
        def paused = false
        while (true) {
            sleep(1000)
            def res = sql "show routine load for ${negJob}"
            def state = res[0][8].toString()
            def reason = res[0][17].toString()
            log.info("negative job state: ${state}, reason: ${reason}".toString())
            if (state == "PAUSED" && reason.contains("need to specify the sequence column")) {
                paused = true
                break
            }
            count++
            if (count >= 60) {
                break
            }
        }
        assertTrue(paused,
                "fill_missing_columns=false must keep the original behavior and fail with sequence column error")

        // no data should be loaded for the paused job
        sql "sync"
        def negCount = sql "select count(*) from ${negTable}"
        assertEquals(0, negCount[0][0])
    } finally {
        try {
            sql "stop routine load for ${negJob}"
        } catch (Exception e) {
            log.info("stop negative routine load failed: ${e.getMessage()}".toString())
        }
        sql "DROP TABLE IF EXISTS ${negTable}"
    }

    // ---------------------------------------------------------------------------------
    // Same-name mapping case: fill_missing_columns = true with COLUMNS(score = score + 1).
    // The mapping target `score` references the same-named source column, so the file slot
    // for `score` must stay available; the other base columns (id/name/update_time) are
    // auto-filled. Without preserving the base scan descriptor for `score`, the reduced plan
    // drops the `score` source slot while the mapping still references it.
    // ---------------------------------------------------------------------------------
    def sameTable = "test_routine_load_fill_missing_columns_same"
    def sameJob = "test_routine_load_fill_missing_columns_same_job"
    try {
        sql "DROP TABLE IF EXISTS ${sameTable}"
        sql """
            CREATE TABLE ${sameTable} (
                id INT NOT NULL,
                name VARCHAR(50) NULL,
                score INT NULL,
                update_time BIGINT NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sql "sync"

        sql """
            CREATE ROUTINE LOAD ${sameJob} ON ${sameTable}
            COLUMNS(score = score + 1)
            PROPERTIES
            (
                "format" = "json",
                "fill_missing_columns" = "true",
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200",
                "strict_mode" = "false"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "${topic}",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """
        sql "sync"

        // the job must reach RUNNING and must not pause with a planning error
        def count = 0
        while (true) {
            sleep(1000)
            def res = sql "show routine load for ${sameJob}"
            def state = res[0][8].toString()
            def reason = res[0][17].toString()
            log.info("same-name mapping job state: ${state}, reason: ${reason}".toString())
            if (state == "RUNNING") {
                break
            }
            count++
            if (count >= 60) {
                assertEquals("RUNNING", state)
                break
            }
        }

        count = 0
        while (true) {
            def res = sql "select count(*) from ${sameTable}"
            if (res[0][0] >= 3) {
                break
            }
            if (count >= 60) {
                log.error("same-name mapping routine load can not load data for long time")
                assertEquals(3, res[0][0])
                break
            }
            sleep(5000)
            count++
        }
        sql "sync"

        // score is mapped to score + 1; the same-named source column is still read from json
        def rows = sql "select id, name, score, update_time from ${sameTable} order by id"
        assertEquals(3, rows.size())
        assertEquals(1, rows[0][0])
        assertEquals("alice", rows[0][1].toString())
        assertEquals(11, rows[0][2])
        assertEquals(100L, rows[0][3])
        assertEquals(2, rows[1][0])
        assertEquals("bob", rows[1][1].toString())
        assertEquals(21, rows[1][2])
        assertEquals(200L, rows[1][3])
        assertEquals(3, rows[2][0])
        assertEquals("carol", rows[2][1].toString())
        assertEquals(31, rows[2][2])
        assertEquals(300L, rows[2][3])
    } finally {
        try {
            sql "stop routine load for ${sameJob}"
        } catch (Exception e) {
            log.info("stop same-name mapping routine load failed: ${e.getMessage()}".toString())
        }
        sql "DROP TABLE IF EXISTS ${sameTable}"
    }
}
