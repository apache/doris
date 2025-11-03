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

suite("test_routine_load_multi_table_rename", "p0,fault_injection,nonConcurrent") {
    def kafkaCsvTopic = "multi_table_rename_test"
    def tableName1 = "test_multi_table_rename_tbl1"
    def tableName2 = "test_multi_table_rename_tbl2"
    def jobName = "test_multi_table_rename_job"

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
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

        def producer = new KafkaProducer<>(props)
        
        try {
            sql """ DROP TABLE IF EXISTS ${tableName1} """
            sql """ DROP TABLE IF EXISTS ${tableName2} """
            
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName1} (
                    id INT,
                    name VARCHAR(100),
                    age INT
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
            """
            
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName2} (
                    id INT,
                    score DOUBLE
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
            """
            
            sql "sync"

            // Enable debug point to block commit
            GetDebugPoint().enableDebugPointForAllBEs("MultiTablePipe.exec_plans.block_commit")
            
            sql """
                CREATE ROUTINE LOAD ${jobName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "100",
                    "max_batch_size" = "1048576",
                    "format" = "csv"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            
            // Wait for routine load to start
            def startTime = System.currentTimeMillis()
            def timeout = 60000 // 60 seconds
            def started = false
            while (System.currentTimeMillis() - startTime < timeout) {
                def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
                if (result.size() > 0 && result[0][8] == "RUNNING") {
                    started = true
                    break
                }
                Thread.sleep(1000)
            }
            assertTrue(started, "Routine load job should start")
            
            // Send test data
            def data1 = "${tableName1},1,Alice,25"
            def data2 = "${tableName2},1,95.5"
            producer.send(new ProducerRecord<>(kafkaCsvTopic, null, data1))
            producer.send(new ProducerRecord<>(kafkaCsvTopic, null, data2))
            producer.flush()
            
            // Wait a bit for data to be consumed
            Thread.sleep(3000)
            
            // Rename tables
            sql """ ALTER TABLE ${tableName1} RENAME ${tableName1}_old """
            sql """ ALTER TABLE ${tableName2} RENAME ${tableName2}_old """
            
            // Create new tables with same names
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName1} (
                    id INT,
                    name VARCHAR(100),
                    age INT
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
            """
            
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName2} (
                    id INT,
                    score DOUBLE
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
            """
            
            sql "sync"
            
            // Unblock commit
            GetDebugPoint().disableDebugPointForAllBEs("MultiTablePipe.exec_plans.block_commit")
            
            // Wait for data to be loaded
            Thread.sleep(10000)
            
            def newTable1Count = sql "SELECT COUNT(*) FROM ${tableName1}"
            def newTable2Count = sql "SELECT COUNT(*) FROM ${tableName2}"
            def oldTable1Count = sql "SELECT COUNT(*) FROM ${tableName1}_old"
            def oldTable2Count = sql "SELECT COUNT(*) FROM ${tableName2}_old"
            
            log.info("New table1 (${tableName1}) count: ${newTable1Count[0][0]}")
            log.info("New table2 (${tableName2}) count: ${newTable2Count[0][0]}")
            log.info("Old table1 (${tableName1}_old) count: ${oldTable1Count[0][0]}")
            log.info("Old table2 (${tableName2}_old) count: ${oldTable2Count[0][0]}")
            
            // Data should be in old tables, not new tables
            assertEquals(0, newTable1Count[0][0] as int, "New table1 should be empty")
            assertEquals(0, newTable2Count[0][0] as int, "New table2 should be empty")
            assertTrue(oldTable1Count[0][0] as int > 0, "Old table1 should have data")
            assertTrue(oldTable2Count[0][0] as int > 0, "Old table2 should have data")
            
        } finally {
            try {
                sql "STOP ROUTINE LOAD FOR ${jobName}"
            } catch (Exception e) {
                log.warn("Failed to stop routine load: ${e.message}")
            }
            
            try {
                GetDebugPoint().disableDebugPointForAllBEs("MultiTablePipe.exec_plans.block_commit")
            } catch (Exception e) {
                log.warn("Failed to disable debug point: ${e.message}")
            }
            
            try_sql "DROP TABLE IF EXISTS ${tableName1}"
            try_sql "DROP TABLE IF EXISTS ${tableName2}"
            try_sql "DROP TABLE IF EXISTS ${tableName1}_old"
            try_sql "DROP TABLE IF EXISTS ${tableName2}_old"
            
            if (producer != null) {
                producer.close()
            }
        }
    }
}

