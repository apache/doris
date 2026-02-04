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

import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.producer.ProducerRecord

suite("test_routine_load_with_aes_encrypt", "p0") {
    def tableName1 = "test_routine_load_aes_encrypt_direct_key"
    def tableName2 = "test_routine_load_aes_encrypt_with_encryptkey"
    def kafkaTopic1 = "test_routine_load_aes_encrypt_direct"
    def kafkaTopic2 = "test_routine_load_aes_encrypt_key"
    def jobName1 = "routine_load_aes_direct_key_job"
    def jobName2 = "routine_load_aes_encryptkey_job"
    def dbName = context.dbName
    def encryptKeyName = "routine_load_aes_key"
    def aesKey = "F3229A0B371ED2D9441B830D21A390C3"

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        // Test data
        def testData = [
            "1,Alice,hello world",
            "2,Bob,doris is great",
            "3,Charlie,test encryption"
        ]

        // Send test data to both Kafka topics
        [kafkaTopic1, kafkaTopic2].each { topic ->
            testData.each { line ->
                logger.info("Sending to Kafka topic ${topic}: ${line}")
                def record = new ProducerRecord<>(topic, null, line)
                producer.send(record).get()
            }
        }
        producer.flush()

        // ============================================================
        // Test 1: AES_ENCRYPT with direct key string
        // ============================================================
        log.info("Test 1: Routine load with AES_ENCRYPT using direct key string")

        sql """ DROP TABLE IF EXISTS ${tableName1} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
                `id` int(11) NULL,
                `name` string NULL,
                `encrypted_data` string NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        try {
            sql """
                CREATE ROUTINE LOAD ${jobName1} ON ${tableName1}
                COLUMNS TERMINATED BY ",",
                COLUMNS(id, name, tmp_data, encrypted_data=TO_BASE64(AES_ENCRYPT(tmp_data, '${aesKey}')))
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaTopic1}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            RoutineLoadTestUtils.waitForTaskFinish(runSql, jobName1, tableName1, 2)

            // Verify data count
            qt_sql_count_direct """ SELECT count(*) FROM ${tableName1} """

            // Verify encrypted data can be decrypted
            qt_sql_decrypt_direct """
                SELECT id, name, AES_DECRYPT(FROM_BASE64(encrypted_data), '${aesKey}') as decrypted_data
                FROM ${tableName1}
                ORDER BY id
            """
        } finally {
            sql "STOP ROUTINE LOAD FOR ${jobName1}"
            sql """ DROP TABLE IF EXISTS ${tableName1} """
        }

        // ============================================================
        // Test 2: AES_ENCRYPT with ENCRYPTKEY (KEY syntax)
        // ============================================================
        log.info("Test 2: Routine load with AES_ENCRYPT using ENCRYPTKEY (KEY syntax)")

        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                `id` int(11) NULL,
                `name` string NULL,
                `encrypted_data` string NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        // Create encryptkey
        try_sql """ DROP ENCRYPTKEY IF EXISTS ${encryptKeyName} """
        sql """ CREATE ENCRYPTKEY ${encryptKeyName} AS "${aesKey}" """

        // Verify encryptkey was created
        def keyRes = sql """ SHOW ENCRYPTKEYS FROM ${dbName} """
        log.info("Encryptkeys: ${keyRes}")
        assertTrue(keyRes.size() >= 1, "Encryptkey should be created")

        try {
            sql """
                CREATE ROUTINE LOAD ${jobName2} ON ${tableName2}
                COLUMNS TERMINATED BY ",",
                COLUMNS(id, name, tmp_data, encrypted_data=TO_BASE64(AES_ENCRYPT(tmp_data, KEY ${dbName}.${encryptKeyName})))
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaTopic2}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            RoutineLoadTestUtils.waitForTaskFinish(runSql, jobName2, tableName2, 2)

            // Verify data count
            qt_sql_count_encryptkey """ SELECT count(*) FROM ${tableName2} """

            // Verify encrypted data can be decrypted using ENCRYPTKEY
            qt_sql_decrypt_with_encryptkey """
                SELECT id, name, AES_DECRYPT(FROM_BASE64(encrypted_data), KEY ${dbName}.${encryptKeyName}) as decrypted_data
                FROM ${tableName2}
                ORDER BY id
            """

            // Verify encrypted data can also be decrypted using direct key string (same key value)
            qt_sql_decrypt_with_direct_key """
                SELECT id, name, AES_DECRYPT(FROM_BASE64(encrypted_data), '${aesKey}') as decrypted_data
                FROM ${tableName2}
                ORDER BY id
            """
        } finally {
            sql "STOP ROUTINE LOAD FOR ${jobName2}"
            sql """ DROP TABLE IF EXISTS ${tableName2} """
            try_sql """ DROP ENCRYPTKEY IF EXISTS ${encryptKeyName} """
        }
    }
}
