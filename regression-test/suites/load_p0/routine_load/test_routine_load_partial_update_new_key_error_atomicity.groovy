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
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert

suite("test_routine_load_partial_update_new_key_error_atomicity", "nonConcurrent") {
    // 使用时间戳后缀确保每次运行使用不同 topic，避免历史消息污染
    def topicSuffix = System.currentTimeMillis()
    // Phase-1: 原子性测试 topic（含新 key 的坏批次）
    def kafkaBadTopic = "test_rl_pu_new_key_error_bad_${topicSuffix}"
    // Phase-2: 恢复测试 topic（仅老 key 的合法数据）
    def kafkaGoodTopic = "test_rl_pu_new_key_error_good_${topicSuffix}"

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_rl_pu_new_key_error_atomicity"
        def badJob = "test_pu_new_key_error_atomicity_job_${topicSuffix}"
        def recoveryJob = "test_pu_new_key_error_recovery_job_${topicSuffix}"

        // 基于 loadedRows 的等待 helper
        def waitForLoadedRows = { String jobName, long expectedLoadedRows, int maxWait = 60 ->
            def waited = 0
            while (waited < maxWait) {
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                def statJson = new groovy.json.JsonSlurper().parseText(res[0][14].toString())
                long loaded = statJson.loadedRows as long
                logger.info("waitForLoadedRows[${jobName}]: state=${state}, loadedRows=${loaded}, expected>=${expectedLoadedRows}")
                if (state == "RUNNING" && loaded >= expectedLoadedRows) break
                if (waited >= maxWait - 1) {
                    Assert.fail("Timeout waiting for loadedRows >= ${expectedLoadedRows}, got ${loaded}")
                }
                sleep(1000)
                waited++
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` int NOT NULL,
                `c1` int,
                `c2` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        // 插入基线数据
        sql """
            INSERT INTO ${tableName} VALUES
            (1, 1, 1),
            (2, 2, 2),
            (3, 3, 3)
        """
        sql "sync"

        // 验证基线数据
        def baselineRows = sql "SELECT * FROM ${tableName} ORDER BY k"
        Assert.assertEquals(3, baselineRows.size())
        logger.info("Baseline data verified: ${baselineRows}")

        try {
            sql """
                CREATE ROUTINE LOAD ${badJob} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c1)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "partial_columns" = "true",
                    "partial_update_new_key_behavior" = "error"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaBadTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // 发送包含老 key 和新 key 的混合数据
            // 在 new_key_behavior=error 模式下，新 key 应导致整批失败
            def mixedData = [
                "1,100",   // 老 key — 合法的 partial update
                "2,200",   // 老 key — 合法的 partial update
                "10,1000", // 新 key — 非法，应触发 error
                "11,1100"  // 新 key — 非法，应触发 error
            ]

            mixedData.each { line ->
                logger.info("Sending mixed data to Kafka bad topic: ${line}")
                def record = new ProducerRecord<>(kafkaBadTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // 等待任务失败（task abort）
            RoutineLoadTestUtils.waitForTaskAbort(runSql, badJob)
            def state = sql "show routine load for ${badJob}"
            logger.info("Job state after mixed batch: ${state[0][8].toString()}")
            logger.info("Job error info: ${state[0][17].toString()}")

            // 核心断言 1: 坏批次整体失败，老 key 不应被局部更新
            // 表数据应仍为基线值
            def afterFailRows = sql "SELECT * FROM ${tableName} ORDER BY k"
            Assert.assertEquals("基线数据行数应不变", 3, afterFailRows.size())
            Assert.assertEquals("key=1 的 c1 应仍为基线值 1", 1, afterFailRows[0][1])
            Assert.assertEquals("key=2 的 c1 应仍为基线值 2", 2, afterFailRows[1][1])
            Assert.assertEquals("key=3 的 c1 应仍为基线值 3", 3, afterFailRows[2][1])
            // 不应有新 key 10, 11
            def newKeyCount = sql "SELECT count(*) FROM ${tableName} WHERE k IN (10, 11)"
            Assert.assertEquals("不应存在新 key", 0L, newKeyCount[0][0])
            logger.info("Atomicity assertion passed: mixed batch fully rejected")

            // Phase-2: 恢复阶段
            // 原 job (badJob) 会不断重试同一坏 offset，不能直接 resume。
            // 方案：停掉 badJob，用干净的 recoveryTopic 创建新 job 验证恢复能力。
            sql "STOP ROUTINE LOAD FOR ${badJob}"

            // 发送只包含老 key 的合法数据到恢复 topic
            def goodData = [
                "1,101",
                "2,202"
            ]
            goodData.each { line ->
                logger.info("Sending good data to recovery topic: ${line}")
                def record = new ProducerRecord<>(kafkaGoodTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // 创建新 job，指向干净的 recoveryTopic
            sql """
                CREATE ROUTINE LOAD ${recoveryJob} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c1)
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "partial_columns" = "true",
                    "partial_update_new_key_behavior" = "error"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaGoodTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // 等待 recovery job 消费 2 条合法数据（MOW partial update 行数不增加，用 loadedRows）
            waitForLoadedRows(recoveryJob, 2)

            // 核心断言 2: 恢复 job 成功写入合法数据。
            // 这里检查的是用户可见的最终结果，不应开启 skip_delete_bitmap。
            def finalRows = sql "SELECT k, c1, c2 FROM ${tableName} ORDER BY k"
            logger.info("Final rows after recovery: ${finalRows}")
            Assert.assertEquals("最终应有 3 行", 3, finalRows.size())
            // key=1: c1 应更新为 101，c2 保持 1
            Assert.assertEquals(1, finalRows[0][0])
            Assert.assertEquals(101, finalRows[0][1])
            Assert.assertEquals(1, finalRows[0][2])
            // key=2: c1 应更新为 202，c2 保持 2
            Assert.assertEquals(2, finalRows[1][0])
            Assert.assertEquals(202, finalRows[1][1])
            Assert.assertEquals(2, finalRows[1][2])
            // key=3: 不变
            Assert.assertEquals(3, finalRows[2][0])
            Assert.assertEquals(3, finalRows[2][1])
            Assert.assertEquals(3, finalRows[2][2])

            // 聚合断言
            def aggResult = sql "SELECT count(*), count(distinct k), sum(k) FROM ${tableName}"
            Assert.assertEquals(3L, aggResult[0][0])
            Assert.assertEquals(3L, aggResult[0][1])
            Assert.assertEquals(6L, aggResult[0][2])
            logger.info("Recovery assertion passed: good batch consumed correctly via recovery job")

        } catch (Exception e) {
            logger.error("Test failed: ${e.getMessage()}")
            throw e
        } finally {
            try { sql "STOP ROUTINE LOAD FOR ${badJob}" } catch (Exception e) { logger.warn("stop badJob: ${e.message}") }
            try { sql "STOP ROUTINE LOAD FOR ${recoveryJob}" } catch (Exception e) { logger.warn("stop recoveryJob: ${e.message}") }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName} force"
        }
    }
}
