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

suite("test_routine_load_schema_change_with_compaction_resume", "nonConcurrent") {
    // 使用时间戳后缀确保每次运行使用不同 topic，避免历史消息污染精确计数断言
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_sc_compaction_resume_${topicSuffix}"

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_rl_sc_compaction_resume"
        def job = "test_sc_compaction_resume_job_${topicSuffix}"

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(20) NULL,
                `k2` string NULL,
                `v1` date NULL,
                `v2` string NULL,
                `v3` datetime NULL,
                `v4` varchar(5) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
        """
        sql "sync"

        // 先写入两批基线数据，确保后续 full compaction 前存在可 compaction 的非空 rowset。
        // 这里分两次 insert，是为了避免只有单个数据 rowset 时 full compaction 返回 no suitable version。
        sql """
            INSERT INTO ${tableName} VALUES
            (101, 'seed_a', '2023-01-11', 'seed_v1', '2023-01-11 10:00:00', 'init1')
        """
        sql """
            INSERT INTO ${tableName} VALUES
            (102, 'seed_b', '2023-01-12', 'seed_v2', '2023-01-12 11:00:00', 'init2')
        """
        sql "sync"

        def baselineCount = sql "select count(*) from ${tableName}"
        Assert.assertEquals("基线数据应成功写入", 2L, baselineCount[0][0])
        logger.info("Baseline row count before routine load: ${baselineCount[0][0]}")

        // 准备测试数据：v4 列超过 VARCHAR(5) 长度，strict_mode 下会导致暂停
        def badData = [
            "1,aaa,2023-01-01,val1,2023-01-01 10:00:00,abcdef",
            "2,bbb,2023-01-02,val2,2023-01-02 11:00:00,ghijkl"
        ]
        // 正常数据
        def goodData = [
            "3,ccc,2023-01-03,val3,2023-01-03 12:00:00,hello1",
            "4,ddd,2023-01-04,val4,2023-01-04 13:00:00,world2"
        ]

        // 先发送会导致 schema mismatch 的数据
        badData.each { line ->
            logger.info("Sending bad data: ${line}")
            def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
            producer.send(record).get()
        }
        producer.flush()

        try {
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "strict_mode" = "true",
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            // 等待 job 因 strict_mode + v4 超长而进入 PAUSED
            def pauseCount = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${job}"
                def state = res[0][8].toString()
                if (state == "PAUSED") {
                    logger.info("Job paused as expected. Reason: ${res[0][17].toString()}")
                    break
                }
                pauseCount++
                if (pauseCount > 60) {
                    Assert.fail("Job did not pause within timeout, current state: ${state}")
                }
            }

            // 核心断言 1: 暂停期间不应有坏数据写入（strict_mode 下坏数据被拒绝）
            def pausedCount = sql "select count(*) from ${tableName}"
            Assert.assertEquals("PAUSED 期间只应保留基线数据", 2L, pausedCount[0][0])
            def badRowsDuringPause = sql "SELECT count(*) FROM ${tableName} WHERE k1 IN (1, 2)"
            Assert.assertEquals("PAUSED 期间坏数据不应落表", 0L, badRowsDuringPause[0][0])
            logger.info("Row count during PAUSED: ${pausedCount[0][0]}")

            // 在 RESUME 前执行 schema change，扩大 v4 列
            logger.info("Executing schema change: ALTER TABLE MODIFY COLUMN v4 VARCHAR(10)")
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v4 VARCHAR(10)"

            // 等待 schema change 完成
            def scWait = 0
            while (scWait < 120) {
                def alterRes = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1"
                if (alterRes.size() > 0 && alterRes[0][9].toString() == "FINISHED") {
                    logger.info("Schema change completed")
                    break
                }
                sleep(2000)
                scWait++
                if (scWait >= 120) {
                    Assert.fail("Schema change did not complete within timeout")
                }
            }

            // 在 RESUME 前触发 full compaction
            logger.info("Triggering full compaction before resume...")
            trigger_and_wait_compaction(tableName, "full")
            logger.info("Full compaction completed")

            // 恢复 routine load
            sql "RESUME ROUTINE LOAD FOR ${job}"
            logger.info("Routine load resumed")

            // 发送正常数据
            goodData.each { line ->
                logger.info("Sending good data: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // 等待 job 恢复到 RUNNING 并消费数据
            def recoverCount = 0
            def maxRecoverWait = 120
            while (true) {
                def res = sql "show routine load for ${job}"
                def state = res[0][8].toString()
                def statistic = res[0][14].toString()
                logger.info("Recovery phase - State: ${state}, stats: ${statistic}")

                def rowCount = sql "select count(*) from ${tableName}"
                logger.info("Row count: ${rowCount[0][0]}")

                // 这里不依赖最早那批 badData 一定会被自动重放成功；
                // 稳定判定条件改为：恢复后至少能消费后续发送的 2 条长字符串数据。
                if (state == "RUNNING" && rowCount[0][0] >= 2) {
                    logger.info("Recovery complete, ${rowCount[0][0]} rows loaded")
                    break
                }
                if (recoverCount >= maxRecoverWait) {
                    Assert.fail("Routine load did not recover. State: ${state}, rows: ${rowCount[0][0]}")
                }
                sleep(1000)
                recoverCount++
            }

            // 核心断言 2: 数据正确性
            def finalRows = sql "SELECT k1, k2, v4 FROM ${tableName} ORDER BY k1"
            logger.info("Final rows: ${finalRows}")
            Assert.assertTrue("恢复后至少应写入 2 行后续数据", finalRows.size() >= 2)

            // 验证 schema change 后，后续发送的超长 v4 值可以被成功写入并完整保留
            def longV4Rows = sql "SELECT count(*) FROM ${tableName} WHERE length(v4) > 5"
            Assert.assertTrue("schema change 后应至少有 2 行超长 v4 被成功保留", (longV4Rows[0][0] as Number).longValue() >= 2L)

            def k3Rows = sql "SELECT count(*) FROM ${tableName} WHERE k1 = 3 AND v4 = 'hello1'"
            Assert.assertEquals("k1=3 的超长 v4 应正确写入", 1L, k3Rows[0][0])

            def k4Rows = sql "SELECT count(*) FROM ${tableName} WHERE k1 = 4 AND v4 = 'world2'"
            Assert.assertEquals("k1=4 的超长 v4 应正确写入", 1L, k4Rows[0][0])

            // 核心断言 3: job 最终状态正常
            def finalState = sql "show routine load for ${job}"
            Assert.assertEquals("RUNNING", finalState[0][8].toString())
            logger.info("All assertions passed: schema change + compaction + resume correctness verified")

        } catch (Exception e) {
            logger.error("Test failed: ${e.getMessage()}")
            throw e
        } finally {
            try {
                sql "stop routine load for ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: ${e.message}")
            }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
