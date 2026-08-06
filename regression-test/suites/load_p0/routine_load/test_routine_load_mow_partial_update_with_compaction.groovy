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

suite("test_routine_load_mow_partial_update_with_compaction", "nonConcurrent") {
    // 使用时间戳后缀确保每次运行使用不同 topic，避免历史消息污染精确计数断言
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_mow_pu_compaction_${topicSuffix}"

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_rl_mow_pu_compaction"
        def job = "test_mow_pu_compaction_job_${topicSuffix}"

        // 基于 loadedRows 统计的等待 helper，适用于 MOW partial update（行数不增加的场景）
        def waitForLoadedRows = { String jobName, long expectedLoadedRows, int maxWait = 60 ->
            def waited = 0
            while (waited < maxWait) {
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                def statJson = new groovy.json.JsonSlurper().parseText(res[0][14].toString())
                long loaded = statJson.loadedRows as long
                logger.info("waitForLoadedRows: state=${state}, loadedRows=${loaded}, expected>=${expectedLoadedRows}")
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
                `c2` int,
                `c3` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            );
        """

        // 插入基线数据
        sql """
            INSERT INTO ${tableName} VALUES
            (1, 1, 11, 111),
            (2, 2, 22, 222),
            (3, 3, 33, 333)
        """
        sql "sync"

        // 验证基线
        def baselineRows = sql "SELECT k, c1, c2, c3 FROM ${tableName} ORDER BY k"
        Assert.assertEquals(3, baselineRows.size())
        logger.info("Baseline data: ${baselineRows}")

        try {
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c1, c2)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "partial_columns" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // 第一批 partial update: 更新 key=1,2 的 c1,c2
            def batch1 = [
                "1,10,110",
                "2,20,220"
            ]
            batch1.each { line ->
                logger.info("Sending batch1: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // 等待第一批消费完成（batch1 共 2 条，累计 loadedRows >= 2）
            // 不用 waitForTaskFinish(count>3)，因为基线已有 3 行，行数不会增加
            waitForLoadedRows(job, 2)
            logger.info("Batch1 consumed, triggering full compaction...")

            // 暂停 routine load，避免 compaction 和消费并行引发不确定性
            sql "PAUSE ROUTINE LOAD FOR ${job}"

            // 等待 job 进入 PAUSED
            def pauseWait = 0
            while (pauseWait < 30) {
                def state = sql "show routine load for ${job}"
                if (state[0][8].toString() == "PAUSED") break
                sleep(1000)
                pauseWait++
            }
            def pausedState = sql "show routine load for ${job}"
            Assert.assertEquals("Routine load 应成功进入 PAUSED 再触发 compaction", "PAUSED", pausedState[0][8].toString())

            // 触发 full compaction
            trigger_and_wait_compaction(tableName, "full")
            logger.info("Full compaction completed after batch1")

            // 恢复 routine load
            sql "RESUME ROUTINE LOAD FOR ${job}"

            // 第二批 partial update: 更新 key=1,3 的 c1,c2
            def batch2 = [
                "1,11,1110",
                "3,30,330"
            ]
            batch2.each { line ->
                logger.info("Sending batch2: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // 等待第二批消费完成（batch1+batch2 累计 loadedRows >= 4）
            // 同上不用行数判断
            waitForLoadedRows(job, 4)

            // 核心断言 1: 最终行数
            def finalCount = sql "SELECT count(*) FROM ${tableName}"
            Assert.assertEquals("应有 3 行", 3L, finalCount[0][0])

            def distinctCount = sql "SELECT count(distinct k) FROM ${tableName}"
            Assert.assertEquals("不应有重复 key", 3L, distinctCount[0][0])

            // 核心断言 2: 按主键排序后的精确行集
            // key=1: c1 被第二批覆盖为 11, c2 被覆盖为 1110, c3 不变 111
            // key=2: c1 被第一批更新为 20, c2 更新为 220, c3 不变 222
            // key=3: c1 被第二批更新为 30, c2 更新为 330, c3 不变 333
            def finalRows = sql "SELECT k, c1, c2, c3 FROM ${tableName} ORDER BY k"
            logger.info("Final rows: ${finalRows}")

            Assert.assertEquals(3, finalRows.size())

            // key=1
            Assert.assertEquals(1, finalRows[0][0])
            Assert.assertEquals(11, finalRows[0][1])
            Assert.assertEquals(1110, finalRows[0][2])
            Assert.assertEquals(111, finalRows[0][3])

            // key=2
            Assert.assertEquals(2, finalRows[1][0])
            Assert.assertEquals(20, finalRows[1][1])
            Assert.assertEquals(220, finalRows[1][2])
            Assert.assertEquals(222, finalRows[1][3])

            // key=3
            Assert.assertEquals(3, finalRows[2][0])
            Assert.assertEquals(30, finalRows[2][1])
            Assert.assertEquals(330, finalRows[2][2])
            Assert.assertEquals(333, finalRows[2][3])

            // 核心断言 3: 聚合校验
            def aggResult = sql "SELECT count(*), sum(k) FROM ${tableName}"
            Assert.assertEquals(3L, aggResult[0][0])
            Assert.assertEquals(6L, aggResult[0][1])

            logger.info("All assertions passed: MOW partial update + compaction correctness verified")

        } catch (Exception e) {
            logger.error("Test failed: ${e.getMessage()}")
            throw e
        } finally {
            try {
                sql "STOP ROUTINE LOAD FOR ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: ${e.message}")
            }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName} force"
        }
    }
}
