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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.Assert

suite("test_routine_load_job_info_system_table","p0") {
    def uniqueSuffix = System.currentTimeMillis()
    def jobName = "test_job_info_system_table_invaild_${uniqueSuffix}"
    def scheduleJobName = "test_job_info_system_table_schedule_${uniqueSuffix}"
    def tableName = "test_job_info_system_table"
    def scheduleTableName = "test_job_info_system_table_schedule"
    def scheduleTopic = "test_job_info_system_table_schedule_${uniqueSuffix}"
    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = null
        def adminClient = null
        try {
            def adminProps = new Properties()
            adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
            adminClient = AdminClient.create(adminProps)

            sql """
            DROP TABLE IF EXISTS ${tableName}
            """
            sql """
            DROP TABLE IF EXISTS ${scheduleTableName}
            """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         NULL,
                k03 TINYINT         NULL,
                k04 SMALLINT        NULL,
                k05 INT             NULL,
                k06 BIGINT          NULL,
                k07 LARGEINT        NULL,
                k08 FLOAT           NULL,
                k09 DOUBLE          NULL,
                k10 DECIMAL(9,1)    NULL,
                k11 DECIMALV3(9,1)  NULL,
                k12 DATETIME        NULL,
                k13 DATEV2          NULL,
                k14 DATETIMEV2      NULL,
                k15 CHAR            NULL,
                k16 VARCHAR         NULL,
                k17 STRING          NULL,
                k18 JSON            NULL,
                kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         NOT NULL DEFAULT "1",
                kd03 SMALLINT        NOT NULL DEFAULT "2",
                kd04 INT             NOT NULL DEFAULT "3",
                kd05 BIGINT          NOT NULL DEFAULT "4",
                kd06 LARGEINT        NOT NULL DEFAULT "5",
                kd07 FLOAT           NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
                kd11 DATE            NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            NULL,
                
                INDEX idx_inverted_k104 (`k05`) USING INVERTED,
                INDEX idx_inverted_k110 (`k11`) USING INVERTED,
                INDEX idx_inverted_k113 (`k13`) USING INVERTED,
                INDEX idx_inverted_k114 (`k14`) USING INVERTED,
                INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
                INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_bitmap_k104 (`k02`) USING INVERTED,
                INDEX idx_bitmap_k110 (`kd01`) USING INVERTED
                
            )
            DUPLICATE KEY(k00)
            PARTITION BY RANGE(k01)
            (
                PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
                PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
                PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
            )
            DISTRIBUTED BY HASH(k00) BUCKETS 32
            PROPERTIES (
                "bloom_filter_columns"="k05",
                "replication_num" = "1"
            );
            """
            sql """
                CREATE ROUTINE LOAD ${jobName} on ${tableName}
                COLUMNS(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18),
                COLUMNS TERMINATED BY "|"
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "test_job_info_system_table_invaild",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def count = 0
            while (true) {
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (state[0][8] == "PAUSED") {
                    break
                }
                if (count >= 30) {
                    assertEquals(1, 2)
                    break
                }
                sleep(1000)
                count++
            }
            def res = sql "SELECT JOB_NAME FROM information_schema.routine_load_jobs WHERE CURRENT_ABORT_TASK_NUM > 0 OR IS_ABNORMAL_PAUSE = TRUE"
            log.info("res: ${res}".toString())
            assertTrue(res.toString().contains("${jobName}"))

            def computeGroupRes = sql "SELECT JOB_NAME, COMPUTE_GROUP FROM information_schema.routine_load_jobs WHERE JOB_NAME = '${jobName}'"
            log.info("compute group res: ${computeGroupRes}".toString())
            assertTrue(computeGroupRes.size() > 0)
            assertNotNull(computeGroupRes[0][1])

            sql """
            CREATE TABLE IF NOT EXISTS ${scheduleTableName}
            (
                k00 INT             NOT NULL,
                k01 VARCHAR(32)     NULL
            )
            DUPLICATE KEY(k00)
            DISTRIBUTED BY HASH(k00) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            """

            adminClient.createTopics([new NewTopic(scheduleTopic, 1, (short) 1)]).all().get()
            producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)
            RoutineLoadTestUtils.sendTestDataToKafka(producer, [scheduleTopic], ["1|a"])
            producer.flush()

            sql """
                CREATE ROUTINE LOAD ${scheduleJobName} on ${scheduleTableName}
                COLUMNS(k00,k01),
                COLUMNS TERMINATED BY "|"
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${scheduleTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            count = 0
            def lastTaskScheduleTimeRes = sql """
                SELECT JOB_NAME, LAST_TASK_SCHEDULE_TIME
                FROM information_schema.routine_load_jobs
                WHERE JOB_NAME = '${scheduleJobName}'
                  AND LAST_TASK_SCHEDULE_TIME != ''
            """
            while (lastTaskScheduleTimeRes.size() == 0) {
                if (count >= 60) {
                    Assert.fail("LAST_TASK_SCHEDULE_TIME was not updated for ${scheduleJobName}")
                }
                def state = sql "show routine load for ${scheduleJobName}"
                def rowCount = sql "SELECT count(*) FROM ${scheduleTableName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("row count: ${rowCount}".toString())
                log.info("last task schedule time res: ${lastTaskScheduleTimeRes}".toString())
                sleep(1000)
                lastTaskScheduleTimeRes = sql """
                    SELECT JOB_NAME, LAST_TASK_SCHEDULE_TIME
                    FROM information_schema.routine_load_jobs
                    WHERE JOB_NAME = '${scheduleJobName}'
                      AND LAST_TASK_SCHEDULE_TIME != ''
                """
                count++
            }
            log.info("last task schedule time res: ${lastTaskScheduleTimeRes}".toString())
            assertTrue(lastTaskScheduleTimeRes.size() > 0)
            assertNotNull(lastTaskScheduleTimeRes[0][1])
            assertTrue(lastTaskScheduleTimeRes[0][1].toString().length() > 0)

            // Verify SHOW ROUTINE LOAD also includes LastTaskScheduleTime
            def showRoutineLoadRes = sql "SHOW ROUTINE LOAD FOR ${scheduleJobName}"
            log.info("show routine load res: ${showRoutineLoadRes}".toString())
            assertTrue(showRoutineLoadRes.size() > 0)
            // LastTaskScheduleTime should be the last column (index 23, after ComputeGroup at index 22)
            def lastTaskScheduleTimeFromShow = showRoutineLoadRes[0][23]
            log.info("LastTaskScheduleTime from SHOW ROUTINE LOAD: ${lastTaskScheduleTimeFromShow}".toString())
            assertNotNull(lastTaskScheduleTimeFromShow)
            assertTrue(lastTaskScheduleTimeFromShow.toString().length() > 0)
            // Verify consistency between SHOW ROUTINE LOAD and system table
            assertEquals(lastTaskScheduleTimeRes[0][1].toString(), lastTaskScheduleTimeFromShow.toString())
        } finally {
            try_sql "stop routine load for ${scheduleJobName}"
            try_sql "stop routine load for ${jobName}"
            if (producer != null) {
                producer.close()
            }
            if (adminClient != null) {
                adminClient.close()
            }
        }
    }
}
