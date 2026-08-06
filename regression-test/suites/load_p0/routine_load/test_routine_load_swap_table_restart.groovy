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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.RoutineLoadTestUtils

suite("test_routine_load_swap_table_restart", "docker") {
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
    def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)
    def topic = "test_routine_load_swap_table_restart"
    def tblTest = "rl_swap_tbl_test"
    def tblMain = "rl_swap_tbl_main"
    def jobName = "test_rl_swap_table_job"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
        def runSql = { String q -> sql q }

        sql "DROP TABLE IF EXISTS ${tblTest}"
        sql "DROP TABLE IF EXISTS ${tblMain}"
        def createTableSql = { name ->
            """
            CREATE TABLE IF NOT EXISTS ${name} (
                `k1` int(20)  NULL,
                `k2` string   NULL,
                `v1` date     NULL,
                `v2` string   NULL,
                `v3` datetime NULL,
                `v4` string   NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
            """
        }
        sql createTableSql(tblTest)
        sql createTableSql(tblMain)

        try {
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tblTest}
                COLUMNS TERMINATED BY ","
                PROPERTIES ("max_batch_interval" = "5")
                FROM KAFKA
                (
                    "kafka_broker_list"              = "${kafka_broker}",
                    "kafka_topic"                    = "${topic}",
                    "property.group.id"              = "test-swap-table-consumer-group",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // Send data and wait for routine load to be RUNNING with rows loaded
            RoutineLoadTestUtils.sendTestDataToKafka(producer, [topic])
            RoutineLoadTestUtils.waitForTaskFinish(runSql, jobName, tblTest, 0)

            // SWAP tblMain with tblTest:
            //   after swap, tableId of tblTest (the RL target) is now registered under tblMain name
            sql "ALTER TABLE ${tblMain} REPLACE WITH TABLE ${tblTest} PROPERTIES('swap' = 'true')"
            // DROP tblTest (now holds the original empty tblMain data)
            sql "DROP TABLE IF EXISTS ${tblTest}"
            logger.info("Swapped and dropped ${tblTest}")
        } finally {
            // Restart FE to trigger gsonPostProcess() replay
            cluster.restartFrontends()
            sleep(30000)
            context.reconnectFe()
            logger.info("FE restarted and reconnected")

            def res = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            def stateAfterRestart = res[0][8].toString()
            logger.info("Routine load state after restart: ${stateAfterRestart}, reason: ${res[0][17]}")

            assertNotEquals("CANCELLED", stateAfterRestart,
                "Routine load must NOT be CANCELLED after FE restart following SWAP TABLE + DROP TABLE")

            sql "STOP ROUTINE LOAD FOR ${jobName}"
            sql "DROP TABLE IF EXISTS ${tblMain}"
        }
    }

    producer.close()
}
