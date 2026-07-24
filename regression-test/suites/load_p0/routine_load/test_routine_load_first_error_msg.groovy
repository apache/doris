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

suite("test_routine_load_first_error_msg", "p0") {
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)
    def kafkaTopic = "test_routine_load_first_error_msg_${System.currentTimeMillis()}"
    def jobName = "test_routine_load_first_error_msg"

    def adminProps = new Properties()
    adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
    def adminClient = AdminClient.create(adminProps)
    try {
        adminClient.createTopics([new NewTopic(kafkaTopic, 1, (short) 1)]).all().get()
    } finally {
        adminClient.close()
    }

    try {
        sql "DROP TABLE IF EXISTS test_routine_load_first_error_msg"
        sql """
            CREATE TABLE test_routine_load_first_error_msg (
                id INT,
                name STRING
            )
            PARTITION BY RANGE(id) (
                PARTITION p0 VALUES LESS THAN ("10")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE ROUTINE LOAD ${jobName} ON test_routine_load_first_error_msg
            COLUMNS TERMINATED BY "|"
            PROPERTIES (
                "max_error_number" = "10",
                "max_filter_ratio" = "1.0",
                "max_batch_interval" = "5"
            )
            FROM KAFKA (
                "kafka_broker_list" = "${kafkaBroker}",
                "kafka_topic" = "${kafkaTopic}",
                "property.kafka_default_offsets" = "OFFSET_END"
            )
        """

        def count = 0
        while (true) {
            def showResult = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            if (showResult[0][8].toString() == "RUNNING") {
                break
            }
            if (count++ > 60) {
                Assert.fail("Routine Load job did not enter RUNNING state")
            }
            sleep(1000)
        }

        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)
        try {
            RoutineLoadTestUtils.sendTestDataToKafka(
                    producer, [kafkaTopic], ["100|bad_row", "1|valid_row"])
            producer.flush()
        } finally {
            producer.close()
        }

        count = 0
        while (true) {
            def jobInfo = sql """
                SELECT ERROR_LOG_URLS, FIRST_ERROR_MSG
                FROM information_schema.routine_load_jobs
                WHERE JOB_NAME = '${jobName}'
            """
            def showResult = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            def loadedRows = sql "SELECT COUNT(*) FROM test_routine_load_first_error_msg"
            def informationSchemaErrorLogUrls = jobInfo[0][0].toString()
            def firstErrorMsg = jobInfo[0][1]
            def showErrorLogUrls = showResult[0][18].toString()
            def showFirstErrorMsg = showResult[0][23]
            if (loadedRows[0][0] == 1 && firstErrorMsg != null
                    && firstErrorMsg.toString().contains("bad_row")
                    && showFirstErrorMsg != null
                    && showFirstErrorMsg.toString().contains("bad_row")) {
                assertEquals(informationSchemaErrorLogUrls, showErrorLogUrls)
                assertFalse(informationSchemaErrorLogUrls.contains("first_error_msg:"))
                assertFalse(showErrorLogUrls.contains("first_error_msg:"))
                assertEquals(firstErrorMsg.toString(), showFirstErrorMsg.toString())
                break
            }
            if (count++ > 60) {
                Assert.fail("First error message was not reported by routine_load_jobs and SHOW ROUTINE LOAD: "
                        + "loadedRows=${loadedRows[0][0]}, firstErrorMsg=${firstErrorMsg}, "
                        + "showFirstErrorMsg=${showFirstErrorMsg}, "
                        + "informationSchemaErrorLogUrlsEmpty=${informationSchemaErrorLogUrls.isEmpty()}, "
                        + "showErrorLogUrlsEmpty=${showErrorLogUrls.isEmpty()}")
            }
            sleep(1000)
        }
    } finally {
        try_sql "STOP ROUTINE LOAD FOR ${jobName}"
    }
}
