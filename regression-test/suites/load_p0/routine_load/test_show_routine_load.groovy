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

suite("test_show_routine_load","p0") {
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"

    try {
        sql """
            CREATE ROUTINE LOAD testShow
            COLUMNS TERMINATED BY "|"
            PROPERTIES
            (
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "multi_table_load_invalid_table",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """

        sql """
            CREATE ROUTINE LOAD testShow1
            COLUMNS TERMINATED BY "|"
            PROPERTIES
            (
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "multi_table_load_invalid_table",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """
        sql "sync"

        String db = context.config.getDbNameByFile(context.file)
        log.info("reason of state changed: ${db}".toString())

        def res = sql "show routine load for ${db}.testShow"
        log.info("reason of state changed: ${res.size()}".toString())
        assertTrue(res.size() == 1)

        res = sql "show routine load for testShow"
        log.info("reason of state changed: ${res.size()}".toString())
        assertTrue(res.size() == 1)

        res = sql "show all routine load"
        log.info("reason of state changed: ${res.size()}".toString())
        assertTrue(res.size() > 1)

        res = sql "SHOW ROUTINE LOAD LIKE \"%testShow%\""
        log.info("reason of state changed: ${res.size()}".toString())
        assertTrue(res.size() == 2)
    } finally {
        sql "stop routine load for testShow"
        sql "stop routine load for testShow1"
    }
}
