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

suite("test_routine_load_name","p0") {
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    try {
        sql """
                CREATE ROUTINE LOAD test_routine_load_name_too_much_filler_filler_filler_filler_filler_filler_filler
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
    } catch (Exception e) {
        log.info("exception: ${e.toString()}".toString())
        assertEquals(e.toString().contains("Incorrect ROUTINE LOAD NAME name"), true)
        assertEquals(e.toString().contains("required format is"), true)
        assertEquals(e.toString().contains("Maybe routine load job name is longer than 64 or contains illegal characters"), true)
    }
}