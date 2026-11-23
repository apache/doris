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

suite("test_routine_load_interval_bounds") {
    // Zero would make BE stop before consuming any records. FE must reject it and negative
    // intervals, as well as the first overflowing second count and Long.MAX_VALUE.
    // Multi-table CREATE needs no table or rows. The source must not be contacted because
    // job properties are checked first. ALTER must also fail before it looks up a stored job.
    for (String seconds : ["-1", "0", "9223372036854776", "9223372036854775807"]) {
        test {
            sql """
                CREATE ROUTINE LOAD interval_bounds_job
                PROPERTIES ("max_batch_interval" = "${seconds}")
                FROM KAFKA (
                    "kafka_broker_list" = "127.0.0.1:1",
                    "kafka_topic" = "interval_bounds_topic"
                )
            """
            exception "max_batch_interval must be between 1 and 9223372036854775"
        }
        test {
            sql """
                ALTER ROUTINE LOAD FOR interval_bounds_job
                PROPERTIES ("max_batch_interval" = "${seconds}")
            """
            exception "max_batch_interval must be between 1 and 9223372036854775"
        }
    }
}
