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

suite("test_fe_metrics") {
    httpTest {
        endpoint context.config.feHttpAddress
        uri "/metrics"
        op "get"
        check { code, body ->
            logger.debug("code:${code} body:${body}");
            assertEquals(200, code)
            assertTrue(body.contains("jvm_heap_size_bytes"))
            assertTrue(body.contains("jvm_gc"))
            assertTrue(body.contains("unhealthy_table_num"))
            assertTrue(body.contains("unhealthy_column_num"))
            assertTrue(body.contains("unhealthy_table_rate"))
            assertTrue(body.contains("unhealthy_column_rate"))
            assertTrue(body.contains("empty_table_num"))
            assertTrue(body.contains("empty_table_column_num"))
            assertTrue(body.contains("failed_analyze_task"))
            assertTrue(body.contains("invalid_stats"))
            assertTrue(body.contains("high_priority_queue_length"))
            assertTrue(body.contains("mid_priority_queue_length"))
            assertTrue(body.contains("low_priority_queue_length"))
            assertTrue(body.contains("very_low_priority_queue_length"))
            assertTrue(body.contains("not_analyzed_table_num"))
        }
    }

    httpTest {
        endpoint context.config.feHttpAddress
        uri "/metrics?type=json"
        op "get"
        check { code, body ->
            logger.debug("code:${code} body:${body}");
            assertEquals(200, code)
            assertTrue(body.contains("jvm_heap_size_bytes"))
            assertTrue(body.contains("jvm_gc"))
            assertTrue(body.contains("unhealthy_table_num"))
            assertTrue(body.contains("unhealthy_column_num"))
            assertTrue(body.contains("unhealthy_table_rate"))
            assertTrue(body.contains("unhealthy_column_rate"))
            assertTrue(body.contains("empty_table_num"))
            assertTrue(body.contains("empty_table_column_num"))
            assertTrue(body.contains("failed_analyze_task"))
            assertTrue(body.contains("invalid_stats"))
            assertTrue(body.contains("high_priority_queue_length"))
            assertTrue(body.contains("mid_priority_queue_length"))
            assertTrue(body.contains("low_priority_queue_length"))
            assertTrue(body.contains("very_low_priority_queue_length"))
            assertTrue(body.contains("not_analyzed_table_num"))
        }
    }
}
