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

suite("test_session_concurrency_limits") {
    def variables = [
        "parallel_pipeline_task_num",
        "colocate_max_parallel_num",
        "max_scanners_concurrency",
        "max_file_scanners_concurrency",
        "min_scanners_concurrency",
        "min_file_scanners_concurrency",
        "parallel_scan_max_scanners_count",
        "send_batch_parallelism",
        "load_stream_per_node"
    ]
    def upperBound = 256

    variables.each { variable ->
        def original = (sql "SELECT @@${variable}")[0][0]
        def originalGlobal = (sql "SELECT @@global.${variable}")[0][0]
        try {
            sql "SET ${variable} = ${upperBound}"
            "order_qt_${variable}_upper_bound"("SELECT @@${variable} = ${upperBound}")

            [upperBound + 1, 2147483647L].each { invalid ->
                test {
                    sql "SET ${variable} = ${invalid}"
                    exception "${variable} value should less than or equal ${upperBound}"
                }
            }
            test {
                sql "SET GLOBAL ${variable} = ${upperBound + 1}"
                exception "${variable} value should less than or equal ${upperBound}"
            }
            test {
                sql "SELECT /*+ SET_VAR(${variable}=${upperBound + 1}) */ 1"
                exception "Can not set session variable '${variable}'"
            }
            "order_qt_${variable}_unchanged"(
                    "SELECT @@${variable} = ${upperBound}, @@global.${variable} = ${originalGlobal}")
        } finally {
            sql "SET ${variable} = ${original}"
        }
    }

    def originalPipeline = (sql "SELECT @@parallel_pipeline_task_num")[0][0]
    try {
        sql "SET parallel_pipeline_task_num = 0"
        order_qt_pipeline_auto "SELECT @@parallel_pipeline_task_num"
        test {
            sql "SET parallel_pipeline_task_num = -1"
            exception "parallel_pipeline_task_num value should greater than or equal 0"
        }
    } finally {
        sql "SET parallel_pipeline_task_num = ${originalPipeline}"
    }

    ["colocate_max_parallel_num", "load_stream_per_node"].each { variable ->
        [0, -1].each { invalid ->
            test {
                sql "SET ${variable} = ${invalid}"
                exception "${variable} value should greater than or equal 1"
            }
        }
    }
}
