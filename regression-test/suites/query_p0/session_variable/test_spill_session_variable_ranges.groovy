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

suite("test_spill_session_variable_ranges", "p0") {
    // Variable, minimum, interior value, maximum, historical out-of-range value.
    def ranges = [
        ["spill_hash_join_partition_count", 2L, 5L, 32L, 64L],
        ["spill_aggregation_partition_count", 2L, 5L, 32L, 64L],
        ["spill_repartition_max_depth", 1L, 5L, 128L, 256L],
        ["spill_buffer_size_bytes", 1048576L, 2097152L, 268435456L, 1024L],
        ["spill_aggregation_sink_mem_limit_bytes", 1048576L, 2097152L, 4294967296L, 68719476736L],
        ["spill_sort_merge_mem_limit_bytes", 1048576L, 2097152L, 4294967296L, 8589934592L]
    ]
    ranges.each { bounds ->
        def (variable, minimum, interior, maximum, historical) = bounds
        def saved = sql("SHOW VARIABLES LIKE '${variable}'")[0][1]
        try {
            [minimum - 1, minimum, interior, maximum, maximum + 1, 0L, -1L, historical].eachWithIndex { value, index ->
                sql "SET SESSION ${variable}=${value}"
                "order_qt_${variable}_${index}"("SELECT @@${variable}")
            }
            "order_qt_${variable}_show"("SHOW VARIABLES LIKE '${variable}'")
            sql "SET SESSION ${variable}=${interior}"
            "order_qt_${variable}_hint"("SELECT /*+ SET_VAR(${variable}=${historical}) */ @@${variable}")
            "order_qt_${variable}_restored"("SELECT @@${variable}")
            test {
                sql "SET SESSION ${variable}='not_an_integer'"
                exception "Incorrect argument type to variable '${variable}'"
            }
            "order_qt_${variable}_unchanged_after_invalid"("SELECT @@${variable}")
            sql "SET SESSION ${variable}=DEFAULT"
            "order_qt_${variable}_default"("SELECT @@${variable}")
        } finally {
            sql "SET SESSION ${variable}=${saved}"
        }
    }
}
