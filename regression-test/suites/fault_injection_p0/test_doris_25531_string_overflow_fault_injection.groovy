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

suite("test_doris_25531_string_overflow_fault_injection", "nonConcurrent") {
    def debugPoint = "ColumnStr.convert_column_if_overflow.max_string_size"
    def forcedMaxStringSize = 31

    sql """ DROP TABLE IF EXISTS test_doris_25531_string_overflow_fault_injection """
    sql """
        CREATE TABLE test_doris_25531_string_overflow_fault_injection (
            k INT,
            dt DATETIMEV2
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO test_doris_25531_string_overflow_fault_injection VALUES
            (1, '2024-01-01 00:00:00'),
            (2, '2024-01-01 01:00:00')
    """

    def joinWithConvertTz = """
        SELECT /*+ SET_VAR(parallel_pipeline_task_num=1) */
               l.k,
               r.k,
               CAST(convert_tz(l.dt, 'UTC', 'Asia/Shanghai') AS STRING) AS converted_dt
        FROM test_doris_25531_string_overflow_fault_injection l
        JOIN test_doris_25531_string_overflow_fault_injection r
          ON repeat(CAST(convert_tz(l.dt, 'UTC', 'Asia/Shanghai') AS STRING), 2)
           = repeat(CAST(convert_tz(r.dt, 'UTC', 'Asia/Shanghai') AS STRING), 2)
        ORDER BY 1, 2
    """

    qt_sql_without_debug_point joinWithConvertTz

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().enableDebugPointForAllBEs(debugPoint, [max_string_size: forcedMaxStringSize])
        qt_sql_with_debug_point joinWithConvertTz
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(debugPoint)
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
