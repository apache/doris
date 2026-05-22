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
    def forcedOverflowSize = "31"

    def normalizeRows = { rows ->
        rows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }

    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    getBackendIpHttpPort(backendIdToBackendIP, backendIdToBackendHttpPort)

    def originalStringOverflowSize = "4294967295"
    if (!backendIdToBackendIP.isEmpty()) {
        def backendId = backendIdToBackendIP.keySet()[0]
        def (code, out, err) = show_be_config(
                backendIdToBackendIP.get(backendId), backendIdToBackendHttpPort.get(backendId))
        logger.info("show BE config: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        def configList = parseJson(out.trim())
        for (Object entry in (List) configList) {
            def values = (List<String>) entry
            if (values[0] == "string_overflow_size") {
                originalStringOverflowSize = values[2]
                break
            }
        }
    }

    def expectStringOverflow = { query ->
        test {
            sql query
            check { result, exception, startTime, endTime ->
                assert exception != null: "Expected query to fail with string overflow"
                def details = exception.toString()
                logger.info("Expected string overflow exception: ${details}".toString())
                assert details.contains("string column length is too large")
            }
        }
    }

    def overflowQuery = """
        SELECT repeat(v, 20)
        FROM test_doris_25531_string_overflow_error
        ORDER BY 1
    """

    sql """ DROP TABLE IF EXISTS test_doris_25531_string_overflow_error """
    sql """
        CREATE TABLE test_doris_25531_string_overflow_error (
            k INT,
            v STRING
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO test_doris_25531_string_overflow_error VALUES
            (1, 'a'),
            (2, 'b')
    """

    try {
        update_all_be_config("string_overflow_size", forcedOverflowSize)

        assertEquals([["a"], ["b"]],
                normalizeRows(sql("SELECT v FROM test_doris_25531_string_overflow_error ORDER BY k")))
        expectStringOverflow(overflowQuery)
    } finally {
        update_all_be_config("string_overflow_size", originalStringOverflowSize)
    }

    assertEquals([["aaaaaaaaaaaaaaaaaaaa"], ["bbbbbbbbbbbbbbbbbbbb"]], normalizeRows(sql(overflowQuery)))
}
