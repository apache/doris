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

import groovy.json.JsonSlurper
import java.util.regex.Matcher;
import java.util.regex.Pattern;

suite("test_bloom_filter_hit") {
    // Helper method to extract and validate RowsBloomFilterFiltered value
    def validateBloomFilterFiltered = { profileString ->
        log.info("Profile content: ${profileString}")
        // Pattern to match "RowsBloomFilterFiltered:" followed by a number (with optional units and parentheses)
        Pattern pattern = Pattern.compile("RowsBloomFilterFiltered:\\s+(\\d+(?:\\.\\d+)?[KMG]?)(?:\\s+\\((\\d+)\\))?")
        Matcher matcher = pattern.matcher(profileString)
        if (matcher.find()) {
            def value1 = matcher.group(1)  // First number (possibly with K/M/G suffix)
            def value2 = matcher.group(2)  // Second number in parentheses (if exists)
            // Parse the first value
            def numValue = 0L
            if (value1.endsWith("K")) {
                numValue = (Double.parseDouble(value1.substring(0, value1.length() - 1)) * 1000).toLong()
            } else if (value1.endsWith("M")) {
                numValue = (Double.parseDouble(value1.substring(0, value1.length() - 1)) * 1000000).toLong()
            } else if (value1.endsWith("G")) {
                numValue = (Double.parseDouble(value1.substring(0, value1.length() - 1)) * 1000000000).toLong()
            } else {
                numValue = Long.parseLong(value1)
            }
            log.info("Extracted RowsBloomFilterFiltered value: ${numValue}")
            assertTrue(numValue > 0)
            return true
        } else {
            fail("Could not find RowsBloomFilterFiltered in profile output")
            return false
        }
    }

    def be_num = sql "show backends;"
    if (be_num.size() > 1) {
        // not suitable for multiple be cluster.
        return
    }

    def tableName = "test_bloom_filter_hit"
    sql "set profile_level = 2;"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "C_COMMENT"
        );
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'compress_type', 'GZ'
        set 'columns', "c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp"
        file """${getS3Url()}/regression/tpch/sf0.1/customer.tbl.gz"""

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    sql """ SET enable_profile = true """
    sql """ set parallel_scan_min_rows_per_scanner = 2097152; """

    profile("sql_c_comment_ok") {
        run {
            sql "/* sql_c_comment_ok */ select C_COMMENT from ${tableName} where C_COMMENT='OK'"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  15.0K  (15000)"))
        }
    }

    sql """ SET enable_profile = false """

    // test ipv for bloom filter
    sql """ DROP TABLE IF EXISTS test_ip_bf """
    sql """
        CREATE TABLE IF NOT EXISTS test_ip_bf (
          `id` int,
          `ip_v6` ipv6,
          `ip_v4` ipv4
        )
        ENGINE = OLAP DUPLICATE KEY (`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "ip_v6, ip_v4"
        );
    """
    sql """
            insert into test_ip_bf values
                    (1, '::', '::'),
                    (2, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', "192.168.1.1"),
                    (3, 'ef8d:3d6a:869b:2582:7200:aa46:4dcd:2bd4', "192.168.1.11"),
                    (4, '4a3e:dc26:1819:83e6:9ee5:7239:ff44:aee8', "192.168.11.1"),
                    (5, 'b374:22dc:814a:567b:6013:78a7:032d:05c8', '127.0.0.1'),
                    (6, '1326:d47e:2417:83c0:bd35:fc82:34dc:953a', '192.168.104.0'),
                    (7, '8ffa:65cb:6554:5c3e:fb87:3f91:29da:2891', '192.168.103.255'),
                    (8, 'def7:1488:6fb7:0c70:aa66:df25:6a43:5d89', '192.168.100.1'),
                    (9, 'd3fa:09a9:af08:0c8b:44ab:8f75:0b11:e997', '192.168.107.1'),
                    (10, NULL, NULL); """


    sql """ SET enable_profile = true """
    sql """ set parallel_scan_min_rows_per_scanner = 2097152; """

    // bf filter
    sql """ SET parallel_pipeline_task_num=1 """

    profile("sql_ip_v6_ok") {
        run {
            sql "/* sql_ip_v6_ok */ select * from test_ip_bf where ip_v6='4a3e:dc26:1819:83e6:9ee5:7239:ff44:aee8'"
            sleep(1000)
        }

        check { profileString, exception ->
            validateBloomFilterFiltered(profileString)
        }
    }

    profile("sql_ip_v4_ok") {
        run {
            sql "/* sql_ip_v4_ok */ select * from test_ip_bf where ip_v4='192.168.11.1'"
            sleep(1000)
        }

        check { profileString, exception ->
            validateBloomFilterFiltered(profileString)
        }
    }
}
