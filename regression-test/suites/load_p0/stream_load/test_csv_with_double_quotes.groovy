
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

suite("test_csv_with_double_quotes", "p0") {
    def tableName = "test_csv_with_double_quotes"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4)  NULL,
            `v2` string  NULL,
            `v3` date  NULL,
            `v4` datetime  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','

        file 'csv_with_double_quotes.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    sql """truncate table ${tableName}"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'trim_double_quotes', 'true'

        file 'csv_with_double_quotes.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    def create_table = { testTablex ->
                    sql """
                        CREATE TABLE `${testTablex}` (
                            `name` varchar(48) NULL,
                            `age` bigint(20) NULL,
                            `agent_id` varchar(256) NULL
                            ) ENGINE=OLAP
                            DUPLICATE KEY(`name`)
                            COMMENT 'OLAP'
                            DISTRIBUTED BY RANDOM BUCKETS 10
                            PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1",
                            "is_being_synced" = "false",
                            "storage_format" = "V2",
                            "light_schema_change" = "true",
                            "disable_auto_compaction" = "false",
                            "enable_single_replica_compaction" = "false"
                            ); 
                        """
    }

    def tableName1 = "test_single_quotes"
    try {
        sql "DROP TABLE IF EXISTS ${tableName1}"

        create_table.call(tableName1)

        streamLoad {
            table "${tableName1}"

            set 'column_separator', ','
            set 'trim_double_quotes', 'true'

            file 'test_single_quote.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(1, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
            }
        }

        qt_sql_test_single_quote "SELECT * FROM ${tableName1} order by name"

    } finally {
        sql "DROP TABLE IF EXISTS ${tableName1}"
    }
}
