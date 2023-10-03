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

suite("test_group_commit_http_stream") {
    def db = "regression_test_load_p0_http_stream"
    def tableName = "test_group_commit_http_stream"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${tableName}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def getAlterTableState = {
        def retry = 0
        while (true) {
            sleep(8000)
            def state = sql "show alter table column where tablename = '${tableName}' order by CreateTime desc "
            logger.info("alter table retry: ${retry},  state: ${state}")
            if (state.size() > 0 && state[0][9] == "FINISHED") {
                return true
            }
            retry++
            if (retry >= 40) {
                return false
            }
        }
        return false
    }

    try {
        // create table
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(1100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        PARTITION BY RANGE (id) (
            PARTITION plessThan1 VALUES LESS THAN ("0"),
            PARTITION plessThan2 VALUES LESS THAN ("100")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        // stream load with compress file
        String[] compressionTypes = new String[]{"gz", "bz2", /*"lzo",*/ "lz4frame"} //, "deflate"}
        for (final def compressionType in compressionTypes) {
            def fileName = "test_compress.csv." + (compressionType.equals("lz4frame") ? "lz4" : compressionType)
            streamLoad {
                set 'version', '1'
                set 'sql', """
                    insert into ${db}.${tableName} select * from http_stream
                    ("format"="csv", "compress_type"="${compressionType}", "column_separator"=",")
                """
                set 'group_commit', 'true'
                file "${fileName}"
                unset 'label'

                time 10000 // limit inflight 10s
            }
        }

        // stream load with 2 columns
        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName}(id, name) select c1, c2 from http_stream
                    ("format"="csv", "column_separator"=",")
            """

            set 'group_commit', 'true'
            file "test_stream_load1.csv"
            unset 'label'

            time 10000 // limit inflight 10s
        }

        // stream load with different column order
        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName}(score, id, name) select c1, c2, c3 from http_stream
                    ("format"="csv", "column_separator"="|")
            """

            set 'group_commit', 'true'
            file "test_stream_load2.csv"
            unset 'label'

            time 10000 // limit inflight 10s
        }

        // stream load with where condition
        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName}(id, name) select c1, c2 from http_stream 
                    ("format"="csv", "column_separator"=",") where c1 > 5
            """

            set 'group_commit', 'true'
            file "test_stream_load1.csv"
            unset 'label'

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertTrue(json.GroupCommit)
                // assertEquals(2, json.NumberTotalRows)
                assertEquals(1, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                // assertEquals(1, json.NumberUnselectedRows)
            }
        }

        // stream load with mapping
        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName} select c1, c2, c1 * 10 from http_stream
                    ("format"="csv", "column_separator"=",")
            """

            set 'group_commit', 'true'
            file "test_stream_load1.csv"
            unset 'label'

            time 10000 // limit inflight 10s
        }

        // stream load with filtered rows
        // TODO enable strict_mode
        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName} 
                    select c1, c2, c3 from http_stream ("format"="csv", "column_separator"=",") where c2 = 'a'
            """

            set 'group_commit', 'true'
            file "test_stream_load3.csv"
            set 'max_filter_ratio', '0.7'
            unset 'label'

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertTrue(json.GroupCommit)
                // assertEquals(6, json.NumberTotalRows)
                // assertEquals(2, json.NumberLoadedRows)
                // assertEquals(3, json.NumberFilteredRows)
                // assertEquals(1, json.NumberUnselectedRows)
                // assertFalse(json.ErrorURL.isEmpty())
            }
        }

        // stream load with label
        streamLoad {
            set 'version', '1'
            def label = 'l_' + System.currentTimeMillis()
            set 'sql', """
                    insert into ${db}.${tableName} with label ${label} select * from http_stream
                    ("format"="csv", "column_separator"="|")
            """

            set 'group_commit', 'true'
            file "test_stream_load2.csv"

            time 10000 // limit inflight 10s
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
            }
        }

        getRowCount(7)
        qt_sql " SELECT * FROM ${tableName} order by id, name, score asc; "
    } finally {
        // try_sql("DROP TABLE ${tableName}")
    }

    // stream load with large data and schema change
    tableName = "test_stream_load_lineorder"
    try {
        sql """ DROP TABLE IF EXISTS `${tableName}` """
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
            `lo_orderkey` bigint(20) NOT NULL COMMENT "",
            `lo_linenumber` bigint(20) NOT NULL COMMENT "",
            `lo_custkey` int(11) NOT NULL COMMENT "",
            `lo_partkey` int(11) NOT NULL COMMENT "",
            `lo_suppkey` int(11) NOT NULL COMMENT "",
            `lo_orderdate` int(11) NOT NULL COMMENT "",
            `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
            `lo_shippriority` int(11) NOT NULL COMMENT "",
            `lo_quantity` bigint(20) NOT NULL COMMENT "",
            `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
            `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
            `lo_discount` bigint(20) NOT NULL COMMENT "",
            `lo_revenue` bigint(20) NOT NULL COMMENT "",
            `lo_supplycost` bigint(20) NOT NULL COMMENT "",
            `lo_tax` bigint(20) NOT NULL COMMENT "",
            `lo_commitdate` bigint(20) NOT NULL COMMENT "",
            `lo_shipmode` varchar(11) NOT NULL COMMENT ""
            )
            PARTITION BY RANGE(`lo_orderdate`)
            (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
            PARTITION p1993 VALUES [("19930101"), ("19940101")),
            PARTITION p1994 VALUES [("19940101"), ("19950101")),
            PARTITION p1995 VALUES [("19950101"), ("19960101")),
            PARTITION p1996 VALUES [("19960101"), ("19970101")),
            PARTITION p1997 VALUES [("19970101"), ("19980101")),
            PARTITION p1998 VALUES [("19980101"), ("19990101")))
            DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 4
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        // load data
        def columns = """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
            lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
            lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode"""

        new Thread(() -> {
            Thread.sleep(3000)
            // do light weight schema change
            sql """ alter table ${tableName} ADD column sc_tmp varchar(100) after lo_revenue; """

            assertTrue(getAlterTableState())

            // do hard weight schema change
            def new_columns = """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
            lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
            lo_revenue,lo_supplycost,lo_tax,lo_shipmode,lo_commitdate"""
            sql """ alter table ${tableName} order by (${new_columns}); """
        }).start();

        for (int i = 0; i < 4; i++) {

            streamLoad {
                set 'version', '1'
                sql """
                    insert into ${db}.${tableName} ($columns)
                    select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 from http_stream
                    ("format"="csv", "compress_type"="GZ", "column_separator"="|")
                """

                set 'group_commit', 'true'
                unset 'label'

                file """${getS3Url()}/regression/ssb/sf0.1/lineorder.tbl.gz"""

                time 10000 // limit inflight 10s

                // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

                // if declared a check callback, the default check condition will ignore.
                // So you must check all condition
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load ${i}, result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    if (json.NumberLoadedRows != 600572) {
                        logger.warn("Stream load ${i}, loaded rows: ${json.NumberLoadedRows}")
                    }
                    assertTrue(json.LoadBytes > 0)
                    assertTrue(json.GroupCommit)
                }
            }
        }

        getRowCount(2402288)
        qt_sql """ select count(*) from ${tableName} """

        assertTrue(getAlterTableState())
    } finally {
        // try_sql("DROP TABLE ${tableName}")
    }
}