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

suite("test_group_commit_stream_load_lineitem_multiple_table") {
    def stream_load_table_base = "test_stream_load_lineitem_multiple_table"
    def columns = """l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, 
l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment"""
    int[] rowCountArray = new int[]{600572, 599397, 600124, 599647, 599931, 601365, 599301, 600504, 599715, 600659};
    def getRowCount = { expectedRowCount, table_name ->
        def retry = 0
        while (retry < 60) {
            try {
                def rowCount = sql "select count(*) from ${table_name}"
                logger.info("rowCount: " + rowCount + ", retry: " + retry)
                if (rowCount[0][0] >= expectedRowCount) {
                    break
                }
            } catch (Exception e) {
                logger.info("select count get exception", e);
            }
            retry++
            sleep(5000)
        }
    }

    def create_stream_load_table = { table_name ->
        sql """ drop table if exists ${table_name}; """

        sql """
   CREATE TABLE ${table_name} (
    l_shipdate    DATEV2 NOT NULL,
    l_orderkey    bigint NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimalv3(15, 2) NOT NULL,
    l_extendedprice  decimalv3(15, 2) NOT NULL,
    l_discount    decimalv3(15, 2) NOT NULL,
    l_tax         decimalv3(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATEV2 NOT NULL,
    l_receiptdate DATEV2 NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);
        """
    }

    def checkStreamLoadResult = { exception, result, total_rows, loaded_rows, filtered_rows, unselected_rows ->
        if (exception != null) {
            throw exception
        }
        log.info("Stream load result: ${result}".toString())
        def json = parseJson(result)
        assertEquals("success", json.Status.toLowerCase())
        assertEquals(total_rows, json.NumberTotalRows)
        assertEquals(loaded_rows, json.NumberLoadedRows)
        assertEquals(filtered_rows, json.NumberFilteredRows)
        assertEquals(unselected_rows, json.NumberUnselectedRows)
        if (filtered_rows > 0) {
            assertFalse(json.ErrorURL.isEmpty())
        } else {
            assertTrue(json.ErrorURL == null || json.ErrorURL.isEmpty())
        }
    }

    def do_stream_load = { i, table_name ->
        logger.info("file:" + i)

        streamLoad {
            table table_name

            set 'column_separator', '|'
            set 'columns', columns + ",lo_dummy"
            set 'group_commit', 'async_mode'
            unset 'label'
            file """${getS3Url()}/regression/tpch/sf1/lineitem.tbl.""" + i

            check { result, exception, startTime, endTime ->
                checkStreamLoadResult(exception, result, rowCountArray[i - 1], rowCountArray[i - 1], 0, 0)
            }
        }
        getRowCount(rowCountArray[i - 1], table_name)
    }

    def process = {
        def threads = []
        for (int k = 1; k <= 10; k++) {
            int n = k;
            String table_name = stream_load_table_base + "_" + n;
            create_stream_load_table(table_name)
            threads.add(Thread.startDaemon {
                do_stream_load(n, table_name)
            })
        }
        for (Thread th in threads) {
            th.join()
        }

        for (int k = 1; k <= 10; k++) {
            String table_name = stream_load_table_base + "_" + k;
            qt_sql """ select count(*) from ${table_name}; """
        }
    }

    try {
        process()
    } finally {

    }


}