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

enum SC {
    TRUNCATE_TABLE(1),
    ADD_COLUMN(2),
    DELETE(3),
    DROP_COLUMN(4),
    CHANGE_ORDER(5)
    private int value

    SC(int value) {
        this.value = value
    }

    int getValue() {
        return value
    }
}

enum STATE {
    NORMAL(1),
    BEFORE_ADD_COLUMN(2),
    DROP_COLUMN(3)
    private int value

    STATE(int value) {
        this.value = value
    }

    int getValue() {
        return value
    }
}

suite("test_group_commit_http_stream_lineitem_schema_change") {
    def db = "regression_test_insert_p2"
    def stream_load_table = "test_http_stream_lineitem_schema_change_sf1"
    int[] rowCountArray = new int[]{600572, 599397, 600124, 599647, 599931, 601365, 599301, 600504, 599715, 600659};
    def total = 0;
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
            Thread.sleep(5000)
        }
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

    def create_stream_load_table = { table_name ->
        // create table
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
    "enable_mow_light_delete" = "true",
    "replication_num" = "1"
);
        """
    }

    def create_stream_load_table_less_column = { table_name ->
        // create table
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
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "enable_mow_light_delete" = "true",
    "replication_num" = "1"
);
        """

    }

    def insert_data = { i, table_name ->
        int j = 0;
        while (true) {
            if (j >= 18) {
                throw new Exception("""fail to much time""")
            }
            try {
                streamLoad {
                    set 'version', '1'
                    set 'sql', """
                    insert into ${db}.${table_name}(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, 
l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,
l_shipmode,l_comment) select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16 from http_stream
                    ("format"="csv", "column_separator"="|")
            """

                    set 'group_commit', 'async_mode'
                    file """${getS3Url()}/regression/tpch/sf1/lineitem.tbl.""" + i
                    unset 'label'

                    check { result, exception, startTime, endTime ->
                        checkStreamLoadResult(exception, result, rowCountArray[i - 1], rowCountArray[i - 1], 0, 0)
                    }
                }
                break
            } catch (Exception e) {
                Thread.sleep(10000)
            }
            j++;
        }
        total += rowCountArray[i - 1];
    }

    def insert_data_less_column = { i, table_name ->
        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${stream_load_table}(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, 
l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_shipinstruct,l_shipmode,
l_comment) select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c14, c15, c16 from http_stream
                    ("format"="csv", "column_separator"="|")
            """

            set 'group_commit', 'async_mode'
            file """${getS3Url()}/regression/tpch/sf1/lineitem.tbl.""" + i
            unset 'label'

            check { result, exception, startTime, endTime ->
                checkStreamLoadResult(exception, result, rowCountArray[i - 1], rowCountArray[i - 1], 0, 0)
            }
        }
        total += rowCountArray[i - 1];
    }

    def getAlterTableState = { table_name ->
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${table_name}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
    }

    def truncate = { table_name ->
        create_stream_load_table(table_name)
        total = 0;
        for (int i = 1; i <= 10; i++) {
            logger.info("process file:" + i)
            if (i == 5) {
                getRowCount(total, table_name)
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ truncate table ${table_name}; """
                        break
                    } catch (Exception e) {
                        logger.info("select count get exception", e);
                    }
                    Thread.sleep(2000)
                    retry++
                }
                total = 0;
            }
            insert_data(i, table_name)
        }
        logger.info("process truncate total:" + total)
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def delete = { table_name ->
        create_stream_load_table(table_name)
        total = 0;
        for (int i = 1; i <= 10; i++) {
            logger.info("process file:" + i)
            if (i == 5) {
                getRowCount(total, table_name)
                def retry = 0
                while (retry < 10) {
                    try {
                        def rowCount = sql """select count(*) from ${table_name} where l_orderkey >=1000000 and l_orderkey <=5000000;"""
                        logger.info("rowCount:" + rowCount)
                        sql """ delete from ${table_name} where l_orderkey >=1000000 and l_orderkey <=5000000; """
                        total -= rowCount[0][0]
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    Thread.sleep(2000)
                    retry++
                }
            }
            insert_data(i, table_name)
        }
        logger.info("process delete total:" + total)
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def drop_column = { table_name ->
        create_stream_load_table(table_name)
        total = 0;
        for (int i = 1; i <= 10; i++) {
            logger.info("process file:" + i)
            if (i == 5) {
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ alter table ${table_name} DROP column l_receiptdate; """
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    Thread.sleep(2000)
                    retry++
                }
            }
            if (i < 5) {
                insert_data(i, table_name)
            } else {
                insert_data_less_column(i, table_name)
            }
        }
        logger.info("process drop column total:" + total)
        assertTrue(getAlterTableState(table_name), "drop column should success")
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def add_column = { table_name ->
        create_stream_load_table_less_column(table_name)
        total = 0;
        for (int i = 1; i <= 10; i++) {
            logger.info("process file:" + i)
            if (i == 5) {
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ alter table ${table_name} ADD column l_receiptdate DATEV2 after l_commitdate; """
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    Thread.sleep(2000)
                    retry++
                }
            }
            if (i < 5) {
                insert_data_less_column(i, table_name)
            } else {
                insert_data(i, table_name)
            }
        }
        logger.info("process add column total:" + total)
        assertTrue(getAlterTableState(table_name), "add column should success")
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def change_order = { table_name ->
        create_stream_load_table(table_name)
        total = 0;
        for (int k = 0; k < 2; k++) {
            logger.info("round:" + k)
            for (int i = 1; i <= 10; i++) {
                logger.info("process file:" + i)
                if (k == 0 && i == 2) {
                    def retry = 0
                    while (retry < 10) {
                        try {
                            sql """ alter table ${table_name} order by (l_orderkey,l_shipdate,l_linenumber, l_partkey,l_suppkey,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment); """
                            break
                        } catch (Exception e) {
                            log.info("exception:", e)
                        }
                        Thread.sleep(2000)
                        retry++
                    }
                }
                insert_data(i, table_name)
            }
        }
        logger.info("process change order total:" + total)
        assertTrue(getAlterTableState(table_name), "modify column order should success")
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }


    def process = { table_name ->
        for (int i = 1; i <= 5; i++) {
            switch (i) {
                case SC.TRUNCATE_TABLE.value:
                    truncate(table_name)
                    break
                case SC.DELETE.value:
                    delete(table_name)
                    break
                case SC.DROP_COLUMN.value:
                    drop_column(table_name)
                    break
                case SC.ADD_COLUMN.value:
                    add_column(table_name)
                    break
                case SC.CHANGE_ORDER.value:
                    change_order(table_name)
                    break
            }
        }
    }

    try {
        process(stream_load_table)
    } finally {

    }

}