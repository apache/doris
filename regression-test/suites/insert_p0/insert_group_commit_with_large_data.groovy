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

import com.mysql.cj.jdbc.StatementImpl

suite("insert_group_commit_with_large_data") {
    def db = "regression_test_insert_p0"
    def testTable = "insert_group_commit_with_large_data"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${testTable}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        if (result != expected_row_count) {
            logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
        }
        assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'PREPARE'"))
        assertTrue(serverInfo.contains("'label':'group_commit_"))
    }

    try {
        // create table
        sql """ drop table if exists ${testTable}; """

        sql """
            CREATE TABLE `${testTable}` (
                `id` int(11) NOT NULL,
                `name` varchar(1100) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "group_commit_interval_ms" = "40",
                "replication_num" = "1"
            );
            """

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql """ set group_commit = async_mode; """
            sql """ use ${db}; """

            // insert into 5000 rows
            def insert_sql = """ insert into ${testTable} values(1, 'a', 10)  """
            for (def i in 2..5000) {
                insert_sql += """, (${i}, 'a', 10) """
            }
            group_commit_insert insert_sql, 5000
            getRowCount(5000)

            // data size is large than 4MB, need " set global max_allowed_packet = 5508950 "
            /*def name_value = ""
            for (def i in 0..1024) {
                name_value += 'a'
            }
            insert_sql = """ insert into ${testTable} values(1, '${name_value}', 10)  """
            for (def i in 2..5000) {
                insert_sql += """, (${i}, '${name_value}', 10) """
            }
            result = sql """ ${insert_sql} """
            group_commit_insert insert_sql, 5000
            getRowCount(10000)
            */
        }
    } finally {
        // try_sql("DROP TABLE ${testTable}")
    }

    // test generated column
    testTable = "test_group_commit_generated_column"
    sql """ drop table if exists ${testTable}; """
    sql """create table ${testTable}(a int,b int,c double generated always as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a) PROPERTIES("replication_num" = "1", "group_commit_interval_ms" = "40");"""
    sql " set group_commit = async_mode; "
    group_commit_insert "INSERT INTO ${testTable} values(6,7,default);", 1
    group_commit_insert "INSERT INTO ${testTable}(a,b) values(1,2);", 1
    group_commit_insert "INSERT INTO ${testTable} values(3,5,default);", 1
    getRowCount(3)
    qt_select1  "select * from ${testTable} order by 1,2,3;"

    streamLoad {
        table "${testTable}"

        set 'column_separator', ','
        set 'columns', 'a,dummy,b'
        file "test_group_commit_1.csv"
        unset 'label'
        set 'group_commit', 'async_mode'
        set 'strict_mode', 'true'

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(4, json.NumberTotalRows)
        }
    }
    getRowCount(7)
    qt_select2  "select * from ${testTable} order by 1,2,3;"

    try {
        sql """set group_commit = off_mode;"""
        sql "drop table if exists gc_ctas1"
        sql "drop table if exists gc_ctas2"
        sql "drop table if exists gc_ctas3"
        sql '''
            CREATE TABLE IF NOT EXISTS `gc_ctas1` (
                `k1` varchar(5) NULL,
                `k2` varchar(5) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        '''
        sql '''
            CREATE TABLE IF NOT EXISTS `gc_ctas2` (
                `k1` varchar(10) NULL,
                `k2` varchar(10) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        '''
        sql ''' insert into gc_ctas1 values('11111','11111'); '''
        sql ''' insert into gc_ctas2 values('1111111111','1111111111'); '''
        sql "sync"
        order_qt_select_cte1 """ select * from gc_ctas1; """
        order_qt_select_cte2 """ select * from gc_ctas2; """
        sql """set group_commit = async_mode;"""
        sql '''
            create table `gc_ctas3`(k1, k2) 
            PROPERTIES("replication_num" = "1") 
            as select * from gc_ctas1
                union all 
                select * from gc_ctas2;
        '''
        sql  " insert into gc_ctas3 select * from gc_ctas1 union all select * from gc_ctas2;"
        sql "sync"
        order_qt_select_cte3 """ select * from gc_ctas3; """
    } finally {
    }
}
