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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_load_and_schema_change_row_store", "p0") {
    def dataFile = """${getS3Url()}/regression/datatypes/test_scalar_types_10w.csv"""

    // define dup key table1
    def testTable = "tbl_scalar_types_dup"
    sql "DROP TABLE IF EXISTS ${testTable} FORCE"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` bigint(11) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_bigint` bigint(20) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_date` date NULL,
            `c_datetime` datetime NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "row_store_columns" = "k1,c_bool,c_tinyint,c_bigint,c_decimal,c_decimalv3,c_datev2,c_string");
        """


    // load data
    streamLoad {
        table testTable
        file dataFile
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(100000, json.NumberTotalRows)
            assertEquals(100000, json.NumberLoadedRows)
        }
    }

    sql "set topn_opt_limit_threshold = 100"
    qt_sql "SELECT * from ${testTable} order by 1, 2, 3 limit 10"
    qt_sql "SELECT * from ${testTable} where c_tinyint = 10 order by 1, 2, 3 limit 10 "

    sql """
          ALTER table ${testTable} MODIFY COLUMN c_int BIGINT;
          """
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }
    def wait_job_done = { tableName ->
        def max_try_time = 100
        while (max_try_time--){
              String result = getJobState("${tableName}")
              if (result == "FINISHED") {
                   break
              } else {
                   sleep(2000)
                   if (max_try_time < 1){
                        assertEquals(1,2)
                   }
              }
         }
    }

    sql "DROP TABLE IF EXISTS tbl_scalar_types_dup_1 FORCE"
    sql """
        CREATE TABLE IF NOT EXISTS tbl_scalar_types_dup_1 (
            `k1` bigint(11) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_bigint` bigint(20) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_date` date NULL,
            `c_datetime` datetime NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    wait_job_done.call("tbl_scalar_types_dup")
    sql "INSERT INTO tbl_scalar_types_dup_1 SELECT * from tbl_scalar_types_dup"
    sql """alter table tbl_scalar_types_dup_1 set ("bloom_filter_columns" = "c_largeint")"""    
    wait_job_done.call("tbl_scalar_types_dup_1")
    sql """alter table tbl_scalar_types_dup_1 set ("store_row_column" = "true")"""    
    wait_job_done.call("tbl_scalar_types_dup_1")
    qt_sql "select sum(length(__DORIS_ROW_STORE_COL__)) from tbl_scalar_types_dup_1"
    sql """
         ALTER table tbl_scalar_types_dup_1 ADD COLUMN new_column1 INT default "123";
    """
    sql "select /*+ SET_VAR(enable_nereids_planner=true)*/ * from tbl_scalar_types_dup_1 where k1 = -2147303679"
    sql """insert into tbl_scalar_types_dup_1(new_column1) values (9999999)"""
    qt_sql """select length(__DORIS_ROW_STORE_COL__) from tbl_scalar_types_dup_1 where new_column1 = 9999999"""

    explain {
        sql("select /*+ SET_VAR(enable_nereids_planner=true)*/ * from tbl_scalar_types_dup_1 where k1 = -2147303679")
        contains "SHORT-CIRCUIT"
    } 
    sql """alter table tbl_scalar_types_dup_1 set ("row_store_columns" = "k1,c_datetimev2")"""    
    wait_job_done.call("tbl_scalar_types_dup_1")
    qt_sql "select sum(length(__DORIS_ROW_STORE_COL__)) from tbl_scalar_types_dup_1"
    sql "set enable_short_circuit_query_access_column_store = false"
    test {
        sql "select /*+ SET_VAR(enable_nereids_planner=true,enable_short_circuit_query_access_column_store=false)*/ * from tbl_scalar_types_dup_1 where k1 = -2147303679"
        exception("Not support column store")
    }
    explain {
        sql("select /*+ SET_VAR(enable_nereids_planner=true)*/ k1, c_datetimev2 from tbl_scalar_types_dup_1 where k1 = -2147303679")
        contains "SHORT-CIRCUIT"
    } 
    qt_sql "select /*+ SET_VAR(enable_nereids_planner=true)*/ k1, c_datetimev2 from tbl_scalar_types_dup_1 where k1 = -2147303679"

    sql """alter table tbl_scalar_types_dup_1 set ("row_store_columns" = "k1,c_decimalv3")"""    
    wait_job_done.call("tbl_scalar_types_dup_1")
    test {
        sql "select /*+ SET_VAR(enable_nereids_planner=true,enable_short_circuit_query_access_column_store=false)*/ k1,c_datetimev2 from tbl_scalar_types_dup_1 where k1 = -2147303679"
        exception("Not support column store")
    }
    qt_sql "select /*+ SET_VAR(enable_nereids_planner=true,enable_short_circuit_query_access_column_store=false)*/ k1, c_decimalv3 from tbl_scalar_types_dup_1 where k1 = -2147303679"
    sql "set enable_short_circuit_query_access_column_store = true"
}
