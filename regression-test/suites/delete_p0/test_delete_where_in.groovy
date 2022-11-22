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
suite("test_delete_where_in", "delete_p0") {

        def tb_name = "test_delete_where_in"

        sql """DROP TABLE IF EXISTS ${tb_name};"""
        // create table 
        sql  """CREATE TABLE ${tb_name}(
                `k1`  INT(50)     COMMENT '*****',
                `k2`  VARCHAR(50) COMMENT '*****',
                `k3`  VARCHAR(50) COMMENT '*****',
                `k4`  VARCHAR(50) COMMENT '*****',
                `k5`  INT(50)     COMMENT '*****',
                `k6`  VARCHAR(50) COMMENT '*****',
                `k7`  VARCHAR(50) COMMENT '*****',
                `k8`  VARCHAR(50) COMMENT '*****',
                `k9`  VARCHAR(50) COMMENT '*****',
                `k10` INT(50)     COMMENT '*****',
                `k11` VARCHAR(50) COMMENT '*****',
                `k12` VARCHAR(50) COMMENT '*****',
                `k13` VARCHAR(50) COMMENT '*****',
                `k14` VARCHAR(50) COMMENT '*****',
                `k15` INT(50)     COMMENT '*****',
                `k16` VARCHAR(50) COMMENT '*****',
                `k17` VARCHAR(50) COMMENT '*****',
                `k18` VARCHAR(50) COMMENT '*****',
                `k19` VARCHAR(50) COMMENT '*****',
                `k20` INT(50)     COMMENT '*****'
                )
                DUPLICATE KEY (`k1`,`k2`)
                DISTRIBUTED BY HASH (`k1`) BUCKETS 3
                PROPERTIES("replication_num" = "1");
            """ 

          // streamload ：100000
        streamLoad {
            table tb_name
            set 'compress_type', 'GZ'
            set 'column_separator', ','
            set 'columns', 'k1, k2 ,k3 ,k4 ,k5 ,k6 ,k7 ,k8 ,k9 ,k10 ,k11 ,k12 ,k13 ,k14 ,k15 ,k16 ,k17 ,k18 ,k19 ,k20'
            file """${getS3Url()+ '/regression/delete/delete_test_data'}.gz"""
            //time 10000 // limit inflight 10s
            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
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
        def loadRowCount = sql "select count(*) from ${tb_name};"
        logger.info("select count(*) from ${loadRowCount};")
        

        //delete test
        sql """ DELETE FROM ${tb_name} where k1 in (1,3000,2500,900,13); """
        def count_1= qt_sql """select count(*) from ${tb_name}"""
        logger.info("delete_1 : ${count_1}")

        sql """ DELETE FROM ${tb_name} where k1 < 10000 and k2 in('a','c','b','f');"""
        def count_2= qt_sql """select count(*) from ${tb_name}"""
        logger.info("delete_2 : ${count_2}")

        sql """ DELETE FROM ${tb_name} where k5 > 100000 and k8 in ('a','b','d');"""
        def count_3= qt_sql """select count(*) from ${tb_name}"""
        logger.info("delete_3 : ${count_3}")

        sql """ DELETE FROM ${tb_name} where k1 > 80000 and k12 in ('a','b','d','e') and k2 in ('c','f','g');"""
        def count_4= qt_sql """select count(*) from ${tb_name}"""
        logger.info("delete_4 : ${count_4}")

        sql """ DELETE FROM ${tb_name} where k5 >50000 and k1 < 80000;"""
        def count_5= qt_sql """select count(*) from ${tb_name}"""
        logger.info("delete_5 : ${count_5}")

        //drop table
        qt_sql """truncate table ${tb_name}"""
        qt_sql """drop table ${tb_name}"""
        
}
