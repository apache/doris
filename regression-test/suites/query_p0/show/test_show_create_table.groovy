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

suite("test_show_create_table", "query,arrow_flight_sql") {
    String tb_name = "tb_show_create_table";
    try {  
        sql """drop table if exists ${tb_name} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tb_name}(
                datek1 datev2 COMMENT "a",
                datetimek1 datetimev2 COMMENT "b",
                datetimek2 datetimev2(3) COMMENT "c",
                datetimek3 datetimev2(6) COMMENT "d",
                datev1 datev2 MAX NOT NULL COMMENT "e",
                datetimev1 datetimev2 MAX NOT NULL COMMENT "f",
                datetimev2 datetimev2(3) MAX NOT NULL COMMENT "g",
                datetimev3 datetimev2(6) MAX NOT NULL COMMENT "h"
            )
            AGGREGATE KEY (datek1, datetimek1, datetimek2, datetimek3)
            DISTRIBUTED BY HASH(datek1) BUCKETS 5 properties("replication_num" = "1");
        """
        
        def res = sql "show create table `${tb_name}`"
        assertTrue(res.size() != 0)

        sql """drop table if exists ${tb_name} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tb_name}(
                datek1 datev2 COMMENT "a",
                datetimek1 datetimev2 COMMENT "b",
                datetimek2 datetimev2(3) COMMENT "c",
                datetimek3 datetimev2(6) COMMENT "d",
                datev1 datev2 NOT NULL COMMENT "e",
                datetimev1 datetimev2 NOT NULL COMMENT "f",
                datetimev2 datetimev2(3) NOT NULL COMMENT "g",
                datetimev3 datetimev2(6) NOT NULL COMMENT "h"
            )
            DUPLICATE KEY (datek1, datetimek1, datetimek2, datetimek3)
            DISTRIBUTED BY RANDOM BUCKETS 5 properties("replication_num" = "1");
        """
        
        res = sql "show create table `${tb_name}`"
        assertTrue(res.size() != 0)

    } finally {

        try_sql("DROP TABLE IF EXISTS `${tb_name}`")
    }
   
}
