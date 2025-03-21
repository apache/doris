  
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

suite("test_temp_partitions_table") {
    def table = "test_temp_partitions_table"
    def table2 = "test_temp_partitions_table_2"
   
   sql """
        CREATE TABLE ${table}  (
            id BIGINT,
            val BIGINT,
            str VARCHAR(114)
        ) DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ('5'),
            PARTITION `p2` VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "replication_num"="1"
        );
    """

    sql """ALTER TABLE ${table}   ADD TEMPORARY PARTITION tp1 VALUES [("15"), ("20"));"""

    sql "INSERT INTO ${table}   VALUES(1, 1,'1')"
    sql "INSERT INTO ${table}   VALUES(7, 1,'3')"
    sql "INSERT INTO ${table}   TEMPORARY PARTITION(tp1) values(16,1234, 't');"
    sql "SYNC"
    qt_sql_1 "SELECT * FROM ${table}   PARTITION p1"
    qt_sql_2 "SELECT * FROM ${table}   PARTITION p2"
    qt_sql_3 """select * from ${table}   temporary partition(tp1);"""
    sql "DROP table ${table} "
    sql "recover table ${table} "

    qt_sql_recover1 "SELECT * FROM ${table}   PARTITION p1"
    qt_sql_recover2 "SELECT * FROM ${table}   PARTITION p2"
    qt_sql_recover3 """select * from ${table}   temporary partition(tp1);"""

    sql "DROP table ${table} "
    sql "recover table ${table}  as ${table2}"    

    qt_sql_newrecover1 "SELECT * FROM ${table2}   PARTITION p1"
    qt_sql_newrecover2 "SELECT * FROM ${table2}   PARTITION p2"
    qt_sql_newrecover3 """select * from ${table2}   temporary partition(tp1);"""

}