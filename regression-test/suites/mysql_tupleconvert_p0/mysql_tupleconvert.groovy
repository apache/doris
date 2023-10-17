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

suite("mysql_tuple_convert_test") {
    
    testTable = "doule_faster_convert_test"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `col_0` INT,
            `col_1` DOUBLE,
            `col_2` FLOAT,
            `col_3` ARRAY<FLOAT>,
            `col_4` ARRAY<DOUBLE>)
        ENGINE=OLAP DUPLICATE KEY(`col_0`) DISTRIBUTED BY HASH(`col_0`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """INSERT INTO ${testTable} VALUES (1, 0.33, 3.1415926, [0.33, 0.67], [3.1415926, 0.878787878]);"""
    qt_select """SELECT /*+SET_VAR(faster_float_convert=false)*/ * FROM ${testTable};"""
    qt_select """SELECT /*+SET_VAR(faster_float_convert=true)*/ * FROM ${testTable};"""
    // make sure we can convert number from string to value
    sql """INSERT INTO ${testTable} SELECT 2, col_1, col_2, col_3, col_4 from ${testTable} where col_0 = 1;"""
    qt_select """SELECT * FROM ${testTable} ORDER BY col_0;"""
    sql """SET faster_float_convert=false;"""
    sql "DROP TABLE IF EXISTS ${testTable}"
}
