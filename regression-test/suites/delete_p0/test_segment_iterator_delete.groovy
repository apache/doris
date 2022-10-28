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

suite("test_segment_iterator_delete") {
    def tableName = "delete_regression_test_segment_iterator"
    def tableName_dict = "delete_regression_test_segment_iterator_column_dictionary"

    // test duplicate key
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (c1 int NOT NULL, c2 int NOT NULL , c3 int not null ) ENGINE=OLAP DUPLICATE KEY(c1, c2) COMMENT "OLAP" DISTRIBUTED BY HASH(c3) BUCKETS 1 
    PROPERTIES ( "replication_num" = "1" );"""

    sql """ DROP TABLE IF EXISTS ${tableName_dict} """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName_dict} (
        `tinyint_key` tinyint(4) NOT NULL,
        `char_50_key` char(50) NOT NULL,
        `character_key` varchar(500) NOT NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`tinyint_key`, `char_50_key`, `character_key`)
    DISTRIBUTED BY HASH(`tinyint_key`) BUCKETS 1
    PROPERTIES ( "replication_num" = "1" );
    """

    sql """INSERT INTO ${tableName} VALUES (1,1,1)"""
    sql """INSERT INTO ${tableName} VALUES (2,2,2)"""
    sql """INSERT INTO ${tableName} VALUES (3,3,3)"""
    sql """INSERT INTO ${tableName} VALUES (1,1,1)"""
    sql """INSERT INTO ${tableName} VALUES (2,2,2)"""
    sql """INSERT INTO ${tableName} VALUES (3,3,3)"""

    // delete first key
    sql """delete from ${tableName} where c1 = 1;"""
    sql """INSERT INTO ${tableName} VALUES (4,4,4)"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""

    // delete second key
    sql """delete from ${tableName} where c2 = 2;"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""

    // delete by multi columns
    sql """INSERT INTO ${tableName} VALUES (5,5,5)"""
    sql """INSERT INTO ${tableName} VALUES (6,6,6)"""
    sql """delete from ${tableName} where c1 = 5 and c2=5;"""
    sql """delete from ${tableName} where c1 = 5 and c2=6;"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""


    // delete by value columns
    sql """INSERT INTO ${tableName} VALUES (7,7,7)"""
    sql """INSERT INTO ${tableName} VALUES (8,8,8)"""
    sql """delete from ${tableName} where c3 = 7;"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""


    sql """drop table ${tableName} force"""

    // test unique key
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (c1 int NOT NULL, c2 int NOT NULL , c3 int not null ) ENGINE=OLAP UNIQUE KEY(c1, c2) COMMENT "OLAP" DISTRIBUTED BY HASH(c1) BUCKETS 1 
    PROPERTIES ( "replication_num" = "1" );"""

    sql """INSERT INTO ${tableName} VALUES (1,1,1)"""
    sql """INSERT INTO ${tableName} VALUES (1,1,3)"""
    sql """INSERT INTO ${tableName} VALUES (2,2,2)"""
    sql """INSERT INTO ${tableName} VALUES (3,3,3)"""

    // delete first key
    sql """delete from ${tableName} where c1 = 1;"""
    sql """INSERT INTO ${tableName} VALUES (4,4,4)"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""

    // delete second key
    sql """delete from ${tableName} where c2 = 2;"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""

    // delete by multi columns
    sql """INSERT INTO ${tableName} VALUES (5,5,5)"""
    sql """INSERT INTO ${tableName} VALUES (6,6,6)"""
    sql """delete from ${tableName} where c1 = 5 and c2=5;"""
    sql """delete from ${tableName} where c1 = 5 and c2=6;"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=false) */ * from ${tableName};"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName};"""

    sql """drop table ${tableName} force"""

    // delete ColumnDictionary
    sql """INSERT INTO ${tableName_dict} VALUES(1, 'dddd', 'adgs'), (1, 'SSS', 'sk6S0'), (1, 'ttt', 'zdges');"""
    sql """delete from ${tableName_dict} where character_key < "sk6S0";"""
    order_qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tableName_dict} order by tinyint_key, char_50_key;"""
}
