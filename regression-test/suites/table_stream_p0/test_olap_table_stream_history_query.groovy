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

suite("test_olap_table_stream_history_query") {
    sql "DROP DATABASE IF EXISTS test_olap_table_stream_history_query_db"
    sql "CREATE DATABASE test_olap_table_stream_history_query_db"
    sql "USE test_olap_table_stream_history_query_db"

    // mow table
    sql """
        CREATE TABLE `tbl1` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """
    sql """ 
        insert into tbl1 values (1, 's1'),(2, 's2'),(3, 's3')
    """

    sql """
        CREATE STREAM `s1` ON TABLE tbl1
        COMMENT 'test stream 1'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'true'
        );
    """

    // duplicate table
    sql """
        CREATE TABLE `tbl2` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """ 
        insert into tbl2 values (1, 's1'),(2, 's2'),(3, 's3')
    """

    sql """
        CREATE STREAM `s2` ON TABLE tbl2
        COMMENT 'test stream 3'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'true'
        );
    """
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__ from s1"
    qt_sql "select * from s1 where __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select * from s1 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select * from s1 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND' order by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select * from s1 where __DORIS_STREAM_SEQUENCE_COL__='-1' order by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_SEQUENCE_COL__ having __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_CHANGE_TYPE_COL__ having __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s1 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s1 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s1 group by 1"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s1 group by 1"

    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__ from s2"
    qt_sql "select * from s2 where __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select * from s2 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select * from s2 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND' order by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select * from s2 where __DORIS_STREAM_SEQUENCE_COL__='-1' order by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_SEQUENCE_COL__ having __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_CHANGE_TYPE_COL__ having __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s2 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s2 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s2 group by 1"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s2 group by 1"

    sql "SET show_hidden_columns=true;"

      qt_sql "select __DORIS_STREAM_SEQUENCE_COL__ from s1"
    qt_sql "select * from s1 where __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select * from s1 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select * from s1 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND' order by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select * from s1 where __DORIS_STREAM_SEQUENCE_COL__='-1' order by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_SEQUENCE_COL__ having __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select count(*) from s1 group by __DORIS_STREAM_CHANGE_TYPE_COL__ having __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s1 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s1 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s1 group by 1"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s1 group by 1"

    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__ from s2"
    qt_sql "select * from s2 where __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select * from s2 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select * from s2 where __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND' order by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select * from s2 where __DORIS_STREAM_SEQUENCE_COL__='-1' order by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_SEQUENCE_COL__ having __DORIS_STREAM_SEQUENCE_COL__='-1'"
    qt_sql "select count(*) from s2 group by __DORIS_STREAM_CHANGE_TYPE_COL__ having __DORIS_STREAM_CHANGE_TYPE_COL__='APPEND'"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s2 group by __DORIS_STREAM_SEQUENCE_COL__"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s2 group by __DORIS_STREAM_CHANGE_TYPE_COL__"
    qt_sql "select __DORIS_STREAM_SEQUENCE_COL__, count(*) from s2 group by 1"
    qt_sql "select __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from s2 group by 1"

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_history_query_db"
}