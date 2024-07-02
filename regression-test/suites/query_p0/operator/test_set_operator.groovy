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

suite("test_set_operators", "query,p0,arrow_flight_sql") {

    sql """
        DROP TABLE IF EXISTS t1;
    """

    sql """
        CREATE TABLE t1 (col1 varchar(11451) not null, col2 int not null, col3 int not null)
        UNIQUE KEY(col1)
        DISTRIBUTED BY HASH(col1)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
        );
    """

    sql """
        insert into t1 values(1, 2, 3);
    """

    sql """insert into t1 values(4, 5, 6);"""
    sql """insert into t1 values(7, 1, 9);"""
    sql """insert into t1 values(3, 8, 2);"""
    sql """insert into t1 values(5, 2, 1);"""

    sql """
        DROP TABLE IF EXISTS t2;
    """
    sql """
        CREATE TABLE t2 (col1 varchar(32) not null, col2 int not null, col3 int not null, col4 int not null)
        DISTRIBUTED BY HASH(col3)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1"
        );
    """
    sql """
        DROP TABLE IF EXISTS t3;
    """
    sql """
        CREATE TABLE t3 (col1 varchar(32) null, col2 int not null, col3 int not null, col4 int not null)
        DISTRIBUTED BY HASH(col3)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1"
        );
    """

    sql """insert into t3 values('21',5,1,7);"""
    sql """insert into t3 values('7',9,1,5);"""
    sql """insert into t3 values('3',5,3,4);"""
    sql """insert into t3 values('9',8,0,7);"""

    sql """insert into t2 values('1',5,1,7);"""
    sql """insert into t2 values('51',9,1,5);"""
    sql """insert into t2 values('6',5,3,4);"""
    sql """insert into t2 values('9',8,0,7);"""

    sql 'sync'
    order_qt_select """
        select
            col1
        from
             t1
        union all
        select
            coalesce(t2.col1, t3.col2)  c
        from
            t2
        full join
            t3 
            on t2.col1=t3.col1;
    """

    order_qt_select_minus """
        select col1, col1 from t1 minus select col1, col1 from t2;
    """

    order_qt_select_except """
        select col1, col1 from t1 except select col1, col1 from t2;
    """
}
