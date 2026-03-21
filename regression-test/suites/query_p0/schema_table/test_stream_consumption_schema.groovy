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

suite("test_stream_consumption_schema") {
    sql "DROP DATABASE IF EXISTS test_stream_consumption_db"
    sql "CREATE DATABASE test_stream_consumption_db"
    sql "USE test_stream_consumption_db"

    // single partition table
    sql """
        CREATE TABLE `tbl1` (
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
        CREATE STREAM `s1` ON TABLE tbl1
        COMMENT 'test stream 1'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'true'
        );
    """

    sql """
        CREATE STREAM `s2` ON TABLE tbl1
        COMMENT 'test stream 2'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'false'
        );
    """


    // range partition table
    sql """
        CREATE TABLE `tbl2` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        PARTITION BY RANGE(`sid`)
        (
            PARTITION p1 VALUES LESS THAN (2),
            PARTITION p2 VALUES [(2),(3))
        )    
        DISTRIBUTED BY HASH(`sname`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """ 
        insert into tbl2 values (1, 's1')
    """

    sql """
        CREATE STREAM `s3` ON TABLE tbl2
        COMMENT 'test stream 3'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'true'
        );
    """

    sql """
        CREATE STREAM `s4` ON TABLE tbl2
        COMMENT 'test stream 4'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'false'
        );
    """

    // list partition table
    sql """
        CREATE TABLE `tbl3` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        PARTITION BY LIST(`sid`)
        (
            PARTITION p1 VALUES IN (1),
            PARTITION p2 VALUES IN (2)
        )    
        DISTRIBUTED BY HASH(`sname`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

      sql """ 
        insert into tbl3 values (1, 's1')
    """

     sql """
        CREATE STREAM `s5` ON TABLE tbl3
        COMMENT 'test stream 5'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'true'
        );
    """

    sql """
        CREATE STREAM `s6` ON TABLE tbl3
        COMMENT 'test stream 6'
        PROPERTIES(
            'type' = 'default',
            'show_initial_rows' = 'false'
        );
    """

    qt_sql "select DB_NAME,STREAM_NAME,UNIT,CONSUMPTION_STATUS,LAG,LAST_CONSUMPTION_TIME from information_schema.stream_consumption where DB_NAME = 'test_stream_consumption_db' order by STREAM_NAME;"
    sql "DROP DATABASE IF EXISTS test_stream_consumption_db"
}