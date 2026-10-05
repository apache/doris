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

suite("test_olap_table_stream_history_consumption") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_olap_table_stream_history_consumption_db"
    sql "CREATE DATABASE test_olap_table_stream_history_consumption_db"
    sql "USE test_olap_table_stream_history_consumption_db"

    // single partition table
    sql """
        CREATE TABLE `tbl1` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "binlog.enable" = "true",
        "binlog.format" = "ROW"
        ); 
    """
    sql """ 
        insert into tbl1 values (1, 's1')
    """

    sql """
        CREATE STREAM `s1` ON TABLE tbl1
        COMMENT 'test stream 1'
        PROPERTIES(
            'show_initial_rows' = 'true'
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
        "replication_allocation" = "tag.location.default: 1",
        "binlog.enable" = "true",
        "binlog.format" = "ROW"
        ); 
    """

    sql """ 
        insert into tbl2 values (2, 's2')
    """

    sql """
        CREATE STREAM `s2` ON TABLE tbl2
        COMMENT 'test stream 3'
        PROPERTIES(
            'show_initial_rows' = 'true'
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
            PARTITION p2 VALUES IN (2),
            PARTITION p3 VALUES IN (3)
        )    
        DISTRIBUTED BY HASH(`sname`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "binlog.enable" = "true",
        "binlog.format" = "ROW"
        ); 
    """

    sql """ 
        insert into tbl3 values (3, 's3')
    """

    sql """
        CREATE STREAM `s3` ON TABLE tbl3
        COMMENT 'test stream 5'
        PROPERTIES(
            'show_initial_rows' = 'true'
        );
    """

    qt_sql "select DB_NAME,STREAM_NAME,UNIT,CONSUMPTION_STATUS,LAG,LAST_CONSUMPTION_TIME from information_schema.table_stream_consumption where DB_NAME = 'test_olap_table_stream_history_consumption_db' order by STREAM_NAME, UNIT;"
    qt_sql "select * from s1"
    qt_sql "select * from s2"
    qt_sql "select * from s3"
    sql """
        CREATE TABLE `target` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """
    sql "SET show_hidden_columns=false;"
    sql """ 
        insert into target select * from s1;
    """
    sql """ 
        insert into target select * from s2;
    """
    sql """ 
        insert into target select * from s3;
    """
    qt_sql "select * from s1"
    qt_sql "select * from s2"
    qt_sql "select * from s3"
    qt_sql "select * from target order by sid"
    def consumptionRows = sql """
        select DB_NAME, STREAM_NAME, UNIT, CONSUMPTION_STATUS, LAG
        from information_schema.table_stream_consumption
        where DB_NAME = 'test_olap_table_stream_history_consumption_db'
        order by STREAM_NAME, UNIT
    """
    assertEquals(6, consumptionRows.size())

    assertEquals("test_olap_table_stream_history_consumption_db", consumptionRows[0][0].toString())
    assertEquals("s1", consumptionRows[0][1].toString())
    assertEquals("tbl1", consumptionRows[0][2].toString())
    assertTrue(consumptionRows[0][3] != null)
    assertTrue(consumptionRows[0][3].toString() != "N/A")
    assertEquals("0", consumptionRows[0][4].toString())

    assertEquals("s2", consumptionRows[1][1].toString())
    assertEquals("p1", consumptionRows[1][2].toString())
    assertEquals("N/A", consumptionRows[1][3].toString())
    assertEquals("0", consumptionRows[1][4].toString())

    assertEquals("s2", consumptionRows[2][1].toString())
    assertEquals("p2", consumptionRows[2][2].toString())
    assertTrue(consumptionRows[2][3] != null)
    assertTrue(consumptionRows[2][3].toString() != "N/A")
    assertEquals("0", consumptionRows[2][4].toString())

    assertEquals("s3", consumptionRows[3][1].toString())
    assertEquals("p1", consumptionRows[3][2].toString())
    assertEquals("N/A", consumptionRows[3][3].toString())
    assertEquals("0", consumptionRows[3][4].toString())

    assertEquals("s3", consumptionRows[4][1].toString())
    assertEquals("p2", consumptionRows[4][2].toString())
    assertEquals("N/A", consumptionRows[4][3].toString())
    assertEquals("0", consumptionRows[4][4].toString())

    assertEquals("s3", consumptionRows[5][1].toString())
    assertEquals("p3", consumptionRows[5][2].toString())
    assertTrue(consumptionRows[5][3] != null)
    assertTrue(consumptionRows[5][3].toString() != "N/A")
    assertEquals("0", consumptionRows[5][4].toString())

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_history_consumption_db"
}
