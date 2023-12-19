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

suite("test_multi_partition_key", "p0") {

    // TODO: remove it after we add implicit cast check in Nereids
    sql "set enable_nereids_dml=false"

    def random = new Random()
    sql "set enable_insert_strict=true"
    def createTable = { String tableName, String partitionInfo /* param */  ->
        sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} ( 
            k1 TINYINT NOT NULL, 
            k2 SMALLINT NOT NULL, 
            k3 INT NOT NULL, 
            k4 BIGINT NOT NULL, 
            k5 LARGEINT NOT NULL,
            k6 DATETIME NOT NULL, 
            v1 DATE REPLACE NOT NULL, 
            v2 CHAR REPLACE NOT NULL, 
            v3 VARCHAR(4096) REPLACE NOT NULL, 
            v4 FLOAT SUM NOT NULL, 
            v5 DOUBLE SUM NOT NULL, 
            v6 DECIMAL(20,7) SUM NOT NULL ) 
        ${partitionInfo}
        DISTRIBUTED BY HASH(k1,k2,k3) BUCKETS ${random.nextInt(300) + 1}
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
    }
    def testPartitionTbl = { String tableName, String partitionInfo, Boolean ifDropTbl /* param */ ->
        createTable(tableName, partitionInfo)
        streamLoad {
            table tableName
            set "column_separator", ","
            file "partition_table.csv"
        }
        sql "sync"
        test {
            sql "select * from ${tableName} order by k1, k2"
            resultFile "partition_table.out"
        }
        def result = sql "SHOW PARTITIONS FROM ${tableName}"
        assertTrue(result.size() > 1)
        if (ifDropTbl) {
            try_sql """DROP TABLE ${tableName}"""
        }

    }
    // test multi partition columns
    testPartitionTbl(
            "test_multi_partition_key_1",
            """
            PARTITION BY RANGE(k1,k2) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("0","-1"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("126","126"), 
              PARTITION partition_f VALUES LESS THAN ("127") )""",
            true
    )

    // partition columns are int & datetime
    test {
        sql """
        CREATE TABLE err_1 ( 
          k1 TINYINT NOT NULL,  
          k5 DATETIME NOT NULL, 
          v1 DATE REPLACE NOT NULL) 
        PARTITION BY RANGE(k1,k5) ( 
          PARTITION partition_a VALUES LESS THAN ("-127","2010-01-01"), 
          PARTITION partition_e VALUES LESS THAN ("4","2010-01-01"), 
          PARTITION partition_f VALUES LESS THAN MAXVALUE ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 53
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        exception "Invalid datetime value: 2010-01-01"
    }
    testPartitionTbl(
            "test_multi_partition_key_2",
            """
            PARTITION BY RANGE(k1,k6) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","2010-01-01 00:00:00"), 
              PARTITION partition_b VALUES LESS THAN ("1","2010-01-02 00:00:00"), 
              PARTITION partition_c VALUES LESS THAN ("2","2010-01-03 00:00:00"), 
              PARTITION partition_d VALUES LESS THAN ("3","2010-01-04 00:00:00"), 
              PARTITION partition_e VALUES LESS THAN ("4","2010-01-01 00:00:00"), 
              PARTITION partition_f VALUES LESS THAN MAXVALUE )
            """,
            false
    )
    qt_sql1 "select * from test_multi_partition_key_2 partition (partition_a) order by k1, k2 "
    qt_sql2 "select * from test_multi_partition_key_2 partition (partition_b) order by k1, k2"
    qt_sql3 "select * from test_multi_partition_key_2 partition (partition_c) order by k1, k2"
    qt_sql4 "select * from test_multi_partition_key_2 partition (partition_d) order by k1, k2"
    qt_sql5 "select * from test_multi_partition_key_2 partition (partition_e) order by k1, k2"
    qt_sql6 "select * from test_multi_partition_key_2 partition (partition_f) order by k1, k2"

    // partition columns should be key in aggregate table
    test {
        sql """
        CREATE TABLE err_2 ( 
          k1 TINYINT NOT NULL,  
          v1 DATE REPLACE NOT NULL) 
        AGGREGATE KEY(k1) 
        PARTITION BY RANGE(k1,v1) ( 
          PARTITION partition_a VALUES LESS THAN ("-127","2010-01-01"), 
          PARTITION partition_e VALUES LESS THAN ("4","2010-01-01"), 
          PARTITION partition_f VALUES LESS THAN MAXVALUE )
        DISTRIBUTED BY HASH(k1) BUCKETS 226
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        exception " The partition column could not be aggregated column"
    }
    // test the default value of partition value: minvalue
    testPartitionTbl(
            "test_default_minvalue",
            """
            PARTITION BY RANGE(k2,k1) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("126","126"), 
              PARTITION partition_f VALUES LESS THAN ("500") )
            """,
            false
    )
    // expect partition_f range: [ [126, 126] ~ [500, -128] )
    def ret = sql "SHOW PARTITIONS FROM test_default_minvalue WHERE PartitionName='partition_f'"
    assertTrue(ret[0][6].contains("[500, -128]"))

    // partition columns error
    test {
        sql """
        CREATE TABLE err_3 ( 
          k1 TINYINT NOT NULL, 
          v1 DATE REPLACE NOT NULL)
        PARTITION BY RANGE(k1,k1) ( 
          PARTITION partition_a VALUES LESS THAN ("-127","1"),  
          PARTITION partition_f VALUES LESS THAN MAXVALUE ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 68
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        exception "Duplicated partition column k1"
    }
    // partition range intersected
    test {
        sql """
        CREATE TABLE err_4 ( 
          k1 TINYINT NOT NULL, 
          k2 SMALLINT NOT NULL, 
          v1 DATE REPLACE NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        PARTITION BY RANGE(k1,k2) ( 
          PARTITION partition_a VALUES LESS THAN ("-127"), 
          PARTITION partition_b VALUES LESS THAN ("5","0"), 
          PARTITION partition_c VALUES LESS THAN ("5","100"), 
          PARTITION partition_d VALUES LESS THAN ("5","10"), 
          PARTITION partition_f VALUES LESS THAN MAXVALUE ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 258
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        exception "Range [types: [TINYINT, SMALLINT]; keys: [5, 0]; ..types: [TINYINT, SMALLINT]; keys: [5, 10]; ) " +
                "is intersected with range: " +
                "[types: [TINYINT, SMALLINT]; keys: [5, 0]; ..types: [TINYINT, SMALLINT]; keys: [5, 100]; )"
    }
    // partition range error
    test {
        sql """
        CREATE TABLE err_5 ( 
          k1 TINYINT NOT NULL, 
          k2 SMALLINT NOT NULL, 
          v1 DATE REPLACE NOT NULL) 
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1,k2) ( 
          PARTITION partition_a VALUES LESS THAN ("-127"), 
          PARTITION partition_b VALUES LESS THAN ("5","0"), 
          PARTITION partition_c VALUES LESS THAN ("5","100"), 
          PARTITION partition_d VALUES LESS THAN ("5","300"), 
          PARTITION partition_e VALUES LESS THAN ("5"), 
          PARTITION partition_f VALUES LESS THAN MAXVALUE ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 268
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        exception "[types: [TINYINT, SMALLINT]; keys: [-127, -32768]; ..types: [TINYINT, SMALLINT]; keys: [5, -32768]; )" +
                " is intersected with range: " +
                "[types: [TINYINT, SMALLINT]; keys: [-127, -32768]; ..types: [TINYINT, SMALLINT]; keys: [5, 0]; )"
    }
    // partitioned by multi columns and add partition
    testPartitionTbl(
            "test_multi_col_test_partition_add",
            """
            PARTITION BY RANGE(k1,k2) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("0","-1"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("30"), 
              PARTITION partition_f VALUES LESS THAN ("30","500") )
            """,
            false
    )
    // error msg check
    test {
        sql "ALTER TABLE test_multi_col_test_partition_add ADD PARTITION partition_add VALUES LESS THAN ('30', '1000') " +
                "DISTRIBUTED BY hash(k1) BUCKETS 5"
        exception "Cannot assign hash distribution with different distribution cols. new is: [`k1` TINYINT NOT NULL] default is: [`k1` TINYINT NOT NULL, `k2` SMALLINT NOT NULL, `k3` INT NOT NULL]"
    }

    sql "ALTER TABLE test_multi_col_test_partition_add ADD PARTITION partition_add VALUES LESS THAN ('30', '1000') "
    def ret_add_p = sql "SHOW PARTITIONS FROM test_multi_col_test_partition_add WHERE PartitionName='partition_add'"
    assertTrue(ret[0][6].contains("[500, -128]"))
    test {
        sql "ALTER TABLE test_multi_col_test_partition_add ADD PARTITION add_partition_wrong " +
                "VALUES LESS THAN ('30', '800') DISTRIBUTED BY hash(k1) BUCKETS 5"
        exception "is intersected with range"
    }
    // partitioned by multi columns and drop partition column
    testPartitionTbl(
            "test_multi_col_test_partition_drop",
            """
            PARTITION BY RANGE(k1,k2) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"),
              PARTITION partition_c VALUES LESS THAN ("0","-1"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("30"), 
              PARTITION partition_f VALUES LESS THAN ("30","500") )
            """,
            false
    )
    sql "ALTER TABLE test_multi_col_test_partition_drop DROP PARTITION partition_d"
    def ret_drop_p = sql "SHOW PARTITIONS FROM test_multi_col_test_partition_drop WHERE PartitionName='partition_d'"
    assertEquals(0, ret_drop_p.size())
    sql "ALTER TABLE test_multi_col_test_partition_drop ADD PARTITION partition_dd VALUES LESS THAN ('0','0') "
    ret_drop_p = sql "SHOW PARTITIONS FROM test_multi_col_test_partition_drop WHERE PartitionName='partition_dd'"
    assertTrue(ret_drop_p[0][6].contains("[0, 0]"))
    // null value in the lowest partition, if drop the partition null is deleted.
    sql """drop table if exists test_multi_col_test_partition_null_value"""
    sql """
        CREATE TABLE test_multi_col_test_partition_null_value ( 
            k1 TINYINT NULL, 
            k2 SMALLINT NULL, 
            k3 INT NULL, 
            k4 BIGINT NULL, 
            k5 LARGEINT NULL,
            k6 DATETIME NULL, 
            v1 DATE REPLACE NULL, 
            v2 CHAR REPLACE NULL, 
            v3 VARCHAR(4096) REPLACE NULL, 
            v4 FLOAT SUM NULL, 
            v5 DOUBLE SUM NULL, 
            v6 DECIMAL(20,7) SUM NULL ) 
           
        PARTITION BY RANGE(k2,k1) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("126","126"), 
              PARTITION partition_f VALUES LESS THAN ("500") )
            
        DISTRIBUTED BY HASH(k1,k2,k3) BUCKETS 18
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
    """
    sql "insert into test_multi_col_test_partition_null_value " +
            "values(0, NULL, 0, 0, 0, '2000-01-01 00:00:00', '2000-01-01', 'a', 'a', 0.001, -0.001, 0.001)"
    qt_sql7 "select k1 from test_multi_col_test_partition_null_value partition(partition_a) where k2 is null"
    sql "ALTER TABLE test_multi_col_test_partition_null_value DROP PARTITION partition_a"
    test {
        sql "insert into test_multi_col_test_partition_null_value " +
                "values(0, NULL, 0, 0, 0, '2000-01-01 00:00:00', '2000-01-01', 'a', 'a', 0.001, -0.001, 0.001)"
        exception "Insert has filtered data in strict mode"
    }
    qt_sql8 "select k1 from test_multi_col_test_partition_null_value where k2 is null"
    // partition columns and add key column
    testPartitionTbl(
            "test_multi_col_test_partition_key_add_col",
            """
            PARTITION BY RANGE(k2,k1) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("126","126"), 
              PARTITION partition_f VALUES LESS THAN ("500") )
            """,
            false
    )
    sql "ALTER TABLE test_multi_col_test_partition_key_add_col ADD COLUMN add_key int NOT NULL DEFAULT '0' AFTER k1"
    assertEquals("FINISHED", getAlterColumnFinalState("test_multi_col_test_partition_key_add_col"))
    sql "insert into test_multi_col_test_partition_key_add_col " +
            "values(0, 100, 0, 0, 0, 0, '2000-01-01 00:00:00', '2000-01-01', 'a', 'a', 0.001, -0.001, 0.001)"
    qt_sql9 "select * from test_multi_col_test_partition_key_add_col order by k1, k2"
    // can not drop partition column
    testPartitionTbl(
            "test_multi_column_drop_partition_column",
            """
            PARTITION BY RANGE(k2,k1) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("0","0"), 
              PARTITION partition_e VALUES LESS THAN ("126","126"), 
              PARTITION partition_f VALUES LESS THAN ("500") )
            """,
            false
    )
    test {
        sql "ALTER TABLE test_multi_column_drop_partition_column DROP COLUMN k1"
        exception "Can not drop key column when table has value column with REPLACE aggregation method"
    }
    // drop replace value
    sql "ALTER TABLE test_multi_column_drop_partition_column DROP COLUMN v1, DROP COLUMN v2, DROP COLUMN v3"
    assertEquals("FINISHED", getAlterColumnFinalState("test_multi_column_drop_partition_column"))
    test {
        sql "ALTER TABLE test_multi_column_drop_partition_column DROP COLUMN k1"
        exception "Partition column[k1] cannot be dropped"
    }
    sql "insert into test_multi_column_drop_partition_column " +
            "values(100, 0, 0, 0, 0, '2000-01-01 00:00:00', 0.001, -0.001, 0.001)"
    qt_sql10 "select * from test_multi_column_drop_partition_column order by k2, k3, k4"
    // test partition delete, todo:
    sql "DELETE FROM test_multi_column_drop_partition_column PARTITION partition_d WHERE k2 = 10 AND k3 > 10"
    qt_sql11 "select * from test_multi_column_drop_partition_column order by k2, k3, k4"
    // test partition rollup, create & drop
    testPartitionTbl(
            'test_multi_col_test_rollup',
            """
            PARTITION BY RANGE(k2,k1) ( 
              PARTITION partition_a VALUES LESS THAN ("-127","-127"), 
              PARTITION partition_b VALUES LESS THAN ("-1","-1"), 
              PARTITION partition_c VALUES LESS THAN ("10"), 
              PARTITION partition_d VALUES LESS THAN ("10","1"), 
              PARTITION partition_e VALUES LESS THAN ("126","126"), 
              PARTITION partition_f VALUES LESS THAN ("500") )
            """,
            false
    )
    sql "ALTER TABLE test_multi_col_test_rollup ADD ROLLUP idx (k1,k3,k4,v5)"
    assertEquals("FINISHED", getAlterRollupFinalState("test_multi_col_test_rollup"))
    List<List<Object>> table_schema = sql "desc test_multi_col_test_rollup all"
    assertEquals(17, table_schema.size())
    assertEquals("idx", table_schema[13][0])
    // test partition drop rollup,
    sql "ALTER TABLE test_multi_col_test_rollup DROP ROLLUP idx"
    assertEquals("FINISHED", getAlterRollupFinalState("test_multi_col_test_rollup"))
    table_schema = sql "desc test_multi_col_test_rollup all"
    assertEquals(12, table_schema.size())
    qt_sql12 "select * from test_multi_col_test_rollup order by k1, k2"
    // test partition modify, check partition replication_num
    sql "ALTER TABLE test_multi_col_test_rollup MODIFY PARTITION partition_a SET( 'replication_num' = '1')"
    ret = sql "SHOW PARTITIONS FROM test_multi_col_test_rollup WHERE PartitionName='partition_a'"
    assertEquals('1', ret[0][9])
    // create table with range partition
    testPartitionTbl(
            "test_multi_column_fixed_range_1",
            """
            PARTITION BY RANGE(k1,k2) ( 
              PARTITION partition_a VALUES [("-127","-127"), ("10","-1")), 
              PARTITION partition_b VALUES [("10","-1"), ("40","0")), 
              PARTITION partition_c VALUES [("126","126"), ("127")) )
            """,
            true
    )

    // partition value range
    testPartitionTbl(
            "test_multi_column_fixed_range_1",
            """
            PARTITION BY RANGE(k1,k2) ( 
              PARTITION partition_a VALUES [("-127","-127"), ("10","10")), 
              PARTITION partition_b VALUES [("10","100"), ("40","0")), 
              PARTITION partition_c VALUES [("126","126"), ("127")) )
            """,
            false
    )
    // add partition with range
    sql "ALTER TABLE test_multi_column_fixed_range_1 ADD PARTITION partition_add VALUES LESS THAN ('50','1000') "
    ret = sql "SHOW PARTITIONS FROM test_multi_column_fixed_range_1 WHERE PartitionName='partition_add'"
    assertEquals(1, ret.size(), )
    test {
        sql "ALTER TABLE test_multi_column_fixed_range_1 ADD PARTITION add_partition_wrong VALUES LESS THAN ('50','800')"
        exception "is intersected with range"
    }
    // test multi column insert
    sql """drop table if exists test_multi_col_insert"""
    sql """
        CREATE TABLE IF NOT EXISTS test_multi_col_insert ( 
            k1 TINYINT NOT NULL, 
            k2 SMALLINT NOT NULL) 
        PARTITION BY RANGE(k1,k2) ( 
              PARTITION partition_a VALUES [("-127","-127"), ("10","10")), 
              PARTITION partition_b VALUES [("10","100"), ("40","0")), 
              PARTITION partition_c VALUES [("126","126"), ("127")) )
        DISTRIBUTED BY HASH(k1,k2) BUCKETS 1
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
    test {
        sql "insert into test_multi_col_insert values (-127, -200)"
        exception "Insert has filtered data in strict mode"
    }
    sql "insert into test_multi_col_insert values (10, -100)"
    test {
        sql "insert into test_multi_col_insert values (10, 50)"
        exception "Insert has filtered data in strict mode"

    }
    sql "insert into test_multi_col_insert values (10, 100)"
    sql "insert into test_multi_col_insert values (30, -32768)"
    test {
        sql "insert into test_multi_col_insert values (30, -32769)"
        exception "Number out of range[-32769]. type: SMALLINT"
    }
    test {
        sql "insert into test_multi_col_insert values (50, -300)"
        exception "Insert has filtered data in strict mode"
    }
    test {
        sql "insert into test_multi_col_insert values (127, -300)"
        exception "Insert has filtered data in strict mode"
    }
    qt_sql13 "select * from test_multi_col_insert order by k1, k2"

    try_sql "drop table if exists test_default_minvalue"
    try_sql "drop table if exists test_multi_col_insert"
    try_sql "drop table if exists test_multi_col_test_partition_add"
    try_sql "drop table if exists test_multi_col_test_partition_drop"
    try_sql "drop table if exists test_multi_col_test_partition_key_add_col"
    try_sql "drop table if exists test_multi_col_test_partition_null_value"
    try_sql "drop table if exists test_multi_col_test_rollup"
    try_sql "drop table if exists test_multi_column_drop_partition_column"
    try_sql "drop table if exists test_multi_column_fixed_range_1"
    try_sql "drop table if exists test_multi_partition_key_2"

    sql """set enable_nereids_planner=false"""
    test {
        sql """
            CREATE TABLE IF NOT EXISTS test_multi_col_ddd (
                k1 TINYINT NOT NULL, 
                k2 SMALLINT NOT NULL) 
            PARTITION BY RANGE(k1,k2) ( 
                  PARTITION partition_a VALUES [("-127","-127"), ("10", MAXVALUE)), 
                  PARTITION partition_b VALUES [("10","100"), ("40","0")), 
                  PARTITION partition_c VALUES [("126","126"), ("127")) )
            DISTRIBUTED BY HASH(k1,k2) BUCKETS 1
            PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        exception "Not support MAXVALUE in multi partition range values"
    }

}
