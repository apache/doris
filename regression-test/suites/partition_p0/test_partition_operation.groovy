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

suite("test_partition_operation", "p1") {
    def random = new Random()
    // create table -> load -> check load data
    def createTableAndLoad = { String tableName, String partitionInfo, String distributionInfo ->
        sql """DROP TABLE IF EXISTS ${tableName}"""
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
            AGGREGATE KEY(k1,k2,k3,k4,k5,k6) 
            ${partitionInfo} 
            ${distributionInfo}
            PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
        streamLoad {
            table tableName
            set "column_separator", ","
            file "./multi_partition/partition_table.csv"
        }
        sql """sync"""
        test {
            sql "select * from ${tableName} order by k1, k2"
            resultFile "./multi_partition/partition_table.out"
        }

    }
    def checkTablePartitionExists = { String tableName, String partitionName ->
        def result = sql "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'"
        assertTrue(result.size() == 1)
    }
    def  checkTablePartitionNotExists = { String tableName, String partitionName ->
        def result = sql "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'"
        assertTrue(result.size() == 0)
    }
    // add partition, insert, check
    createTableAndLoad(
            "test_add_partition_1",
            """
            PARTITION BY RANGE(k3) ( 
              PARTITION partition_a VALUES LESS THAN ("50"), 
              PARTITION partition_b VALUES LESS THAN ("200"), 
              PARTITION partition_c VALUES LESS THAN ("1000"), 
              PARTITION partition_d VALUES LESS THAN ("65535000") ) """,
            "DISTRIBUTED BY HASH(k1) BUCKETS 13"
    )
    sql """
        ALTER TABLE test_add_partition_1 ADD PARTITION add_1 VALUES LESS THAN ("65536000")
    """
    checkTablePartitionExists("test_add_partition_1", "add_1")
    sql "insert into test_add_partition_1 values (50, 500, 5000, 65535990, 5000000, '2060-01-01 00:00:00', " +
            "'2010-02-28', '=', 'asdfg', 50.555, 505.55522, 545.005)"
    qt_sql1 "select * from test_add_partition_1 order by k1"

    // add partition value overlap
    test {
        sql """
            ALTER TABLE test_add_partition_1 ADD PARTITION add_1_err VALUES LESS THAN ("100")
        """
        exception "is intersected with range"
    }
    test {
        sql """
            ALTER TABLE test_add_partition_1 ADD PARTITION add_1_err VALUES LESS THAN ("50")
        """
        exception "is intersected with range"
    }
    // add partition with different bucket num, become bigger,smaller ok
    sql """ALTER TABLE test_add_partition_1 ADD PARTITION add_2 VALUES LESS THAN ("65537000") 
            DISTRIBUTED BY HASH(k1) BUCKETS 20"""
    checkTablePartitionExists("test_add_partition_1", "add_2")
    sql "insert into test_add_partition_1 values(60, 600, 6000, 65536990, 6000000, '2070-01-01 00:00:00', " +
            "'2010-03-01', '-', 'qwert', 60.555, 605.55522, 645.005)"
    qt_sql2 "select * from test_add_partition_1 order by k1"
    sql """ALTER TABLE test_add_partition_1 ADD PARTITION add_3 VALUES LESS THAN ("65538000") 
            DISTRIBUTED BY HASH(k1) BUCKETS 37"""
    checkTablePartitionExists("test_add_partition_1", "add_3")
    sql "insert into test_add_partition_1 values(70, 700, 7000, 65537990, 7000000, '2080-01-01 00:00:00', " +
            "'2010-03-02', '+', 'zxcvb', 70.555, 705.55522, 745.005)"
    qt_sql3 "select * from test_add_partition_1 order by k1"
    sql """ALTER TABLE test_add_partition_1 ADD PARTITION add_4 VALUES LESS THAN ("65540000") 
            DISTRIBUTED BY HASH(k1) BUCKETS 7"""
    checkTablePartitionExists("test_add_partition_1", "add_4")
    sql "insert into test_add_partition_1 values(80, 800, 8000, 65539990, 8000000, '2090-01-01 00:00:00', " +
            "'2010-03-03', '>', 'uiopy', 80.555, 805.55522, 845.005)"
    qt_sql4 "select * from test_add_partition_1 order by k1"
    // add partition with 0 bucket
    test {
        sql """ALTER TABLE test_add_partition_1 ADD PARTITION add_0_err VALUES LESS THAN ("2147483647") 
                DISTRIBUTED BY HASH(k1) BUCKETS 0"""
        exception "Cannot assign hash distribution buckets less than 1"
    }

    // add partition with different distribution key
    // todo err msg need be fixed
    // ERROR 1105 (HY000): errCode = 2, detailMessage = Cannot assign hash distribution with different distribution cols.
    // new is: [`k1` tinyint(4) NOT NULL, `k2` smallint(6) NOT NULL] default is: [`k1` tinyint(4) NOT NULL, `k2` smallint(6) NOT NULL]
    test {
        sql """ALTER TABLE test_add_partition_1 ADD PARTITION add_2_err VALUES LESS THAN ("2147483647") 
                DISTRIBUTED BY HASH(k1, k2) BUCKETS 13"""
        exception "Cannot assign hash distribution with different distribution cols"
    }
    // add partition with different ditribution type: hash -> random
    test {
        sql """ALTER TABLE test_add_partition_1 ADD PARTITION add_2_err VALUES LESS THAN ("2147483647") 
                DISTRIBUTED BY RANDOM BUCKETS 13"""
        exception "Cannot assign different distribution type. default is: HASH"
    }

    // test drop partitions
    createTableAndLoad(
            "test_drop_partition_1",
            """
                PARTITION BY RANGE(k3) ( 
                  PARTITION partition_a VALUES LESS THAN ("50"), 
                  PARTITION partition_b VALUES LESS THAN ("200"), 
                  PARTITION partition_c VALUES LESS THAN ("1000"), 
                  PARTITION partition_d VALUES LESS THAN ("65535000") )""",
            "DISTRIBUTED BY HASH(k1) BUCKETS 13"
    )
    // drop a not exists partition
    test {
        sql """ALTER TABLE test_drop_partition_1 DROP PARTITION not_exists_partition"""
        exception "Error in list of partitions to not_exists_partition"
    }
    // drop all exists partition
    sql """ALTER TABLE test_drop_partition_1 DROP PARTITION partition_a"""
    checkTablePartitionNotExists("test_drop_partition_1", "partition_a")
    sql """ALTER TABLE test_drop_partition_1 DROP PARTITION partition_b"""
    checkTablePartitionNotExists("test_drop_partition_1", "partition_b")
    sql """ALTER TABLE test_drop_partition_1 DROP PARTITION partition_c, DROP PARTITION partition_d"""
    checkTablePartitionNotExists("test_drop_partition_1", "partition_c")
    checkTablePartitionNotExists("test_drop_partition_1", "partition_d")
    qt_sql5 "select * from test_drop_partition_1 order by k1, k2"
    // after drop all partition, add a partiion
    sql """ALTER TABLE test_drop_partition_1 ADD PARTITION add_1 VALUES LESS THAN ("0")"""
    checkTablePartitionExists("test_drop_partition_1", "add_1")

    createTableAndLoad(
            "test_drop_partition_2",
            """
              PARTITION BY RANGE(k3) ( 
              PARTITION partition_a VALUES LESS THAN ("300"), 
              PARTITION partition_b VALUES LESS THAN ("500"), 
              PARTITION partition_c VALUES LESS THAN ("900"), 
              PARTITION partition_d VALUES LESS THAN ("2200"), 
              PARTITION partition_e VALUES LESS THAN ("2300"), 
              PARTITION partition_f VALUES LESS THAN ("2800"), 
              PARTITION partition_g VALUES LESS THAN ("4000") ) """,
            "DISTRIBUTED BY HASH(k1) BUCKETS 13"
    )
    // drop partition times
    sql """ALTER TABLE test_drop_partition_2 DROP PARTITION partition_a"""
    checkTablePartitionNotExists("test_drop_partition_2", "partition_a")
    qt_sql6 "select * from test_drop_partition_2 order by k1"
    sql """ALTER TABLE test_drop_partition_2 ADD PARTITION add_1 VALUES LESS THAN ("5000")"""
    checkTablePartitionExists("test_drop_partition_2", "add_1")
    sql """ALTER TABLE test_drop_partition_2 ADD PARTITION add_2 VALUES LESS THAN ("6000")"""
    checkTablePartitionExists("test_drop_partition_2", "add_2")

    sql """ALTER TABLE test_drop_partition_2 DROP PARTITION partition_c"""
    checkTablePartitionNotExists("test_drop_partition_2", "partition_c")
    qt_sql7 "select * from test_drop_partition_2 order by k1"
    sql """ALTER TABLE test_drop_partition_2 DROP PARTITION partition_e"""
    checkTablePartitionNotExists("test_drop_partition_2", "partition_e")
    qt_sql8 "select * from test_drop_partition_2 order by k1"
    sql """ALTER TABLE test_drop_partition_2 ADD PARTITION add_3 VALUES LESS THAN MAXVALUE"""
    checkTablePartitionExists("test_drop_partition_2", "add_3")

    sql """ALTER TABLE test_drop_partition_2 DROP PARTITION partition_g"""
    checkTablePartitionNotExists("test_drop_partition_2", "partition_g")
    qt_sql9 "select * from test_drop_partition_2 order by k1"

    // drop partition, add partition, times
    createTableAndLoad(
            "test_add_drop_partition_times",
            """
                PARTITION BY RANGE(k3)( 
                  PARTITION partition_a VALUES LESS THAN ("300"), 
                  PARTITION partition_b VALUES LESS THAN ("500"), 
                  PARTITION partition_c VALUES LESS THAN ("900"), 
                  PARTITION partition_d VALUES LESS THAN ("2200"), 
                  PARTITION partition_e VALUES LESS THAN ("2300"), 
                  PARTITION partition_f VALUES LESS THAN ("3800"), 
                  PARTITION partition_g VALUES LESS THAN ("4000") )""",
            "DISTRIBUTED BY HASH(k1) BUCKETS 13"
    )
    for (int repeat_times = 0; repeat_times < 10; repeat_times++) {
        sql "ALTER TABLE test_add_drop_partition_times DROP PARTITION partition_g"
        checkTablePartitionNotExists("test_add_drop_partition_times", "partition_g")
        sql "ALTER TABLE test_add_drop_partition_times ADD PARTITION partition_g VALUES LESS THAN ('4000') " +
                "DISTRIBUTED BY HASH(k1) BUCKETS ${random.nextInt(300) + 1}"
        checkTablePartitionExists("test_add_drop_partition_times", "partition_g")
    }
    test {
        sql "select * from test_add_drop_partition_times order by k1, k2"
        resultFile "./multi_partition/partition_table.out"
    }

    // add multi partitions and drop
    createTableAndLoad(
            "test_add_drop_partition_times_1",
            """
                PARTITION BY RANGE(k3)( 
                  PARTITION partition_a VALUES LESS THAN ("300"), 
                  PARTITION partition_b VALUES LESS THAN ("500"), 
                  PARTITION partition_c VALUES LESS THAN ("900"), 
                  PARTITION partition_d VALUES LESS THAN ("2200"), 
                  PARTITION partition_e VALUES LESS THAN ("2300"), 
                  PARTITION partition_f VALUES LESS THAN ("3800"), 
                  PARTITION partition_g VALUES LESS THAN ("4000") )""",
            "DISTRIBUTED BY HASH(k1) BUCKETS 13"
    )
    for (int repeat_times = 1; repeat_times < 30; repeat_times++) {
        sql "ALTER TABLE test_add_drop_partition_times_1 ADD PARTITION partition_add_${repeat_times.toString()} " +
                "VALUES LESS THAN ('${4000 + repeat_times * 10}') " +
                "DISTRIBUTED BY HASH(k1) BUCKETS ${random.nextInt(300) + 1}"
        checkTablePartitionExists("test_add_drop_partition_times_1", "partition_add_${repeat_times.toString()}")
    }
    for (int repeat_times = 1; repeat_times < 30; repeat_times++) {
        sql "ALTER TABLE test_add_drop_partition_times_1 DROP PARTITION partition_add_${repeat_times.toString()}"
        checkTablePartitionNotExists("test_add_drop_partition_times_1", "partition_add_${repeat_times.toString()}")
    }
    test {
        sql "select * from test_add_drop_partition_times_1 order by k1, k2"
        resultFile "./multi_partition/partition_table.out"
    }


    // add partition to non partition table
    sql """
        CREATE TABLE IF NOT EXISTS test_non_partition_tbl ( 
          k1 TINYINT NOT NULL, 
          k2 SMALLINT NOT NULL, 
          k3 INT NOT NULL, 
          k4 BIGINT NOT NULL, 
          k5 DATETIME NOT NULL, 
          v1 DATE REPLACE NOT NULL, 
          v2 CHAR REPLACE NOT NULL, 
          v3 VARCHAR(4096) REPLACE NOT NULL, 
          v4 FLOAT SUM NOT NULL, 
          v5 DOUBLE SUM NOT NULL, 
          v6 DECIMAL(20,7) SUM NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5) DISTRIBUTED BY HASH(k1) BUCKETS 13 
        PROPERTIES("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """ALTER TABLE test_non_partition_tbl ADD PARTITION add_1 VALUES LESS THAN ("2147483647") 
                DISTRIBUTED BY HASH(k1, k2) BUCKETS 13"""
        exception ""
    }
    try_sql "DROP TABLE IF EXISTS test_add_partition_1"
    try_sql "DROP TABLE IF EXISTS test_drop_partition_1"
    try_sql "DROP TABLE IF EXISTS test_drop_partition_2"
    try_sql "DROP TABLE IF EXISTS test_add_drop_partition_times_1"
    try_sql "DROP TABLE IF EXISTS test_add_drop_partition_times_2"
    try_sql "DROP TABLE IF EXISTS test_non_partition_tbl"
}
