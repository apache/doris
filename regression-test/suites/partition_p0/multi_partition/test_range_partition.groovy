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

suite("test_range_partition", "p0") {
    def random = new Random()

    def createTable = { String tableName, String partitionInfo, String distributionInfo ->
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} ( 
            k1 TINYINT NOT NULL, 
            k2 SMALLINT NOT NULL, 
            k3 INT NOT NULL, 
            k4 BIGINT NOT NULL, 
            k5 LARGEINT NOT NULL,
            k6 DATETIME NOT NULL, 
            v1 DATE NOT NULL, 
            v2 CHAR NOT NULL, 
            v3 VARCHAR(4096) NOT NULL, 
            v4 FLOAT NOT NULL, 
            v5 DOUBLE NOT NULL, 
            v6 DECIMAL(20,7) NOT NULL ) 
            DUPLICATE KEY(k1, k2, k3, k4, k5, k6)
            ${partitionInfo}
            ${distributionInfo}
            PROPERTIES("replication_allocation" = "tag.location.default: 1")
        """
    }
    // when partition key is tinyint column, with different distribution type
    def testTinyintPartition = {
        def tinyIntPartition = """
        PARTITION BY RANGE(k1) (
            PARTITION partition_a VALUES LESS THAN ("-127"), 
            PARTITION partition_b VALUES LESS THAN ("-1"), 
            PARTITION partition_c VALUES LESS THAN ("0"), 
            PARTITION partition_d VALUES LESS THAN ("1"), 
            PARTITION partition_e VALUES LESS THAN ("126"), 
            PARTITION partition_f VALUES LESS THAN ("127") 
        )
        """
        def distributions = [
                "DISTRIBUTED BY RANDOM BUCKETS ${random.nextInt(300) + 1}",
                "DISTRIBUTED BY HASH(k1) BUCKETS ${random.nextInt(300) + 1}",
                "DISTRIBUTED BY HASH(k2) BUCKETS ${random.nextInt(300) + 1}",
                "DISTRIBUTED BY HASH(k3, k4, k5) BUCKETS ${random.nextInt(300) + 1}",
                "DISTRIBUTED BY HASH(k2, k3, k4, k5) BUCKETS ${random.nextInt(300) + 1}",
                "DISTRIBUTED BY HASH(k1, k2, k3, k4, k5) BUCKETS ${random.nextInt(300) + 1}"
        ]
        def idx = 0
        for (String distributionInfo in distributions) {
            createTable("tinyint_partition_tb_${idx}", tinyIntPartition, distributionInfo)
            streamLoad {
                table "tinyint_partition_tb_${idx}"
                set "column_separator", ","
                file "partition_table.csv"
            }
            sql """sync"""
            test {
                sql "select * from tinyint_partition_tb_${idx} order by k1, k2"
                resultFile "partition_table.out"
            }
            def result = sql "SHOW PARTITIONS FROM tinyint_partition_tb_${idx}"
            assertTrue(result.size() == 6)
            try_sql """DROP TABLE tinyint_partition_tb_${idx}"""
            idx += 1
        }
    }

    // tinyint
    testTinyintPartition()

    // create table -> stream load -> check -> drop table
    def testPartitionTbl = { String tableName, String partitionInfo, String distributInfo ->
        createTable(tableName, partitionInfo, distributInfo)
        streamLoad {
            table tableName
            set "column_separator", ","
            file "partition_table.csv"
        }
        sql """sync"""
        test {
            sql "select * from ${tableName} order by k1, k2"
            resultFile "partition_table.out"
        }
        def result = sql "SHOW PARTITIONS FROM ${tableName}"
        assertTrue(result.size() > 1)
        try_sql """DROP TABLE ${tableName}"""
    }

    // smallint partition key, random distribute
    testPartitionTbl(
            "test_smallint_partition_random",
            """
            PARTITION BY RANGE(k2) 
            ( PARTITION partition_a VALUES LESS THAN ("10"), 
              PARTITION partition_b VALUES LESS THAN ("20"), 
              PARTITION partition_c VALUES LESS THAN ("30"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE) 
            """,
            "DISTRIBUTED BY RANDOM BUCKETS 13"
    )
    // smallint partition key, hash distribute
    testPartitionTbl(
            "test_smallint_partition_hash",
            """
            PARTITION BY RANGE(k2) 
            ( PARTITION partition_a VALUES LESS THAN ("-32767"), 
              PARTITION partition_b VALUES LESS THAN ("-32766"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("1"), 
              PARTITION partition_e VALUES LESS THAN ("32766"), 
              PARTITION partition_f VALUES LESS THAN ("32767")) 
            """,
            "DISTRIBUTED BY hash(k2) BUCKETS 13"
    )
    // int partition key, random distribute
    testPartitionTbl(
            "test_int_partition_random",
            """
            PARTITION BY RANGE(k3) 
            ( PARTITION partition_a VALUES LESS THAN ("-2147483647"), 
              PARTITION partition_b VALUES LESS THAN ("-2147483646"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("1"), 
              PARTITION partition_e VALUES LESS THAN ("2147483646"), 
              PARTITION partition_f VALUES LESS THAN ("2147483647"))
            """,
            "DISTRIBUTED BY RANDOM BUCKETS 13"
    )
    // int partition key, hash distribute
    testPartitionTbl(
            "test_int_partition_hash",
            """
            PARTITION BY RANGE(k3) 
            ( PARTITION partition_a VALUES LESS THAN ("100"), 
              PARTITION partition_b VALUES LESS THAN ("200"), 
              PARTITION partition_c VALUES LESS THAN ("300"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE) 
            """,
            "DISTRIBUTED BY hash(k3) BUCKETS 13"
    )
    // bigint partition key, random distribute
    testPartitionTbl(
            "test_bigint_partition_random",
            """
            PARTITION BY RANGE(k4) 
            ( PARTITION partition_a VALUES LESS THAN ("1000"), 
              PARTITION partition_b VALUES LESS THAN ("2000"), 
              PARTITION partition_c VALUES LESS THAN ("3000"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE)
            """,
            "DISTRIBUTED BY RANDOM BUCKETS 13"
    )
    // bigint partition key, hash distribute
    testPartitionTbl(
            "test_bigint_partition_hash",
            """
            PARTITION BY RANGE(k4) 
            ( PARTITION partition_a VALUES LESS THAN ("-9223372036854775807"), 
              PARTITION partition_b VALUES LESS THAN ("-9223372036854775806"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("1"), 
              PARTITION partition_e VALUES LESS THAN ("9223372036854775806"), 
              PARTITION partition_f VALUES LESS THAN ("9223372036854775807")) 
            """,
            "DISTRIBUTED BY hash(k4) BUCKETS 13"
    )
    // largetint partition key, random distribute
    testPartitionTbl(
            "test_largeint_partition_random",
            """
            PARTITION BY RANGE(k5) 
            ( PARTITION partition_a VALUES LESS THAN ("1000"), 
              PARTITION partition_b VALUES LESS THAN ("2000"), 
              PARTITION partition_c VALUES LESS THAN ("3000"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE)
            """,
            "DISTRIBUTED BY RANDOM BUCKETS 13"
    )
    // largetint partition key, hash distribute
    testPartitionTbl(
            "test_largeint_partition_hash",
            """
            PARTITION BY RANGE(k5) 
            ( PARTITION partition_a VALUES LESS THAN ("-170141183460469231731687303715884105727"), 
              PARTITION partition_b VALUES LESS THAN ("-170141183460469231731687303715884105726"), 
              PARTITION partition_c VALUES LESS THAN ("0"), 
              PARTITION partition_d VALUES LESS THAN ("1"), 
              PARTITION partition_e VALUES LESS THAN ("170141183460469231731687303715884105726"), 
              PARTITION partition_f VALUES LESS THAN ("170141183460469231731687303715884105727")) 
            """,
            "DISTRIBUTED BY hash(k5) BUCKETS 13"
    )
    // don't support decimal as partition key
    test {
        sql """
            CREATE TABLE test_decimal_err (k1 decimal(9, 3), v1 int) DUPLICATE KEY(k1) 
            PARTITION BY RANGE(k1) 
            ( PARTITION partition_a VALUES LESS THAN ("100"), 
              PARTITION partition_b VALUES LESS THAN ("200"), 
              PARTITION partition_c VALUES LESS THAN ("300"))
            DISTRIBUTED BY hash(k1) BUCKETS 13
            """
        exception "Column[k1] type[DECIMAL32] cannot be a range partition key"
    }
    // don't support boolean as partition key
    test {
        sql """
            CREATE TABLE test_boolean_err (k1 boolean, v1 int) DUPLICATE KEY(k1) 
            PARTITION BY RANGE(k1) 
            ( PARTITION partition_a VALUES LESS THAN ("1"))
            DISTRIBUTED BY hash(k1) BUCKETS 13
            """
        exception "Column[k1] type[BOOLEAN] cannot be a range partition key"
    }
    // datetime partition key, random distribute
    testPartitionTbl(
            "test_datetime_partition_random",
            """
            PARTITION BY RANGE(k6) 
            ( PARTITION partition_a VALUES LESS THAN ("2017-01-01 00:00:00"), 
              PARTITION partition_b VALUES LESS THAN ("2027-01-01 00:00:00"), 
              PARTITION partition_c VALUES LESS THAN ("2039-01-01 00:00:00"), 
              PARTITION partition_d VALUES LESS THAN MAXVALUE)
            """,
            "DISTRIBUTED BY RANDOM BUCKETS 13"
    )
    // datetime partition key, hash distribute
    testPartitionTbl(
            "test_datetime_partition_hash",
            """
            PARTITION BY RANGE(k6) 
            ( PARTITION partition_a VALUES LESS THAN ("1900-01-01 00:00:00"), 
            PARTITION partition_b VALUES LESS THAN ("2027-01-01 23:59:59"), 
            PARTITION partition_c VALUES LESS THAN ("2039-01-01 00:00:00"), 
            PARTITION partition_d VALUES LESS THAN MAXVALUE) 
            """,
            "DISTRIBUTED BY hash(k6) BUCKETS 13"
    )
    // don't support char as partition key
    test {
        sql """
            CREATE TABLE test_char_err (k1 char(5), v1 int) DUPLICATE KEY(k1) 
            PARTITION BY RANGE(k1) 
            ( PARTITION partition_a VALUES LESS THAN ("12345"))
            DISTRIBUTED BY hash(k1) BUCKETS 13
            """
        exception "Column[k1] type[CHAR] cannot be a range partition key"
    }
    // don't support varchar as partition key
    test {
        sql """
            CREATE TABLE test_varchar_err (k1 varchar(5), v1 int) DUPLICATE KEY(k1) 
            PARTITION BY RANGE(k1) 
            ( PARTITION partition_a VALUES LESS THAN ("12345"))
            DISTRIBUTED BY hash(k1) BUCKETS 13
            """
        exception "Column[k1] type[VARCHAR] cannot be a range partition key"
    }
    // date value column as partition key, hash distribute
    testPartitionTbl(
            "test_date_value_partition_hash",
            """
            PARTITION BY RANGE(v1) 
            ( PARTITION partition_a VALUES LESS THAN ("2010-01-10"), 
              PARTITION partition_b VALUES LESS THAN ("2010-01-20"), 
              PARTITION partition_c VALUES LESS THAN ("2010-01-31"), 
              PARTITION partition_d VALUES LESS THAN ("2010-12-31"))
            """,
            "DISTRIBUTED BY hash(k1) BUCKETS 13"
    )
    // date value column as partition key, random distribute
    testPartitionTbl(
            "test_date_value_partition_random",
            """
            PARTITION BY RANGE(v1) 
            ( PARTITION partition_a VALUES LESS THAN ("2010-01-10"), 
              PARTITION partition_b VALUES LESS THAN ("2010-01-20"), 
              PARTITION partition_c VALUES LESS THAN ("2010-01-31"), 
              PARTITION partition_d VALUES LESS THAN ("2010-12-31"))
            """,
            "DISTRIBUTED BY RANDOM BUCKETS 13"
    )

    // create one table without datetime partition, but with date string
    sql """
        CREATE TABLE IF NOT EXISTS range_date_cast_to_datetime_range_partition ( 
            id int,
            name string,
            pdate DATETIME ) 
        PARTITION BY RANGE(pdate)(
            PARTITION pd20230418 VALUES less than ("2023-04-20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1 properties("replication_num" = "1")
        """
    sql "insert into range_date_cast_to_datetime_range_partition values (1, 'name', '2023-04-19 08:08:30')"
    sql "drop table range_date_cast_to_datetime_range_partition"

    sql """
        CREATE TABLE IF NOT EXISTS range_date_cast_to_datetime_range_partition ( 
            id int,
            name string,
            pdate DATETIME ) 
        PARTITION BY RANGE(pdate)(
            PARTITION pd20230418 VALUES less than ("2023-04-20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1 properties("replication_num" = "1")
        """
    sql "insert into range_date_cast_to_datetime_range_partition values (1, 'name', '2023-04-19 08:08:30')"
    sql "drop table range_date_cast_to_datetime_range_partition"
}
