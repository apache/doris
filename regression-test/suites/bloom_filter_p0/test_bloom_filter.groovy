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
suite("test_bloom_filter","nonConcurrent") {
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }
    // todo: test bloom filter, such alter table bloom filter, create table with bloom filter
    sql "SHOW ALTER TABLE COLUMN"

    // bloom filter index for ARRAY column
    def test_array_tb = "test_array_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_array_tb}"""
    test {
        sql """CREATE TABLE IF NOT EXISTS ${test_array_tb} (
                `k1` int(11) NOT NULL,
                `a1` array<boolean> NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 5
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "k1,a1"
            )"""
        exception "not supported in bloom filter index"
    }

    sql """CREATE TABLE IF NOT EXISTS ${test_array_tb} (
            `k1` int(11) NOT NULL,
            `a1` array<boolean> NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1",
                "bloom_filter_columns" = "k1"
        )"""
    test {
        sql """ALTER TABLE ${test_array_tb} SET("bloom_filter_columns" = "k1,a1")"""
        exception "not supported in bloom filter index"
    }

    // bloom filter index for STRUCT column
    def test_struct_tb = "test_struct_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_struct_tb}"""

    test {
        sql """CREATE TABLE IF NOT EXISTS ${test_struct_tb} (
                `k1` int(11) NOT NULL,
                `s1` struct<f1:int, f2:char(5)> NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 5
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "k1,s1"
            )"""
        exception "not supported in bloom filter index"
    }

    sql """CREATE TABLE IF NOT EXISTS ${test_struct_tb} (
            `k1` int(11) NOT NULL,
            `s1` struct<f1:int, f2:char(5)> NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1",
                "bloom_filter_columns" = "k1"
        )"""
    test {
        sql """ALTER TABLE ${test_struct_tb} SET("bloom_filter_columns" = "k1,s1")"""
        exception "not supported in bloom filter index"
    }

    // bloom filter index for MAP column
    def test_map_tb = "test_map_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_map_tb}"""

    test {
        sql """CREATE TABLE IF NOT EXISTS ${test_map_tb} (
                `k1` int(11) NOT NULL,
                `m1` map<int, char(5)> NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 5
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "k1,m1"
            )"""
        exception "not supported in bloom filter index"
    }

    sql """CREATE TABLE IF NOT EXISTS ${test_map_tb} (
            `k1` int(11) NOT NULL,
            `m1` map<int, char(5)> NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1",
                "bloom_filter_columns" = "k1"
        )"""
    test {
        sql """ALTER TABLE ${test_map_tb} SET("bloom_filter_columns" = "k1,m1")"""
        exception "not supported in bloom filter index"
    }

    // bloom filter index for json column
    def test_json_tb = "test_json_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_json_tb}"""

    test {
        sql """CREATE TABLE IF NOT EXISTS ${test_json_tb} (
                `k1` int(11) NOT NULL,
                `j1` json NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 5
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "k1,j1"
            )"""
        exception "not supported in bloom filter index"
    }

    sql """CREATE TABLE IF NOT EXISTS ${test_json_tb} (
            `k1` int(11) NOT NULL,
            `j1` json NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1",
                "bloom_filter_columns" = "k1"
        )"""
    test {
        sql """ALTER TABLE ${test_json_tb} SET("bloom_filter_columns" = "k1,j1")"""
        exception "not supported in bloom filter index"
    }

    // bloom filter index for datetime/date/decimal columns
    def test_datetime_tb = "test_datetime_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_datetime_tb}"""
    sql """ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'false')"""
    sql """ADMIN SET FRONTEND CONFIG ('disable_datev1' = 'false')"""
    sql """CREATE TABLE IF NOT EXISTS ${test_datetime_tb} (
            a int,
            b int,
            c int,
            d DATETIMEV1,
            d2 DATETIMEV2,
            da DATEv1,
            dav2 DATEV2,
            dec decimal(10,2),
            dec2 decimalv2(10,2)
        ) ENGINE=OLAP
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 5
        PROPERTIES (
            "replication_num" = "1"
        )"""
    sql """INSERT INTO ${test_datetime_tb} VALUES
        (1,1,1,"2024-12-17 20:00:00", "2024-12-17 20:00:00", "2024-12-17", "2024-12-17", "3.32", "3.32"),
        (1,1,1,"2024-12-17 20:00:00", "2024-12-17 20:00:00", "2024-12-17", "2024-12-17", "3.32", "3.32"),
        (2,2,2,"2024-12-18 20:00:00", "2024-12-18 20:00:00", "2024-12-18", "2024-12-18", "3.33", "3.33"),
        (3,3,3,"2024-12-22 20:00:00", "2024-12-22 20:00:00", "2024-12-22", "2024-12-22", "4.33", "4.33")"""
    sql """ALTER TABLE ${test_datetime_tb} SET ("bloom_filter_columns" = "d,d2,da,dav2,dec,dec2")"""
    wait_for_latest_op_on_table_finish(test_datetime_tb, timeout)
    qt_select_datetime_v1 """SELECT * FROM ${test_datetime_tb} WHERE d IN ("2024-12-17 20:00:00", "2024-12-18 20:00:00") order by a"""
    qt_select_datetime_v2 """SELECT * FROM ${test_datetime_tb} WHERE d2 IN ("2024-12-17 20:00:00", "2024-12-18 20:00:00") order by a"""
    qt_select_date_v1 """SELECT * FROM ${test_datetime_tb} WHERE da IN ("2024-12-17", "2024-12-18") order by a"""
    qt_select_date_v2 """SELECT * FROM ${test_datetime_tb} WHERE dav2 IN ("2024-12-17", "2024-12-18") order by a"""
    sql """ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'true')"""
    sql """ADMIN SET FRONTEND CONFIG ('disable_datev1' = 'true')"""

    def test_dynamic_fpp_tb = "test_dynamic_fpp_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_dynamic_fpp_tb}"""
    sql """CREATE TABLE IF NOT EXISTS ${test_dynamic_fpp_tb} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 5
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "id",
            "bloom_filter_fpp" = "0.03"
    )"""
    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.03"])
        sql """ INSERT INTO ${test_dynamic_fpp_tb} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve') """
    } catch (e) {
        logger.info("catch exception: ${e}")
        assert(false)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create");
    }

    sql """ALTER TABLE ${test_dynamic_fpp_tb} SET("bloom_filter_fpp" = "0.02")"""
    wait_for_latest_op_on_table_finish(test_dynamic_fpp_tb, timeout)

    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.02"])
        sql """INSERT INTO ${test_dynamic_fpp_tb} VALUES
            (6, 'Grace'),
            (7, 'Henry'),
            (8, 'Ivy'),
            (9, 'Jack'),
            (10, 'Kate')"""
    } catch (e) {
        logger.info("catch exception: ${e}")
        assert(false)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create");
    }
}
