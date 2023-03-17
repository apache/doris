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

suite("all_types_test", "alltypes") {

    def tableNameAgg = "agg_test"

    sql "DROP TABLE IF EXISTS ${tableNameAgg}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableNameAgg} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR,
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
                k10 DECIMAL,
                k11 BOOLEAN,
                k12 DATEV2,
                k13 DATETIMEV2,
                k14 DECIMALV3,
                k15 JSONB REPLACE,
                k16 STRING REPLACE,
                k17 FLOAT MAX,
                k18 DOUBLE MAX,
                k19 HLL HLL_UNION,
                k20 BITMAP BITMAP_UNION,
                k21 QUANTILE_STATE QUANTILE_UNION
            )
            AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
    """
    test {
        sql """
        insert into ${tableNameAgg} values
            (1, 10, 100, 100, 'a', 'b', '1999-01-08', '1999-01-08 02:05:06', 10000, 1, 0, '1999-01-08', '1999-01-08 02:05:06', 1, '"a"', 'text1', 1.1, 1.23, hll_hash(11), to_bitmap(11), to_quantile_state(11)),
            (2, 11, 101, 101, 'b', 'c', '1952-01-05', '1989-01-08 04:05:06', 10001, 2, 1, '1952-01-05', '1989-01-08 04:05:06', 2, '[123, 456]', 'text2', 1.2, 2.23, hll_hash(12), to_quantile_state(12)),
            (3, 12, 102, 102, 'c', 'd', '1936-02-08', '2005-01-09 04:05:06', 10002, 3, 0, '1936-02-08', '2005-01-09 04:05:06', 3, '{"k1":"v31", "k2": 300}', 'text3', 3.3, 3.23, hll_hash(13), to_quantile_state(13)),
            (4, 13, 103, 103, 'q', 'h', '1949-07-08', '2010-01-02 04:03:06', 10003, 4, 1, '1949-07-08', '2010-01-02 04:03:06', 4, '100', 'text4', 4.3, 4.23, hll_hash(14), to_quantile_state(14)),
            (5, 14, 104, 104, 'w', 'j', '1987-04-09', '2010-01-02 05:09:06', 10004, 5, 0, '1987-04-09', '2010-01-02 05:09:06', 5, 'false', 'text5', 5.3, 5.23, hll_hash(15), to_quantile_state(15)),
            (6, 15, 105, 105, 'e', 'k', '1923-04-08', '2010-01-02 02:05:06', 10005, 6, 1, '1923-04-08', '2010-01-02 02:05:06', 6, 'true', 'text6', 6.3, 6.23, hll_hash(16), to_quantile_state(16))
    """
        exception "errCode = 2, detailMessage = to_quantile_statefunction must have two children"
    }

    
    sql """
        select * from ${tableNameAgg};
    """

    sql "DROP TABLE ${tableNameAgg}"

    test {
        def tableNameUni = "uni_test"

        sql "DROP TABLE IF EXISTS ${tableNameUni}"

        sql """
        CREATE TABLE IF NOT EXISTS ${tableNameUni} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR,
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
                k10 DECIMAL,
                k11 BOOLEAN,
                k12 DATEV2,
                k13 DATETIMEV2,
                k14 DECIMALV3,
                k15 JSONB,
                k16 STRING,
                k17 FLOAT,
                k18 DOUBLE,
                k19 ARRAY<TINYINT>
            )
            UNIQUE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
        exception "ARRAY column can't support aggregation REPLACE"

        sql """
        insert into ${tableNameUni} values
            (1, 10, 100, 100, 'a', 'b', '1999-01-08', '1999-01-08 02:05:06', 10000, 1, 0, '1999-01-08', '1999-01-08 02:05:06', 1, '"a"', 'text1', 1.1, 1.23, [1, 2]),
            (2, 11, 101, 101, 'b', 'c', '1952-01-05', '1989-01-08 04:05:06', 10001, 2, 1, '1952-01-05', '1989-01-08 04:05:06', 2, '[123, 456]', 'text2', 1.2, 2.23, [2, 3]),
            (3, 12, 102, 102, 'c', 'd', '1936-02-08', '2005-01-09 04:05:06', 10002, 3, 0, '1936-02-08', '2005-01-09 04:05:06', 3, '{"k1":"v31", "k2": 300}', 'text3', 3.3, 3.23, [3, 4]),
            (4, 13, 103, 103, 'q', 'h', '1949-07-08', '2010-01-02 04:03:06', 10003, 4, 1, '1949-07-08', '2010-01-02 04:03:06', 4, '100', 'text4', 4.3, 4.23, [4, 5]),
            (5, 14, 104, 104, 'w', 'j', '1987-04-09', '2010-01-02 05:09:06', 10004, 5, 0, '1987-04-09', '2010-01-02 05:09:06', 5, 'false', 'text5', 5.3, 5.23, [5, 6]),
            (6, 15, 105, 105, 'e', 'k', '1923-04-08', '2010-01-02 02:05:06', 10005, 6, 1, '1923-04-08', '2010-01-02 02:05:06', 6, 'true', 'text6', 6.3, 6.23, [6, 7])
        """

        sql """
        select * from ${tableNameUni};
        """

        sql "DROP TABLE IF EXISTS ${tableNameUni}"
    }

    
    

    def tableNameDup = "dup_test"

    sql "DROP TABLE IF EXISTS ${tableNameDup}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableNameDup} (
            k1 TINYINT,
            k2 SMALLINT,
            k3 INT,
            k4 BIGINT,
            k5 CHAR,
            k6 VARCHAR,
            k7 DATE,
            k8 DATETIME,
            k9 LARGEINT,
            k10 DECIMAL,
            k11 BOOLEAN,
            k12 DATEV2,
            k13 DATETIMEV2,
            k14 DECIMALV3,
            k15 JSONB,
            k16 STRING,
            k17 FLOAT,
            k18 DOUBLE,
            k19 ARRAY<TINYINT>
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
    """

    sql """
        insert into ${tableNameDup} values
            (1, 10, 100, 100, 'a', 'b', '1999-01-08', '1999-01-08 02:05:06', 10000, 1, 0, '1999-01-08', '1999-01-08 02:05:06', 1, '"a"', 'text1', 1.1, 1.23, [1, 2]),
            (2, 11, 101, 101, 'b', 'c', '1952-01-05', '1989-01-08 04:05:06', 10001, 2, 1, '1952-01-05', '1989-01-08 04:05:06', 2, '[123, 456]', 'text2', 1.2, 2.23, [2, 3]),
            (3, 12, 102, 102, 'c', 'd', '1936-02-08', '2005-01-09 04:05:06', 10002, 3, 0, '1936-02-08', '2005-01-09 04:05:06', 3, '{"k1":"v31", "k2": 300}', 'text3', 3.3, 3.23, [3, 4]),
            (4, 13, 103, 103, 'q', 'h', '1949-07-08', '2010-01-02 04:03:06', 10003, 4, 1, '1949-07-08', '2010-01-02 04:03:06', 4, '100', 'text4', 4.3, 4.23, [4, 5]),
            (5, 14, 104, 104, 'w', 'j', '1987-04-09', '2010-01-02 05:09:06', 10004, 5, 0, '1987-04-09', '2010-01-02 05:09:06', 5, 'false', 'text5', 5.3, 5.23, [5, 6]),
            (6, 15, 105, 105, 'e', 'k', '1923-04-08', '2010-01-02 02:05:06', 10005, 6, 1, '1923-04-08', '2010-01-02 02:05:06', 6, 'true', 'text6', 6.3, 6.23, [6, 7])
    """
    sql """
        select * from ${tableNameDup};
    """

    sql "DROP TABLE ${tableNameDup}"

}