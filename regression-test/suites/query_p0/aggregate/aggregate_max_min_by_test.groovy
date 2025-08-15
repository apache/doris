/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("aggregate_max_min_by_test") {

    sql """ set ENABLE_DECIMAL256 = true; """
    sql """DROP TABLE IF EXISTS aggregate_max_min_by_test;"""
    sql"""
    CREATE TABLE IF NOT EXISTS aggregate_max_min_by_test (
    `k0` INT,
    `k1` BOOLEAN,
    `k2` TINYINT,
    `k3` SMALLINT,
    `k4` INT,
    `k5` BIGINT,
    `k6` LARGEINT,
    `k7` FLOAT,
    `k8` DOUBLE,
    `k9` DECIMAL(8,3),
    `k10` DECIMAL(16,3),
    `k11` DECIMAL(32,3),
    `k12` DECIMAL(64,3),
    `k13` DATE,
    `k14` DATETIME,
    `k15` STRING,
    ) ENGINE=OLAP
    DUPLICATE KEY(`k0`)
    DISTRIBUTED BY HASH(`k0`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
    );
    """

    sql """
INSERT INTO aggregate_max_min_by_test
(k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k14, k15)
VALUES
(8,   TRUE,   80,   8008,   80008,   800080008,   8000800080008,  8.08,   8.008,   8.008,   8008.008,   8008008.008,   8008008008.008,   '2025-08-08', '2025-08-08 08:08:08', 'text_8'),
(3,   FALSE,  30,   3003,   30003,   300030003,   3000300030003,  3.03,   3.003,   3.003,   3003.003,   3003003.003,   3003003003.003,   '2025-03-03', '2025-03-03 03:03:03', 'text_3'),
(15,  TRUE,   150,  15015,  150015,  1500150015,  15001500150015, 15.15,  15.015,  15.015,  15015.015,  15015015.015,  15015015015.015,  '2025-05-15', '2025-05-15 15:15:15', 'text_15'),
(1,   TRUE,   10,   1001,   10001,   100010001,   1000100010001,  1.01,   1.001,   1.001,   1001.001,   1001001.001,   1001001001.001,   '2025-01-01', '2025-01-01 01:01:01', 'text_1'),
(12,  FALSE,  120,  12012,  120012,  1200120012,  12001200120012, 12.12,  12.012,  12.012,  12012.012,  12012012.012,  12012012012.012,  '2025-12-12', '2025-12-12 12:12:12', 'text_12'),
(5,   TRUE,   50,   5005,   50005,   500050005,   5000500050005,  5.05,   5.005,   5.005,   5005.005,   5005005.005,   5005005005.005,   '2025-05-05', '2025-05-05 05:05:05', 'text_5'),
(10,  FALSE,  100,  10010,  100010,  1000100010,  10001000100010,10.10,  10.010,  10.010,  10010.010,  10010010.010,  10010010010.010,  '2025-10-10', '2025-10-10 10:10:10', 'text_10'),
(7,   TRUE,   70,   7007,   70007,   700070007,   7000700070007,  7.07,   7.007,   7.007,   7007.007,   7007007.007,   7007007007.007,   '2025-07-07', '2025-07-07 07:07:07', 'text_7'),
(2,   FALSE,  20,   2002,   20002,   200020002,   2000200020002,  2.02,   2.002,   2.002,   2002.002,   2002002.002,   2002002002.002,   '2025-02-02', '2025-02-02 02:02:02', 'text_2'),
(14,  TRUE,   140,  14014,  140014,  1400140014,  14001400140014,14.14,  14.014,  14.014,  14014.014,  14014014.014,  14014014014.014,  '2025-06-14', '2025-06-14 14:14:14', 'text_14'),
(9,   FALSE,  90,   9009,   90009,   900090009,   9000900090009,  9.09,   9.009,   9.009,   9009.009,   9009009.009,   9009009009.009,   '2025-09-09', '2025-09-09 09:09:09', 'text_9'),
(6,   TRUE,   60,   6006,   60006,   600060006,   6000600060006,  6.06,   6.006,   6.006,   6006.006,   6006006.006,   6006006006.006,   '2025-06-06', '2025-06-06 06:06:06', 'text_6');
    """

    qt_sql """
    SELECT  max_by(k0,k2), max_by(k0,k3), max_by(k0,k4), max_by(k0,k5), max_by(k0,k6),
       max_by(k0,k7), max_by(k0,k8), max_by(k0,k9), max_by(k0,k10), max_by(k0,k11),
       max_by(k0,k12), max_by(k0,k13), max_by(k0,k14), max_by(k0,k15)
FROM aggregate_max_min_by_test;
    """

    qt_sql """
SELECT  min_by(k0,k2), min_by(k0,k3), min_by(k0,k4), min_by(k0,k5), min_by(k0,k6),
       min_by(k0,k7), min_by(k0,k8), min_by(k0,k9), min_by(k0,k10), min_by(k0,k11),
       min_by(k0,k12), min_by(k0,k13), min_by(k0,k14), min_by(k0,k15)
FROM aggregate_max_min_by_test;
    """


    qt_sql """
SELECT max_by(k2 , k0), max_by(k3 , k0), max_by(k4 , k0), max_by(k5 , k0), max_by(k6 , k0),
       max_by(k7 , k0), max_by(k8 , k0), max_by(k9 , k0), max_by(k10, k0), max_by(k11, k0),
       max_by(k12, k0), max_by(k13, k0), max_by(k14, k0), max_by(k15, k0)
FROM aggregate_max_min_by_test;
    """


    qt_sql """
SELECT min_by(k2 , k0), min_by(k3 , k0), min_by(k4 , k0), min_by(k5 , k0), min_by(k6 , k0),
       min_by(k7 , k0), min_by(k8 , k0), min_by(k9 , k0), min_by(k10, k0), min_by(k11, k0),
       min_by(k12, k0), min_by(k13, k0), min_by(k14, k0), min_by(k15, k0)
FROM aggregate_max_min_by_test;
    """

}