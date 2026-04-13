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

suite("test_cte_subquery_many") {
    multi_sql """
        drop database if exists test_cte_subquery_many_db;
        create database test_cte_subquery_many_db;
        use test_cte_subquery_many_db;

        CREATE TABLE IF NOT EXISTS tbl_1 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_2 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_3 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_4 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_5 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_6 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_7 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_8 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_9 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_10 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_11 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_12 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_13 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_14 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_15 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_16 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_17 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_18 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_19 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        CREATE TABLE IF NOT EXISTS tbl_20 (
            id BIGINT,
            col1 VARCHAR(255),
            col2 INT,
            col3 DECIMAL(10,2),
            col4 DATE,
            col5 BOOLEAN,
            related_id BIGINT,
            join_key VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");


        INSERT INTO tbl_1 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_2 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_3 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_4 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_5 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_6 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_7 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_8 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_9 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_10 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_11 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_12 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_13 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_14 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_15 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_16 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_17 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_18 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_19 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');

        INSERT INTO tbl_20 VALUES (1, 'data1-1', 10, 15.0, '2023-01-01', TRUE, 1, 'key1');
    """
    qt_sql """
        WITH cte AS (
        SELECT 
            t0.id,
            SUM(t0.col2) AS agg_value,
            CASE 
            WHEN SUM(t0.col2) > 0 THEN 'BASE'
            WHEN SUM(t0.col2) BETWEEN 0 AND 0 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_1 t0
        INNER JOIN tbl_6 j0 
            ON t0.join_key = j0.join_key
        WHERE EXISTS (SELECT sq0.id 
        FROM (SELECT 
            t1.id,
            SUM(t1.col2) AS agg_value,
            CASE 
            WHEN SUM(t1.col2) > 100 THEN 'BASE'
            WHEN SUM(t1.col2) BETWEEN 50 AND 100 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_2 t1
        LEFT JOIN tbl_7 j1 
            ON t1.join_key = j1.join_key
        WHERE NOT EXISTS (SELECT sq1.id 
        FROM (SELECT 
            t2.id,
            SUM(t2.col2) AS agg_value,
            CASE 
            WHEN SUM(t2.col2) > 200 THEN 'BASE'
            WHEN SUM(t2.col2) BETWEEN 100 AND 200 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_3 t2
        RIGHT JOIN tbl_8 j2 
            ON t2.join_key = j2.join_key
        WHERE t2.id IN (SELECT sq2.id 
        FROM (SELECT 
            t3.id,
            SUM(t3.col2) AS agg_value,
            CASE 
            WHEN SUM(t3.col2) > 300 THEN 'BASE'
            WHEN SUM(t3.col2) BETWEEN 150 AND 300 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_4 t3
        INNER JOIN tbl_9 j3 
            ON t3.join_key = j3.join_key
        WHERE t3.id NOT IN (SELECT sq3.id 
        FROM (SELECT 
            t4.id,
            SUM(t4.col2) AS agg_value,
            CASE 
            WHEN SUM(t4.col2) > 400 THEN 'BASE'
            WHEN SUM(t4.col2) BETWEEN 200 AND 400 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_5 t4
        LEFT JOIN tbl_10 j4 
            ON t4.join_key = j4.join_key
        WHERE EXISTS (SELECT sq4.id 
        FROM (SELECT 
            t5.id,
            SUM(t5.col2) AS agg_value,
            CASE 
            WHEN SUM(t5.col2) > 500 THEN 'BASE'
            WHEN SUM(t5.col2) BETWEEN 250 AND 500 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_6 t5
        RIGHT JOIN tbl_11 j5 
            ON t5.join_key = j5.join_key
        WHERE NOT EXISTS (SELECT sq5.id 
        FROM (SELECT 
            t6.id,
            SUM(t6.col2) AS agg_value,
            CASE 
            WHEN SUM(t6.col2) > 600 THEN 'BASE'
            WHEN SUM(t6.col2) BETWEEN 300 AND 600 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_7 t6
        INNER JOIN tbl_12 j6 
            ON t6.join_key = j6.join_key
        WHERE t6.id IN (SELECT sq6.id 
        FROM (SELECT 
            t7.id,
            SUM(t7.col2) AS agg_value,
            CASE 
            WHEN SUM(t7.col2) > 700 THEN 'BASE'
            WHEN SUM(t7.col2) BETWEEN 350 AND 700 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_8 t7
        LEFT JOIN tbl_13 j7 
            ON t7.join_key = j7.join_key
        WHERE t7.id NOT IN (SELECT sq7.id 
        FROM (SELECT 
            t8.id,
            SUM(t8.col2) AS agg_value,
            CASE 
            WHEN SUM(t8.col2) > 800 THEN 'BASE'
            WHEN SUM(t8.col2) BETWEEN 400 AND 800 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_9 t8
        RIGHT JOIN tbl_14 j8 
            ON t8.join_key = j8.join_key
        WHERE EXISTS (SELECT sq8.id 
        FROM (SELECT 
            t9.id,
            SUM(t9.col2) AS agg_value,
            CASE 
            WHEN SUM(t9.col2) > 900 THEN 'BASE'
            WHEN SUM(t9.col2) BETWEEN 450 AND 900 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_10 t9
        INNER JOIN tbl_15 j9 
            ON t9.join_key = j9.join_key
        WHERE NOT EXISTS (SELECT sq9.id 
        FROM (SELECT 
            t10.id,
            SUM(t10.col2) AS agg_value,
            CASE 
            WHEN SUM(t10.col2) > 1000 THEN 'BASE'
            WHEN SUM(t10.col2) BETWEEN 500 AND 1000 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_11 t10
        LEFT JOIN tbl_16 j10 
            ON t10.join_key = j10.join_key
        WHERE t10.id IN (SELECT sq10.id 
        FROM (SELECT 
            t11.id,
            SUM(t11.col2) AS agg_value,
            CASE 
            WHEN SUM(t11.col2) > 1100 THEN 'BASE'
            WHEN SUM(t11.col2) BETWEEN 550 AND 1100 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_12 t11
        RIGHT JOIN tbl_17 j11 
            ON t11.join_key = j11.join_key
        WHERE t11.id NOT IN (SELECT sq11.id 
        FROM (SELECT 
            t12.id,
            SUM(t12.col2) AS agg_value,
            CASE 
            WHEN SUM(t12.col2) > 1200 THEN 'BASE'
            WHEN SUM(t12.col2) BETWEEN 600 AND 1200 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_13 t12
        INNER JOIN tbl_18 j12 
            ON t12.join_key = j12.join_key
        WHERE EXISTS (SELECT sq12.id 
        FROM (SELECT 
            t13.id,
            SUM(t13.col2) AS agg_value,
            CASE 
            WHEN SUM(t13.col2) > 1300 THEN 'BASE'
            WHEN SUM(t13.col2) BETWEEN 650 AND 1300 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_14 t13
        LEFT JOIN tbl_19 j13 
            ON t13.join_key = j13.join_key
        WHERE NOT EXISTS (SELECT sq13.id 
        FROM (SELECT 
            t14.id,
            SUM(t14.col2) AS agg_value,
            CASE 
            WHEN SUM(t14.col2) > 1400 THEN 'BASE'
            WHEN SUM(t14.col2) BETWEEN 700 AND 1400 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_15 t14
        RIGHT JOIN tbl_20 j14 
            ON t14.join_key = j14.join_key
        WHERE t14.id IN (SELECT sq14.id 
        FROM (SELECT 
            t15.id,
            SUM(t15.col2) AS agg_value,
            CASE 
            WHEN SUM(t15.col2) > 1500 THEN 'BASE'
            WHEN SUM(t15.col2) BETWEEN 750 AND 1500 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_16 t15
        INNER JOIN tbl_1 j15 
            ON t15.join_key = j15.join_key
        WHERE t15.id NOT IN (SELECT sq15.id 
        FROM (SELECT 
            t16.id,
            SUM(t16.col2) AS agg_value,
            CASE 
            WHEN SUM(t16.col2) > 1600 THEN 'BASE'
            WHEN SUM(t16.col2) BETWEEN 800 AND 1600 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_17 t16
        LEFT JOIN tbl_2 j16 
            ON t16.join_key = j16.join_key
        WHERE EXISTS (SELECT sq16.id 
        FROM (SELECT 
            t17.id,
            SUM(t17.col2) AS agg_value,
            CASE 
            WHEN SUM(t17.col2) > 1700 THEN 'BASE'
            WHEN SUM(t17.col2) BETWEEN 850 AND 1700 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_18 t17
        RIGHT JOIN tbl_3 j17 
            ON t17.join_key = j17.join_key
        WHERE NOT EXISTS (SELECT sq17.id 
        FROM (SELECT 
            t18.id,
            SUM(t18.col2) AS agg_value,
            CASE 
            WHEN SUM(t18.col2) > 1800 THEN 'BASE'
            WHEN SUM(t18.col2) BETWEEN 900 AND 1800 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_19 t18
        INNER JOIN tbl_4 j18 
            ON t18.join_key = j18.join_key
        WHERE t18.id IN (SELECT sq18.id 
        FROM (SELECT 
            t19.id,
            SUM(t19.col2) AS agg_value,
            CASE 
            WHEN SUM(t19.col2) > 1900 THEN 'BASE'
            WHEN SUM(t19.col2) BETWEEN 950 AND 1900 THEN 'BASE'
            ELSE 'BASE'
        END AS case_label
        FROM tbl_20 t19
        LEFT JOIN tbl_5 j19 
            ON t19.join_key = j19.join_key
        WHERE t19.id NOT IN (SELECT sq19.id 
        FROM (SELECT 
            id, 
            col2 AS agg_value,
            'BASE' AS case_label
        FROM tbl_1
        WHERE col3 > 10 
        LIMIT 1) sq19
        WHERE sq19.agg_value = j19.col2
            AND sq19.case_label LIKE '%BASE%'
            AND j19.col2 BETWEEN 100 AND 1000)
            AND t19.col5 = true
            AND j19.col1 LIKE '%data%'
            AND t19.col3 IN (SELECT col3 FROM tbl_3 WHERE id > 0)
        GROUP BY t19.id) sq18
        WHERE sq18.agg_value = j18.col2
            AND sq18.case_label LIKE '%BASE%'
            AND j18.col2 BETWEEN 10 AND 100)
            AND t18.col5 = true
            AND j18.col1 LIKE '%data%'
            AND t18.col3 IN (SELECT col3 FROM tbl_2 WHERE id > 0)
        GROUP BY t18.id) sq17
        WHERE sq17.agg_value = j17.col2
            AND sq17.case_label LIKE '%BASE%'
            AND j17.col2 BETWEEN 100 AND 1000)
            AND t17.col5 = true
            AND j17.col1 LIKE '%data%'
            AND t17.col3 IN (SELECT col3 FROM tbl_1 WHERE id > 0)
        GROUP BY t17.id) sq16
        WHERE sq16.agg_value = j16.col2
            AND sq16.case_label LIKE '%BASE%'
            AND j16.col2 BETWEEN 10 AND 100)
            AND t16.col5 = true
            AND j16.col1 LIKE '%data%'
            AND t16.col3 IN (SELECT col3 FROM tbl_20 WHERE id > 0)
        GROUP BY t16.id) sq15
        WHERE sq15.agg_value = j15.col2
            AND sq15.case_label LIKE '%BASE%'
            AND j15.col2 BETWEEN 100 AND 1000)
            AND t15.col5 = true
            AND j15.col1 LIKE '%data%'
            AND t15.col3 IN (SELECT col3 FROM tbl_19 WHERE id > 0)
        GROUP BY t15.id) sq14
        WHERE sq14.agg_value = j14.col2
            AND sq14.case_label LIKE '%BASE%'
            AND j14.col2 BETWEEN 10 AND 100)
            AND t14.col5 = true
            AND j14.col1 LIKE '%data%'
            AND t14.col3 IN (SELECT col3 FROM tbl_18 WHERE id > 0)
        GROUP BY t14.id) sq13
        WHERE sq13.agg_value = j13.col2
            AND sq13.case_label LIKE '%BASE%'
            AND j13.col2 BETWEEN 100 AND 1000)
            AND t13.col5 = true
            AND j13.col1 LIKE '%data%'
            AND t13.col3 IN (SELECT col3 FROM tbl_17 WHERE id > 0)
        GROUP BY t13.id) sq12
        WHERE sq12.agg_value = j12.col2
            AND sq12.case_label LIKE '%BASE%'
            AND j12.col2 BETWEEN 10 AND 100)
            AND t12.col5 = true
            AND j12.col1 LIKE '%data%'
            AND t12.col3 IN (SELECT col3 FROM tbl_16 WHERE id > 0)
        GROUP BY t12.id) sq11
        WHERE sq11.agg_value = j11.col2
            AND sq11.case_label LIKE '%BASE%'
            AND j11.col2 BETWEEN 100 AND 1000)
            AND t11.col5 = true
            AND j11.col1 LIKE '%data%'
            AND t11.col3 IN (SELECT col3 FROM tbl_15 WHERE id > 0)
        GROUP BY t11.id) sq10
        WHERE sq10.agg_value = j10.col2
            AND sq10.case_label LIKE '%BASE%'
            AND j10.col2 BETWEEN 10 AND 100)
            AND t10.col5 = true
            AND j10.col1 LIKE '%data%'
            AND t10.col3 IN (SELECT col3 FROM tbl_14 WHERE id > 0)
        GROUP BY t10.id) sq9
        WHERE sq9.agg_value = j9.col2
            AND sq9.case_label LIKE '%BASE%'
            AND j9.col2 BETWEEN 100 AND 1000)
            AND t9.col5 = true
            AND j9.col1 LIKE '%data%'
            AND t9.col3 IN (SELECT col3 FROM tbl_13 WHERE id > 0)
        GROUP BY t9.id) sq8
        WHERE sq8.agg_value = j8.col2
            AND sq8.case_label LIKE '%BASE%'
            AND j8.col2 BETWEEN 10 AND 100)
            AND t8.col5 = true
            AND j8.col1 LIKE '%data%'
            AND t8.col3 IN (SELECT col3 FROM tbl_12 WHERE id > 0)
        GROUP BY t8.id) sq7
        WHERE sq7.agg_value = j7.col2
            AND sq7.case_label LIKE '%BASE%'
            AND j7.col2 BETWEEN 100 AND 1000)
            AND t7.col5 = true
            AND j7.col1 LIKE '%data%'
            AND t7.col3 IN (SELECT col3 FROM tbl_11 WHERE id > 0)
        GROUP BY t7.id) sq6
        WHERE sq6.agg_value = j6.col2
            AND sq6.case_label LIKE '%BASE%'
            AND j6.col2 BETWEEN 10 AND 100)
            AND t6.col5 = true
            AND j6.col1 LIKE '%data%'
            AND t6.col3 IN (SELECT col3 FROM tbl_10 WHERE id > 0)
        GROUP BY t6.id) sq5
        WHERE sq5.agg_value = j5.col2
            AND sq5.case_label LIKE '%BASE%'
            AND j5.col2 BETWEEN 100 AND 1000)
            AND t5.col5 = true
            AND j5.col1 LIKE '%data%'
            AND t5.col3 IN (SELECT col3 FROM tbl_9 WHERE id > 0)
        GROUP BY t5.id) sq4
        WHERE sq4.agg_value = j4.col2
            AND sq4.case_label LIKE '%BASE%'
            AND j4.col2 BETWEEN 10 AND 100)
            AND t4.col5 = true
            AND j4.col1 LIKE '%data%'
            AND t4.col3 IN (SELECT col3 FROM tbl_8 WHERE id > 0)
        GROUP BY t4.id) sq3
        WHERE sq3.agg_value = j3.col2
            AND sq3.case_label LIKE '%BASE%'
            AND j3.col2 BETWEEN 100 AND 1000)
            AND t3.col5 = true
            AND j3.col1 LIKE '%data%'
            AND t3.col3 IN (SELECT col3 FROM tbl_7 WHERE id > 0)
        GROUP BY t3.id) sq2
        WHERE sq2.agg_value = j2.col2
            AND sq2.case_label LIKE '%BASE%'
            AND j2.col2 BETWEEN 10 AND 100)
            AND t2.col5 = true
            AND j2.col1 LIKE '%data%'
            AND t2.col3 IN (SELECT col3 FROM tbl_6 WHERE id > 0)
        GROUP BY t2.id) sq1
        WHERE sq1.agg_value = j1.col2
            AND sq1.case_label LIKE '%BASE%'
            AND j1.col2 BETWEEN 100 AND 1000)
            AND t1.col5 = true
            AND j1.col1 LIKE '%data%'
            AND t1.col3 IN (SELECT col3 FROM tbl_5 WHERE id > 0)
        GROUP BY t1.id) sq0
        WHERE sq0.agg_value = j0.col2
            AND sq0.case_label LIKE '%BASE%'
            AND j0.col2 BETWEEN 10 AND 100)
            AND t0.col5 = true
            AND j0.col1 LIKE '%data%'
            AND t0.col3 IN (SELECT col3 FROM tbl_4 WHERE id > 0)
        GROUP BY t0.id
        )
        SELECT * FROM cte;
    """
}
