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

suite("test_many_multi_join", "nereids_p0") {
    def DBname = "nereids_regression_test_many_multi_join"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"
    
    multi_sql """
        drop table if exists table1;
        drop table if exists table2;
        drop table if exists table3;
        drop table if exists table4;
        drop table if exists table5;
        drop table if exists table6;
        drop table if exists table7;
        drop table if exists table8;
        drop table if exists table9;
        drop table if exists table10;
        drop table if exists table11;
        drop table if exists table12;
        drop table if exists table13;
        drop table if exists table14;
        drop table if exists table15;
        drop table if exists table16;
        drop table if exists table17;
        drop table if exists table18;
        drop table if exists table19;
        drop table if exists table20;
        drop table if exists table21;

        drop table if exists seq;
        CREATE TABLE seq (number INT) ENGINE=OLAP UNIQUE KEY(number)
        DISTRIBUTED BY HASH(number) BUCKETS 1
        PROPERTIES("replication_num" = "1");

        INSERT INTO seq VALUES (1),(2),(3),(4),(5);


        CREATE TABLE IF NOT EXISTS table1 (
                                            id BIGINT NOT NULL,
                                            value1 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table2 (
                                            id BIGINT NOT NULL,
                                            value2 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table3 (
                                            id BIGINT NOT NULL,
                                            value3 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table4 (
            id BIGINT NOT NULL,
            value4 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table5 (
                                            id BIGINT NOT NULL,
                                            value5 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table6 (
                                            id BIGINT NOT NULL,
                                            value6 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table7 (
                                            id BIGINT NOT NULL,
                                            value7 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table8 (
                                            id BIGINT NOT NULL,
                                            value8 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table9 (
                                            id BIGINT NOT NULL,
                                            value9 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table10 (
                                            id BIGINT NOT NULL,
                                            value10 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table11 (
                                            id BIGINT NOT NULL,
                                            value11 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table12 (
                                            id BIGINT NOT NULL,
                                            value12 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table13 (
                                            id BIGINT NOT NULL,
                                            value13 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table14 (
                                            id BIGINT NOT NULL,
                                            value14 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table15 (
                                            id BIGINT NOT NULL,
                                            value15 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table16 (
                                            id BIGINT NOT NULL,
                                            value16 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table17 (
                                            id BIGINT NOT NULL,
                                            value17 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table18 (
                                            id BIGINT NOT NULL,
                                            value18 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table19 (
                                            id BIGINT NOT NULL,
                                            value19 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table20 (
                                            id BIGINT NOT NULL,
                                            value20 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );

        CREATE TABLE IF NOT EXISTS table21 (
                                            id BIGINT NOT NULL,
                                            value21 DECIMAL(20,6),
            related_id BIGINT,
            ts DATETIME
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES (
                        "replication_num" = "1"
                    );


        INSERT INTO table1
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table2
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table3
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table4
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table5
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table6
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table7
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table8
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table9
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table10
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table11
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table12
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table13
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table14
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table15
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table16
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table17
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table18
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table19
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table20
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;

        INSERT INTO table21
        SELECT
            number,
            RAND()*2,
            CAST(RAND()*2 AS BIGINT),
            NOW() - INTERVAL CAST(RAND()*365 AS INT) DAY
        FROM seq WHERE number = 2;


        INSERT INTO table1 (id, value1, related_id, ts) VALUES
        (1, -9.432227, 114, '2025-06-17 17:28:08'),
        (2, -2.109207, 130, '2025-06-05 17:28:08'),
        (3, -4.487591, 118, '2025-06-15 17:28:08'),
        (4, -8.823451, 133, '2025-06-13 17:28:08'),
        (5, 7.741022, 138, '2025-06-08 17:28:08');
    """

    sql """
        SELECT * FROM (SELECT
        t1.id,
        (SELECT sum(ratio) FROM (SELECT
        t2.id,
        (SELECT sum(ratio) FROM (SELECT
        t3.id,
        (SELECT sum(ratio) FROM (SELECT
        t4.id,
        (SELECT sum(ratio) FROM (SELECT
        t5.id,
        (SELECT sum(ratio) FROM (SELECT
        t6.id,
        (SELECT sum(ratio) FROM (SELECT
        t7.id,
        (SELECT sum(ratio) FROM (SELECT
        t8.id,
        (SELECT sum(ratio) FROM (SELECT
        t9.id,
        (SELECT sum(ratio) FROM (SELECT
        t10.id,
        (SELECT sum(ratio) FROM (SELECT
        t11.id,
        (SELECT sum(ratio) FROM (SELECT
        t12.id,
        (SELECT sum(ratio) FROM (SELECT
        t13.id,
        (SELECT sum(ratio) FROM (SELECT
        t14.id,
        (SELECT sum(ratio) FROM (SELECT
        t15.id,
        (SELECT sum(ratio) FROM (SELECT
        t16.id,
        (SELECT sum(ratio) FROM (SELECT
        t17.id,
        (SELECT sum(ratio) FROM (SELECT
        t18.id,
        (SELECT sum(ratio) FROM (SELECT
        t19.id,
        (SELECT sum(ratio) FROM (SELECT
        t20.id,
        (SELECT sum(ratio) FROM (SELECT
        t21.id,
        (SELECT sum(ratio) FROM (SELECT
        t22.id,
        (SELECT sum(ratio) FROM (SELECT
        t23.id,
        (SELECT sum(ratio) FROM (SELECT
        t24.id,
        (SELECT sum(ratio) FROM (SELECT
        t25.id,
        (SELECT sum(ratio) FROM (SELECT
        t26.id,
        (SELECT sum(ratio) FROM (SELECT
        t27.id,
        (SELECT sum(ratio) FROM (SELECT
        t28.id,
        (SELECT sum(ratio) FROM (SELECT
        t29.id,
        (SELECT sum(ratio) FROM (SELECT
        t30.id,
        (SELECT sum(ratio) FROM (SELECT
        t31.id,
        (SELECT sum(ratio) FROM (SELECT
        t32.id,
        (SELECT sum(ratio) FROM (SELECT
        t33.id,
        (SELECT sum(ratio) FROM (SELECT
        t34.id,
        (SELECT sum(ratio) FROM (SELECT
        t35.id,
        (SELECT sum(ratio) FROM (SELECT
        t36.id,
        (SELECT sum(ratio) FROM (SELECT
        t37.id,
        (SELECT sum(ratio) FROM (SELECT
        t38.id,
        (SELECT sum(ratio) FROM (SELECT
        t39.id,
        (SELECT sum(ratio) FROM (SELECT
        t40.id,
        (SELECT sum(ratio) FROM (SELECT
        t41.id,
        (SELECT sum(ratio) FROM (SELECT
        t42.id,
        (SELECT sum(ratio) FROM (SELECT
        t43.id,
        (SELECT sum(ratio) FROM (SELECT
        t44.id,
        (SELECT sum(ratio) FROM (SELECT
        t45.id,
        (SELECT sum(ratio) FROM (SELECT
        t46.id,
        (SELECT sum(ratio) FROM (SELECT
        t47.id,
        (SELECT sum(ratio) FROM (SELECT
        t48.id,
        (SELECT sum(ratio) FROM (SELECT
        t49.id,
        (SELECT total FROM (SELECT
        SUM(t50.value11) AS total
        FROM table11 t50
        WHERE t50.related_id = t49.id
        ) AS layer50) * 1.0 / COUNT(*) AS ratio
        FROM table10 t49
        WHERE EXISTS (
        SELECT 1 FROM table11
        WHERE id = t49.related_id
        )
        GROUP BY t49.id) AS layer49) * 1.0 / COUNT(*) AS ratio
        FROM table9 t48
        WHERE EXISTS (
        SELECT 1 FROM table10
        WHERE id = t48.related_id
        )
        GROUP BY t48.id) AS layer48) * 1.0 / COUNT(*) AS ratio
        FROM table8 t47
        WHERE EXISTS (
        SELECT 1 FROM table9
        WHERE id = t47.related_id
        )
        GROUP BY t47.id) AS layer47) * 1.0 / COUNT(*) AS ratio
        FROM table7 t46
        WHERE EXISTS (
        SELECT 1 FROM table8
        WHERE id = t46.related_id
        )
        GROUP BY t46.id) AS layer46) * 1.0 / COUNT(*) AS ratio
        FROM table6 t45
        WHERE EXISTS (
        SELECT 1 FROM table7
        WHERE id = t45.related_id
        )
        GROUP BY t45.id) AS layer45) * 1.0 / COUNT(*) AS ratio
        FROM table5 t44
        WHERE EXISTS (
        SELECT 1 FROM table6
        WHERE id = t44.related_id
        )
        GROUP BY t44.id) AS layer44) * 1.0 / COUNT(*) AS ratio
        FROM table4 t43
        WHERE EXISTS (
        SELECT 1 FROM table5
        WHERE id = t43.related_id
        )
        GROUP BY t43.id) AS layer43) * 1.0 / COUNT(*) AS ratio
        FROM table3 t42
        WHERE EXISTS (
        SELECT 1 FROM table4
        WHERE id = t42.related_id
        )
        GROUP BY t42.id) AS layer42) * 1.0 / COUNT(*) AS ratio
        FROM table2 t41
        WHERE EXISTS (
        SELECT 1 FROM table3
        WHERE id = t41.related_id
        )
        GROUP BY t41.id) AS layer41) * 1.0 / COUNT(*) AS ratio
        FROM table1 t40
        WHERE EXISTS (
        SELECT 1 FROM table2
        WHERE id = t40.related_id
        )
        GROUP BY t40.id) AS layer40) * 1.0 / COUNT(*) AS ratio
        FROM table20 t39
        WHERE EXISTS (
        SELECT 1 FROM table1
        WHERE id = t39.related_id
        )
        GROUP BY t39.id) AS layer39) * 1.0 / COUNT(*) AS ratio
        FROM table19 t38
        WHERE EXISTS (
        SELECT 1 FROM table20
        WHERE id = t38.related_id
        )
        GROUP BY t38.id) AS layer38) * 1.0 / COUNT(*) AS ratio
        FROM table18 t37
        WHERE EXISTS (
        SELECT 1 FROM table19
        WHERE id = t37.related_id
        )
        GROUP BY t37.id) AS layer37) * 1.0 / COUNT(*) AS ratio
        FROM table17 t36
        WHERE EXISTS (
        SELECT 1 FROM table18
        WHERE id = t36.related_id
        )
        GROUP BY t36.id) AS layer36) * 1.0 / COUNT(*) AS ratio
        FROM table16 t35
        WHERE EXISTS (
        SELECT 1 FROM table17
        WHERE id = t35.related_id
        )
        GROUP BY t35.id) AS layer35) * 1.0 / COUNT(*) AS ratio
        FROM table15 t34
        WHERE EXISTS (
        SELECT 1 FROM table16
        WHERE id = t34.related_id
        )
        GROUP BY t34.id) AS layer34) * 1.0 / COUNT(*) AS ratio
        FROM table14 t33
        WHERE EXISTS (
        SELECT 1 FROM table15
        WHERE id = t33.related_id
        )
        GROUP BY t33.id) AS layer33) * 1.0 / COUNT(*) AS ratio
        FROM table13 t32
        WHERE EXISTS (
        SELECT 1 FROM table14
        WHERE id = t32.related_id
        )
        GROUP BY t32.id) AS layer32) * 1.0 / COUNT(*) AS ratio
        FROM table12 t31
        WHERE EXISTS (
        SELECT 1 FROM table13
        WHERE id = t31.related_id
        )
        GROUP BY t31.id) AS layer31) * 1.0 / COUNT(*) AS ratio
        FROM table11 t30
        WHERE EXISTS (
        SELECT 1 FROM table12
        WHERE id = t30.related_id
        )
        GROUP BY t30.id) AS layer30) * 1.0 / COUNT(*) AS ratio
        FROM table10 t29
        WHERE EXISTS (
        SELECT 1 FROM table11
        WHERE id = t29.related_id
        )
        GROUP BY t29.id) AS layer29) * 1.0 / COUNT(*) AS ratio
        FROM table9 t28
        WHERE EXISTS (
        SELECT 1 FROM table10
        WHERE id = t28.related_id
        )
        GROUP BY t28.id) AS layer28) * 1.0 / COUNT(*) AS ratio
        FROM table8 t27
        WHERE EXISTS (
        SELECT 1 FROM table9
        WHERE id = t27.related_id
        )
        GROUP BY t27.id) AS layer27) * 1.0 / COUNT(*) AS ratio
        FROM table7 t26
        WHERE EXISTS (
        SELECT 1 FROM table8
        WHERE id = t26.related_id
        )
        GROUP BY t26.id) AS layer26) * 1.0 / COUNT(*) AS ratio
        FROM table6 t25
        WHERE EXISTS (
        SELECT 1 FROM table7
        WHERE id = t25.related_id
        )
        GROUP BY t25.id) AS layer25) * 1.0 / COUNT(*) AS ratio
        FROM table5 t24
        WHERE EXISTS (
        SELECT 1 FROM table6
        WHERE id = t24.related_id
        )
        GROUP BY t24.id) AS layer24) * 1.0 / COUNT(*) AS ratio
        FROM table4 t23
        WHERE EXISTS (
        SELECT 1 FROM table5
        WHERE id = t23.related_id
        )
        GROUP BY t23.id) AS layer23) * 1.0 / COUNT(*) AS ratio
        FROM table3 t22
        WHERE EXISTS (
        SELECT 1 FROM table4
        WHERE id = t22.related_id
        )
        GROUP BY t22.id) AS layer22) * 1.0 / COUNT(*) AS ratio
        FROM table2 t21
        WHERE EXISTS (
        SELECT 1 FROM table3
        WHERE id = t21.related_id
        )
        GROUP BY t21.id) AS layer21) * 1.0 / COUNT(*) AS ratio
        FROM table1 t20
        WHERE EXISTS (
        SELECT 1 FROM table2
        WHERE id = t20.related_id
        )
        GROUP BY t20.id) AS layer20) * 1.0 / COUNT(*) AS ratio
        FROM table20 t19
        WHERE EXISTS (
        SELECT 1 FROM table1
        WHERE id = t19.related_id
        )
        GROUP BY t19.id) AS layer19) * 1.0 / COUNT(*) AS ratio
        FROM table19 t18
        WHERE EXISTS (
        SELECT 1 FROM table20
        WHERE id = t18.related_id
        )
        GROUP BY t18.id) AS layer18) * 1.0 / COUNT(*) AS ratio
        FROM table18 t17
        WHERE EXISTS (
        SELECT 1 FROM table19
        WHERE id = t17.related_id
        )
        GROUP BY t17.id) AS layer17) * 1.0 / COUNT(*) AS ratio
        FROM table17 t16
        WHERE EXISTS (
        SELECT 1 FROM table18
        WHERE id = t16.related_id
        )
        GROUP BY t16.id) AS layer16) * 1.0 / COUNT(*) AS ratio
        FROM table16 t15
        WHERE EXISTS (
        SELECT 1 FROM table17
        WHERE id = t15.related_id
        )
        GROUP BY t15.id) AS layer15) * 1.0 / COUNT(*) AS ratio
        FROM table15 t14
        WHERE EXISTS (
        SELECT 1 FROM table16
        WHERE id = t14.related_id
        )
        GROUP BY t14.id) AS layer14) * 1.0 / COUNT(*) AS ratio
        FROM table14 t13
        WHERE EXISTS (
        SELECT 1 FROM table15
        WHERE id = t13.related_id
        )
        GROUP BY t13.id) AS layer13) * 1.0 / COUNT(*) AS ratio
        FROM table13 t12
        WHERE EXISTS (
        SELECT 1 FROM table14
        WHERE id = t12.related_id
        )
        GROUP BY t12.id) AS layer12) * 1.0 / COUNT(*) AS ratio
        FROM table12 t11
        WHERE EXISTS (
        SELECT 1 FROM table13
        WHERE id = t11.related_id
        )
        GROUP BY t11.id) AS layer11) * 1.0 / COUNT(*) AS ratio
        FROM table11 t10
        WHERE EXISTS (
        SELECT 1 FROM table12
        WHERE id = t10.related_id
        )
        GROUP BY t10.id) AS layer10) * 1.0 / COUNT(*) AS ratio
        FROM table10 t9
        WHERE EXISTS (
        SELECT 1 FROM table11
        WHERE id = t9.related_id
        )
        GROUP BY t9.id) AS layer9) * 1.0 / COUNT(*) AS ratio
        FROM table9 t8
        WHERE EXISTS (
        SELECT 1 FROM table10
        WHERE id = t8.related_id
        )
        GROUP BY t8.id) AS layer8) * 1.0 / COUNT(*) AS ratio
        FROM table8 t7
        WHERE EXISTS (
        SELECT 1 FROM table9
        WHERE id = t7.related_id
        )
        GROUP BY t7.id) AS layer7) * 1.0 / COUNT(*) AS ratio
        FROM table7 t6
        WHERE EXISTS (
        SELECT 1 FROM table8
        WHERE id = t6.related_id
        )
        GROUP BY t6.id) AS layer6) * 1.0 / COUNT(*) AS ratio
        FROM table6 t5
        WHERE EXISTS (
        SELECT 1 FROM table7
        WHERE id = t5.related_id
        )
        GROUP BY t5.id) AS layer5) * 1.0 / COUNT(*) AS ratio
        FROM table5 t4
        WHERE EXISTS (
        SELECT 1 FROM table6
        WHERE id = t4.related_id
        )
        GROUP BY t4.id) AS layer4) * 1.0 / COUNT(*) AS ratio
        FROM table4 t3
        WHERE EXISTS (
        SELECT 1 FROM table5
        WHERE id = t3.related_id
        )
        GROUP BY t3.id) AS layer3) * 1.0 / COUNT(*) AS ratio
        FROM table3 t2
        WHERE EXISTS (
        SELECT 1 FROM table4
        WHERE id = t2.related_id
        )
        GROUP BY t2.id) AS layer2) * 1.0 / COUNT(*) AS ratio
        FROM table2 t1
        WHERE EXISTS (
        SELECT 1 FROM table3
        WHERE id = t1.related_id
        )
        GROUP BY t1.id) AS layer1;
    """
}

