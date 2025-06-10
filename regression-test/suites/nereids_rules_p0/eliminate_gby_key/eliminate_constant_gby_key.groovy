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

suite("eliminate_constant_gby_key") {
    sql """DROP TABLE IF EXISTS eliminate_constant_gby_key_t;"""
    sql """CREATE TABLE eliminate_constant_gby_key_t (
        c1 INT,
        c2 VARCHAR(50),
        c3 DECIMAL(10,2),
        c4 DATETIME,
        c5 BOOLEAN
    ) distributed by hash(c1) properties("replication_num"="1");"""

    sql """INSERT INTO eliminate_constant_gby_key_t (c1, c2, c3, c4, c5) VALUES 
    (1, 'Apple', 10.50, '2023-01-01 10:00:00', true),
    (2, 'Banana', 20.75, '2023-01-02 11:30:00', false),
    (3, 'Cherry', 15.25, '2023-01-03 09:15:00', true),
    (4, 'Date', 30.00, '2023-01-04 14:45:00', false),
    (5, 'Elderberry', 12.99, '2023-01-05 16:20:00', true),
    (0, 'Fig', 5.50, '2023-01-06 08:00:00', false),
    (-1, 'Grape', 8.25, '2023-01-07 12:30:00', true),
    (NULL, 'Honeydew', NULL, NULL, NULL),
    (10, 'Iceberg', 18.40, '2023-01-08 13:10:00', false),
    (100, 'Jackfruit', 42.99, '2023-01-09 17:55:00', true);
    """

    def funAList = [
        "TIMESTAMPDIFF(YEAR, NOW(), NOW())",
        """(TO_DATE(CASE
        WHEN ('2024-01-08' < '2024-02-18') THEN '2023-12-19'
        WHEN (c4 < '2024-01-01') THEN '2026-02-18'
        ELSE DATE_ADD(c4, INTERVAL 365 DAY) END))"""
    ]

    def testCases = [
        [desc: "select funA, c1, funA+c1 group by funA, c1",
         sql: { funA -> """
        SELECT 
            ${funA} AS funA, 
            c1, 
            ${funA} + c1 AS 'funA+c1' 
        FROM eliminate_constant_gby_key_t 
        GROUP BY ${funA}, c1
        ORDER BY 1, 2, 3
        """ }],

        [desc: "select funA, c1, funA+c1 group by funA, c1, funA+c1",
         sql: { funA -> """
        SELECT 
            ${funA} AS funA, 
            c1, 
            ${funA} + c1 AS 'funA+c1' 
        FROM eliminate_constant_gby_key_t 
        GROUP BY ${funA}, c1, ${funA} + c1
        ORDER BY 1, 2, 3
        """ }],

        [desc: "select count(distinct funA), funA, c1 group by funA,c1",
         sql: { funA -> """
        SELECT 
            COUNT(DISTINCT ${funA}) AS 'count(distinct funA)', 
            ${funA} AS funA, 
            c1 
        FROM eliminate_constant_gby_key_t 
        GROUP BY ${funA}, c1
        ORDER BY 1, 2, 3
        """ }],

        [desc: "select count(funA), funA, c1 group by funA, c1",
         sql: { funA -> """
        SELECT 
            COUNT(${funA}) AS 'count(funA)', 
            ${funA} AS funA, 
            c1 
        FROM eliminate_constant_gby_key_t 
        GROUP BY ${funA}, c1
        ORDER BY 1, 2, 3
        """ }],

        [desc: "select COUNT(distinct funA+1), funA, c1 group by funA,c1",
         sql: { funA -> """
        SELECT 
            COUNT(DISTINCT ${funA} + 1) AS 'count(distinct funA+1)', 
            ${funA} AS funA, 
            c1 
        FROM eliminate_constant_gby_key_t 
        GROUP BY ${funA}, c1
        ORDER BY 1, 2, 3
        """ }],

        [desc: "select max(funA+1), funA, c1 group by funA, c1",
         sql: { funA -> """
        SELECT 
            MAX(${funA} + 1) AS 'max(funA+1)', 
            ${funA} AS funA, 
            c1 
        FROM eliminate_constant_gby_key_t 
        GROUP BY ${funA}, c1
        ORDER BY 1, 2, 3
        """ }],

        [desc: "select max(funA+c1), funA, c2 group by funA, c2",
         sql: { funA -> """
        SELECT
            MAX(${funA} + c1) AS 'max(funA+c1)',
            ${funA} AS funA,
            c2
        FROM eliminate_constant_gby_key_t
        GROUP BY ${funA}, c2
        ORDER BY 1, 2, 3
        """ }]
    ]

    def idx = 1
    funAList.each { funA ->
        testCases.each { testCase ->
            quickTest("test_${idx}", testCase.sql(funA))
            idx++
        }
    }

    qt_gby_key_is_constant_expr_not_literal """
    SELECT 
        count(DISTINCT  from_unixtime(1742860744.003242)) AS 'max(distinct funA)', 
        from_unixtime(1742860744.003242) AS funA, 
        c1 
    FROM eliminate_constant_gby_key_t 
    GROUP BY from_unixtime(1742860744.003242), c1
    order by 1,2,3
    """

    qt_test_gby_key_is_all_constant """
    SELECT 
        count(DISTINCT  from_unixtime(1742860744.003242)) AS 'max(distinct funA)', 
        from_unixtime(1742860744.003242) AS funA, 
        (TO_DATE(CASE
        WHEN ('2024-01-08' < '2024-02-18') THEN '2023-12-19'
        WHEN (c4 < '2024-01-01') THEN '2026-02-18'
        ELSE DATE_ADD(c4, INTERVAL 365 DAY) END))
        c1 
    FROM eliminate_constant_gby_key_t 
    GROUP BY from_unixtime(1742860744.003242), (TO_DATE(CASE
        WHEN ('2024-01-08' < '2024-02-18') THEN '2023-12-19'
        WHEN (c4 < '2024-01-01') THEN '2026-02-18'
        ELSE DATE_ADD(c4, INTERVAL 365 DAY) END)), 'abc'
    order by 1,2,3,4
    """

    qt_duplicate_gby_key """
    SELECT 
        from_unixtime(1742860744.003242),
         from_unixtime(1742860744.003242)
        c1 
    FROM eliminate_constant_gby_key_t 
    GROUP BY from_unixtime(1742860744.003242), from_unixtime(1742860744.003242),'abc',c1
    order by 1,2,3,4
    """
}