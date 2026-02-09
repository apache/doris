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

suite("test_datelike_false_alarm") {
    sql "DROP TABLE IF EXISTS dt_t_left;"
    sql """
        CREATE TABLE dt_t_left (
            id INT,
            name STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql "DROP TABLE IF EXISTS dt_t_right;"
    sql """
        CREATE TABLE dt_t_right (
            id INT,
            event_time DATETIME
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO dt_t_left VALUES
        (1, 'match'),
        (2, 'no_match'),
        (3, 'match');
    """
    sql """ 
        INSERT INTO dt_t_right VALUES
        (1, '2024-01-15 10:23:45'),
        (3, '2024-02-20 08:00:00');
    """
    sql "DROP TABLE IF EXISTS dt_one_row;"
    sql """
        CREATE TABLE dt_one_row (
            k INT
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql "INSERT INTO dt_one_row VALUES (1);"

    qt_sql """
        SELECT
            t.id,
            t.name,
            t.event_time,
            date_trunc('day', t.event_time) AS trunc_day,
            last_day(t.event_time) AS last_day,
            to_monday(t.event_time) AS to_monday,
            from_microsecond( unix_timestamp(t.event_time) * 1000000 ) AS microsecond,
            unix_timestamp( CAST(t.event_time AS VARCHAR), "%Y-%m-%d %H:%i:%s" ) AS unix_timestamp
        FROM (
            SELECT
                l.id,
                l.name,
                r.event_time
            FROM dt_t_left l
            LEFT JOIN dt_t_right r
                ON l.id = r.id
        ) t
        LEFT JOIN dt_one_row o
            ON o.k = 1
        ORDER BY t.id;
    """
}