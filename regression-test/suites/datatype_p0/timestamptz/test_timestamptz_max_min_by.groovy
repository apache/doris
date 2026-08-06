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


suite("test_timestamptz_max_min_by") {

    sql "set time_zone = '+00:00';"

    // Bug 7: TIMESTAMPTZ as return value (first argument of max_by / min_by)
    sql "DROP TABLE IF EXISTS t_max_by_value;"
    sql """
        CREATE TABLE t_max_by_value (
            score    INT,
            ts_value TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(score)
        DISTRIBUTED BY HASH(score) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO t_max_by_value VALUES
        (1, CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6))),
        (2, CAST('2024-01-01 08:00:00 +08:00' AS TIMESTAMPTZ(6)));
    """
    // CAST to VARCHAR for deterministic string output across sessions
    order_qt_max_by_value """
        SELECT CAST(max_by(ts_value, score) AS VARCHAR(64)),
               CAST(min_by(ts_value, score) AS VARCHAR(64))
        FROM t_max_by_value;
    """

    // Bug 8: TIMESTAMPTZ as order key (second argument of max_by / min_by)
    sql "DROP TABLE IF EXISTS t_max_by_key;"
    sql """
        CREATE TABLE t_max_by_key (
            payload VARCHAR(64),
            ts_key  TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(payload)
        DISTRIBUTED BY HASH(payload) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO t_max_by_key VALUES
        ('alpha', CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6))),
        ('beta',  CAST('2024-01-01 02:00:00 +00:00' AS TIMESTAMPTZ(6)));
    """
    order_qt_max_by_key """
        SELECT max_by(payload, ts_key),
               min_by(payload, ts_key)
        FROM t_max_by_key;
    """
}
