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

suite("test_create_view_star_replace") {
    sql "DROP VIEW IF EXISTS view_star_replace_v"
    sql "DROP VIEW IF EXISTS view_star_replace_nested"
    sql "DROP TABLE IF EXISTS view_star_replace_t"
    sql """CREATE TABLE view_star_replace_t (
        a INT, b INT, name VARCHAR(50), payload VARIANT
    ) DUPLICATE KEY(a)
    DISTRIBUTED BY HASH(a) BUCKETS 1
    PROPERTIES ("replication_num" = "1")"""
    sql """INSERT INTO view_star_replace_t VALUES
        (1, 2, 'Alice', '{"score":10}'),
        (3, 4, 'Bob', '{"score":20}'),
        (5, NULL, NULL, NULL)"""

    sql """CREATE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(99 AS a) FROM view_star_replace_t"""
    qt_constant_definition "SHOW CREATE VIEW view_star_replace_v"
    order_qt_constant "SELECT * FROM view_star_replace_v"
    order_qt_constant_direct "SELECT * EXCEPT(payload) REPLACE(99 AS a) FROM view_star_replace_t"

    sql """ALTER VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(a + 1 AS a, upper(name) AS name) FROM view_star_replace_t"""
    qt_expression_definition "SHOW CREATE VIEW view_star_replace_v"
    order_qt_expression "SELECT * FROM view_star_replace_v"
    order_qt_expression_direct """SELECT * EXCEPT(payload)
        REPLACE(a + 1 AS a, upper(name) AS name) FROM view_star_replace_t"""

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT l.* EXCEPT(payload) REPLACE(l.a + r.b AS a)
        FROM view_star_replace_t l JOIN view_star_replace_t r ON l.a = r.a"""
    order_qt_join "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(
            CASE WHEN a > 1 AND b IS NOT NULL THEN cast(b AS BIGINT) ELSE 0 END AS b,
            concat(coalesce(name, 'missing'), '!') AS name) FROM view_star_replace_t"""
    order_qt_case "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(cast(payload['score'] AS INT) AS b) FROM view_star_replace_t"""
    order_qt_variant "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(row_number() OVER (ORDER BY a) AS b) FROM view_star_replace_t"""
    order_qt_window "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) FROM view_star_replace_t"""
    order_qt_except "SELECT * FROM view_star_replace_v"
    sql """CREATE VIEW view_star_replace_nested AS
        SELECT * REPLACE(b + 10 AS b) FROM view_star_replace_v"""
    order_qt_nested "SELECT * FROM view_star_replace_nested"

    sql """CREATE OR REPLACE VIEW view_star_replace_v(x, y, z) AS
        SELECT * EXCEPT(payload) REPLACE(a * 2 AS a) FROM view_star_replace_t"""
    order_qt_explicit_columns "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * REPLACE(`a``b` + 1 AS `a``b`) FROM (SELECT a AS `a``b`, b FROM view_star_replace_t) q"""
    order_qt_backquote "SELECT * FROM view_star_replace_v"
    qt_backquote_definition "SHOW CREATE VIEW view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(concat(name, 'O''Reilly') AS name) FROM view_star_replace_t"""
    order_qt_string "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(array_map(x -> x + a, [1, 2]) AS b) FROM view_star_replace_t"""
    order_qt_lambda "SELECT * FROM view_star_replace_v"

    sql """CREATE OR REPLACE VIEW view_star_replace_v AS
        SELECT * EXCEPT(payload) REPLACE(map('first', a, 'second', b) AS b) FROM view_star_replace_t"""
    order_qt_map "SELECT a, b['first'], b['second'], name FROM view_star_replace_v"

    test {
        sql """CREATE OR REPLACE VIEW view_star_replace_v AS
            SELECT * REPLACE(1 AS missing) FROM view_star_replace_t"""
        exception "Unknown column"
    }
    test {
        sql """CREATE OR REPLACE VIEW view_star_replace_v AS
            SELECT * EXCEPT(a) REPLACE(1 AS a) FROM view_star_replace_t"""
        exception "is in excepts"
    }
}
