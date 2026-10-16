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

// Test single-column null-safe equal ( <=> ) hash join on string keys.
// Null keys produced by expressions must match each other (NULL <=> NULL is true),
// and must never match a real empty string key.
suite("test_null_safe_eq_join_string_key") {
    sql """ DROP TABLE IF EXISTS test_nsej_string_p """
    sql """ DROP TABLE IF EXISTS test_nsej_string_c """

    sql """
        CREATE TABLE test_nsej_string_p (
          pk int,
          ch char(10) not null,
          v tinyint null
        ) duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1");
    """
    sql """ insert into test_nsej_string_p values (1,'x',3),(2,'x',NULL) """

    sql """
        CREATE TABLE test_nsej_string_c (
          pk int,
          s varchar(100) null
        ) duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1");
    """
    sql """ insert into test_nsej_string_c values (1, NULL), (2, 'x3') """

    // Minimal case: NULL key comes from an expression, NULL <=> NULL must match, so (2,1) is expected.
    order_qt_minimal_left_join """
        SELECT p.pk, c.pk AS cpk
        FROM test_nsej_string_p p LEFT JOIN test_nsej_string_c c
          ON CONCAT(COALESCE(p.ch,''), CAST((p.v % 5) AS STRING)) <=> c.s
        ORDER BY 1, 2;
    """

    order_qt_minimal_inner_join """
        SELECT p.pk, c.pk AS cpk
        FROM test_nsej_string_p p INNER JOIN test_nsej_string_c c
          ON CONCAT(COALESCE(p.ch,''), CAST((p.v % 5) AS STRING)) <=> c.s
        ORDER BY 1, 2;
    """

    // The null-safe equal join must return the same result as its semantic oracle.
    order_qt_minimal_oracle """
        SELECT p.pk, c.pk AS cpk
        FROM test_nsej_string_p p LEFT JOIN test_nsej_string_c c
          ON CONCAT(COALESCE(p.ch,''), CAST((p.v % 5) AS STRING)) = c.s
            OR (CONCAT(COALESCE(p.ch,''), CAST((p.v % 5) AS STRING)) IS NULL AND c.s IS NULL)
        ORDER BY 1, 2;
    """

    sql """ DROP TABLE IF EXISTS test_nsej_string_a """
    sql """ DROP TABLE IF EXISTS test_nsej_string_b """

    sql """
        CREATE TABLE test_nsej_string_a (
          pk int,
          v int null
        ) duplicate key(pk)
        distributed by hash(pk) buckets 3
        properties("replication_num" = "1");
    """
    sql """
        insert into test_nsej_string_a values
        (0,0),(1,NULL),(2,2),(3,NULL),(4,4),(5,NULL),(6,6),(7,NULL),(8,8),(9,NULL)
    """

    sql """
        CREATE TABLE test_nsej_string_b (
          pk int,
          s string null
        ) duplicate key(pk)
        distributed by hash(pk) buckets 3
        properties("replication_num" = "1");
    """
    sql """
        insert into test_nsej_string_b values
        (0,'0'),(1,'2'),(2,'4'),(3,NULL),(4,NULL),(5,'8'),(6,'10')
    """

    // Mixed blocks: NULL keys from expression may hold residual bytes of evaluated nested values,
    // they must still match NULL keys on the other side.
    order_qt_mixed_inner_join """
        SELECT a.pk, b.pk AS bpk
        FROM test_nsej_string_a a INNER JOIN test_nsej_string_b b
          ON CAST(a.v AS STRING) <=> b.s
        ORDER BY 1, 2;
    """

    order_qt_mixed_left_join """
        SELECT a.pk, b.pk AS bpk
        FROM test_nsej_string_a a LEFT JOIN test_nsej_string_b b
          ON CAST(a.v AS STRING) <=> b.s
        ORDER BY 1, 2;
    """

    order_qt_mixed_oracle """
        SELECT a.pk, b.pk AS bpk
        FROM test_nsej_string_a a INNER JOIN test_nsej_string_b b
          ON CAST(a.v AS STRING) = b.s OR (CAST(a.v AS STRING) IS NULL AND b.s IS NULL)
        ORDER BY 1, 2;
    """

    // A normalized null key must not collide with a real empty string key.
    sql """ DROP TABLE IF EXISTS test_nsej_string_e """
    sql """ DROP TABLE IF EXISTS test_nsej_string_f """

    sql """
        CREATE TABLE test_nsej_string_e (
          pk int,
          v int null
        ) duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1");
    """
    sql """ insert into test_nsej_string_e values (1,10),(2,NULL) """

    sql """
        CREATE TABLE test_nsej_string_f (
          pk int,
          s string null
        ) duplicate key(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1");
    """
    sql """ insert into test_nsej_string_f values (1,NULL),(2,'') """

    // Row (1,10) yields a real empty string key, row (2,NULL) yields a null key.
    // Expected matches: (1,2) by empty string, (2,1) by NULL <=> NULL.
    order_qt_empty_string_inner_join """
        SELECT e.pk, f.pk AS fpk
        FROM test_nsej_string_e e INNER JOIN test_nsej_string_f f
          ON IF(e.v IS NOT NULL, '', NULL) <=> f.s
        ORDER BY 1, 2;
    """
}
