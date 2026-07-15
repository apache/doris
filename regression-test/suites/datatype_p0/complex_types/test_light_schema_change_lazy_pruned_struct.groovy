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

suite("test_light_schema_change_lazy_pruned_struct") {
    sql "set batch_size = 4"
    sql "set enable_prune_nested_column = true"

    sql "DROP TABLE IF EXISTS test_light_schema_change_lazy_pruned_struct"
    sql """
        CREATE TABLE test_light_schema_change_lazy_pruned_struct (
            id INT,
            s STRUCT<b:INT, c:VARCHAR(20)>,
            b INT,
            a INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    // Keep these rows in the old physical schema. After the ALTER, s.a is read by
    // DefaultValueColumnIterator for this rowset.
    sql """
        INSERT INTO test_light_schema_change_lazy_pruned_struct VALUES
            (1, named_struct('b', 1, 'c', 'abc'), 21, 11),
            (2, named_struct('b', 2, 'c', 'abc'), 22, 12),
            (3, named_struct('b', 0, 'c', 'abc'), 23, 13),
            (4, named_struct('b', 4, 'c', 'def'), 24, 14),
            (5, named_struct('b', 5, 'c', 'abc'), 20, 15),
            (6, named_struct('b', 6, 'c', 'abc'), 26, 10),
            (7, named_struct('b', 7, 'c', 'abc'), 27, 17),
            (8, named_struct('b', 8, 'c', 'def'), 28, 18)
    """

    sql """
        ALTER TABLE test_light_schema_change_lazy_pruned_struct
        MODIFY COLUMN s STRUCT<b:INT, c:VARCHAR(20), a:INT>
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName='test_light_schema_change_lazy_pruned_struct'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    // Also keep a rowset written with the new physical schema in the same scan.
    sql """
        INSERT INTO test_light_schema_change_lazy_pruned_struct VALUES
            (9, named_struct('b', 9, 'c', 'abc', 'a', 90), 29, 19),
            (10, named_struct('b', 0, 'c', 'abc', 'a', 100), 30, 20)
    """

    // s.a is a lazy output column, while s.b and s.c are predicate columns.
    // On old rowsets s.a must replace its predicate-phase placeholders instead of
    // appending defaults to them during lazy materialization.
    order_qt_default_value_as_lazy_output """
        SELECT s.a
        FROM test_light_schema_change_lazy_pruned_struct
        WHERE s.b > 0
          AND s.c = 'abc'
          AND b > 20
          AND a > 10
    """

    // Exercise the inverse phase assignment: the default-backed child is used by
    // a predicate and an old physical child is materialized lazily.
    order_qt_default_value_as_predicate """
        SELECT s.b
        FROM test_light_schema_change_lazy_pruned_struct
        WHERE s.a IS NULL
          AND s.c = 'abc'
          AND b > 20
          AND a > 10
    """
}
