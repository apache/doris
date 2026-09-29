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

suite("test_constant_column_schema_evolution") {
    sql "DROP TABLE IF EXISTS test_constant_column_schema_evolution_dup"
    sql "DROP TABLE IF EXISTS test_constant_column_schema_evolution_unique"

    // Case 1: keep every schema generation in a separate rowset. Historical rowsets do not have
    // c_default or c_null physically, so their values must come from constant iterators.
    sql """
        CREATE TABLE test_constant_column_schema_evolution_dup (
            `k` INT NOT NULL,
            `payload` VARCHAR(20) NOT NULL
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO test_constant_column_schema_evolution_dup VALUES
            (1, 'old_1'),
            (2, 'old_2')
    """

    sql """
        ALTER TABLE test_constant_column_schema_evolution_dup
        ADD COLUMN `c_default` INT NOT NULL DEFAULT "10"
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_dup'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    sql """
        ALTER TABLE test_constant_column_schema_evolution_dup
        ADD COLUMN `c_null` INT NULL
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_dup'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    order_qt_dup_old_rowset_defaults """
        SELECT k, payload, c_default, c_null
        FROM test_constant_column_schema_evolution_dup
        ORDER BY k
    """

    // Case 2: mix old constant-backed rows with physical values and test equality, range, and NULL
    // predicates against both kinds of rowset.
    sql """
        INSERT INTO test_constant_column_schema_evolution_dup
            (k, payload, c_default, c_null) VALUES
            (3, 'new_5', 5, NULL),
            (4, 'new_10', 10, 8),
            (5, 'new_15', 15, 20)
    """

    order_qt_dup_equal_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default = 10
        ORDER BY k
    """
    order_qt_dup_less_than_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default < 10
        ORDER BY k
    """
    order_qt_dup_greater_than_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default > 10
        ORDER BY k
    """
    order_qt_dup_between_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default BETWEEN 10 AND 10
        ORDER BY k
    """
    order_qt_dup_is_null """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_null IS NULL
        ORDER BY k
    """
    order_qt_dup_is_not_null """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_null IS NOT NULL
        ORDER BY k
    """

    // Case 3: the delete predicate matches the default supplied for historical rowsets as well as
    // a physical value in the post-ALTER rowset.
    sql """
        DELETE FROM test_constant_column_schema_evolution_dup
        WHERE c_default = 10
    """
    sql "SYNC"

    order_qt_dup_after_delete_on_default """
        SELECT k, payload, c_default, c_null
        FROM test_constant_column_schema_evolution_dup
        ORDER BY k
    """

    // Case 4: a rowset written after the delete predicate must not be filtered by that predicate.
    sql """
        INSERT INTO test_constant_column_schema_evolution_dup
            (k, payload, c_default, c_null) VALUES
            (6, 'after_delete', 10, 30)
    """

    sql """
        ALTER TABLE test_constant_column_schema_evolution_dup
        DROP COLUMN `c_default`
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_dup'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    // Case 5: reusing the name with a different type gives the replacement a new unique id. The
    // historical INT values and delete predicate must still resolve through the old id.
    sql """
        ALTER TABLE test_constant_column_schema_evolution_dup
        ADD COLUMN `c_default` VARCHAR(20) NOT NULL DEFAULT 'fresh'
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_dup'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    order_qt_dup_after_same_name_readd """
        SELECT k, payload, c_null, c_default
        FROM test_constant_column_schema_evolution_dup
        ORDER BY k
    """

    sql """
        INSERT INTO test_constant_column_schema_evolution_dup
            (k, payload, c_null, c_default) VALUES
            (7, 'new_fresh', 40, 'fresh'),
            (8, 'new_aaa', 50, 'aaa'),
            (9, 'new_zzz', 60, 'zzz')
    """

    // Case 6: predicates on the replacement VARCHAR column must use its new default and UID.
    order_qt_dup_readd_equal_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default = 'fresh'
        ORDER BY k
    """
    order_qt_dup_readd_less_than_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default < 'fresh'
        ORDER BY k
    """
    order_qt_dup_readd_greater_than_default """
        SELECT k
        FROM test_constant_column_schema_evolution_dup
        WHERE c_default > 'fresh'
        ORDER BY k
    """

    // Case 7: use a separate unique-key table to exercise StatisticsColumnIterator. There are no
    // duplicate keys, updates, or deletes, so MIN/MAX has unambiguous visible-row semantics.
    sql """
        CREATE TABLE test_constant_column_schema_evolution_unique (
            `k` INT NOT NULL,
            `payload` VARCHAR(20) NOT NULL
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    // The final schema generation below uses VARCHAR, so keep storage MIN/MAX enabled for both
    // numeric and string constants throughout the pushdown comparison.
    sql "SET enable_pushdown_string_minmax = true"

    sql """
        INSERT INTO test_constant_column_schema_evolution_unique VALUES
            (1, 'old_1'),
            (2, 'old_2')
    """

    sql """
        ALTER TABLE test_constant_column_schema_evolution_unique
        ADD COLUMN `c_stats` INT NOT NULL DEFAULT "10"
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_unique'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    // Compare scan and MIN/MAX pushdown while all rows use the synthesized INT default.
    sql "SET enable_pushdown_minmax_on_unique = false"
    explain {
        sql("SELECT MIN(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=NONE"
    }
    order_qt_stats_old_rowset_without_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    sql "SET enable_pushdown_minmax_on_unique = true"
    explain {
        sql("SELECT MIN(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("SELECT MAX(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    order_qt_stats_old_rowset_with_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    // Case 8: compare scan and MIN/MAX pushdown after physical INT values are added.
    sql """
        INSERT INTO test_constant_column_schema_evolution_unique
            (k, payload, c_stats) VALUES
            (3, 'new_5', 5),
            (4, 'new_10', 10),
            (5, 'new_15', 15)
    """

    sql "SET enable_pushdown_minmax_on_unique = false"
    order_qt_stats_mixed_rowsets_without_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    sql "SET enable_pushdown_minmax_on_unique = true"
    explain {
        sql("SELECT MIN(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("SELECT MAX(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    order_qt_stats_mixed_rowsets_with_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    // Case 9: drop and re-add the statistics column as VARCHAR. Historical rowsets must use the
    // replacement UID and its new string default in both normal and pushed-down reads.
    sql """
        ALTER TABLE test_constant_column_schema_evolution_unique
        DROP COLUMN `c_stats`
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_unique'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    sql """
        ALTER TABLE test_constant_column_schema_evolution_unique
        ADD COLUMN `c_stats` VARCHAR(20) NOT NULL DEFAULT 'm'
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_column_schema_evolution_unique'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    sql "SET enable_pushdown_minmax_on_unique = false"
    order_qt_stats_readd_old_rowsets_without_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    sql "SET enable_pushdown_minmax_on_unique = true"
    explain {
        sql("SELECT MIN(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("SELECT MAX(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    order_qt_stats_readd_old_rowsets_with_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    // Case 10: compare scan and MIN/MAX pushdown after physical VARCHAR values are added.
    sql """
        INSERT INTO test_constant_column_schema_evolution_unique
            (k, payload, c_stats) VALUES
            (6, 'new_a', 'a'),
            (7, 'new_m', 'm'),
            (8, 'new_z', 'z')
    """

    sql "SET enable_pushdown_minmax_on_unique = false"
    order_qt_stats_readd_mixed_rowsets_without_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    sql "SET enable_pushdown_minmax_on_unique = true"
    explain {
        sql("SELECT MIN(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    explain {
        sql("SELECT MAX(c_stats) FROM test_constant_column_schema_evolution_unique")
        contains "pushAggOp=MINMAX"
    }
    order_qt_stats_readd_mixed_rowsets_with_pushdown """
        SELECT MIN(c_stats), MAX(c_stats)
        FROM test_constant_column_schema_evolution_unique
    """

    sql "SET enable_pushdown_minmax_on_unique = false"
    sql "SET enable_pushdown_string_minmax = false"
}
