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

// MIN/MAX push-down no longer looks at the declared column length. The storage layer decides per
// segment: a zone map bound cut to 512 bytes is not a value the column holds, so those segments
// read the rows instead.
suite("test_pushdown_zonemap_minmax") {
    def longValue = "z" * 600
    def exactValue = "d" * 512

    // A VARCHAR wider than 512 used to be excluded by its declared length alone, even when every
    // value in it was short.
    sql "DROP TABLE IF EXISTS test_string_minmax_wide"
    sql """
        CREATE TABLE test_string_minmax_wide (
            `id` INT NOT NULL,
            `v` VARCHAR(65533) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ INSERT INTO test_string_minmax_wide VALUES (1, "aaa"), (2, "zzz") """
    explain {
        sql "select min(v), max(v) from test_string_minmax_wide"
        contains "pushAggOp=MINMAX"
    }
    qt_wide_short "select min(v), max(v) from test_string_minmax_wide"

    // A value past the 512-byte cut: the plan still pushes down, and the storage layer falls back
    // per segment so the answer stays a value the table holds.
    sql """ INSERT INTO test_string_minmax_wide VALUES (3, "${longValue}") """
    explain {
        sql "select min(v), max(v) from test_string_minmax_wide"
        contains "pushAggOp=MINMAX"
    }
    qt_wide_long "select min(v), length(max(v)), right(max(v), 4) from test_string_minmax_wide"

    // A VARCHAR filled to exactly 512 bytes is cut as well. FE pushed this down before too,
    // because the declared length is not over 512, and the raised bound answered MAX with a value
    // that was never inserted.
    sql "DROP TABLE IF EXISTS test_string_minmax_512"
    sql """
        CREATE TABLE test_string_minmax_512 (
            `id` INT NOT NULL,
            `v` VARCHAR(512) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ INSERT INTO test_string_minmax_512 VALUES (1, "aaa"), (2, "${exactValue}") """
    qt_512_max "select length(max(v)), right(max(v), 4) from test_string_minmax_512"
    qt_512_eq "select count(*) from test_string_minmax_512 where v = '${exactValue}'"

    // The switch says whether a cut bound may answer MIN/MAX. It is off by default, so the answer
    // is always a value the table holds. Statistics collection turns it on and takes the cut bound
    // rather than reading every row.
    sql "DROP TABLE IF EXISTS test_string_minmax_str"
    sql """
        CREATE TABLE test_string_minmax_str (
            `id` INT NOT NULL,
            `v` STRING NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ INSERT INTO test_string_minmax_str VALUES (1, "aaa"), (2, "${longValue}") """

    // Off by default: the plan still pushes down, and the segment whose bound was cut reads its
    // rows, so both answers are values the table holds.
    explain {
        sql "select min(v), max(v) from test_string_minmax_str"
        contains "pushAggOp=MINMAX"
    }
    qt_str_off "select min(v), length(max(v)), right(max(v), 4) from test_string_minmax_str"

    // On: the cut bound answers straight away. It is the 512-byte prefix with its last byte
    // raised, so the max ends in '{', one past the 'z' that was inserted.
    sql "set force_pushdown_zonemap_minmax = true"
    explain {
        sql "select min(v), max(v) from test_string_minmax_str"
        contains "pushAggOp=MINMAX"
    }
    qt_str_on "select length(max(v)), right(max(v), 4) from test_string_minmax_str"
    sql "set force_pushdown_zonemap_minmax = false"

    // The delete predicate is on k and the query reads only v. The scan adds k to the columns it
    // reads, and the zone map answer must fill only v.
    sql "DROP TABLE IF EXISTS test_minmax_delete_other_column"
    // Auto compaction is off: it would apply the delete to the data and drop the predicate.
    sql """
        CREATE TABLE test_minmax_delete_other_column (
            `k` VARCHAR(20) NOT NULL,
            `v` DATETIME NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """
    // The zone map still covers the deleted row, so it is neither the min nor the max.
    sql """
        INSERT INTO test_minmax_delete_other_column VALUES
            ('a', '2022-04-27 16:00:33'), ('b', '2022-04-27 16:00:59'), ('1', '2022-04-27 16:00:40')
    """
    sql "DELETE FROM test_minmax_delete_other_column WHERE k = '1'"
    // Without this, FE answers MIN/MAX from its cache and the query never reaches BE.
    sql "set disable_nereids_rules = 'REWRITE_SIMPLE_AGG_TO_CONSTANT'"
    sql "set force_pushdown_zonemap_minmax = true"
    explain {
        sql "select min(v), max(v) from test_minmax_delete_other_column"
        contains "pushAggOp=MINMAX"
    }
    qt_delete_other_column "select min(v), max(v) from test_minmax_delete_other_column"
    sql "set force_pushdown_zonemap_minmax = false"
    sql "set disable_nereids_rules = ''"
}
