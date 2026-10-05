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

suite("rf_pushdown_expr") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_join_reorder=true"
    sql "set enable_runtime_filter_prune=false"
    sql "set forbid_unknown_col_stats=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "drop table if exists rf_expr_t1"
    sql """
        CREATE TABLE rf_expr_t1 (
            id INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            city VARCHAR(20) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql "drop table if exists rf_expr_t2"
    sql """
        CREATE TABLE rf_expr_t2 (
            id INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            city VARCHAR(20) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Test 1: substring() on varchar should NOT produce RF at scan level
    explain {
        sql "shape plan select * from rf_expr_t1 join rf_expr_t2 on substring(rf_expr_t1.name, 1, 2) = substring(rf_expr_t2.name, 1, 2)"
        notContains "build RFs"
    }

    // Test 2: abs() on numeric column SHOULD produce RF at scan level
    explain {
        sql "shape plan select * from rf_expr_t1 join rf_expr_t2 on abs(rf_expr_t1.id) = rf_expr_t2.id"
        contains "build RFs"
    }

    // Test 3: bare varchar = varchar SHOULD produce RF at scan level
    explain {
        sql "shape plan select * from rf_expr_t1 join rf_expr_t2 on rf_expr_t1.name = rf_expr_t2.name"
        contains "build RFs"
    }

    // Test 4: cast-wrapped varchar SHOULD produce RF at scan level
    // explain {
    //     sql "shape plan select * from rf_expr_t1 join rf_expr_t2 on cast(rf_expr_t1.name as varchar(100)) = rf_expr_t2.name"
    //     contains "build RFs"
    // }

    // Test 5: expand RF through inner join creates separate RF objects per target (V2-style)
    // Query: t1 join t2 on t1.id=t2.id join t3 on t3.id=t1.id
    // t3.id RF should expand through the inner join t1.id=t2.id to also target t2.id
    // V2-style: produces separate RF objects — one for t1.id, one for t2.id (expanded)
    sql "drop table if exists rf_expr_t3"
    sql """
        CREATE TABLE rf_expr_t3 (
            id INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            city VARCHAR(20) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql "set expand_runtime_filter_by_inner_join=true"
    // Both t1 and t2 should apply RFs (expanded through inner join gives t2 an extra RF)
    // Verify that expansion produces 3 RFs total (RF0, RF1, RF2)
    explain {
        sql """shape plan
            select /*+ leading(rf_expr_t1 rf_expr_t2 rf_expr_t3) */ *
            from rf_expr_t1
            join rf_expr_t2 on rf_expr_t1.id = rf_expr_t2.id
            join rf_expr_t3 on rf_expr_t3.id = rf_expr_t1.id"""
        contains "build RFs"
        contains "RF2"
    }
    sql "set expand_runtime_filter_by_inner_join=false"
}
