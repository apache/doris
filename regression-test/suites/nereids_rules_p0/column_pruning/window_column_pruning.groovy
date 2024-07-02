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

suite("window_column_pruning") {
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql """
        DROP TABLE IF EXISTS window_column_pruning
       """
    sql """
    CREATE TABLE IF NOT EXISTS window_column_pruning(
      `id` int NULL,
      `c1` int NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    // should prune
    explain {
        sql "select id from (select id, row_number() over (partition by id) as rn from window_column_pruning) tmp where id > 1;"
        notContains "row_number"
    }

    // should not prune
    explain {
        sql "select id, rn from (select id, row_number() over (partition by id) as rn from window_column_pruning) tmp where id > 1;"
        contains "row_number"
    }

    // should prune rank, but not prune row_number
    explain {
        sql "select id, rn1 from (select id, row_number() over (partition by id) as rn1, rank() over (partition by id) as rk from window_column_pruning) tmp where id > 1;"
        contains "row_number"
        notContains "rank"
    }

    // prune through union all
    explain {
        sql "select id from (select id, rank() over() px from window_column_pruning union all select id, rank() over() px from window_column_pruning) a"
        notContains "rank"
    }
}

