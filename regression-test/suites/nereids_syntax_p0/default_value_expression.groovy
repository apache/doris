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

suite("nereids_default_value_expression", "p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tbl = "tbl_default_expr"

    try {
        sql "DROP TABLE IF EXISTS ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
              id INT NOT NULL,
              d DATEV2 NOT NULL DEFAULT to_date(now()),
              dt DATETIMEV2(3) NOT NULL DEFAULT now(3),
              s STRING NOT NULL DEFAULT concat('a-', cast(to_date(now()) as string))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        sql "INSERT INTO ${tbl}(id) VALUES (1)"
        sql "sync"
        qt_default_expr_select "SELECT id, d = CURRENT_DATE, dt IS NOT NULL, s = concat('a-', cast(CURRENT_DATE as string)) FROM ${tbl} ORDER BY id"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tbl}")
    }

    test {
        sql """
            CREATE TABLE ${tbl}_bad_ref (
              k1 INT,
              v1 INT DEFAULT k1 + 1
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        exception "Default value expression cannot contain column reference"
    }

    test {
        sql """
            CREATE TABLE ${tbl}_bad_rand (
              k1 INT,
              v1 DOUBLE DEFAULT rand()
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        exception "non-deterministic"
    }

    def mowTbl = "${tbl}_mow"
    try {
        sql "DROP TABLE IF EXISTS ${mowTbl}"
        sql """
            CREATE TABLE ${mowTbl} (
              k1 INT,
              v1 INT NULL,
              d DATEV2 NOT NULL DEFAULT to_date(now())
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES(
              "replication_num" = "1",
              "enable_unique_key_merge_on_write" = "true"
            );
        """

        sql "set enable_unique_key_partial_update=true"
        sql "set allow_partial_update_with_expression_default=false"

        test {
            sql "INSERT INTO ${mowTbl}(k1) VALUES (1)"
            exception "Partial update is not supported for table with expression default value"
        }

        sql "set allow_partial_update_with_expression_default=true"
        sql "INSERT INTO ${mowTbl}(k1) VALUES (1)"
        sql "sync"
        qt_default_expr_partial_update_bypass "SELECT k1, v1 IS NULL, d IS NOT NULL FROM ${mowTbl} ORDER BY k1"
    } finally {
        try {
            sql "set allow_partial_update_with_expression_default=false"
            sql "set enable_unique_key_partial_update=false"
            sql "sync"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${mowTbl}")
        }
    }
}
