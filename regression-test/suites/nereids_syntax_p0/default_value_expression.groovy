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

    sql "DROP TABLE IF EXISTS tbl_default_expr"
    sql """
        CREATE TABLE tbl_default_expr (
          id INT NOT NULL,
          d DATEV2 NOT NULL DEFAULT to_date(now()),
          dt DATETIMEV2(3) NOT NULL DEFAULT now(3),
          s STRING NOT NULL DEFAULT concat('a-', DATE_FORMAT(now(), '%M %e, %Y'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql "INSERT INTO tbl_default_expr(id) VALUES (1)"
    sql "sync"
    qt_default_expr_select "SELECT id, d = CURRENT_DATE, dt IS NOT NULL, s = concat('a-', DATE_FORMAT(now(), '%M %e, %Y')) FROM tbl_default_expr ORDER BY id"

    test {
        sql "DROP TABLE IF EXISTS tbl_default_expr_bad_ref"
        sql """
            CREATE TABLE tbl_default_expr_bad_ref (
              k1 INT,
              v1 INT DEFAULT k1 + 1
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        exception "Default value expression cannot contain column reference"
    }

    test {
        sql "DROP TABLE IF EXISTS tbl_default_expr_bad_rand"
        sql """
            CREATE TABLE tbl_default_expr_bad_rand (
              k1 INT,
              v1 DOUBLE DEFAULT rand()
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        exception "non-deterministic"
    }

    try {
        sql "DROP TABLE IF EXISTS tbl_default_expr_mow"
        sql """
            CREATE TABLE tbl_default_expr_mow (
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
            sql "INSERT INTO tbl_default_expr_mow(k1) VALUES (1)"
            exception "Partial update is not supported for table with expression default value"
        }

        sql "set allow_partial_update_with_expression_default=true"
        sql "INSERT INTO tbl_default_expr_mow(k1) VALUES (1)"
        sql "sync"
        qt_default_expr_partial_update_bypass "SELECT k1, v1 IS NULL, d IS NOT NULL FROM tbl_default_expr_mow ORDER BY k1"
    } finally {
        sql "set allow_partial_update_with_expression_default=false"
        sql "set enable_unique_key_partial_update=false"
        sql "sync"
    }
}
