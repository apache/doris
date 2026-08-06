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

suite("test_constraint_change_rewrite_mtmv", "mtmv") {
    String db = context.config.getDbNameByFile(context.file)
    String p = "test_constraint_change_rewrite_mtmv"
    String tOrders = "${p}_orders"
    String tLineitem = "${p}_lineitem"
    String mvName = "${p}_mv"

    sql """use ${db}"""
    sql """SET enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """SET enable_materialized_view_rewrite=true"""
    sql """SET enable_nereids_timeout = false"""

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tOrders}"""
    sql """DROP TABLE IF EXISTS ${tLineitem}"""

    sql """CREATE TABLE `${tOrders}` (
      `o_orderkey` BIGINT NOT NULL,
      `o_custkey` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`)
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 1
    PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

    sql """CREATE TABLE `${tLineitem}` (
      `l_orderkey` BIGINT NOT NULL,
      `l_linenumber` INT NULL,
      `l_comment` VARCHAR(44) NULL,
      `l_shipdate` DATE NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`l_orderkey`)
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 1
    PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

    sql """
    INSERT INTO ${tOrders} (
        o_orderkey, o_custkey, o_comment, o_orderdate
    ) VALUES
    (1001, 3001, 'order1001', '2024-01-15'),
    (1002, 3002, 'order1002', '2024-02-20'),
    (1003, 3003, 'order1003', '2024-03-05');
    """

    sql """
    INSERT INTO ${tLineitem} (
        l_orderkey, l_linenumber, l_comment, l_shipdate
    ) VALUES
    (1001, 1, 'order1001_line1', '2024-01-18'),
    (1002, 1, 'order1002_line1', '2024-02-22'),
    (1003, 1, 'order1003_line1', '2024-03-08');
    """

    sql """ANALYZE TABLE ${tLineitem} WITH SYNC;"""
    sql """ANALYZE TABLE ${tOrders} WITH SYNC;"""
    sql """ALTER TABLE ${tLineitem} MODIFY COLUMN l_comment SET STATS ('row_count'='3');"""
    sql """ALTER TABLE ${tOrders} MODIFY COLUMN o_comment SET STATS ('row_count'='3');"""

    def mvSql = """
        SELECT
            o_orderkey, o_custkey, o_comment,
            l_orderkey, l_comment
        FROM ${tOrders}
        INNER JOIN ${tLineitem}
            ON o_orderkey = l_orderkey
    """
    create_async_mv(db, mvName, mvSql)

    def querySql = """SELECT o_orderkey, o_custkey, o_comment FROM ${tOrders}"""
    // Phase 1: No PK/FK yet; the lineitem side of the MV join cannot be eliminated by PK-FK rules, rewrite should fail.
    mv_rewrite_fail(querySql, mvName)

    // Phase 2: Add PK on lineitem and FK on orders referencing lineitem; query can match the MV safely, rewrite should succeed.
    sql """ALTER TABLE ${tLineitem} ADD CONSTRAINT ${p}_lineitem_pk
            PRIMARY KEY (l_orderkey)"""
    sql """ALTER TABLE ${tOrders} ADD CONSTRAINT ${p}_orders_fk
            FOREIGN KEY (o_orderkey) REFERENCES ${tLineitem}(l_orderkey)"""
    mv_rewrite_success_without_check_chosen(querySql, mvName)

    // Phase 3: Drop the FK only; join elimination no longer has FK support, rewrite should fail again.
    sql """ALTER TABLE ${tOrders} DROP CONSTRAINT ${p}_orders_fk"""
    mv_rewrite_fail(querySql, mvName)

    // Phase 4: Re-add FK so rewrite succeeds again; then drop PK on lineitem (referenced side loses primary key), rewrite should fail;
    sql """ALTER TABLE ${tOrders} ADD CONSTRAINT ${p}_orders_fk
            FOREIGN KEY (o_orderkey) REFERENCES ${tLineitem}(l_orderkey)"""
    mv_rewrite_success_without_check_chosen(querySql, mvName)

    sql """ALTER TABLE ${tLineitem} DROP CONSTRAINT ${p}_lineitem_pk"""
    mv_rewrite_fail(querySql, mvName)
}
