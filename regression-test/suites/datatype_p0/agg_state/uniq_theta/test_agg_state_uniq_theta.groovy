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

suite("test_agg_state_uniq_theta") {
    sql "set enable_agg_state=true"

    sql "DROP TABLE IF EXISTS uniq_theta_src"
    sql """
        CREATE TABLE uniq_theta_src (
            k1 int null,
            v_int int null,
            v_str varchar(64) null
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO uniq_theta_src VALUES
        (1, 10, 'a'), (1, 20, 'b'), (1, 20, 'b'), (1, null, null),
        (2, 30, 'c'), (2, 40, 'd'), (2, 50, 'e')
    """

    // state -> merge round-trip equals direct uniq_theta (nulls excluded)
    qt_state_merge_basic """
        SELECT uniq_theta_merge(uniq_theta_state(v_int)) FROM uniq_theta_src
    """
    qt_direct """
        SELECT uniq_theta(v_int) FROM uniq_theta_src
    """

    // persist state into an AGGREGATE table, then merge per key.
    // Note: INSERT ... SELECT ... GROUP BY into an agg_state column is not
    // supported by the planner (same limitation as ndv_state), so use
    // value-style inserts like datatype_p0/agg_state/hll/hll.groovy.
    sql "DROP TABLE IF EXISTS uniq_theta_agg"
    sql """
        CREATE TABLE uniq_theta_agg (
            k1 int null,
            s agg_state<uniq_theta(int)> generic
        )
        AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """
    // key 1 -> {10, 20, 20} = 2 distinct; multi-insert same key auto-merges state
    sql "INSERT INTO uniq_theta_agg VALUES (1, uniq_theta_state(cast(10 as int)))"
    sql "INSERT INTO uniq_theta_agg VALUES (1, uniq_theta_state(cast(20 as int)))"
    sql "INSERT INTO uniq_theta_agg VALUES (1, uniq_theta_state(cast(20 as int)))"
    // key 2 -> {30, 40} = 2 distinct
    sql "INSERT INTO uniq_theta_agg VALUES (2, uniq_theta_state(cast(30 as int)))"
    sql "INSERT INTO uniq_theta_agg VALUES (2, uniq_theta_state(cast(40 as int)))"

    qt_agg_merge_by_key """
        SELECT k1, uniq_theta_merge(s) FROM uniq_theta_agg GROUP BY k1 ORDER BY k1
    """

    // union across partitions then merge to global cardinality (4 distinct total)
    qt_union_then_merge """
        SELECT uniq_theta_merge(us) FROM (
            SELECT uniq_theta_union(s) AS us FROM uniq_theta_agg GROUP BY k1
        ) t
    """

    // string state column
    sql "DROP TABLE IF EXISTS uniq_theta_agg_str"
    sql """
        CREATE TABLE uniq_theta_agg_str (
            k1 int null,
            s agg_state<uniq_theta(varchar(64))> generic
        )
        AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO uniq_theta_agg_str VALUES (1, uniq_theta_state(cast('a' as varchar(64))))"
    sql "INSERT INTO uniq_theta_agg_str VALUES (1, uniq_theta_state(cast('b' as varchar(64))))"
    sql "INSERT INTO uniq_theta_agg_str VALUES (2, uniq_theta_state(cast('c' as varchar(64))))"
    qt_str_state_merge """
        SELECT uniq_theta_merge(s) FROM uniq_theta_agg_str
    """
}
