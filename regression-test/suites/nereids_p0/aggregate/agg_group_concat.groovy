/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("agg_group_concat") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "drop table if exists agg_group_concat_table"

    sql """
        CREATE TABLE IF NOT EXISTS `agg_group_concat_table` (
            `kint` int(11) not null,
            `kstr` string not null,
            `kastr` array<string> not null
        ) engine=olap
        DISTRIBUTED BY HASH(`kint`) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """
        INSERT INTO `agg_group_concat_table` VALUES 
        ( 1, 'string1', ['s11', 's12', 's13'] ),
        ( 1, 'string2', ['s21', 's22', 's23'] ),
        ( 2, 'string3', ['s31', 's32', 's33'] );
    """

    multi_sql """
        select group_concat_state(kstr) from agg_group_concat_table;
        select group_concat_union(group_concat_state(kstr)) from agg_group_concat_table;
        select group_concat_merge(group_concat_state(kstr)) from agg_group_concat_table;

        select group_concat_state(distinct kstr) from agg_group_concat_table;
        select group_concat_union(group_concat_state(kstr)) from agg_group_concat_table;
        select group_concat_merge(group_concat_state(kstr)) from agg_group_concat_table;

        select multi_distinct_group_concat_state(kstr) from agg_group_concat_table;
        select multi_distinct_group_concat_union(multi_distinct_group_concat_state(kstr)) from agg_group_concat_table;
        select multi_distinct_group_concat_merge(multi_distinct_group_concat_state(kstr)) from agg_group_concat_table;
    """

    test {
        sql "select group_concat_state(kstr order by kint) from agg_group_concat_table;"
        exception "doesn't support order by expression"
    }

    test {
        sql "select multi_distinct_group_concat_state(kstr order by kint) from agg_group_concat_table;"
        exception "doesn't support order by expression"
    }

    test {
        sql "select count(kstr order by kint) from agg_group_concat_table;"
        exception "doesn't support order by expression"
    }

    sql """select multi_distinct_sum(kint) from agg_group_concat_table;"""

    // these 2 should work, uncomment these after fixing bug in be
    // sql """select group_concat(distinct kstr order by kint) from agg_group_concat_table;"""
    // sql """select multi_distinct_group_concat(kstr order by kint) from agg_group_concat_table;"""
}