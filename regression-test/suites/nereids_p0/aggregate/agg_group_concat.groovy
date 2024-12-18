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
            `kbint` int(11) not null,
            `kstr` string not null,
            `kstr2` string not null,
            `kastr` array<string> not null
        ) engine=olap
        DISTRIBUTED BY HASH(`kint`) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """
        INSERT INTO `agg_group_concat_table` VALUES 
        ( 1, 1, 'string1', 'string3', ['s11', 's12', 's13'] ),
        ( 1, 2, 'string2', 'string1', ['s21', 's22', 's23'] ),
        ( 2, 3, 'string3', 'string2', ['s31', 's32', 's33'] ),
        ( 1, 1, 'string1', 'string3', ['s11', 's12', 's13'] ),
        ( 1, 2, 'string2', 'string1', ['s21', 's22', 's23'] ),
        ( 2, 3, 'string3', 'string2', ['s31', 's32', 's33'] );
    """

    sql """select group_concat_state(kstr) from agg_group_concat_table;"""
    sql """select group_concat_union(group_concat_state(kstr)) from agg_group_concat_table;"""
    sql """select group_concat_merge(group_concat_state(kstr)) from agg_group_concat_table;"""

    sql """select group_concat_state(distinct kstr) from agg_group_concat_table;"""
    sql """select group_concat_union(group_concat_state(kstr)) from agg_group_concat_table;"""
    sql """select group_concat_merge(group_concat_state(kstr)) from agg_group_concat_table;"""

    sql """select multi_distinct_group_concat_state(kstr) from agg_group_concat_table;"""
    sql """select multi_distinct_group_concat_union(multi_distinct_group_concat_state(kstr)) from agg_group_concat_table;"""
    sql """select multi_distinct_group_concat_merge(multi_distinct_group_concat_state(kstr)) from agg_group_concat_table;"""

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

    sql """select multi_distinct_sum(kint) from agg_group_concat_table order by 1;"""

    sql """select group_concat(distinct kstr order by kint), group_concat(distinct kstr2 order by kbint) from agg_group_concat_table order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr order by kint), multi_distinct_group_concat(kstr2 order by kbint) from agg_group_concat_table order by 1,2;"""
    sql """select group_concat(distinct kstr), group_concat(distinct kstr2) from agg_group_concat_table order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr), multi_distinct_group_concat(kstr2) from agg_group_concat_table order by 1,2;"""

    sql """select group_concat(distinct kstr order by kint), group_concat(distinct kstr2 order by kbint) from agg_group_concat_table group by kbint order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr order by kint), multi_distinct_group_concat(kstr2 order by kbint) from agg_group_concat_table group by kbint order by 1,2;"""
    sql """select group_concat(distinct kstr), group_concat(distinct kstr2) from agg_group_concat_table group by kbint order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr), multi_distinct_group_concat(kstr2) from agg_group_concat_table group by kbint order by 1,2;"""

    sql """select group_concat(distinct kstr order by kbint), group_concat(distinct kstr2 order by kint) from agg_group_concat_table group by kint order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr order by kbint), multi_distinct_group_concat(kstr2 order by kint) from agg_group_concat_table group by kint order by 1,2;"""
    sql """select group_concat(distinct kstr), group_concat(distinct kstr2) from agg_group_concat_table group by kint order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr), multi_distinct_group_concat(kstr2) from agg_group_concat_table group by kint order by 1,2;"""

    sql """select group_concat(distinct kstr order by kint), group_concat(kstr2 order by kbint) from agg_group_concat_table order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr order by kint), group_concat(kstr2 order by kbint) from agg_group_concat_table order by 1,2;"""
    sql """select group_concat(distinct kstr), group_concat(kstr2) from agg_group_concat_table order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr), group_concat(kstr2) from agg_group_concat_table order by 1,2;"""

    sql """select group_concat(distinct kstr order by kint), group_concat(kstr2 order by kbint) from agg_group_concat_table group by kbint order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr order by kint), group_concat(kstr2 order by kbint) from agg_group_concat_table group by kbint order by 1,2;"""
    sql """select group_concat(distinct kstr), group_concat(kstr2) from agg_group_concat_table group by kbint order by 1,2;"""
    sql """select multi_distinct_group_concat(kstr), group_concat(kstr2) from agg_group_concat_table group by kbint order by 1,2;"""

    sql "drop table if exists test_distinct_multi"
    sql """
    create table test_distinct_multi(a int, b int, c int, d varchar(10), e date) distributed by hash(a) properties('replication_num'='1'); 
    """
    sql """
    insert into test_distinct_multi values(1,2,3,'abc','2024-01-02'),(1,2,4,'abc','2024-01-03'),(2,2,4,'abcd','2024-01-02'),(1,2,3,'abcd','2024-01-04'),(1,2,4,'eee','2024-02-02'),(2,2,4,'abc','2024-01-02'); 
    """
    qt_test "select group_concat( distinct d order by d) from test_distinct_multi order by 1; "
}