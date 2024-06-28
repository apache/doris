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

suite("agg_4_phase") {
    sql "SET enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "drop table if exists agg_4_phase_tbl"
    sql """
        CREATE TABLE agg_4_phase_tbl (
            id int(11) NULL,
            gender int,
            name varchar(20),
            age int
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
        ); 
        """
    sql """
        insert into agg_4_phase_tbl values 
        (0, 0, "aa", 10), (1, 1, "bb",20), (2, 2, "cc", 30), (1, 1, "bb",20),
        (0, 0, "aa", 10), (1, 1, "bb",20), (2, 2, "cc", 30), (1, 1, "bb",20);
    """
    def test_sql = """
        select
            count(distinct id)
        from agg_4_phase_tbl;
        """
    explain {
        sql(test_sql)
        contains ":VAGGREGATE (merge finalize)"
        contains ":VEXCHANGE"
        contains ":VAGGREGATE (update serialize)"
        contains ":VAGGREGATE (merge serialize)"
        contains ":VAGGREGATE (update serialize)"
    }
    qt_4phase (test_sql)

    sql """select GROUP_CONCAT(distinct name, " ") from agg_4_phase_tbl;"""

    sql """select /*+SET_VAR(disable_nereids_rules='TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,THREE_PHASE_AGGREGATE_WITH_DISTINCT,FOUR_PHASE_AGGREGATE_WITH_DISTINCT')*/ GROUP_CONCAT(distinct name, " ") from agg_4_phase_tbl group by gender;"""


    sql "drop table if exists agg_4_phase_tbl2"
    sql "create table agg_4_phase_tbl2(id int, field1 int, field2 varchar(255)) properties('replication_num'='1');"
    sql "insert into agg_4_phase_tbl2 values(1, -10, null), (1, -10, 'a'), (2, -4, null), (2, -4, 'b'), (3, -4, 'f');\n"

    qt_phase4_multi_distinct """
        select /*+SET_VAR(disable_nereids_rules='TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT,THREE_PHASE_AGGREGATE_WITH_DISTINCT,THREE_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI')*/
            id,
            group_concat(cast(field1 as varchar), ','),
            count(distinct field1),
            group_concat(cast(field2 as varchar), ','),
            count(distinct field2)
        from agg_4_phase_tbl2
        group by id
        order by id"""

    qt_phase4_single_distinct """
        select /*+SET_VAR(disable_nereids_rules='TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT,THREE_PHASE_AGGREGATE_WITH_DISTINCT,THREE_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI')*/
            id,
            group_concat(cast(field1 as varchar), ','),
            count(distinct field1)
        from agg_4_phase_tbl2
        group by id
        order by id"""
}
