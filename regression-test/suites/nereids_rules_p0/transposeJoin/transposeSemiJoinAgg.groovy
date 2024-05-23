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

suite("transposeSemiJoinAgg") {
    // filter about invisible column "DORIS_DELETE_SIGN = 0" has no impaction on partition pruning
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set partition_pruning_expand_threshold=10;"
    sql "set ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set disable_nereids_rules='PRUNE_EMPTY_PARTITION,TRANSPOSE_LOGICAL_AGG_SEMI_JOIN,TRANSPOSE_LOGICAL_AGG_SEMI_JOIN_PROJECT'"

    sql "drop table if exists T1;"
    sql """
        CREATE TABLE T1 (
        a INT NULL,
        b INT NULL,
        c INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000"
        ); """
    
    sql "drop table if exists T2;"
    sql """
        CREATE TABLE T2 (
        a INT NULL,
        b INT NULL,
        c INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000"
        );
        """

    sql "set enable_runtime_filter_prune=false;"
    sql '''
    alter table T1 modify column a set stats ('ndv'='5999989709', 'num_nulls'='0', 'row_count'='5999989709');
    '''
    sql '''
    alter table T1 modify column b set stats ('ndv'='5999989709', 'num_nulls'='0', 'row_count'='5999989709');
    '''
    sql '''
    alter table T2 modify column a set stats ('ndv'='100', 'num_nulls'='0', 'row_count'='100');
    '''

    sql "drop table if exists T3;"
    sql """
        CREATE TABLE T3 (
        str varchar(100),
        len  int
        ) DUPLICATE KEY(str)
        DISTRIBUTED BY HASH(str) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // RULE: TransposeSemiJoinAggProject
    // 1. group-by(without grouping sets) 
    // agg-leftSemi => leftSemi-agg
    qt_groupby_positive_case """
        explain shape plan
        select T3.a
        from (select a, b, sum(c) from T1 group by a, b) T3
        left semi join T2 on T3.a=T2.a;
    """

    // agg-leftSemi: agg not pushed down
    qt_groupby_negative_case """
        explain shape plan
        select T3.a
        from (select a, b, sum(c) as d from T1 group by a, b) T3
        left semi join T2 on T3.D=T2.a;
        """

    // 2 grouping sets
    // agg-leftSemi => leftSemi-agg
    qt_grouping_positive_case """
        explain shape plan
        select T3.a
        from (select a, b, sum(c) from T1 group by grouping sets ((a, b), (a))) T3
        left semi join T2 on T3.a=T2.a;
    """

    // agg-leftSemi: agg not pushed down
    qt_grouping_negative_case """
        explain shape plan
        select T3.a
        from (select a, b, sum(c) as D from T1 group by grouping sets ((a, b), (a), ())) T3
        left semi join T2 on T3.D=T2.a;
    """

    // RULE: TransposeSemiJoinAgg
    // 1. group-by(without grouping sets) 
    // agg-leftSemi => leftSemi-agg
    qt_groupby_positive_case2 """
        explain shape plan
        select T3.a
        from (select a from T1 group by a) T3
        left semi join T2 on T3.a=T2.a;
    """

    // agg-leftSemi: agg not pushed down
    qt_groupby_negative_case2 """
        explain shape plan
        select T3.D
        from (select sum(c) as D from T1 group by a) T3
        left semi join T2 on T3.D=T2.a;
        """

    // 2 grouping sets
    // agg-leftSemi => leftSemi-agg
    qt_grouping_positive_case2 """
        explain shape plan
        select T3.a
        from (select a from T1 group by grouping sets ((a, b), (a))) T3
        left semi join T2 on T3.a=T2.a;
    """
    // agg-leftSemi: agg not pushed down
    qt_grouping_negative_case2 """
        explain shape plan
        select T3.D
        from (select sum(C) as D from T1 group by grouping sets ((a, b), (a), ())) T3
        left semi join T2 on T3.D=T2.a;
        """
    // https://github.com/apache/doris/issues/31308
    qt_groupby_negative_case3 """
        explain shape plan
        select case when len in (select len from T3) then 1 else 1 end c1 from T3 group by len;
        """
}