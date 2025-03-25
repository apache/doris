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

suite("test_aggregate_window_functions") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // approx_count_distinct 
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        create table test_aggregate_window_functions (
            id bigint,
            name varchar(20),
            province varchar(20)
        ) distributed by hash(id) properties('replication_num' = '1');
    """
    sql """
        insert into test_aggregate_window_functions values
            (1, 'zhangsan', "sichuan"),
            (4, 'zhangsan', "sichuan"),
            (11, 'zhuge', "sichuan"),
            (13, null, "sichuan"),
            (2, 'lisi', "chongqing"),
            (5, 'lisi2', "chongqing"),
            (3, 'wangwu', "hubei"),
            (6, 'wangwu2', "hubei"),
            (12, "quyuan", 'hubei'),
            (7, 'liuxiang', "beijing"),
            (8, 'wangmang', "beijing"),
            (9, 'liuxiang2', "beijing"),
            (10, 'wangmang', "beijing");
    """
    order_qt_agg_window_approx_count_distinct "select province, approx_count_distinct(name) over(partition by province) from test_aggregate_window_functions;"

    // count_by_enum
    order_qt_agg_window_count_by_enum "select province, count_by_enum(name) over(partition by province order by name) from test_aggregate_window_functions;"

    // avg_weighted
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        create table test_aggregate_window_functions (
            id bigint,
            price decimalv3(38, 10),
            count bigint
        ) distributed by hash(id) properties('replication_num' = '1');
    """
    sql """
        insert into test_aggregate_window_functions values
            (1, 123456789.0000000001, 100),
            (1, 223456789.0000000004, 200),
            (1, 323456789.0000000002, 200),
            (1, 423456789.0000000005, 300),
            (1, 523456789.0000000005, null),
            (1, 523456789.0000000005, 1000),
            (1, 523456789.0000000005, 2000),
            (2, 123456789.0000000005, 400),
            (2, 123456789.0000000006, 500),
            (2, 223456789.0000000005, 300),
            (2, 323456789.0000000005, 100),
            (2, 423456789.0000000005, 1000),
            (3, 123456789.1000000005, 200),
            (3, 223456789.6000000005, 1000),
            (3, 223456789.6000000005, null),
            (3, 323456789.1000000005, 2000),
            (3, 423456789.2000000005, 3000);
    """
    order_qt_agg_window_avg_weighted "select id, avg_weighted(price, count) over(partition by id) from test_aggregate_window_functions;"

    // corr
    order_qt_agg_window_corr "select id, corr(price, count) over(partition by id) from test_aggregate_window_functions;"
    // covar_samp
    order_qt_agg_window_covar_samp "select id, covar_samp(price, count) over(partition by id) from test_aggregate_window_functions;"
    // covar_pop
    order_qt_agg_window_covar_pop "select id, covar_pop(price, count) over(partition by id) from test_aggregate_window_functions;"

    // variance_pop
    order_qt_agg_window_variance_pop "select id, variance_pop(price) over(partition by id) from test_aggregate_window_functions;"
    // stddev_pop
    order_qt_agg_window_stddev_pop "select id, stddev_pop(price) over(partition by id) from test_aggregate_window_functions;"

    // variance_samp
    order_qt_agg_window_variance_samp "select id, variance_samp(price) over(partition by id) from test_aggregate_window_functions;"
    // stddev_samp
    order_qt_agg_window_stddev_samp "select id, stddev_samp(price) over(partition by id) from test_aggregate_window_functions;"

    // group_bit_or
    order_qt_agg_window_group_bit_or "select id, group_bit_or(count) over(partition by id) from test_aggregate_window_functions;"
    // group_bit_and
    order_qt_agg_window_group_bit_and "select id, group_bit_and(count) over(partition by id) from test_aggregate_window_functions;"
    // group_bit_xor
    order_qt_agg_window_group_bit_xor "select id, group_bit_xor(count) over(partition by id) from test_aggregate_window_functions;"

    // bitmap_agg
    order_qt_agg_window_bitmap_agg "select id, bitmap_to_string(bitmap_agg(count) over(partition by id)) from test_aggregate_window_functions;"

    // BITMAP_UNION_INT
    order_qt_agg_window_bitmap_union_int "select id, bitmap_union_int(count) over(partition by id) from test_aggregate_window_functions;"

    // histogram
    order_qt_agg_window_histogram "select id, histogram(count) over(partition by id) from test_aggregate_window_functions;"

    // max_by
    order_qt_agg_window_max_by "select id, count, max_by(price, count) over(partition by id order by count) from test_aggregate_window_functions;"
    // min_by
    order_qt_agg_window_min_by "select id, count, min_by(price, count) over(partition by id order by count) from test_aggregate_window_functions;"
    // any_value
    order_qt_agg_window_any_value "select id, any_value(price) over(partition by id order by price) from test_aggregate_window_functions;"

    // percentile
    order_qt_agg_window_percentile "select id, percentile(price, 0.95) over(partition by id) from test_aggregate_window_functions;"
    // percentile_array
    order_qt_agg_window_percentile_array "select id, percentile_array(price, array(0.25, 0.5, 0.75)) over(partition by id) from test_aggregate_window_functions;"
    // percentile_approx
    order_qt_agg_window_percentile_approx "select id, percentile_approx(price, 0.95) over(partition by id) from test_aggregate_window_functions;"
    // percentile_approx_weighted
    order_qt_agg_window_percentile_approx_weighted "select id, percentile_approx_weighted(price, count, 0.95) over(partition by id) from test_aggregate_window_functions;"

    // topn
    order_qt_agg_window_topn "select id, topn(price, 3) over(partition by id) from test_aggregate_window_functions;"    
    // topn_weighted
    order_qt_agg_window_topn_weighted "select id, topn_weighted(price, count, 3) over(partition by id) from test_aggregate_window_functions;"
    // topn_array
    order_qt_agg_window_topn_array "select id, topn_array(price, 3) over(partition by id) from test_aggregate_window_functions;"

    // multi_distinct_count
    order_qt_agg_window_multi_distinct_count "select id, multi_distinct_count(price) over(partition by id) from test_aggregate_window_functions;"

    // multi_distinct_count_distribute_key, FE not implemented yet
    // order_qt_agg_window_multi_distinct_count_distribute_key "select id, multi_distinct_distribute_key(id) over(partition by id) from test_aggregate_window_functions;"
    // order_qt_agg_window_multi_distinct_count_distribute_key "select id, multi_distinct_count_distribute_key(price) over(partition by id) from test_aggregate_window_functions;"

    // multi_distinct_sum
    order_qt_agg_window_multi_distinct_sum "select id, multi_distinct_sum(price) over(partition by id) from test_aggregate_window_functions;"

    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        create table test_aggregate_window_functions (
            id bigint,
            id2 bigint,
            user_ids varchar(64)
        ) distributed by hash(id) properties('replication_num' = '1');
    """
    sql """
        insert into test_aggregate_window_functions values
            (1, 1, '1,2'),
            (1, 3, '1,2'),
            (1, 2, '1,3'),
            (1, 6, null),
            (2, 2, '2,3'),
            (2, 5, '2,3'),
            (2, 9, '2,4'),
            (3, 10, '3'),
            (3, 1, '4'),
            (3, 5, '5'),
            (3, 9, '6');
    """
    // bitmap_union
    order_qt_agg_window_bitmap_union "select id, bitmap_to_string(bitmap_union(bitmap_from_string(user_ids)) over(partition by id)) from test_aggregate_window_functions;"
    // bitmap_intersect
    order_qt_agg_window_bitmap_intersect "select id, bitmap_to_string(bitmap_intersect(bitmap_from_string(user_ids)) over(partition by id)) from test_aggregate_window_functions;"
    // group_bitmap_xor
    order_qt_agg_window_group_bitmap_xor "select id, bitmap_to_string(group_bitmap_xor(bitmap_from_string(user_ids)) over(partition by id)) from test_aggregate_window_functions;"
    // bitmap_union_count
    order_qt_agg_window_bitmap_union_count "select id, bitmap_union_count(bitmap_from_string(user_ids)) over(partition by id) from test_aggregate_window_functions;"

    // collect_list
    order_qt_agg_window_collect_list "select id, collect_list(user_ids) over(partition by id order by user_ids) from test_aggregate_window_functions;"
    // collect_set
    order_qt_agg_window_collect_set "select id, collect_set(user_ids) over(partition by id order by user_ids) from test_aggregate_window_functions;"
    // array_agg
    order_qt_agg_window_array_agg "select id, array_agg(user_ids) over(partition by id order by user_ids) from test_aggregate_window_functions;"

    // group_concat
    order_qt_agg_window_group_concat "select id, group_concat(user_ids) over(partition by id order by user_ids) from test_aggregate_window_functions;"
    // group_concat distinct
    // DISTINCT not allowed in analytic function: group_concat(line 1, pos 11)
    // order_qt_agg_window_group_concat_distinct "select id, group_concat(distinct user_ids) over(partition by id) from test_aggregate_window_functions;"
    // group_concat order by
    // java.sql.SQLException: errCode = 2, detailMessage = Cannot invoke "org.apache.doris.analysis.Expr.getChildren()" because "root" is null
    // order_qt_agg_window_group_concat_order_by "select id, group_concat(user_ids order by id2) over(partition by id) from test_aggregate_window_functions;"
    // sum0
    order_qt_agg_window_sum0 "select id, sum0(id2) over(partition by id) from test_aggregate_window_functions;"

    // group_array_intersect
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        create table test_aggregate_window_functions (
            id bigint,
            user_ids array<varchar(64)>
        ) distributed by hash(id) properties('replication_num' = '1');
    """
    sql """
        insert into test_aggregate_window_functions values
            (1, [1,2]),
            (1, [1,2]),
            (1, [1,3]),
            (1, null),
            (2, [2,3]),
            (2, [2,3]),
            (2, [2,4]),
            (3, [3,4]),
            (3, [4,3,null]),
            (3, [5,3,4]),
            (3, [3,6,4]);
    """
    order_qt_agg_window_group_array_intersect "select id, array_sort(group_array_intersect(user_ids) over(partition by id)) from test_aggregate_window_functions;"

    // hll_union_agg
    sql """drop TABLE if EXISTS test_window_func_hll;"""
    sql """
            create table test_window_func_hll(
                dt date,
                id int,
                name char(10),
                province char(10),
                os char(10),
                pv hll hll_union
            )
            Aggregate KEY (dt,id,name,province,os)
            distributed by hash(id) buckets 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """
            insert into test_window_func_hll
            SELECT
               dt,id,name,province,os,pv
            from (
               SELECT   '2022-05-05' as dt,'10001' as id,'test01' as name,'beijing' as province,'windows' as os,hll_hash('windows') as pv
               union all
               SELECT   '2022-05-05' as dt,'10002' as id,'test01' as name,'beijing' as province,'linux' as os,hll_hash('linux') as pv
               union all
               SELECT   '2022-05-05' as dt,'10003' as id,'test01' as name,'beijing' as province,'macos' as os,hll_hash('macos') as pv
               union all
               SELECT   '2022-05-05' as dt,'10004' as id,'test01' as name,'hebei' as province,'windows' as os,hll_hash('windows') as pv
               union all
               SELECT   '2022-05-06' as dt,'10001' as id,'test01' as name,'shanghai' as province,'windows' as os,hll_hash('windows') as pv
               union all
               SELECT   '2022-05-06' as dt,'10002' as id,'test01' as name,'shanghai' as province,'linux' as os,hll_hash('linux') as pv
               union all
               SELECT   '2022-05-06' as dt,'10003' as id,'test01' as name,'jiangsu' as province,'macos' as os,hll_hash('macos') as pv
               union all
               SELECT   '2022-05-06' as dt,'10004' as id,'test01' as name,'shanxi' as province,'windows' as os,hll_hash('windows') as pv
               union all
               SELECT   '2022-05-07' as dt,'10005' as id,'test01' as name,'shanxi' as province,'windows' as os,hll_empty() as pv
            ) as a
        """
    order_qt_window_func_hll_union_agg "select province, os, hll_union_agg(pv) over(partition by province) from test_window_func_hll;"
    order_qt_window_func_hll_union "select province, os, hll_cardinality(hll_union(pv) over(partition by province)) from test_window_func_hll;"

    // map_agg
    sql "DROP TABLE IF EXISTS `test_map_agg`;"
    sql """
        CREATE TABLE IF NOT EXISTS `test_map_agg` (
            `id` int(11) NOT NULL,
            `label_name` varchar(32) NOT NULL,
            `value_field` string
        )
        DISTRIBUTED BY HASH(`id`)
        PROPERTIES (
        "replication_num" = "1"
        );
     """

    sql """
        insert into `test_map_agg` values
            (1, "LA", "V1_1"),
            (1, "LB", "V1_2"),
            (1, "LC", "V1_3"),
            (2, "LA", "V2_1"),
            (2, "LB", "V2_2"),
            (2, "LC", "V2_3"),
            (3, "LA", "V3_1"),
            (3, "LB", "V3_2"),
            (3, "LC", "V3_3"),
            (4, "LA", "V4_1"),
            (4, "LB", "V4_2"),
            (4, "LC", "V4_3"),
            (5, "LA", "V5_1"),
            (5, "LB", "V5_2"),
            (5, "LC", "V5_3");
    """
    order_qt_map_agg "select id, map_agg(label_name, value_field) over(partition by id) from test_map_agg;"

    // quantile_state
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        CREATE TABLE test_aggregate_window_functions (
            `dt` int(11) NULL COMMENT "",
            `id` int(11) NULL COMMENT "",
            `price` quantile_state QUANTILE_UNION NOT NULL COMMENT ""
           ) ENGINE=OLAP
           AGGREGATE KEY(`dt`, `id`)
           DISTRIBUTED BY HASH(`dt`)
           PROPERTIES (
                  "replication_num" = "1"
            );
        """
    sql """INSERT INTO test_aggregate_window_functions values(20220201,0, to_quantile_state(1, 2048))"""
    sql """INSERT INTO test_aggregate_window_functions values(20220201,1, to_quantile_state(-1, 2048)),
            (20220201,1, to_quantile_state(0, 2048)),(20220201,1, to_quantile_state(1, 2048)),
            (20220201,1, to_quantile_state(2, 2048)),(20220201,1, to_quantile_state(3, 2048))
        """

    // quantile_union
    order_qt_agg_window_quantile_union """select dt, id, quantile_percent(quantile_union(price), 0.5) from test_aggregate_window_functions group by dt, id;"""

    // retention
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        CREATE TABLE test_aggregate_window_functions(
            id TINYINT,
            action STRING,
            time DATETIME
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)
        PROPERTIES (
                 "replication_num" = "1"
           );
    """

    sql """
        INSERT INTO test_aggregate_window_functions VALUES 
        (1,'pv','2022-01-01 08:00:05'),
        (2,'pv','2022-01-01 10:20:08'),
        (1,'buy','2022-01-02 15:30:10'),
        (2,'pv','2022-01-02 17:30:05'),
        (3,'buy','2022-01-01 05:30:09'),
        (3,'buy','2022-01-02 08:10:15'),
        (4,'pv','2022-01-02 21:09:15'),
        (5,'pv','2022-01-01 22:10:53'),
        (5,'pv','2022-01-02 19:10:52'),
        (5,'buy','2022-01-02 20:00:50');
        """
    order_qt_agg_window_retention_0 """
        select id, retention(action='pv' and to_date(time)='2022-01-01',
                              action='buy' and to_date(time)='2022-01-02') as retention 
        from test_aggregate_window_functions
        group by id;"""
    order_qt_agg_window_retention_1 """
        select id, retention(action='pv' and to_date(time)='2022-01-01',
                              action='buy' and to_date(time)='2022-01-02') over (partition by id) as retention 
        from test_aggregate_window_functions;"""

    // sequence_match and sequence_count
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        CREATE TABLE test_aggregate_window_functions(
            `uid` int,
            `date` datetime, 
            `number` int
        ) DISTRIBUTED BY HASH(uid)
        PROPERTIES (
                 "replication_num" = "1"
           );
    """
    sql """
        INSERT INTO test_aggregate_window_functions values
            (1, '2022-11-01 10:41:00', 1),
            (1, '2022-11-01 11:41:00', 5),
            (1, '2022-11-01 12:41:00', 7),
            (1, '2022-11-01 12:42:00', 9),
            (1, '2022-11-01 12:52:00', 1),
            (1, '2022-11-01 13:41:00', 4),
            (1, '2022-11-01 13:51:00', 3),
            (1, '2022-11-01 14:51:00', 5),
            (2, '2022-11-01 20:41:00', 1),
            (2, '2022-11-01 23:51:00', 3),
            (2, '2022-11-01 22:41:00', 7),
            (2, '2022-11-01 22:42:00', 9),
            (2, '2022-11-01 23:41:00', 4);
    """
    order_qt_agg_window_sequence_match "select uid, sequence_match('(?1)(?2)', date, number = 1, number = 5) over(partition by uid) from test_aggregate_window_functions;"
    order_qt_agg_window_sequence_count "select uid, sequence_count('(?1)(?2)', date, number = 1, number = 5) over(partition by uid) from test_aggregate_window_functions;"

    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
       CREATE TABLE test_aggregate_window_functions(
              `id` INT(11) null COMMENT "",
              `a` array<INT> null  COMMENT "",
              `b` array<array<INT>>  null COMMENT "",
              `s` array<String>  null  COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into test_aggregate_window_functions values
    (1,[1,2,3],[[1],[1,2,3],[2]],["ab","123","114514"]),
    (2,[20],[[2]],["cd"]),
    (3,[100],[[1]],["efg"]) , 
    (4,null,[null],null),
    (5,[null,2],[[2],null],[null,'c']);
   """
    // sum_foreach
    order_qt_agg_window_sum_foreach "select id, sum_foreach(a) over(partition by id) from test_aggregate_window_functions;"
    order_qt_agg_window_sum_foreach2 "select id, sum_foreach(a) over(order by id rows between 2 preceding and 1 preceding) from test_aggregate_window_functions;"
    // covar_foreach
    order_qt_agg_window_covar_foreach "select id, covar_foreach(a, a) over(partition by id) from test_aggregate_window_functions;"

    sql "drop table if exists test_aggregate_window_functions"

    sql """
        CREATE TABLE IF NOT EXISTS `test_aggregate_window_functions` (
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
        INSERT INTO `test_aggregate_window_functions` VALUES 
        ( 1, 1, 'string1', 'string3', ['s11', 's12', 's13'] ),
        ( 1, 2, 'string2', 'string1', ['s21', 's22', 's23'] ),
        ( 2, 3, 'string3', 'string2', ['s31', 's32', 's33'] ),
        ( 1, 1, 'string1', 'string3', ['s11', 's12', 's13'] ),
        ( 1, 2, 'string2', 'string1', ['s21', 's22', 's23'] ),
        ( 2, 3, 'string3', 'string2', ['s31', 's32', 's33'] );
    """

    order_qt_agg_window_group_concat_state1 "select kint, group_concat(kstr) over(partition by kint) from test_aggregate_window_functions;"
    sql "select kint, group_concat_union(group_concat_state(kstr)) over(partition by kint) from test_aggregate_window_functions;"
    order_qt_agg_window_group_concat_state_merge "select kint, group_concat_merge(group_concat_state(kstr)) over(partition by kint) from test_aggregate_window_functions;"

    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """ CREATE TABLE IF NOT EXISTS test_aggregate_window_functions (
        tag_group bigint(20) NULL COMMENT "标签组",
        bucket int(11) NOT NULL COMMENT "分桶字段",
        members bitmap BITMAP_UNION  COMMENT "人群") ENGINE=OLAP
        AGGREGATE KEY(tag_group, bucket)
        DISTRIBUTED BY HASH(bucket) BUCKETS 64
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1");
    """
    sql """
        insert into test_aggregate_window_functions values
        (1, 1, bitmap_from_string('1,2,3,4')),
        (2, 1, bitmap_from_string('1,2,3')),
        (3, 1, bitmap_from_string('1,2')),
        (1, 2, bitmap_from_string('2,3,4,5,6')),
        (2, 2, bitmap_from_string('2,3,4')),
        (3, 2, bitmap_from_string('2,3'));
    """
    order_qt_agg_window_orthogonal_bitmap1 "select bucket, bitmap_to_string(orthogonal_bitmap_intersect(members, tag_group, 1, 2, 3) over(partition by bucket)) from test_aggregate_window_functions;"
    order_qt_agg_window_orthogonal_bitmap2 "select bucket, orthogonal_bitmap_intersect_count(members, tag_group, 1, 2, 3) over(partition by bucket) from test_aggregate_window_functions;"
    order_qt_agg_window_orthogonal_bitmap3 "select bucket, orthogonal_bitmap_union_count(members) over(partition by bucket) from test_aggregate_window_functions;"

    // window_funnel
    sql """
        drop table if exists test_aggregate_window_functions;
    """
    sql """
        CREATE TABLE test_aggregate_window_functions(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(event_timestamp) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO test_aggregate_window_functions VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '登录2', '2022-05-14 10:03:00', 'HONOR', 3),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    order_qt_agg_window_window_funnel """
        select user_id, window_funnel(3600, "fixed", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') over(partition by user_id) from test_aggregate_window_functions;
    """

}