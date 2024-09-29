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

suite("test_distinct_agg") {
    sql 'drop table if exists test_distinct_agg_t'

    sql '''
        CREATE TABLE `test_distinct_agg_t` (
            `k1` bigint(20) NULL,
            `k2` varchar(20) NULL,
            `k3` varchar(20) NULL,
            `k4` varchar(20) NULL,
            `k5` varchar(20) NULL,
            `k6` datetime NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    '''

    sql '''
        INSERT INTO `test_distinct_agg_t` (`k1`, `k2`, `k3`, `k4`, `k5`, `k6`) VALUES
            (1, '1234', 'A0', 'C0', '1', '2023-01-10 23:00:00');
    '''

    test {
        sql '''
            select k5, k6, SUM(k3) AS k3 
            from ( 
                select
                    k5,
                    date_format(k6, '%Y-%m-%d') as k6,
                    count(distinct k3) as k3 
                from test_distinct_agg_t 
                where 1=1 
                group by k5, k6
            ) AS temp where 1=1
            group by k5, k6;
        '''
        result([['1', '2023-01-10', 1L]])
    }

    qt_distinct_1 '''SELECT `b`.`dt` AS `dt`
            FROM 
                (SELECT `dt`AS `dt`,
                    count(DISTINCT `role_id`) AS `pay_role`,
                    avg(`cost`) AS `avg_cost`
                FROM 
                    (SELECT `k6` AS `dt`,
                    `k1` AS `role_id`,
                    sum(CAST(`k2` AS INT)) AS `cost`
                    FROM `test_distinct_agg_t`
                    GROUP BY  `dt`, `role_id`) a
                    GROUP BY  `dt`) b 
                WHERE `dt` = '2023-06-18';'''

    sql 'drop view if exists dim_v2'
    sql '''create 
        view `dim_v2` COMMENT 'VIEW' as
        select
        curdate() as `calday`,
            '本日' as `date_tag`
        from
            `test_distinct_agg_t`
        union all
        select
            distinct curdate() as `calday`
            , '本年' as `date_tag`
        from
            `test_distinct_agg_t` t1
        union all
        select
            distinct `t1`.`k1` as `calday`
            , '上年' as `date_tag`
        from
            `test_distinct_agg_t` t1;'''

    sql 'drop view if exists dim_v3'
    sql '''create 
            view `dim_v3` COMMENT 'VIEW' as
            select
                case
                        when `t`.`date_tag` = '月_T+1' then '本月'
                        else `t`.`date_tag`
                    end
                as `date_tag`
            from
                `dim_v2` t 
            left outer join (
                    select
                        distinct `date_tag` as `date_tag`
                    from
                        `dim_v2`
                ) t1 on
                `t`.`date_tag` = `t1`.`date_tag`
            group by
                1;'''

    qt_select1 '''select distinct date_tag from dim_v3 where date_tag='本日';'''

    sql 'drop view if exists dim_v2'
    sql 'drop view if exists dim_v3'
    sql 'drop table if exists test_distinct_agg_t'

    sql "drop table if exists multi_distinct_agg_tab;"

    sql """
    CREATE TABLE `multi_distinct_agg_tab` (
        `k1` bigint(20) NULL,
        `k2` varchar(20) NULL,
        `d1` DECIMAL(18, 0) NULL,
        `d2` DECIMAL(38, 0) NULL
    ) ENGINE = OLAP DUPLICATE KEY(`k1`) DISTRIBUTED BY HASH(`k1`) BUCKETS 2 PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    INSERT INTO
        `multi_distinct_agg_tab` (`k1`, `k2`, `d1`, `d2`)
    VALUES (1, 'aaa', 123, 356),(2, 'bbb', 123, 789), (3, 'ccc', 246, 789);
    """
    sql "sync"

    qt_multi_distinct_1 """
        select
            count(distinct d1),
            count(distinct d2)
        from
            multi_distinct_agg_tab;
    """
    qt_multi_distinct_2 """
        select
            sum(distinct d1),
            sum(distinct d2)
        from
            multi_distinct_agg_tab;
    """
    qt_multi_distinct_3 """
        select
            avg(distinct d1),
            avg(distinct d2)
        from
            multi_distinct_agg_tab;
    """
}