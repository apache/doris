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

suite('test_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // column index start from 0
    def explainAndCheckResult = { equalColumnGroups, tag, sqlStr ->
        "qt_${tag}_shape"          "explain shape plan ${sqlStr}"

        def maxIterate = 3 
        for (int k = 1; k <= maxIterate; k++) {
            def result = sql "${sqlStr}"
            if (result.isEmpty()) {
                continue
            }

            def checkColumnDiffSucc = true
            for (def row : result) {
                // check equal columns
                for (def group : equalColumnGroups) {
                    for (def i = 1; i < group.size(); i++) {
                        assertEquals(row[group[0]], row[group[i]],
                            "expect column ${group[0]} = column ${group[i]}, but failed, rows: ${result}\n")
                    }
                }

                // check not equal columns
                for (int i=0; i < equalColumnGroups.size(); i++) {
                    for (int j=i+1; j<equalColumnGroups.size(); j++) {
                        def val1 = row[equalColumnGroups[i][0]]
                        def val2 = row[equalColumnGroups[j][0]]
                        if (val1 == val2) {
                            checkColumnDiffSucc = false
                            if (k == maxIterate) {
                                assertNotEquals(val1, val2,
                                    "expect column ${equalColumnGroups[i][0]} != column ${equalColumnGroups[j][0]}, but failed, all rows: ${result}\n")
                            }
                        }
                    }
                }
            }
            if (checkColumnDiffSucc) {
                break
            } else {
                log.info("run ${tag} check column diff failed, maybe random() indeed generate same value, so try again, current run times: ${k}")
            }
        }
    }

    qt_filter_through_project_1 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999
        '''

    qt_filter_through_project_2 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999
        '''

    qt_filter_through_project_3 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + random(1, 10) + 200 as b, id + random(1, 10) + 300 as c from t1) t where a > 999 and b > 999
        '''

    qt_filter_through_project_4 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a + random(1, 10) > 999 and b + random(1, 10) > 999
        '''

    qt_filter_through_project_5 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999 limit 10
        '''

    qt_filter_through_project_6 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999 limit 10
        '''

    qt_filter_through_project_7 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + random(1, 10) + 200 as b, id + random(1, 10) + 300 as c from t1) t where a > 999 and b > 999 limit 10
        '''

    qt_filter_through_project_8 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a + random(1, 10) > 999 and b + random(1, 10) > 999 limit 10
        '''

    qt_merge_project_1 '''
        explain shape plan select a as b, a as c from (select id + 100 as a from t1) t
        '''

    qt_merge_project_2 '''
        explain shape plan select a as b, a as c from (select id + random(1, 10) as a from t1) t
        '''

    qt_merge_project_3 '''
        explain shape plan select a as b from (select id + random(1, 10) as a from t1) t
        '''

    qt_merge_project_4 '''
        explain shape plan select a + 10 + a as b from (select id + random(1, 10) as a from t1) t
        '''

    qt_merge_project_5 '''
        explain shape plan select a as b, a + 10 as c from (select id + random(1, 10) as a from t1) t
        '''

    qt_push_down_filter_through_agg '''
        explain shape plan
        select id, sum(id)
        from t1
        where id + random(1, 10) > 5
        group by id
        having id + random(1, 10) > 6
        '''

    def tbl = "unique_scalar_function_tbl1"
    sql "drop table if exists ${tbl} force"
    sql "create table ${tbl} (k int, a double) properties('replication_num' = '1')"
    sql "insert into ${tbl} values (0, 0)"

    // test no aggregate
    explainAndCheckResult [[0], [1]], 'check_equal_no_agg_1', """
            select random(),  random()
        """

    explainAndCheckResult [[0], [1]], 'check_equal_no_agg_2', """
           select a + random(), a + random()
           from ${tbl}
        """

    explainAndCheckResult [[0], [1]], 'check_equal_no_agg_3', """
            select a + random(), a + random()
            from ${tbl}
            where a + random() > 0.01
        """

    explainAndCheckResult [[0], [1], [2], [3]], 'check_equal_no_agg_4', """
            select a + random(), a + random(), sum(a + random()) over(), sum(a + random()) over()
            from ${tbl}
        """

    explainAndCheckResult [[0], [1]], 'check_equal_no_agg_5', """
            select a + random(), a + random()
            from ${tbl}
            qualify sum(a +random()) over (partition by a + random()) > 0.01 and sum(a + random()) over() > 0.01
        """

    explainAndCheckResult [[0], [1], [2]], 'check_equal_no_agg_6', """
            select a + random(), a + random(), sum(a + random()) over (partition by a + random())
            from ${tbl}
            qualify sum(a +random()) over (partition by a + random()) > 0.01 and sum(a + random()) over() > 0.01
        """

    
    // BUG: LOGICAL_SORT can not contains AggregateFunction expression: sum((a + random()))
    // explainAndCheckResult [[0], [1], [2]], 'check_equal_no_agg_7', """
    //         select a + random(), a + random(), sum(a + random()) over (partition by a + random())
    //         from ${tbl}
    //         order by a + random(), a + random(), sum(a+random()) over(), sum(a+random()) over()
    //     """

    // test one row to global aggregate
    explainAndCheckResult [[0], [1], [2], [3], [4], [5]], 'check_equal_one_row_to_agg_1', """
            select random(), random(), sum(random()), sum(random()), sum(random()) over(), sum(random()) over()
        """

    // test project to global aggregate
    explainAndCheckResult [[0], [1], [2], [3]], 'check_equal_project_to_agg_1', """
            select random(), random(), sum(a + random()), sum(a + random())
            from ${tbl}
        """

    explainAndCheckResult [[0], [1]], 'check_equal_project_to_agg_2', """
            select random(), random()
            from ${tbl}
            having random() > 0.01 and random() > 0.01
        """

    explainAndCheckResult [[0], [1]], 'check_equal_project_to_agg_3', """
           select random(), random(), sum(a + random()), sum(a + random())
           from ${tbl}
           having random() > 0.01 and sum(random()) > 0.01
           order by random(), sum(a + random())
        """

    explainAndCheckResult [[0], [1]], 'check_equal_having_to_agg_1', """
            select random(), random()
            from ${tbl}
            having random() > 0.01 and random() > 0.01 and sum(random()) > 0.01 and sum(random()) > 0.01
        """

    // test distinct project to aggregate
    explainAndCheckResult [[0], [1]], 'check_equal_distinct_to_agg_1', """
            select distinct random(), random()
        """

    explainAndCheckResult [[0], [1], [2], [3]], 'check_equal_distinct_to_agg_2', """
            select distinct random(), random(), sum(random()), sum(random())
        """

    explainAndCheckResult [[0], [1]], 'check_equal_distinct_to_agg_3', """
            select distinct a + random(), a + random()
            from ${tbl}
            order by a + random()
        """

    // test with group by
    explainAndCheckResult [[0,1,2,3]], 'check_equal_agg_with_groupby_1', """
            select random(), random(), sum(random()), sum(random())
            from ${tbl}
            group by random()
        """

    explainAndCheckResult [[0,1,2,3]], 'check_equal_agg_with_groupby_2', """
            select random(), random(), sum(random()), sum(random())
            from ${tbl}
            group by random()
            having random() > 0.01
        """

    explainAndCheckResult [[0,1,2,3,4]], 'check_equal_agg_with_groupby_3', """
            select random(), random(), abs(random()), sum(random()), sum(random())
            from ${tbl}
            group by random()
            having random() > 0.01
            order by random()
        """

    explainAndCheckResult [[0],[1],[2],[3]], 'check_equal_agg_with_groupby_4', """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a
        """

    explainAndCheckResult [[0],[1],[2],[3]], 'check_equal_agg_with_groupby_5', """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a
            having a + random() > 0.01
        """

    explainAndCheckResult [[0],[1],[2],[3],[4]], 'check_equal_agg_with_groupby_6', """
            select a + random(), a + random(), abs(a + random()), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a
            having a + random() > 0.01
            order by a + random()
        """

    explainAndCheckResult [[0,1,2,3]], 'check_equal_agg_with_groupby_7', """
            select a + random(), a + random(), sum(a + random()), sum(a + random()) over()
            from ${tbl}
            group by a + random()
        """

    explainAndCheckResult [[0,1,2,3]], 'check_equal_agg_with_groupby_8', """
            select a + random(), a + random(), sum(a + random()), sum(a + random()) over()
            from ${tbl}
            group by a + random()
            having a + random() > 0.01
        """

    explainAndCheckResult [[0,1,2,3,4]], 'check_equal_agg_with_groupby_9', """
            select a + random(), a + random(), abs(a + random()), sum(a + random()), sum(a + random()) over()
            from ${tbl}
            group by a + random()
            having a + random() > 0.01
            order by a + random()
        """

    explainAndCheckResult [[0,1,2,3]], 'check_equal_agg_with_groupby_10', """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random(), a + random()
        """

    explainAndCheckResult [[0,1,2,3]], 'check_equal_agg_with_groupby_11', """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random(), a + random()
            having a + random() > 0.01
        """

    explainAndCheckResult [[0,1,2,3,4]], 'check_equal_agg_with_groupby_12', """
            select a + random(), a + random(), abs(a + random()), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random(), a + random()
            having a + random() > 0.01
            order by a + random()
        """

    explainAndCheckResult [[0,2],[1,3]], 'check_equal_agg_with_groupby_13', """
            select a + random(), a + random() + 0, sum(a + random()), sum(a + random() + 0)
            from ${tbl}
            group by a + random(), a + random() + 0
        """

    explainAndCheckResult [[0,2],[1,3]], 'check_equal_agg_with_groupby_14', """
            select a + random(), a + random() + 0, sum(a + random()), sum(a + random() + 0)
            from ${tbl}
            group by a + random(), a + random() + 0
            having a + random() > 0.01
            order by a + random()
        """

    explainAndCheckResult [[0,2,3],[1,4]], 'check_equal_agg_with_groupby_15', """
            select a + random(), a + random() + 0, abs(a + random()), sum(a + random()), sum(a + random() + 0)
            from ${tbl}
            group by a + random(), a + random() + 0
            having a + random() + 0 > 0.01
            order by a + random() + 1
        """

    // test with repeat
    explainAndCheckResult [[0,2,3],[1,4]], 'check_equal_repeat1', """
            select a + random(), a + random() + 0, abs(a + random()), sum(a + random()), sum(a + random() + 0)
            from ${tbl}
            group by grouping sets((), (a + random()), (a + random()), (a + random(), a + random() + 0))
            having a + random() + 0 > 0.01
            order by a + random() + 1
        """
}
