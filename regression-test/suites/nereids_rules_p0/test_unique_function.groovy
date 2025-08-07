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

suite('test_unique_scalar_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

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

    def tbl = "unique_scalar_function_tbl1"
    sql "drop table if exists ${tbl} force"
    sql "create table ${tbl} (k int, a double) properties('replication_num' = '1')"

    // test no aggregate
    sql """
            select random(),  random(), random() - random()
        """

    sql """
           select a + random(), a + random()
           from ${tbl}
        """

    sql """
            select a + random(), a + random()
            from ${tbl}
            where a + random() > 1.5
        """

    sql """
            select a + random(), a + random(), sum(a + random()) over(), sum(a + random()) over()
            from ${tbl}
        """

    sql """
            select a + random(), a + random()
            from ${tbl}
            qualify sum(a +random()) over (partition by a + random()) > 1.5 and sum(a + random()) over() > 1.5
        """

    sql """
            select a + random(), a + random(), sum(a + random()) over (partition by a + random())
            from ${tbl}
            qualify sum(a +random()) over (partition by a + random()) > 1.5 and sum(a + random()) over() > 1.5
        """

    sql """
            select a + random(), a + random(), sum(a + random()) over (partition by a + random())
            from ${tbl}
            order by a + random(), a + random(), sum(a+random()), sum(a+random())
        """

    // test one row to global aggregate
    sql """
            select random(), random(), sum(random()), sum(random()), sum(random()) over(), sum(random()) over()
        """

    // test project to global aggregate
    sql """
            select random(), random(), sum(a + random()), sum(a + random())
            from ${tbl}
        """

    sql """
            select random(), random()
            from ${tbl}
            having random() > 0.5 and random() > 0.5
        """

    sql """
            select random(), random()
            from ${tbl}
            having random() > 0.5 and random() > 0.5 and sum(random()) > 0.5 and sum(random()) > 0.5
        """

    sql """
           select random(), random(), sum(a + random()), sum(a + random())
           from ${tbl}
           having random() > 0.5 and sum(random()) > 0.5
           order by random(), sum(a + random())
        """

    // test distinct project to aggregate
    sql """
            select distinct random(), random()
        """

    sql """
            select distinct random(), random(), sum(random()), sum(random())
        """

    sql """
            select distinct a + random(), a + random()
            from ${tbl}
            order by a + random()
        """

    // test with group by
    sql """
            select random(), random(), sum(random()), sum(random())
            from ${tbl}
            group by random()
        """

    sql """
            select random(), random(), sum(random()), sum(random())
            from ${tbl}
            group by random()
            having random() > 0.5
        """

    sql """
            select random(), random(), abs(random()), sum(random()), sum(random())
            from ${tbl}
            group by random()
            having random() > 0.5
            order by random()
        """

    sql """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a
        """

    sql """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a
            having a + random() > 0.5
        """

    sql """
            select a + random(), a + random(), abs(a + random()), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a
            having a + random() > 0.5
            order by a + random()
        """

    sql """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random()
        """

    sql """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random()
            having a + random() > 0.5
        """

    sql """
            select a + random(), a + random(), abs(a + random()), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random()
            having a + random() > 0.5
            order by a + random()
        """

    sql """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random(), a + random()
        """

    sql """
            select a + random(), a + random(), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random(), a + random()
            having a + random() > 0.5
        """

    sql """
            select a + random(), a + random(), abs(a + random()), sum(a + random()), sum(a + random())
            from ${tbl}
            group by a + random(), a + random()
            having a + random() > 0.5
            order by a + random()
        """

    sql """
            select a + random(), a + random() + 1, sum(a + random()), sum(a + random() + 1)
            from ${tbl}
            group by a + random(), a + random() + 1
        """

    sql """
            select a + random(), a + random() + 1, sum(a + random()), sum(a + random() + 1)
            from ${tbl}
            group by a + random(), a + random() + 1
            having a + random() > 0.5
        """

    sql """
            select a + random(), a + random() + 1, abs(a + random()), sum(a + random()), sum(a + random() + 1)
            from ${tbl}
            group by a + random(), a + random() + 1
            having a + random() > 0.5
            order by a + random()
        """

    sql """
            select a + random(), a + random() + 1, sum(a + random()), sum(a + random() + 1)
            from ${tbl}
            group by a + random(), a + random() + 1
            having a + random() + 1 > 0.5
        """

    sql """
            select a + random(), a + random() + 1, abs(a + random()), sum(a + random()), sum(a + random() + 1)
            from ${tbl}
            group by a + random(), a + random() + 1
            having a + random() + 1 > 0.5
            order by a + random() + 1
        """

    // test with repeat
}
