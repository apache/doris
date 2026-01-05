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

suite("analyze_repeat") {
    sql """
        SET enable_fallback_to_original_planner=false;
        SET enable_nereids_planner=true;
        SET ignore_shape_nodes='PhysicalDistribute';
        SET disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        SET runtime_filter_mode=OFF;
        SET disable_join_reorder=true;
        DROP TABLE IF EXISTS tbl_analyze_repeat FORCE;
        CREATE TABLE tbl_analyze_repeat (a bigint, b bigint) properties('replication_num' = '1');
        INSERT INTO tbl_analyze_repeat VALUES
            (null, null), (null, 1), (null, 2),
            (1, 2), (1, 3), (1, null),
            (2, 1), (2, 2), (2, 3), (2, null),
            (3, 1), (3, 2);
        """

    explainAndOrderResult 'repeat_1',  '''
        select 10000
        from tbl_analyze_repeat
        group by grouping sets((a, b))
        having grouping(a) = 1
        '''

    explainAndOrderResult 'repeat_2',  '''
        select 10000
        from tbl_analyze_repeat
        group by grouping sets((a, b))
        order by grouping_id(b, a)
        '''

    explainAndResult 'repeat_3',  '''
        select 10000
        from tbl_analyze_repeat
        group by grouping sets((a, b))
        having grouping_id(a, b) > 0
        order by grouping_id(b, a)
        '''

    explainAndOrderResult 'repeat_4',  '''
        select a, grouping(a), grouping(b)
        from tbl_analyze_repeat
        group by grouping sets((a, b))
        having grouping_id(a, b) > 0
        '''

    explainAndOrderResult 'repeat_5',  '''
        select a, grouping(a), grouping(b)
        from tbl_analyze_repeat
        group by grouping sets((a, b))
        order by grouping_id(b, a)
        '''

    explainAndOrderResult 'repeat_6',  '''
        select a, grouping(a), grouping(b)
        from tbl_analyze_repeat
        group by grouping sets((a, b))
        having grouping_id(a, b) > 0
        order by grouping_id(b, a)
        '''

    // check invalid
    test {
        sql '''
            select grouping(100);
            '''

        exception "LOGICAL_ONE_ROW_RELATION can not contains GroupingScalarFunction expression: Grouping(100)"
    }

    test {
        sql '''
            select 1000
            from tbl_analyze_repeat
            group by grouping sets((grouping(a)))
            '''

        exception "GROUP BY expression must not contain grouping functions: Grouping(a)"
    }

    test {
        sql '''
            select 1000
            from tbl_analyze_repeat
            group by grouping(a)
            '''

        exception "GROUP BY expression must not contain grouping functions: Grouping(a)"
    }

    test {
        sql '''
            select grouping(a)
            from tbl_analyze_repeat;
            '''

        exception "LOGICAL_PROJECT should not contain grouping expression 'Grouping(a)', only when GROUP BY GROUPING SET/ROLLUP/CUBE can contain grouping expression"
    }

    test {
        sql '''
            select grouping(a)
            from tbl_analyze_repeat
            group by a
            '''

        exception "LOGICAL_AGGREGATE should not contain grouping expression 'Grouping(a)', only when GROUP BY GROUPING SET/ROLLUP/CUBE can contain grouping expression"
    }

    test {
        sql '''
            select 1000 
            from tbl_analyze_repeat
            having grouping_id(a) > 0
            '''

        exception "LOGICAL_HAVING should not contain grouping expression 'Grouping_Id(a)', only when GROUP BY GROUPING SET/ROLLUP/CUBE can contain grouping expression"
    }

    test {
        sql '''
            select 1000 
            from tbl_analyze_repeat
            order by grouping_id(a)
            '''

        exception "LOGICAL_SORT should not contain grouping expression 'Grouping_Id(a)', only when GROUP BY GROUPING SET/ROLLUP/CUBE can contain grouping expression"
    }

    test {
        sql '''
            select 1000 
            from tbl_analyze_repeat
            having grouping_id(a) > 0
            order by grouping_id(a)
            '''
        exception "LOGICAL_HAVING should not contain grouping expression 'Grouping_Id(a)', only when GROUP BY GROUPING SET/ROLLUP/CUBE can contain grouping expression"
    }

    test {
        sql '''
            select 1000 
            from tbl_analyze_repeat
            group by rollup(b, a + b)
            having grouping_id(a + b) > 0 and grouping_id(a) > 0
            '''
        exception "LOGICAL_HAVING 's GROUPING function 'Grouping_Id(a)', its argument 'a' must appear in GROUP BY clause"
    }

    test {
        sql '''
            select 1000 
            from tbl_analyze_repeat
            group by rollup(b, a + b)
            order by grouping_id(a + b), grouping_id(a)
            '''

        exception "LOGICAL_SORT 's GROUPING function 'Grouping_Id(a)', its argument 'a' must appear in GROUP BY clause"
    }
}

