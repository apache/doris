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

suite("test_repeat_output_slot") {
    sql """
        SET enable_fallback_to_original_planner=false;
        SET enable_nereids_planner=true;
        SET ignore_shape_nodes='PhysicalDistribute';
        SET disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        SET runtime_filter_mode=OFF;
        SET disable_join_reorder=true;

        DROP TABLE IF EXISTS tbl_test_repeat_output_slot FORCE;

        """

    sql """
        CREATE TABLE tbl_test_repeat_output_slot (
            col_datetime_6__undef_signed datetime(6),
            col_varchar_50__undef_signed varchar(50),
            col_varchar_50__undef_signed__index_inverted varchar(50)
        ) engine=olap
        distributed by hash(col_datetime_6__undef_signed) buckets 10
        properties('replication_num' = '1');
        """

    sql """
        INSERT INTO tbl_test_repeat_output_slot VALUES
            (null, null, null), (null, "a", "x"), (null, "a", "y"),
            ('2020-01-02', "b", "x"), ('2020-01-02', 'a', 'x'), ('2020-01-02', 'b', 'y'),
            ('2020-01-03', 'a', 'x'), ('2020-01-03', 'a', 'y'), ('2020-01-03', 'b', 'x'), ('2020-01-03', 'b', 'y'),
            ('2020-01-04', 'a', 'x'), ('2020-01-04', 'a', 'y'), ('2020-01-04', 'b', 'x'), ('2020-01-04', 'b', 'y');
        """

   explainAndOrderResult 'sql_1', '''
        SELECT 100000
        FROM tbl_test_repeat_output_slot
        GROUP BY GROUPING SETS (
                (col_datetime_6__undef_signed, col_varchar_50__undef_signed)
                , ()
                , (col_varchar_50__undef_signed)
                , (col_datetime_6__undef_signed, col_varchar_50__undef_signed)
        );
        '''

   explainAndOrderResult 'sql_2', '''
        SELECT MAX(col_datetime_6__undef_signed) AS total_col_datetime,
               CASE WHEN GROUPING(col_varchar_50__undef_signed__index_inverted) = 1 THEN 'ALL'
                    ELSE CAST(col_varchar_50__undef_signed__index_inverted AS VARCHAR)
                    END AS pretty_val,
               IF(GROUPING_ID(col_varchar_50__undef_signed__index_inverted,
                              col_datetime_6__undef_signed,
                              col_varchar_50__undef_signed) > 0, 1, 0) AS is_agg_row,
               GROUPING_ID(col_varchar_50__undef_signed__index_inverted,
                           col_datetime_6__undef_signed, col_varchar_50__undef_signed) AS having_filter_col,
               col_varchar_50__undef_signed__index_inverted,
               col_datetime_6__undef_signed,
               col_varchar_50__undef_signed
        FROM tbl_test_repeat_output_slot
        GROUP BY GROUPING SETS (
                (col_varchar_50__undef_signed__index_inverted, col_datetime_6__undef_signed, col_varchar_50__undef_signed),
                (),
                (col_varchar_50__undef_signed),
                (col_varchar_50__undef_signed__index_inverted, col_datetime_6__undef_signed, col_varchar_50__undef_signed),
                (col_varchar_50__undef_signed))
        HAVING having_filter_col > 0;
        '''
}
