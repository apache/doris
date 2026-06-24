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

suite("test_short_circuit_evaluation") {
    sql "set batch_size = 4096;"

    // Create test table with various data types
    sql "DROP TABLE IF EXISTS test_short_circuit_eval;"
    sql """
        CREATE TABLE IF NOT EXISTS test_short_circuit_eval (
            id INT,
            col_int INT NULL,
            col_int2 INT NULL,
            col_str VARCHAR(100) NULL,
            col_str2 VARCHAR(100) NULL,
            col_date DATE NULL,
            col_double DOUBLE NULL,
            flag TINYINT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3 
        PROPERTIES("replication_num" = "1");
    """
    
    sql """
        INSERT INTO test_short_circuit_eval VALUES 
            (1, 1, 10, 'a', 'x', '2024-01-01', 1.1, 1),
            (2, 2, 20, 'b', 'y', '2024-02-01', 2.2, 0),
            (3, NULL, 30, 'c', NULL, '2024-03-01', 3.3, 1),
            (4, 4, NULL, NULL, 'z', NULL, NULL, 0),
            (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            (6, 6, 60, 'f', 'w', '2024-06-01', 6.6, 1),
            (7, 7, 10, 'g', 'v', '2024-07-01', 7.7, 0),
            (8, NULL, 80, NULL, 'u', '2024-08-01', NULL, 1),
            (9, 9, NULL, 'i', NULL, NULL, 9.9, NULL),
            (10, 10, 100, 'j', 't', '2024-10-01', 10.1, 0);
    """

    // Helper function to run test with both short_circuit_evaluation settings
    def run_test = { String tag, String query ->
        sql "set short_circuit_evaluation = false;"
        def result_false = sql query
        
        sql "set short_circuit_evaluation = true;"
        def result_true = sql query
        
        // Compare results
        assertEquals(result_false, result_true, "Results differ for ${tag}")
        
        // Output result for qt verification
        sql "set short_circuit_evaluation = true;"
    }

    // ==================== IF function tests ====================
    
    // Simple IF
    run_test("if_simple", "select id, if(col_int > 5, 'big', 'small') from test_short_circuit_eval order by id")
    qt_if_simple "select id, if(col_int > 5, 'big', 'small') from test_short_circuit_eval order by id"
    
    // IF with NULL condition
    run_test("if_null_cond", "select id, if(col_int IS NULL, 'null', 'not_null') from test_short_circuit_eval order by id")
    qt_if_null_cond "select id, if(col_int IS NULL, 'null', 'not_null') from test_short_circuit_eval order by id"
    
    // Nested IF (2 levels)
    run_test("if_nested_2", """
        select id, 
            if(col_int IS NULL, 
               'null',
               if(col_int > 5, 'big', 'small')
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_if_nested_2 """
        select id, 
            if(col_int IS NULL, 
               'null',
               if(col_int > 5, 'big', 'small')
            ) as result
        from test_short_circuit_eval order by id
    """
    
    // Nested IF (3 levels)
    run_test("if_nested_3", """
        select id,
            if(flag IS NULL,
               'flag_null',
               if(flag = 1,
                  if(col_int IS NULL, 'flag1_int_null', 'flag1_int_not_null'),
                  if(col_int > 5, 'flag0_big', 'flag0_small')
               )
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_if_nested_3 """
        select id,
            if(flag IS NULL,
               'flag_null',
               if(flag = 1,
                  if(col_int IS NULL, 'flag1_int_null', 'flag1_int_not_null'),
                  if(col_int > 5, 'flag0_big', 'flag0_small')
               )
            ) as result
        from test_short_circuit_eval order by id
    """

    // IF with expression in branches
    run_test("if_expr_branch", """
        select id,
            if(col_int IS NOT NULL,
               col_int * 2 + 10,
               if(col_int2 IS NOT NULL, col_int2 / 2, -1)
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_if_expr_branch """
        select id,
            if(col_int IS NOT NULL,
               col_int * 2 + 10,
               if(col_int2 IS NOT NULL, col_int2 / 2, -1)
            ) as result
        from test_short_circuit_eval order by id
    """

    // ==================== IFNULL function tests ====================
    
    // Simple IFNULL
    run_test("ifnull_simple", "select id, ifnull(col_int, -1) from test_short_circuit_eval order by id")
    qt_ifnull_simple "select id, ifnull(col_int, -1) from test_short_circuit_eval order by id"
    
    // Nested IFNULL (2 levels)
    run_test("ifnull_nested_2", """
        select id,
            ifnull(col_int, ifnull(col_int2, -999)) as result
        from test_short_circuit_eval order by id
    """)
    qt_ifnull_nested_2 """
        select id,
            ifnull(col_int, ifnull(col_int2, -999)) as result
        from test_short_circuit_eval order by id
    """
    
    // Nested IFNULL (3 levels)
    run_test("ifnull_nested_3", """
        select id,
            ifnull(col_str, ifnull(col_str2, ifnull(cast(col_int as varchar), 'all_null'))) as result
        from test_short_circuit_eval order by id
    """)
    qt_ifnull_nested_3 """
        select id,
            ifnull(col_str, ifnull(col_str2, ifnull(cast(col_int as varchar), 'all_null'))) as result
        from test_short_circuit_eval order by id
    """

    // IFNULL with expression
    run_test("ifnull_expr", """
        select id,
            ifnull(col_int * 2, ifnull(col_int2 + 5, 0)) as result
        from test_short_circuit_eval order by id
    """)
    qt_ifnull_expr """
        select id,
            ifnull(col_int * 2, ifnull(col_int2 + 5, 0)) as result
        from test_short_circuit_eval order by id
    """

    // ==================== COALESCE function tests ====================
    
    // Simple COALESCE
    run_test("coalesce_simple", "select id, coalesce(col_int, col_int2, -1) from test_short_circuit_eval order by id")
    qt_coalesce_simple "select id, coalesce(col_int, col_int2, -1) from test_short_circuit_eval order by id"
    
    // COALESCE with multiple columns
    run_test("coalesce_multi", """
        select id,
            coalesce(col_str, col_str2, 'default') as result
        from test_short_circuit_eval order by id
    """)
    qt_coalesce_multi """
        select id,
            coalesce(col_str, col_str2, 'default') as result
        from test_short_circuit_eval order by id
    """
    
    // Nested COALESCE
    run_test("coalesce_nested", """
        select id,
            coalesce(
                coalesce(col_int, col_int2),
                coalesce(cast(col_double as int), -999)
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_coalesce_nested """
        select id,
            coalesce(
                coalesce(col_int, col_int2),
                coalesce(cast(col_double as int), -999)
            ) as result
        from test_short_circuit_eval order by id
    """

    // COALESCE with many arguments
    run_test("coalesce_many_args", """
        select id,
            coalesce(NULL, col_int, NULL, col_int2, NULL, 0) as result
        from test_short_circuit_eval order by id
    """)
    qt_coalesce_many_args """
        select id,
            coalesce(NULL, col_int, NULL, col_int2, NULL, 0) as result
        from test_short_circuit_eval order by id
    """

    // ==================== CASE function tests ====================
    
    // Simple CASE WHEN
    run_test("case_simple", """
        select id,
            case when col_int IS NULL then 'null'
                 when col_int > 5 then 'big'
                 else 'small'
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_case_simple """
        select id,
            case when col_int IS NULL then 'null'
                 when col_int > 5 then 'big'
                 else 'small'
            end as result
        from test_short_circuit_eval order by id
    """
    
    // CASE with multiple conditions
    run_test("case_multi", """
        select id,
            case when col_int IS NULL then 'int_null'
                 when col_int < 3 then 'tiny'
                 when col_int < 6 then 'small'
                 when col_int < 9 then 'medium'
                 else 'large'
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_case_multi """
        select id,
            case when col_int IS NULL then 'int_null'
                 when col_int < 3 then 'tiny'
                 when col_int < 6 then 'small'
                 when col_int < 9 then 'medium'
                 else 'large'
            end as result
        from test_short_circuit_eval order by id
    """
    
    // Nested CASE (CASE inside CASE)
    run_test("case_nested", """
        select id,
            case when flag IS NULL then 'flag_null'
                 when flag = 1 then
                     case when col_int IS NULL then 'f1_null'
                          when col_int > 5 then 'f1_big'
                          else 'f1_small'
                     end
                 else
                     case when col_int2 IS NULL then 'f0_null'
                          when col_int2 > 50 then 'f0_big'
                          else 'f0_small'
                     end
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_case_nested """
        select id,
            case when flag IS NULL then 'flag_null'
                 when flag = 1 then
                     case when col_int IS NULL then 'f1_null'
                          when col_int > 5 then 'f1_big'
                          else 'f1_small'
                     end
                 else
                     case when col_int2 IS NULL then 'f0_null'
                          when col_int2 > 50 then 'f0_big'
                          else 'f0_small'
                     end
            end as result
        from test_short_circuit_eval order by id
    """
    
    // CASE without ELSE
    run_test("case_no_else", """
        select id,
            case when col_int = 1 then 'one'
                 when col_int = 2 then 'two'
                 when col_int = 3 then 'three'
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_case_no_else """
        select id,
            case when col_int = 1 then 'one'
                 when col_int = 2 then 'two'
                 when col_int = 3 then 'three'
            end as result
        from test_short_circuit_eval order by id
    """
    
    // Simple CASE (value matching)
    run_test("case_value", """
        select id,
            case col_int
                when 1 then 'one'
                when 2 then 'two'
                when 6 then 'six'
                when 10 then 'ten'
                else 'other'
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_case_value """
        select id,
            case col_int
                when 1 then 'one'
                when 2 then 'two'
                when 6 then 'six'
                when 10 then 'ten'
                else 'other'
            end as result
        from test_short_circuit_eval order by id
    """

    // ==================== Mixed nested functions ====================
    
    // IF inside CASE
    run_test("case_with_if", """
        select id,
            case when flag IS NULL then 'flag_null'
                 when flag = 1 then if(col_int IS NULL, 'f1_null', 'f1_not_null')
                 else if(col_int2 > 50, 'f0_big', 'f0_small')
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_case_with_if """
        select id,
            case when flag IS NULL then 'flag_null'
                 when flag = 1 then if(col_int IS NULL, 'f1_null', 'f1_not_null')
                 else if(col_int2 > 50, 'f0_big', 'f0_small')
            end as result
        from test_short_circuit_eval order by id
    """
    
    // CASE inside IF
    run_test("if_with_case", """
        select id,
            if(flag IS NULL,
               'flag_null',
               case when col_int IS NULL then 'null'
                    when col_int > 5 then 'big'
                    else 'small'
               end
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_if_with_case """
        select id,
            if(flag IS NULL,
               'flag_null',
               case when col_int IS NULL then 'null'
                    when col_int > 5 then 'big'
                    else 'small'
               end
            ) as result
        from test_short_circuit_eval order by id
    """
    
    // IFNULL with CASE
    run_test("ifnull_with_case", """
        select id,
            ifnull(
                case when col_int > 5 then col_int * 10 end,
                ifnull(col_int2, -1)
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_ifnull_with_case """
        select id,
            ifnull(
                case when col_int > 5 then col_int * 10 end,
                ifnull(col_int2, -1)
            ) as result
        from test_short_circuit_eval order by id
    """
    
    // COALESCE with IF
    run_test("coalesce_with_if", """
        select id,
            coalesce(
                if(col_int > 5, col_int, NULL),
                if(col_int2 > 50, col_int2, NULL),
                -1
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_coalesce_with_if """
        select id,
            coalesce(
                if(col_int > 5, col_int, NULL),
                if(col_int2 > 50, col_int2, NULL),
                -1
            ) as result
        from test_short_circuit_eval order by id
    """

    // Complex mixed nesting (3+ levels)
    run_test("complex_mixed", """
        select id,
            if(flag IS NULL,
               coalesce(col_int, col_int2, -999),
               case when flag = 1 then
                   ifnull(col_int, 
                       case when col_int2 > 50 then col_int2 
                            else -1 
                       end
                   )
               else
                   coalesce(
                       if(col_int > 5, col_int * 2, NULL),
                       if(col_int2 IS NOT NULL, col_int2 / 2, NULL),
                       0
                   )
               end
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_complex_mixed """
        select id,
            if(flag IS NULL,
               coalesce(col_int, col_int2, -999),
               case when flag = 1 then
                   ifnull(col_int, 
                       case when col_int2 > 50 then col_int2 
                            else -1 
                       end
                   )
               else
                   coalesce(
                       if(col_int > 5, col_int * 2, NULL),
                       if(col_int2 IS NOT NULL, col_int2 / 2, NULL),
                       0
                   )
               end
            ) as result
        from test_short_circuit_eval order by id
    """

    // ==================== Merge-into style complex expression ====================
    // This simulates the pattern from doc2: cond = if(NOT flag IS NULL, if(s_c2 = 10, 0, 1), 2)
    run_test("merge_into_style", """
        select id, col_int, col_int2, flag,
            if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2) as cond,
            if((if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2)) = 2,
               id,
               if((if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2)) = 1,
                  col_int,
                  if((if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2)) = 0,
                     id,
                     NULL
                  )
               )
            ) as c1_result
        from test_short_circuit_eval order by id
    """)
    qt_merge_into_style """
        select id, col_int, col_int2, flag,
            if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2) as cond,
            if((if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2)) = 2,
               id,
               if((if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2)) = 1,
                  col_int,
                  if((if(NOT flag IS NULL, if(col_int2 = 10, 0, 1), 2)) = 0,
                     id,
                     NULL
                  )
               )
            ) as c1_result
        from test_short_circuit_eval order by id
    """

    // ==================== Edge cases with all NULLs ====================
    run_test("all_null_if", """
        select id, if(NULL, col_int, col_int2) as result
        from test_short_circuit_eval order by id
    """)
    qt_all_null_if """
        select id, if(NULL, col_int, col_int2) as result
        from test_short_circuit_eval order by id
    """

    run_test("all_null_coalesce", """
        select id, coalesce(NULL, NULL, NULL, col_int) as result
        from test_short_circuit_eval order by id
    """)
    qt_all_null_coalesce """
        select id, coalesce(NULL, NULL, NULL, col_int) as result
        from test_short_circuit_eval order by id
    """

    // ==================== Tests with constant folding scenarios ====================
    run_test("const_if", """
        select id, 
            if(1 = 1, col_int, col_int2),
            if(1 = 0, col_int, col_int2)
        from test_short_circuit_eval order by id
    """)
    qt_const_if """
        select id, 
            if(1 = 1, col_int, col_int2),
            if(1 = 0, col_int, col_int2)
        from test_short_circuit_eval order by id
    """

    run_test("const_case", """
        select id,
            case when 1 = 1 then col_int
                 when 1 = 0 then col_int2
                 else -1
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_const_case """
        select id,
            case when 1 = 1 then col_int
                 when 1 = 0 then col_int2
                 else -1
            end as result
        from test_short_circuit_eval order by id
    """

    // ==================== Tests with string operations ====================
    run_test("string_if_nested", """
        select id,
            if(col_str IS NULL,
               ifnull(col_str2, 'both_null'),
               concat(col_str, '_', ifnull(col_str2, 'null'))
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_string_if_nested """
        select id,
            if(col_str IS NULL,
               ifnull(col_str2, 'both_null'),
               concat(col_str, '_', ifnull(col_str2, 'null'))
            ) as result
        from test_short_circuit_eval order by id
    """

    // ==================== Tests with arithmetic in branches ====================
    run_test("arithmetic_nested", """
        select id,
            case when col_int IS NULL then 
                     coalesce(col_int2 * 2, col_double, -1)
                 when col_int < 5 then 
                     if(col_int2 IS NULL, col_int * 10, col_int + col_int2)
                 else 
                     ifnull(col_double * col_int, col_int2 - col_int)
            end as result
        from test_short_circuit_eval order by id
    """)
    qt_arithmetic_nested """
        select id,
            case when col_int IS NULL then 
                     coalesce(col_int2 * 2, col_double, -1)
                 when col_int < 5 then 
                     if(col_int2 IS NULL, col_int * 10, col_int + col_int2)
                 else 
                     ifnull(col_double * col_int, col_int2 - col_int)
            end as result
        from test_short_circuit_eval order by id
    """

    // ==================== Deep nesting (4+ levels) ====================
    run_test("deep_nesting", """
        select id,
            if(flag IS NULL,
               'L1_null',
               if(flag = 1,
                  if(col_int IS NULL,
                     if(col_int2 IS NULL, 'L4_both_null', 'L4_int_null'),
                     if(col_int > 5, 'L4_big', 'L4_small')
                  ),
                  case when col_int2 IS NULL then 'L2_f0_null'
                       when col_int2 < 30 then
                           ifnull(col_str, ifnull(col_str2, 'L4_no_str'))
                       when col_int2 < 70 then
                           coalesce(col_str, col_str2, 'L4_medium')
                       else 'L3_large'
                  end
               )
            ) as result
        from test_short_circuit_eval order by id
    """)
    qt_deep_nesting """
        select id,
            if(flag IS NULL,
               'L1_null',
               if(flag = 1,
                  if(col_int IS NULL,
                     if(col_int2 IS NULL, 'L4_both_null', 'L4_int_null'),
                     if(col_int > 5, 'L4_big', 'L4_small')
                  ),
                  case when col_int2 IS NULL then 'L2_f0_null'
                       when col_int2 < 30 then
                           ifnull(col_str, ifnull(col_str2, 'L4_no_str'))
                       when col_int2 < 70 then
                           coalesce(col_str, col_str2, 'L4_medium')
                       else 'L3_large'
                  end
               )
            ) as result
        from test_short_circuit_eval order by id
    """

    // ==================== Multiple columns with same expression pattern ====================
    run_test("multi_column_same_pattern", """
        select id,
            if(col_int IS NULL, -1, col_int) as c1,
            if(col_int2 IS NULL, -2, col_int2) as c2,
            ifnull(col_str, 'null_str') as c3,
            coalesce(col_str2, col_str, 'both_null') as c4,
            case when flag = 1 then 'yes' when flag = 0 then 'no' else 'unknown' end as c5
        from test_short_circuit_eval order by id
    """)
    qt_multi_column_same_pattern """
        select id,
            if(col_int IS NULL, -1, col_int) as c1,
            if(col_int2 IS NULL, -2, col_int2) as c2,
            ifnull(col_str, 'null_str') as c3,
            coalesce(col_str2, col_str, 'both_null') as c4,
            case when flag = 1 then 'yes' when flag = 0 then 'no' else 'unknown' end as c5
        from test_short_circuit_eval order by id
    """

    // Clean up
    sql "DROP TABLE IF EXISTS test_short_circuit_eval;"


    sql """
        set short_circuit_evaluation = true;
    """

    qt_with_array_map"""
        select if(number < 3 , null, array_map(x -> x * x, array(number,number,number))) from numbers("number" = "5");
    """
}
