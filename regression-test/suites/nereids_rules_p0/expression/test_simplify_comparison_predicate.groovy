// Licensed to the Apache Software Foundation (ASF) under one
//
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

// TODO: date datetime comparison still has bug, need fix.
suite('test_simplify_comparison_predicate', 'nonConcurrent') {
    def tbl = 'test_simplify_comparison_predicate_tbl'
    def checkExplain = { expression, resExpression ->
        def checker = { explainString, exception, startTime, endTime ->
            assertNull(exception)
            def foundOutputExprs = false
            def succ = false
            for (def line : explainString.split('\n')) {
                if (foundOutputExprs) {
                    assertTrue(line.contains(resExpression), "'${line}' no contains '${resExpression}'")
                    succ = true
                    break
                }
                if (line.contains('OUTPUT EXPRS:')) {
                    foundOutputExprs = true
                }
            }
            assertTrue(foundOutputExprs)
            assertTrue(succ)
        }

        explain {
            sql "SELECT ${expression} FROM ${tbl}"
            check checker
        }
    }
    def testSimplify = { checkNullColumn, checkNotNullColumn, expression, resExpression ->
        def types = ['']
        def column = ''
        if (expression.contains('{int_like_column}')) {
            column = '{int_like_column}'
            types = ['tinyint', 'smallint', 'int', 'bigint']
        } else if (expression.contains('{decimal_column}')) {
            column = '{decimal_column}'
            types = ['decimal_3_0', 'decimal_5_2']
        } else if (expression.contains('{date_column}')) {
            column = '{date_column}'
            types = ['date', 'datev1']
        } else if (expression.contains('{datetime_column}')) {
            column = '{datetime_column}'
            types = ['datetime_0', 'datetime_3', 'datetimev1']
        }
        for (def type : types) {
            if (type == '') {
                checkExplain expression, resExpression
            } else {
                if (checkNullColumn) {
                    checkExplain expression.replace(column, "c_${type}_null"), resExpression.replace(column, "c_${type}_null")
                }
                if (checkNotNullColumn) {
                    checkExplain expression.replace(column, "c_${type}"), resExpression.replace(column, "c_${type}")
                }
            }
        }
    }

    setFeConfigTemporary([disable_datev1:false, disable_decimalv2:false]) {
        sql """
            DROP TABLE IF EXISTS ${tbl} FORCE;

            CREATE TABLE ${tbl} (
                c_tinyint            tinyint not null default 1,
                c_tinyint_null       tinyint,
                c_smallint           smallint not null default 1,
                c_smallint_null      smallint,
                c_int                int not null default 1,
                c_int_null           int,
                c_bigint             bigint not null default 1,
                c_bigint_null        bigint,
                c_decimal_3_0        decimal(3, 0) not null default 1,
                c_decimal_3_0_null   decimal(3, 0),
                c_decimal_5_2        decimal(5, 2) not null default 1,
                c_decimal_5_2_null   decimal(5, 2),
                c_date               date not null default '2000-01-01',
                c_date_null          date,
                c_datev1             datev1 not null default '2000-01-01',
                c_datev1_null        datev1 null,
                c_datetime_0         datetime(0) not null default '2000-01-01 00:00:00',
                c_datetime_0_null    datetime(0),
                c_datetime_3         datetime(3) not null default '2000-01-01 00:00:00',
                c_datetime_3_null    datetime(3),
                c_datetimev1         datetimev1 not null default '2000-01-01 00:00:00',
                c_datetimev1_null    datetimev1
            )
            PROPERTIES ('replication_num' = '1');

            INSERT INTO ${tbl} VALUES();
            """

        testSimplify true, true, '{int_like_column} = CAST(1.00 as DOUBLE)',  '({int_like_column} = 1)'
        testSimplify true, false, '{int_like_column} = CAST(1.01 as DOUBLE)',  'AND[{int_like_column} IS NULL,NULL]'
        testSimplify false, true, '{int_like_column} = CAST(1.01 as DOUBLE)',  'FALSE'
        testSimplify true, true, '{int_like_column} <=> CAST(1.01 as DOUBLE)',  'FALSE'
        testSimplify true, true, '{int_like_column} > CAST(1.00 as DOUBLE)',  '({int_like_column} > 1)'
        testSimplify true, true, '{int_like_column} < CAST(1.00 as DOUBLE)',  '({int_like_column} < 1)'
        testSimplify true, true, '{int_like_column} > CAST(1.01 as DOUBLE)',  '({int_like_column} > 1)'
        testSimplify true, true, '{int_like_column} >= CAST(1.01 as DOUBLE)',  '({int_like_column} >= 2)'
        testSimplify true, true, '{int_like_column} <= CAST(1.01 as DOUBLE)',  '({int_like_column} <= 1)'
        testSimplify true, true, '{int_like_column} < CAST(1.01 as DOUBLE)',  '({int_like_column} < 2)'
        testSimplify true, true, '{int_like_column} = 1.00',  '({int_like_column} = 1)'
        testSimplify true, true, '{int_like_column} > 1.00',  '({int_like_column} > 1)'
        testSimplify true, true, '{int_like_column} < 1.00',  '({int_like_column} < 1)'
        testSimplify true, false, '{int_like_column} = 1.01',  'AND[{int_like_column} IS NULL,NULL]'
        testSimplify false, true, '{int_like_column} = 1.01',  'FALSE'
        testSimplify true, true, '{int_like_column} <=> 1.01',  'FALSE'
        testSimplify true, true, '{int_like_column} > 1.01',  '({int_like_column} > 1)'
        testSimplify true, true, '{int_like_column} >= 1.01',  '({int_like_column} >= 2)'
        testSimplify true, true, '{int_like_column} <= 1.01',  '({int_like_column} <= 1)'
        testSimplify true, true, '{int_like_column} < 1.01',  '({int_like_column} < 2)'
        testSimplify false, false, 'CAST(c_decimal_3_0_null as DECIMAL(10, 5)) = CAST(1.00 as DECIMAL(10, 5))',  '(c_decimal_3_0_null = 1)'
        testSimplify false, false, 'CAST(c_decimal_3_0_null as DECIMAL(10, 5)) = CAST(1.1 as DECIMAL(10, 5))',  'AND[c_decimal_3_0_null IS NULL,NULL]'
        testSimplify false, false, 'CAST(c_decimal_3_0_null as DECIMAL(10, 5)) > CAST(1.1 as DECIMAL(10, 5))',  '(c_decimal_3_0_null > 1)'
        testSimplify false, false, 'CAST(c_decimal_3_0_null as DECIMAL(10, 5)) >= CAST(1.1 as DECIMAL(10, 5))',  '(c_decimal_3_0_null >= 2)'
        testSimplify false, false, 'CAST(c_decimal_3_0_null as DECIMAL(10, 5)) < CAST(1.1 as DECIMAL(10, 5))',  '(c_decimal_3_0_null < 2)'
        testSimplify false, false, 'CAST(c_decimal_3_0_null as DECIMAL(10, 5)) <= CAST(1.1 as DECIMAL(10, 5))',  '(c_decimal_3_0_null <= 1)'
        testSimplify false, false, 'c_decimal_5_2_null = CAST(1.0 as DECIMAL(10, 5))',  '(c_decimal_5_2_null = 1.00)'
        testSimplify false, false, 'c_decimal_5_2_null = CAST(1.1 as DECIMAL(10, 5))',  '(c_decimal_5_2_null = 1.10)'
        testSimplify false, false, 'c_decimal_5_2_null = CAST(1.12 as DECIMAL(10, 5))',  '(c_decimal_5_2_null = 1.12)'
        testSimplify false, false, 'c_decimal_5_2_null = CAST(1.123 as DECIMAL(10, 5))',  'AND[c_decimal_5_2_null IS NULL,NULL]'
        testSimplify false, false, 'c_decimal_5_2 = CAST(1.123 as DECIMAL(10, 5))',  'FALSE'
        testSimplify false, false, 'c_decimal_5_2_null > CAST(1.123 as DECIMAL(10, 5))',  'c_decimal_5_2_null > 1.12'
        testSimplify false, false, 'c_decimal_5_2_null >= CAST(1.123 as DECIMAL(10, 5))',  'c_decimal_5_2_null >= 1.13'
        testSimplify false, false, 'c_decimal_5_2_null <= CAST(1.123 as DECIMAL(10, 5))',  'c_decimal_5_2_null <= 1.12'
        testSimplify false, false, 'c_decimal_5_2_null < CAST(1.123 as DECIMAL(10, 5))',  'c_decimal_5_2_null < 1.13'
        testSimplify false, false, "CAST(c_datetime_0 AS DATETIME(5)) = '2000-01-01'", "(c_datetime_0 = '2000-01-01 00:00:00')"
        testSimplify false, false, "CAST(c_datetime_0 AS DATETIME(5)) = '2000-01-01 00:00:00.1'", 'FALSE'
        testSimplify false, false, "CAST(c_datetime_0_null AS DATETIME(5)) = '2000-01-01 00:00:00.1'", 'AND[c_datetime_0_null IS NULL,NULL]'
        testSimplify false, false, "CAST(c_datetime_0_null AS DATETIME(5)) <=> '2000-01-01 00:00:00.1'", 'FALSE'
        testSimplify false, false, "CAST(c_datetime_0 AS DATETIME(5)) >= '2000-01-01 00:00:00.1'", "(c_datetime_0 >= '2000-01-01 00:00:01')"
        testSimplify false, false, "CAST(c_datetime_0 AS DATETIME(5)) > '2000-01-01 00:00:00.1'", "(c_datetime_0 > '2000-01-01 00:00:00')"
        testSimplify false, false, "CAST(c_datetime_0 AS DATETIME(5)) <= '2000-01-01 00:00:00.1'", "(c_datetime_0 <= '2000-01-01 00:00:00')"
        testSimplify false, false, "CAST(c_datetime_0 AS DATETIME(5)) < '2000-01-01 00:00:00.1'", "(c_datetime_0 < '2000-01-01 00:00:01')"
        testSimplify false, false, "CAST(c_datetime_3 AS DATETIME(5)) = '2000-01-01'", "(c_datetime_3 = '2000-01-01 00:00:00.000')"
        testSimplify false, false, "CAST(c_datetime_3 AS DATETIME(5)) = '2000-01-01 00:00:00.1234'", 'FALSE'
        testSimplify false, false, "CAST(c_datetime_3_null AS DATETIME(5)) = '2000-01-01 00:00:00.1234'", 'AND[c_datetime_3_null IS NULL,NULL]'
        testSimplify false, false, "CAST(c_datetime_3_null AS DATETIME(5)) <=> '2000-01-01 00:00:00.1234'", 'FALSE'
        testSimplify false, false, "CAST(c_datetime_3 AS DATETIME(5)) >= '2000-01-01 00:00:00.1234'", "(c_datetime_3 >= '2000-01-01 00:00:00.124')"
        testSimplify false, false, "CAST(c_datetime_3 AS DATETIME(5)) > '2000-01-01 00:00:00.1234'", "(c_datetime_3 > '2000-01-01 00:00:00.123')"
        testSimplify false, false, "CAST(c_datetime_3 AS DATETIME(5)) <= '2000-01-01 00:00:00.1234'", "(c_datetime_3 <= '2000-01-01 00:00:00.123')"
        testSimplify false, false, "CAST(c_datetime_3 AS DATETIME(5)) < '2000-01-01 00:00:00.1234'", "(c_datetime_3 < '2000-01-01 00:00:00.124')"
        testSimplify false, false, "c_date = '2000-01-01 00:00:01'", 'FALSE'
        testSimplify false, false, "CAST(c_date_null AS DATETIME(5)) = '2000-01-01 00:00:01'", 'AND[c_date_null IS NULL,NULL]'
        testSimplify false, false, "CAST(c_date_null AS DATETIME(5)) <=> '2000-01-01 00:00:01'", 'FALSE'
        testSimplify false, false, "CAST(c_date AS DATETIME(5)) > '2000-01-01 00:00:01'", "c_date > '2000-01-01'"
        testSimplify false, false, "CAST(c_date AS DATETIME(5)) >= '2000-01-01 00:00:01'", "c_date >= '2000-01-02'"
        testSimplify false, false, "CAST(c_date AS DATETIME(5)) <= '2000-01-01 00:00:01'", "c_date <= '2000-01-01'"
        testSimplify false, false, "CAST(c_date AS DATETIME(5)) < '2000-01-01 00:00:01'", "c_date < '2000-01-02'"

        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    }
}
