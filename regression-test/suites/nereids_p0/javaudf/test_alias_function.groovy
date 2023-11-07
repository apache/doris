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

suite("nereids_test_alias_function") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'use nereids_test_query_db'

    sql 'drop function if exists f1(datetimev2(3), int)'
    sql 'drop function if exists f2(datetimev2(3), int)'
    sql 'drop function if exists f3(int)'
    sql 'drop function if exists f4(int, int)'

    sql '''
    create alias function f1(datetimev2(3), int) with parameter (datetime1, int1) as
        date_trunc(days_sub(datetime1, int1), 'day')
    '''
    sql '''
    CREATE ALIAS FUNCTION f2(DATETIMEV2(3), INT) with PARAMETER (datetime1, int1) as
        DATE_FORMAT(HOURS_ADD(
            date_trunc(datetime1, 'day'),
            add(multiply(floor(divide(HOUR(datetime1), divide(24, int1))), 1), 1)), '%Y%m%d:%H')
    '''
    sql '''
    CREATE ALIAS FUNCTION f3(INT) with PARAMETER (int1) as
        f2(f1('2023-05-20', 2), int1)
    '''

    sql '''
         CREATE ALIAS FUNCTION f4(INT,INT) WITH PARAMETER(n,d) AS add(1,floor(divide(n,d)))
    '''

    test {
        sql 'select cast(f1(\'2023-06-01\', 3) as string);'
        result([['2023-05-29 00:00:00']])
    }
    test {
        sql 'select f2(f1(\'2023-05-20\', 2), 3)'
        result([['20230518:01']])
    }
    test {
        sql 'select f3(4)'
        result([['20230518:01']])
    }
    test {
        sql 'select cast(f1(\'2023-06-01\', k1) as string) from test order by k1'
        result([
                ['2023-05-31 00:00:00'],
                ['2023-05-30 00:00:00'],
                ['2023-05-29 00:00:00']
        ])
    }
    test {
        sql 'select f2(f1(\'2023-05-20\', k1), 4) from test order by k1'
        result([
                ['20230519:01'],
                ['20230518:01'],
                ['20230517:01']
        ])
    }
    test {
        sql 'select f3(k1) from test order by k1'
        result([
                ['20230518:01'],
                ['20230518:01'],
                ['20230518:01']
        ])
    }

    test {
        sql 'select f4(1,2) from test'
        result([
                [1.0d],
                [1.0d],
                [1.0d]
        ])
    }
}