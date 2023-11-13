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

suite('test_alias_function') {
    sql '''
        CREATE ALIAS FUNCTION IF NOT EXISTS f1(DATETIMEV2(3), INT)
            with PARAMETER (datetime1, int1) as date_trunc(days_sub(datetime1, int1), 'day')'''
    sql '''
        CREATE ALIAS FUNCTION IF NOT EXISTS f2(DATETIMEV2(3), int)
            with PARAMETER (datetime1, int1) as DATE_FORMAT(HOURS_ADD(
                date_trunc(datetime1, 'day'),
                add(multiply(floor(divide(HOUR(datetime1), divide(24,int1))), 1), 1)
            ), '%Y%m%d:%H');'''

    test {
        sql '''select f2(f1('2023-03-29', 2), 3)'''
        result([['20230327:01']])
    }

    sql "set enable_nereids_planner=false"

    sql '''
        DROP FUNCTION IF EXISTS legacy_f4()
    '''

    sql '''
        CREATE ALIAS FUNCTION legacy_f4() WITH PARAMETER() AS now()
    '''

    sql '''
        SELECT legacy_f4(), now()
    '''

    sql "set enable_nereids_planner=true"

    sql '''
        SELECT legacy_f4(), now()
    '''

    sql '''
        DROP FUNCTION IF EXISTS legacy_f4()
    '''
}
