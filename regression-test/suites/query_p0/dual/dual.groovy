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

suite('dual') {

    qt_sql 'select 1 from dual'
    qt_sql 'select 1 from dual where 1'
    qt_sql 'select 1 from dual where 1 = 1'
    qt_sql 'select 1 from dual where 0'
    qt_sql 'select 1 from dual where 1 = 0'
    qt_sql 'select 1+1 from dual'

    // Testing constant expressions in more complex contexts
    qt_sql 'select * from (select 1 as a from dual) sub'
    qt_sql 'select 1 from dual group by 1'
    qt_sql 'select 1 from dual having 1'
    qt_sql 'select 1 from dual group by 1 having 1'
    qt_sql 'select 1 from dual order by 1'
    qt_sql 'select 1 from dual order by 1 desc'
    qt_sql 'select 1 from dual order by 1 limit 1'
    qt_sql 'select 1 from dual order by 1 limit 1 offset 1'
    qt_sql 'select 1 from dual where 1 in (1)'
    qt_sql 'select 1 from dual where 1 group by 1'
    qt_sql 'select 1 from dual where 1 having 1'
    qt_sql 'select 1 from dual where 1 group by 1 having 1'
    qt_sql 'select 1 from dual where 1 order by 1'
    qt_sql 'with cte as (select 1 as a from dual) select a from cte'
    qt_sql 'select a from (select 1 as a from dual union all select 2 as a from dual) u'
    qt_sql 'select row_number() over (order by 1) from dual;'

    // Dropping and creating a table named 'dual' to test behavior when dual is a real table
    sql 'drop table if exists `dual`'
    sql '''
        create table `dual` (
            k0 int
        )
        distributed by hash(k0) buckets 16
        properties(
            'replication_num'='1'
        )
    '''
    sql 'insert into `dual` values (1)'
    sql 'insert into `dual` values (2)'
    sql 'insert into `dual` values (3)'

    qt_sql 'select 1 from `dual`'
    qt_sql 'select 1 from dual'

    // Tests for dropping 'dual' and ensuring correct error handling
    test {
        sql 'drop table if exists dual'
        exception """DUAL is keyword, maybe `DUAL`"""
    }
    sql 'drop table if exists `dual`'

    // Test error handling when table does not exist
    test {
        sql "select 1 from `dual`"
        exception "Table [dual] does not exist in database [regression_test_query_p0_dual]"
    }

    // Disable and enable Nereids planner to check behavior differences
    sql "set enable_nereids_planner = false"
    test {
        sql "select 1 from `dual`"
        exception "Unknown table 'dual'"
    }
    sql "set enable_nereids_planner = true"

    // Tests for unknown column errors
    test {
        sql "select a from dual"
        exception "Unknown column 'a' in 'table list'"
    }
    test {
        sql "select 1, a from dual"
        exception "Unknown column 'a' in 'table list'"
    }
}