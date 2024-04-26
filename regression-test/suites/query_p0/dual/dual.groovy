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
    test {
        sql 'drop table if exists dual'
        exception """DUAL is keyword, maybe `DUAL`"""
    }
    sql 'drop table if exists `dual`'

    test {
        sql "select 1 from `dual`"
        exception "Unknown table 'dual'"
    }
    test {
            sql "select a from dual"
            exception "Unknown column 'a' in 'table list'"
    }
    test {
            sql "select 1, a from dual"
            exception "Unknown column 'a' in 'table list'"
    }
}