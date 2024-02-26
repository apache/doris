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

suite('test_default_limit', "arrow_flight_sql") {
    sql 'drop table if exists baseall'
    sql 'drop table if exists bigtable'

    sql '''
        create table baseall (
            k0 int,
            k1 int,
            k2 int
        )
        distributed by hash(k0) buckets 16
        properties(
            'replication_num'='1'
        )
    '''

    sql '''
        create table bigtable (
            k0 int,
            k1 int,
            k2 int
        )
        distributed by hash(k0) buckets 16
        properties(
            'replication_num'='1'
        )
    '''

    def values = (1..15).collect { "(${(int) (it / 8)}, $it, ${it + 1})" }.join(', ')
    sql "insert into baseall values $values"
    sql "insert into baseall values (null, null, null)"
    sql "insert into bigtable select * from baseall"

    for (int i = 0; i < 2; ++i) {
        if (i == 0) {
            sql 'set enable_pipeline_engine=false'
        } else if (i == 1) {
            sql 'set enable_pipeline_engine=true'
        }

        sql 'set default_order_by_limit = -1'
        sql 'set sql_select_limit = -1'

        def res = sql 'select * from baseall'
        assertEquals(res.size(), 16)

        sql 'set default_order_by_limit = 10'
        sql 'set sql_select_limit = 5'

        res = sql 'select * from baseall'
        assertEquals(res.size(), 5)
        res = sql 'select * from baseall order by k1'
        assertEquals(res.size(), 5)
        res = sql 'select * from baseall limit 7'
        assertEquals(res.size(), 7)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set default_order_by_limit = 5'
        sql 'set sql_select_limit = 10'

        res = sql 'select * from baseall'
        assertEquals(res.size(), 10)
        res = sql 'select * from baseall order by k1'
        assertEquals(res.size(), 5)
        res = sql 'select * from baseall limit 7'
        assertEquals(res.size(), 7)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 10)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set sql_select_limit = -1'

        res = sql 'select * from baseall'
        assertEquals(res.size(), 16)
        res = sql 'select * from baseall limit 7'
        assertEquals(res.size(), 7)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 15)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set sql_select_limit = -10'

        res = sql 'select * from baseall'
        assertEquals(res.size(), 16)
        res = sql 'select * from baseall limit 7'
        assertEquals(res.size(), 7)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 15)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set sql_select_limit = 0'

        res = sql 'select * from baseall'
        assertEquals(res.size(), 0)
        res = sql 'select * from baseall limit 7'
        assertEquals(res.size(), 7)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 0)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set sql_select_limit = 5'
        sql 'set default_order_by_limit = -1'

        res = sql 'select * from baseall order by k1'
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set default_order_by_limit = -10'

        res = sql 'select * from baseall order by k1'
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)

        sql 'set default_order_by_limit = 0'

        res = sql 'select * from baseall order by k1'
        assertEquals(res.size(), 0)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
        '''
        assertEquals(res.size(), 5)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2
        '''
        assertEquals(res.size(), 0)
        res = sql '''
            with cte as (
                select baseall.* from baseall, bigtable where baseall.k1 = bigtable.k1
            )
            select * from baseall, (select k1 from cte) c where c.k1 = baseall.k1
            order by c.k1, baseall.k2 limit 8
        '''
        assertEquals(res.size(), 8)
    }
}