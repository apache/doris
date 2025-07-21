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

suite('test_default_limit') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    
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

    // test dml
    sql 'set default_order_by_limit = -1'
    sql 'set sql_select_limit = 1'

    sql """truncate table baseall"""
    sql """truncate table bigtable"""
    sql """drop table if exists unique_table"""
    sql """create table unique_table (
            k0 int,
            k1 int,
            k2 int
        )
        unique key (k0)
        distributed by hash(k0) buckets 16
        properties(
            'replication_num'='1'
        )
    """
    sql """insert into baseall values(1, 1, 1), (2, 2, 2),(3, 3, 3), (4, 4, 4)"""
    sql """insert into unique_table values(1, 1, 1), (2, 2, 2),(3, 3, 3)"""
    sql "sync"
    // should execute successful
    sql "delete from baseall where k0 in (3, 4)"
    sql "sync"
    // should insert 2 lines
    sql "insert into bigtable select * from baseall"
    sql "sync"
    // should update 2 lines
    sql "update unique_table set k1 = 4 where k1 in (2, 3, 4)"
    sql "sync"
    // should delete 2 lines
    sql "delete from unique_table where k0 = 1 or k0 = 2"
    sql "sync"
    sql 'set sql_select_limit = -1'
    qt_baseall_should_delete_2_lines "select * from baseall order by k0"
    qt_unique_should_delete_2_lines_and_update_1_line "select * from unique_table order by k0"
    qt_bigtable_should_insert_2_lines "select * from bigtable order by k0"
}