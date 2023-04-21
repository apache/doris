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

suite("nereids_insert_into_table") {
    sql 'drop table if exists src'
    sql 'drop table if exists target'

    sql '''
        create table src (
            k1 int,
            k2 int,
            k3 int,
            k4 varchar(20),
            k5 varchar(20)
        ) engine=OLAP
        duplicate key(k1)
        distributed by hash(k5) buckets 4
        properties (
            "replication_num" = "1"
        )
    '''

    sql '''
        create table target (
            k1 int,
            k2 int,
            k3 int,
            k4 varchar(20),
            k5 varchar(20)
        ) engine=OLAP
        UNIQUE KEY(k1, k2)
        PARTITION BY RANGE(k1)
        (
            PARTITION `p2` VALUES LESS THAN ("2"),
            PARTITION `p5` VALUES LESS THAN ("5"),
            PARTITION `p10` VALUES LESS THAN ("10"),
            PARTITION `p20` VALUES LESS THAN ("20")
        )
        distributed by hash(k5) buckets 4
        properties (
            "replication_num" = "1"
        )
    '''

    sql '''
        insert into src values
            (1, 2, 3, '4', '5'),
            (4, 5, 6, '7', '8'),
            (12, 13, 14, '15', '16'),
            (null, null, null, null, null)
    '''
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql 'insert into target select * from src'
    test {
        sql 'select * from target order by k1'
        result([
                [null, null, null, null, null],
                [1, 2, 3, '4', '5'],
                [4, 5, 6, '7', '8'],
                [12, 13, 14, '15', '16']
        ])
    }
    sql 'truncate table target'

    test {
        sql '''insert into target select 'x', 'x', 'x', 1, 2'''
    }

    test {
        sql '''insert into target select 1, 2, 3'''
        exception 'insert target table contains 5 slots, but source table contains 3 slots'
    }

    sql 'insert into target partition (p10, p5) select * from src where k1 < 10 and k1 > 2'
    test {
        sql 'select * from target order by k1'
        result([
                [null, null, null, '1', '2'],
                [4, 5, 6, '7', '8']
        ])
    }

    sql 'insert into target partition (p10, p5) (k1, k2, k3, k4, k5) select k1, \'100\', k2, 0, k4 from src where k1 < 10 and k1 > 2'
    test {
        sql 'select * from target order by k1, k2'
        result([
                [null, null, null, '1', '2'],
                [4, 5, 6, '7', '8'],
                [4, 100, 5, '0', '7']
        ])
    }

    sql 'insert into target partition (p10, p5) (k1, k2, k3, k4, k5) with cte as (select 9, k2, k4, 0, k5 from src where k1 < 10 and k1 > 2) select * from cte'
    test {
        sql 'select * from target order by k1, k2'
        result([
                [null, null, null, '1', '2'],
                [4, 5, 6, '7', '8'],
                [4, 100, 5, '0', '7'],
                [9, 5, 7, '0', '8']
        ])
    }

    // test txn model.
    sql 'truncate table target'
    sql 'begin'
    sql 'insert into target select 1, 2, 3, \'4\', \'5\';'
    test {
        sql 'select * from target'
        result([[1, 2, 3, '4', '5']])
    }

    test {
        sql 'insert into target select * from src'
    }
    sql 'commit'
}