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

suite('test_order_by') {
    sql 'use test_query_db'

    sql '''
        create table order_t (
            k1 int,
            k2 int,
            v1 int,
            v2 int
        )
        duplicate key(k1, k2)
        distributed by hash(k1) buckets 4
        properties(
            "replication_num"="1"                
        )
    '''

    sql '''
        insert into t values
            (1, 5, 20, 50),
            (2, 4, 10, 30),
            (3, 3, 40, 40),
            (4, 2, 30, 20),
            (15, 59, 205, 507),
            (25, 49, 105, 307),
            (35, 39, 405, 407),
            (45, 29, 305, 207)
    '''

    def sorted_res = [[3, 3, 40, 40], [4, 2, 30, 20], [15, 59, 205, 507], [25, 49, 105, 307]]
    def sort_sql = [
            'select * from db1.baseall where k1 = 1 limit 4 offset 2',
            'select * from (select * from db1.baseall where k1 = 1) t limit 2, 4',
            'select * from (select * from (select * from db1.baseall where k1 < 5) t1 where k1 = 1) t limit 2, 4',
    ]
    for (s in sort_sql) {
        for (int i = 0; i < 5; ++i) {
            test {
                sql s
                result(sorted_res)
            }

        }
    }

    for (int i = 0; i < 5; ++i) {
        test {
            sql 'select * from (select * from db1.baseall) t lateral view explode([0, 1, 2, 3]) lv as e limit 2, 4'
            result([[3, 3, 40, 40, 0], [4, 2, 30, 20, 1], [15, 59, 205, 507, 2], [25, 49, 105, 307, 3]])
        }
    }
}