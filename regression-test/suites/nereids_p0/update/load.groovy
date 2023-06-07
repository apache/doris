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

suite("load") {
    sql '''
        create table t1 (
            id int,
            id1 int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id, id1)
        distributed by hash(id, id1)
        properties(
            'replication_num'='1',
            "function_column.sequence_col" = "c4"
        );
    '''

    sql '''
        create table t2 (
            id int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id)
        distributed by hash(id)
        properties(
            'replication_num'='1'
        );
    '''

    sql '''
        create table t3 (
            id int
        ) distributed by hash(id)
        properties(
            'replication_num'='1'
        );
    '''

    sql '''
        INSERT INTO t1 VALUES
            (1, 10, 1, '1', 1.0, '2000-01-01'),
            (2, 20, 2, '2', 2.0, '2000-01-02'),
            (3, 30, 3, '3', 3.0, '2000-01-03');
    '''

    sql '''

        INSERT INTO t2 VALUES
            (1, 10, '10', 10.0, '2000-01-10'),
            (2, 20, '20', 20.0, '2000-01-20'),
            (3, 30, '30', 30.0, '2000-01-30'),
            (4, 4, '4', 4.0, '2000-01-04'),
            (5, 5, '5', 5.0, '2000-01-05');
    '''

    sql '''
        INSERT INTO t3 VALUES
            (1),
            (4),
            (5);
    '''
}
