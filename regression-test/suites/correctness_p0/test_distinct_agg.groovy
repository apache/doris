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

suite("test_distinct_agg") {
    sql 'drop table if exists t'

    sql '''
        CREATE TABLE `t` (
            `k1` bigint(20) NULL,
            `k2` varchar(20) NULL,
            `k3` varchar(20) NULL,
            `k4` varchar(20) NULL,
            `k5` varchar(20) NULL,
            `k6` datetime NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    '''

    sql '''
        INSERT INTO `t` (`k1`, `k2`, `k3`, `k4`, `k5`, `k6`) VALUES
            (1, '1234', 'A0', 'C0', '1', '2023-01-10 23:00:00');
    '''

    test {
        sql '''
            select k5, k6, SUM(k3) AS k3 
            from ( 
                select
                    k5,
                    date_format(k6, '%Y-%m-%d') as k6,
                    count(distinct k3) as k3 
                from t 
                where 1=1 
                group by k5, k6
            ) AS temp where 1=1
            group by k5, k6;
        '''
        result([['1', '2023-01-10', 1L]])
    }
}