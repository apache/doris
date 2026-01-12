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

suite("corr_welford") {
    sql """
        drop table if exists test_corr;
    """
    sql """
    create table test_corr(
        id int,
        k1 double,
        k2 double
    ) distributed by hash (id) buckets 1
    properties ("replication_num"="1");
    """
    sql """
    insert into test_corr values 
        (1, 20, 22),
        (1, 10, 20),
        (2, 36, 21),
        (2, 30, 22),
        (2, 25, 20),
        (3, 25, NULL),
        (4, 25, 21),
        (4, 25, 22),
        (4, 25, 20);
    """
    qt_corr_welford_group_by """select id,corr_welford(k1,k2) from test_corr group by id order by id;"""
    qt_corr_welford_empty """select corr_welford(k1,k2) from test_corr where id=999;"""
}
