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

suite("test_full_join_batch_size", "query,p0") {
    sql " drop table if exists test_full_join_batch_size_l; ";
    sql " drop table if exists test_full_join_batch_size_r; ";
    sql """
        create table test_full_join_batch_size_l (
            k1 int,
            v1 int
        ) distributed by hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql """
        create table test_full_join_batch_size_r (
            k1 int,
            v1 int
        ) distributed by hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql """ insert into test_full_join_batch_size_l values (1, 11), (1, 111), (1, 1111) """
    sql """ insert into test_full_join_batch_size_r values (1, null), (1, 211), (1, 311), (1, 411) """

    qt_sql1 """
        select /*+SET_VAR(batch_size=3)*/
               l.k1,
               l.v1,
               r.k1,
               r.v1
        from
               test_full_join_batch_size_l l
               full join test_full_join_batch_size_r r on (
                      r.v1 = 0
                      or r.v1 is null
               )
               and l.k1 = r.k1
                order by l.k1, l.v1, r.k1, r.v1;
    """
}