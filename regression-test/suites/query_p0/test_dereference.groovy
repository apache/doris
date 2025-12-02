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

import com.google.common.collect.Lists

suite("test_dereference") {
    multi_sql """
        drop table if exists test_dereference;
        create table test_dereference(
          id int,
          a array<int>,
          m map<string, int>,
          s struct<a: int, b: double>,
          v variant
        )
        distributed by hash(id)
        properties(
          'replication_num'='1'
        );
        
        insert into test_dereference
        values (1, array(1, 2, 3, 4, 5), map('a', 1, 'b', 2, 'c', 3), struct(1, 2), '{"v": {"v":200}}')
        """

    test {
        sql "select cardinality(a), map_size(m), map_keys(m), map_values(m), m.a, m.b, m.c, s.a, s.b, v.v.v from test_dereference"
        result([[5L, 3L, '["a", "b", "c"]', '[1, 2, 3]', 1, 2, 3, 1, 2d, "200"]])
    }

    multi_sql """
        drop table if exists test_dereference2;
        create table test_dereference2(
          id int,
          s struct<s:struct<s:struct<s:int>>>,
          v variant
        )
        distributed by hash(id)
        properties(
          'replication_num'='1'
        );
        
        insert into test_dereference2
        values (1, struct(struct(struct(100))), '{"v": {"v": 200}}')
        """

    test {
        sql "select s.s.s.s, v.v.v from test_dereference2"
        result([[100, "200"]])
    }

    test {
        sql "select s.a from test_dereference2"
        exception "No such struct field 'a' in 's'"
    }
}