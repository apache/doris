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

suite("regression_test_variant_element_at", "p0")  {
      sql """
        CREATE TABLE IF NOT EXISTS element_fn_test(
            k bigint,
            v variant,
            v1 variant not null,
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """insert into element_fn_test values (1, '{"arr1" : [1, 2, 3]}', '{"arr2" : [4, 5, 6]}')"""
    qt_sql """select array_first((x,y) -> (x - y) < 0, cast(v['arr1'] as array<int>), cast(v1['arr2'] as array<int>)) from element_fn_test"""
}