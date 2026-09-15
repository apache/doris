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

suite("test_gamma") {
    sql " drop table if exists test_gamma"
    sql """
        create table test_gamma (
            k0 int,
            a double not null,
            b double null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select gamma(b) from test_gamma"
    order_qt_empty_not_nullable "select gamma(a) from test_gamma"

    sql "insert into test_gamma values (1, 1, null), (1, 1, null), (1, 1, null)"
    order_qt_all_null "select gamma(b) from test_gamma"

    sql "truncate table test_gamma"
    sql """ insert into test_gamma values
        (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 10, 10),
        (7, 0, 0), (8, -0, -0), (9, -1, -1), (10, -2, -2), (11, -3, -3),
        (12, 0.5, 0.5), (13, -0.5, -0.5), (14, 1.5, 1.5), (15, -1.5, -1.5), (16, -2.5, -2.5),
        (17, 171, 171), (18, 172, 172), (19, 1e308, 1e308),
        (20, cast('nan' as double), cast('nan' as double)),
        (21, cast('inf' as double), cast('inf' as double)),
        (22, cast('-inf' as double), cast('-inf' as double)),
        (23, 123, null);
    """

    order_qt_nullable "select gamma(b) from test_gamma"
    order_qt_not_nullable "select gamma(a) from test_gamma"
    order_qt_nullable_no_null "select gamma(nullable(a)) from test_gamma"
    order_qt_const_nullable "select gamma(NULL) from test_gamma"
    order_qt_const_not_nullable "select gamma(0.5) from test_gamma"
    order_qt_const_nullable_no_null "select gamma(nullable(0.5))"

    testFoldConst """ select gamma(0), gamma(-1), gamma(-2), gamma(0.5), gamma(5), gamma(-2.5), gamma(171), gamma(172), gamma(cast('nan' as double)), gamma(cast('inf' as double)), gamma(cast('-inf' as double)) """
}
