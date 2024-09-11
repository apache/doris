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

suite("test_is_nan") {
    sql " drop table if exists test_is_nan"
    sql """
        create table test_is_nan (
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

    order_qt_empty_nullable "select is_nan(b) from test_is_nan"
    order_qt_empty_not_nullable "select is_nan(a) from test_is_nan"

    sql """ insert into test_is_nan values (1, 1e-100, 1e-100), (2, -1e100, -1e100), (3, 1e100, 1e100), (4, 1, 1), (5, -1, -1),
        (6, 0, 0), (7, -0, -0), (8, 123, 123), (9, 0.1, 0.1), (10, -0.1, -0.1), (11, 1e-15, 1e-15), (12, 0, null),
        (13, 0, nan()), (14, nan(), null);
    """

    order_qt_nullable "select is_nan(b) from test_is_nan"
    order_qt_not_nullable "select is_nan(a) from test_is_nan"
    order_qt_nullable_no_null "select is_nan(nullable(a)) from test_is_nan"
    order_qt_const_nullable "select is_nan(NULL) from test_is_nan" // choose one case to test const multi-rows
    order_qt_const_not_nullable "select is_nan(0.5)"
    order_qt_const_nullable_no_null "select is_nan(nullable(0.5))"
    order_qt_const_nullable_nan "select is_nan(nan()) from test_is_nan" // choose one case to test const multi-rows
    order_qt_const_not_nullable_nan "select is_nan(nan())"
    order_qt_const_nullable_no_null_nan "select is_nan(nullable(nan()))"
}