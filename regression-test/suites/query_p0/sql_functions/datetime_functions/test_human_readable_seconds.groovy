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

suite("test_human_readable_seconds") {
    sql "drop table if exists test_human_readable_seconds"
    sql """
        create table test_human_readable_seconds (
            k0 int,
            a double not null,
            b double null
        )
        distributed by hash(k0)
        properties
        (
            "replication_num" = "1"
        );
    """

    qt_empty_nullable "select human_readable_seconds(b) from test_human_readable_seconds order by k0"
    qt_empty_not_nullable "select human_readable_seconds(a) from test_human_readable_seconds order by k0"

    sql "insert into test_human_readable_seconds values (1, 1, null), (2, 1, null), (3, 1, null)"
    qt_all_null "select human_readable_seconds(b) from test_human_readable_seconds order by k0"

    sql "truncate table test_human_readable_seconds"
    sql """
        insert into test_human_readable_seconds values
        (1, 96, 96),
        (2, 3762, 3762),
        (3, 56363463, 56363463),
        (4, 0, 0),
        (5, -96, -96),
        (6, 0.9, 0.9),
        (7, 1.2, 1.2),
        (8, 61, 61),
        (9, 604800, 604800),
        (10, 3600, null);
    """

    qt_nullable "select human_readable_seconds(b) from test_human_readable_seconds order by k0"
    qt_not_nullable "select human_readable_seconds(a) from test_human_readable_seconds order by k0"
    qt_nullable_no_null "select human_readable_seconds(nullable(a)) from test_human_readable_seconds order by k0"
    qt_const_nullable "select human_readable_seconds(NULL) from test_human_readable_seconds order by k0"
    qt_const_not_nullable "select human_readable_seconds(3762) from test_human_readable_seconds order by k0"
    qt_const_nullable_no_null "select human_readable_seconds(nullable(3762))"
}
