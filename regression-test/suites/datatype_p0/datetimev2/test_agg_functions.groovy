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

suite("test_agg_functions") {
    sql "drop table if exists test_datetimev2_agg_functions"

    sql """
    CREATE TABLE `test_datetimev2_agg_functions` (
      `f1` datetimev2(6)
    )
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    )
    """

    sql """insert into test_datetimev2_agg_functions values
    (null),
    (null),
    ('0000-01-01 00:00:00'),
    ('0000-01-01 00:00:00.000000'),
    ('0000-01-01 00:00:00.000001'),
    ('0000-01-01 00:00:00.123456'),
    ('0000-01-01 00:00:00.999999'),
    ('2023-08-08 20:20:20'),
    ('2023-08-08 20:20:20.000000'),
    ('2023-08-08 20:20:20.000001'),
    ('2023-08-08 20:20:20.123456'),
    ('2023-08-08 20:20:20.999999'),
    ('9999-12-31 23:59:59'),
    ('9999-12-31 23:59:59.000000'),
    ('9999-12-31 23:59:59.000001'),
    ('9999-12-31 23:59:59.123456'),
    ('9999-12-31 23:59:59.999999');
    """
    qt_all "select * from test_datetimev2_agg_functions order by 1"
    qt_count_distinct """
    select multi_distinct_count(f1) from test_datetimev2_agg_functions;
    """

}
