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

suite("test_time_round") {
    qt_cast """
        with tmp as (
            select CONCAT(
                YEAR('2024-02-06 03:37:07.157'), '-',
                LPAD(MONTH('2024-02-06 03:37:07.157'), 2, '0'), '-',
                LPAD(DAY('2024-02-06 03:37:07.157'), 2, '0'), ' ',
                LPAD(HOUR('2024-02-06 03:37:07.157'), 2, '0'), ':',
                LPAD(MINUTE('2024-02-06 03:37:07.157'), 2, '0'), ':',
                LPAD(SECOND('2024-02-06 03:37:07.157'), 2, '0'), '.', "123456789")
            AS generated_string)
            select generated_string, cast(generated_string as DateTime(6)) from tmp
    """
    sql """
        DROP TABLE IF EXISTS test_time_round;
    """
    sql """
        CREATE TABLE test_time_round (`rowid` int, str varchar)
        ENGINE=OLAP
        UNIQUE KEY(`rowid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`rowid`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "lineitem_orders",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """
        insert into test_time_round values
            (1, "2024-02-01 12:13:14.123456"),
            (2, "2024-02-01 12:13:14.1234567"),
            (3, "2024-02-01 12:13:14.12345671"),
            (4, "2024-02-01 12:13:14.1234561"),
            (5, "2024-02-01 12:13:14.12345615")
    """

    qt_cast """
        select *, cast (str as Datetime(6)) from test_time_round order by rowid;
    """
}