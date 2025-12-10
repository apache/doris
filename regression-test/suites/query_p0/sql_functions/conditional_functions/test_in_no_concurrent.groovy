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

suite("test_in_no_concurrent", "nonConcurrent") {

    sql "DROP TABLE IF EXISTS `test_in_no_concurrent_tbl`"
    sql """
        CREATE TABLE `test_in_no_concurrent_tbl` (
        `id` int NULL,
        `c1` char(10) NULL,
        `c2` char(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `c1`, `c2`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
        );
    """

    sql """
        insert into `test_in_no_concurrent_tbl` values
            (1, 'a', 'b'),
            (2, 'c', 'd'),
            (3, 'e', 'f'),
            (4, 'g', 'h'),
            (5, 'i', 'j');
    """

    qt_select """
        select count(*) from `test_in_no_concurrent_tbl`;
    """

    sql """
        delete from `test_in_no_concurrent_tbl` where id > 1 and c2 in ('h', 'f');
    """

    set_be_param("enable_low_cardinality_optimize", "false");

    qt_select1 """
        select count(*) from `test_in_no_concurrent_tbl`;
    """

    qt_select2 """
        select c1 from `test_in_no_concurrent_tbl` order by 1;
    """

    set_be_param("enable_low_cardinality_optimize", "true");

    qt_select3 """
        select c1 from `test_in_no_concurrent_tbl` order by 1;
    """
}
