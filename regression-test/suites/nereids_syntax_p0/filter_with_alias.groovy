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

suite("filter_with_alias") {

    sql "drop database if exists filter_alias_test;"

    sql """ CREATE DATABASE IF NOT EXISTS `filter_alias_test` """

    sql """
        CREATE TABLE `filter_alias_test`.`test` (
        `id` int(11) NOT NULL, 
        `name` varchar(255) NULL
        ) ENGINE = OLAP DUPLICATE KEY(`id`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES (
        "replication_allocation" = "tag.location.default: 1", 
        "in_memory" = "false", "storage_format" = "V2", 
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        insert into `filter_alias_test`.`test` values (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
    """
    test {
        sql " select * from internal.filter_alias_test.test b where internal.filter_alias_test.test.id = 1;"
        exception "Unknown column 'id' in 'internal.filter_alias_test.test' in FILTER clause"
    }

    // Test using alias in WHERE clause directly
    qt_filter_select1 """
        select * from `filter_alias_test`.`test` b where b.id = 1;
    """

    // Test using table name without alias in WHERE clause
    qt_filter_select2 """
        select * from `filter_alias_test`.`test` where id = 1;
    """


    test {
        sql " select * from filter_alias_test.test b where filter_alias_test.test.id = 1;"
        exception "Unknown column 'id' in 'filter_alias_test.test' in FILTER clause"
    }

    qt_filter_select3 """
        select * from filter_alias_test.test where filter_alias_test.test.id = 1;
    """

    qt_filter_select4 """
       select * from filter_alias_test.test b where filter_alias_test.b.id = 1;
    """

    qt_filter_select5 """
         select * from internal.filter_alias_test.test b where internal.filter_alias_test.b.id = 1;
    """

    qt_filter_select6 """
         select * from (select id from filter_alias_test.test as b ) as toms order by id;
    """

    qt_filter_select7 """
         select 111 from (select current_date() as toms) as toms2;
    """

    sql "drop database if exists filter_alias_test;"

}
