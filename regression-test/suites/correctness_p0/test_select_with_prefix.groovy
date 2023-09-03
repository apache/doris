
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

suite("test_select_with_prefix") {
    def tableName = "test_select_with_perfix"
    sql """ DROP TABLE IF EXISTS $tableName """
    sql """
        CREATE TABLE $tableName (
       `c0` varchar(64) NULL,
       `c1` varchar(64) NULL,
       `c2` bigint(20) NOT NULL,
       `c3` char(32) NULL,
       `c4` datetime  NULL,
       `c5` tinyint(4) NOT NULL
     ) ENGINE=OLAP
     UNIQUE KEY(`c0`, `c1`, `c2`, `c3`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`c0`) BUCKETS 1
     PROPERTIES (
     "replication_allocation" = "tag.location.default: 1",
     "enable_unique_key_merge_on_write" = "true"
     );
    """

    sql """
         INSERT INTO $tableName (`c0`, `c1`, `c2`, `c3`, `c4`, `c5`) VALUES
             ('', 'efgh', 1, 'abcdad', '2023-06-27 00:00:00', 1),
             ('', 'mefgh', 1, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcd', 'efgh', 1, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcd', 'efgh', 2, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcd', 'efghf', 2, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcde', 'ab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abcdf', 'ab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abcdf', 'dab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abce', 'dab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abce', 'ldab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abd', 'ldab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('def', 'hh', 2, 'afad', '2023-06-27 00:00:00', 1)
    """

    sql "sync"

    qt_sql "select * from $tableName where c0='abc'"

    qt_sql "select c0 from $tableName where c0='abc'"

    qt_sql "select * from $tableName where c0='abcd'"

    qt_sql "select c0 from $tableName where c0='abcd'"

    qt_sql "select c1 from $tableName where c0='abc'"

    qt_sql "select c1 from $tableName where c0='abcd'"

    qt_sql "select * from $tableName where c0=''"

    qt_sql "select c0 from $tableName where c0=''"
    
    qt_sql "select count(*) from $tableName where c0=''"

    qt_sql "select * from $tableName where c1='efg'"

    qt_sql "select c0 from $tableName where c1='efg'"

    qt_sql "select * from $tableName where c1='efgh'"

    qt_sql "select c0 from $tableName where c1='efgh'"

    sql """ DROP TABLE IF EXISTS $tableName """
    sql """
        CREATE TABLE $tableName (
       `c0` varchar(64) NULL,
       `c1` varchar(64) NULL,
       `c2` bigint(20) NOT NULL,
       `c3` char(32) NULL,
       `c4` datetime  NULL,
       `c5` tinyint(4) NOT NULL
     ) ENGINE=OLAP
     UNIQUE KEY(`c0`, `c1`, `c2`, `c3`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`c0`) BUCKETS 1
     PROPERTIES (
     "replication_allocation" = "tag.location.default: 1",
     "enable_unique_key_merge_on_write" = "false"
     );
    """

    sql """
         INSERT INTO $tableName (`c0`, `c1`, `c2`, `c3`, `c4`, `c5`) VALUES
             ('', 'efgh', 1, 'abcdad', '2023-06-27 00:00:00', 1),
             ('', 'mefgh', 1, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcd', 'efgh', 1, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcd', 'efgh', 2, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcd', 'efghf', 2, 'abcdad', '2023-06-27 00:00:00', 1),
             ('abcde', 'ab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abcdf', 'ab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abcdf', 'dab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abce', 'dab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abce', 'ldab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('abd', 'ldab', 2, '2efraeef', '2023-06-27 00:00:00', 1),
             ('def', 'hh', 2, 'afad', '2023-06-27 00:00:00', 1)
    """

    sql "sync"

    qt_sql "select * from $tableName where c0='abc'"

    qt_sql "select c0 from $tableName where c0='abc'"

    qt_sql "select * from $tableName where c0='abcd'"

    qt_sql "select c0 from $tableName where c0='abcd'"

    qt_sql "select c1 from $tableName where c0='abc'"

    qt_sql "select c1 from $tableName where c0='abcd'"

    qt_sql "select * from $tableName where c0=''"

    qt_sql "select c0 from $tableName where c0=''"
    
    qt_sql "select count(*) from $tableName where c0=''"

    qt_sql "select * from $tableName where c1='efg'"

    qt_sql "select c0 from $tableName where c1='efg'"

    qt_sql "select * from $tableName where c1='efgh'"

    qt_sql "select c0 from $tableName where c1='efgh'"

    sql """ DROP TABLE IF EXISTS $tableName """
}

