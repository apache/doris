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

suite("test_dup_tab_datetime_nullable") {

    def table1 = "test_dup_tab_datetime_nullable"

    sql "drop table if exists ${table1}"

    sql """
 CREATE TABLE `${table1}` (
  `siteid` int(11) NULL COMMENT "",
  `datetime1` datetime NULL COMMENT "",
  `datetime2` datetime NULL COMMENT "",
  `datetime3` datetime NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`siteid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)

    """

    sql """insert into ${table1} values
        (1,'2021-01-01 23:10:01','2021-01-02 23:10:04','2021-01-02 22:10:04'),
        (2,'2021-02-01 23:10:01','2021-02-02 23:10:04','2021-03-02 22:10:04'),
        (3,'2021-03-01 23:10:01','2021-03-02 23:10:04','2021-04-02 22:10:04'),
        (4,'2021-04-01 23:10:01','2021-04-02 23:10:04','2021-05-02 22:10:04'),
        (5,'2021-05-01 23:10:01','2021-05-02 23:10:04','2021-06-02 22:10:04'),
        (null,'2021-06-01 23:10:01',null,'2021-06-02 22:10:04')
"""

    qt_read_single_column_1 "select datetime1 from ${table1}"
    qt_read_single_column_2 "select siteid from ${table1}"

    qt_datetime_as_pred_1 "select datetime3 from ${table1} where datetime3='2021-06-02 22:10:04'"
    qt_datetime_as_pred_2 "select datetime3 from ${table1} where datetime3!='2021-06-02 22:10:04'"

    qt_read_multiple_column_1 "select * from ${table1} where datetime3='2021-06-02 22:10:04'"
    qt_read_multiple_column_2 "select * from ${table1} where datetime3!='2021-06-02 22:10:04'"

    qt_key_is_null "select * from ${table1} where siteid is null"
    qt_key_is_not_null "select * from ${table1} where siteid is not null"
    qt_non_key_is_null "select * from ${table1} where datetime2 is null"
    qt_non_key_is_not_null "select * from ${table1} where datetime2 is not null"

    sql "drop table if exists ${table1}"

}