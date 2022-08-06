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

suite("test_dup_tab_date") {

    def table1 = "test_dup_tab_date"

    sql "drop table if exists ${table1}"

    sql """
CREATE TABLE `${table1}` (
  `siteid` int(11) NOT NULL COMMENT "",
  `date1` date NOT NULL COMMENT "",
  `date2` date NOT NULL COMMENT "",
  `date3` date NOT NULL COMMENT ""
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

    sql """insert into ${table1} values(1, '2021-04-01', '2021-04-02', '2021-04-03'),
        (1, '2021-03-01', '2021-03-02', '2021-03-03'),
        (1, '2021-02-01', '2021-02-02', '2021-02-03'),
        (1, '2021-01-01', '2021-01-02', '2021-01-03')
"""

    qt_sql1 "select date1 from ${table1} order by date1"

    // read single column
    qt_select_1_column "select date1 from ${table1} order by date1"

    // date as pred
    qt_select_pred_1 "select date1 from ${table1} where date1='2021-03-01'"
    qt_select_pred_2 "select date1 from ${table1} where date1='2021-04-01'"
    qt_select_pred_3 "select date2,date1 from ${table1} where date1='2021-04-01'"


    // in pred
    qt_select_in_pred_1 "select date1 from ${table1} where date1 in ('2021-01-01')"
    qt_select_in_pred_2 "select * from ${table1} where date1 in ('2021-01-01')"

    // not in pred
    qt_select_not_in_pred_1 "select date1 from ${table1} where date1 not in ('2021-01-01') order by date1"

    sql "drop table if exists ${table1}"
}
