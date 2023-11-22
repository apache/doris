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

suite("test_decimalv3_overflow") {
    def tblName1 = "test_decimalv3_overflow1"
    sql "drop table if exists ${tblName1}"
	sql """ CREATE  TABLE ${tblName1} (
            `data_time`   date         NOT NULL COMMENT "",
            `c1` decimal(22, 4) NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(`data_time`)
        DISTRIBUTED BY HASH(`data_time`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
	sql "insert into ${tblName1} values('2022-08-01', 104665062791137173.7169)"

	def tblName2 = "test_decimalv3_overflow2"
	sql "drop table if exists ${tblName2}"
    sql """ CREATE  TABLE ${tblName2} (
              `data_time`              date NOT NULL COMMENT "",
              `c2`               decimal(20, 2) NULL COMMENT "",
          ) ENGINE=OLAP
        UNIQUE KEY(`data_time`)
        DISTRIBUTED BY HASH(`data_time`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
    sql "insert into ${tblName2} values('2022-08-01', 705091149953414452.46)"

    qt_sql """ select c2 / 10000 * c1 from ${tblName1}, ${tblName2}; """

    sql """ set check_overflow_for_decimal=true; """

    qt_sql """ select c2 / 10000 * c1 from ${tblName1}, ${tblName2}; """
}
