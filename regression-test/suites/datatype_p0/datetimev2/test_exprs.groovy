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

suite("test_exprs") {

    def table1 = "test_datetimev2_exprs"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      `col` datetimev2(3) NULL COMMENT ""
    ) ENGINE=OLAP
    UNIQUE KEY(`col`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`col`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values('2022-01-01 11:11:11.111'),
            ('2022-01-01 11:11:11.222')
    """
    qt_select_all "select * from ${table1} order by col"

    qt_sql_cast_datetimev2 " select cast(col as datetimev2(5)) col1 from ${table1} order by col1; "
}
