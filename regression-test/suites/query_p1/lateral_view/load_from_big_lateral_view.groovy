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

suite("query_p1") {
    def dbName = "regression_load_from_big_lateral_view"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"
    sql """
        CREATE TABLE IF NOT EXISTS `test` (
        `k1` smallint NULL,
        `k2` int NULL,
        `k3` bigint NULL,
        `k4` largeint NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`,`k2`,`k3`,`k4`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

    sql """insert into test select e1,e1,e1,e1 from (select 1 k1) as t lateral view explode_numbers(100000000) tmp1 as e1;"""

    qt_sql """select count(*) from test;"""
}

