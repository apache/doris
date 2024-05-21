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

suite("test_scan_range", "query,p0") {

    sql "use test_query_db"

    def tableName = "test_scan_range_tbl"

    sql """
        CREATE TABLE `${tableName}` (
          `k1` INT NULL,
          `k2` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "insert into ${tableName} values(1,1)"

    qt_sql_1 "select k1 from ${tableName} where k1 > -2147483648"

    qt_sql_2 "select k1 from ${tableName} where k1 < 2147483647"

    qt_sql_3 "select k1 from ${tableName} where k1 < -2147483648"

    qt_sql_4 "select k1 from ${tableName} where k1 > 2147483647"
}
