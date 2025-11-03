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

suite ("test_lsc_default") {
    def table = "test_lsc_default"
    sql "DROP TABLE IF EXISTS ${table}"

    sql """CREATE TABLE IF NOT EXISTS `${table}` (
        `siteid` int(11) NULL COMMENT "",
        `citycode` int(11) NULL COMMENT "",
        `userid` int(11) NULL COMMENT "",
        `pv` int(11) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""
    
    def result = sql "show create table ${table};"
    assertTrue(result[0][1].contains("\"light_schema_change\" = \"true\""))
}
