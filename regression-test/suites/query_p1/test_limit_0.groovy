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

suite("test_limit_0") {
    def dbName = "test_limit_0"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql """
        CREATE TABLE IF NOT EXISTS `${dbName}` (
        `k1` smallint NULL,
        `k2` int NULL,
        `k3` bigint NULL,
        `k4` varchar(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`,`k2`,`k3`,`k4`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
    def result = sql_meta "select * from (select * from ${dbName} ) a limit 0"
    print result
    assertTrue(result[0][0]=='k1');
    assertTrue(result[0][1]=='SMALLINT');
    assertTrue(result[1][0]=='k2');
    assertTrue(result[1][1]=='INT');
    assertTrue(result[2][0]=='k3');
    assertTrue(result[2][1]=='BIGINT');
    assertTrue(result[3][0]=='k4');
    assertTrue(result[3][1]=='CHAR');
}

