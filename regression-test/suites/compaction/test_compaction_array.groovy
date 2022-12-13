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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_array_compaction") {
    def tableName = "tbl_test_array_compaction"

    // array functions only supported in vectorized engine
    sql """ set enable_vectorized_engine = true """

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, [1, 2, 3]), (2, [4]), (3, [])"""
    qt_sql "SELECT * FROM ${tableName} ORDER BY k1"

    qt_sql "DELETE FROM ${tableName} WHERE k1=2"
    qt_sql "SELECT * FROM ${tableName} ORDER BY k1"
}
