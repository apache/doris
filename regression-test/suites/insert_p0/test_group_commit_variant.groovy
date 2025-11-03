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

suite("test_group_commit_variant") {

    sql "set group_commit=async_mode"

    def testTable = "test_group_commit_variant"

    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                k bigint,
                var variant
                )
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH (`k`) BUCKETS 5
                properties("replication_num" = "1",
                "disable_auto_compaction" = "false");
        """
    
    try {
        sql "insert into ${testTable} (k) values (1),(2);"
        sql "insert into ${testTable} (k) values (3),(4);"
    } catch (Exception e) {
        // should not throw exception
        logger.info(e.getMessage())
        assertTrue(False)
    }
}
