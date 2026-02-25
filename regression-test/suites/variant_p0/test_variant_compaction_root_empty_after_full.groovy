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

suite("test_variant_compaction_root_empty_after_full", "nonConcurrent") {
    def tableName = "test_variant_compaction_root_empty_after_full"

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k bigint,
                v variant <properties("variant_max_subcolumns_count" = "0")>
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V2");
        """

        sql """insert into ${tableName} values (1, '[{"a" : 123, "b" : 456}]')"""
        sql """insert into ${tableName} values (1, '[{"a" : 123, "b" : 456}]')"""

        def before = sql "select cast(v as string) from ${tableName} order by k"
        assertEquals(2, before.size())
        for (def row : before) {
            def s = row[0].toString()
            assertTrue(!s.equals("{}"))
            assertTrue(s.contains("\"a\""))
            assertTrue(s.contains("\"b\""))
        }

        trigger_and_wait_compaction(tableName, "full", 1800)

        def after = sql "select cast(v as string) from ${tableName} order by k"
        assertEquals(2, after.size())
        for (def row : after) {
            def s = row[0].toString()
            assertTrue(!s.equals("{}"), "after=" + s)
            assertTrue(s.contains("\"a\""), "after=" + s)
            assertTrue(s.contains("\"b\""), "after=" + s)
        }
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
