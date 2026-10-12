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

suite("test_show_ttl", "nonConcurrent") {
    String tableName = "test_show_ttl_properties"
    def checkTtlProperty = { long expectedTtl ->
        def rows = sql "SHOW CREATE TABLE ${tableName}"
        assertEquals(1, rows.size())
        assertEquals(tableName, rows[0][0].toString())
        String createTable = rows[0][1].toString()
        def matcher = createTable =~ /"file_cache_ttl_seconds"\s*=\s*"(\d+)"/
        assertTrue(matcher.find(), "Missing file_cache_ttl_seconds in SHOW CREATE TABLE: ${createTable}")
        long actualTtl = matcher.group(1).toLong()
        assertFalse(matcher.find(), "Duplicate file_cache_ttl_seconds in SHOW CREATE TABLE: ${createTable}")
        logger.info("SHOW CREATE TABLE TTL property: table=${tableName}, expected=${expectedTtl}, actual=${actualTtl}")
        assertEquals(expectedTtl, actualTtl)
    }

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                C_CUSTKEY INTEGER NOT NULL,
                C_NAME VARCHAR(25) NOT NULL,
                C_ADDRESS VARCHAR(40) NOT NULL,
                C_NATIONKEY INTEGER NOT NULL,
                C_PHONE CHAR(15) NOT NULL,
                C_ACCTBAL DECIMAL(15,2) NOT NULL,
                C_MKTSEGMENT CHAR(10) NOT NULL,
                C_COMMENT VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32
            PROPERTIES("file_cache_ttl_seconds"="300")
        """
        // Other SHOW CREATE TABLE properties depend on cluster defaults.
        checkTtlProperty(300L)
        sql "ALTER TABLE ${tableName} SET (\"file_cache_ttl_seconds\"=\"0\")"
        checkTtlProperty(0L)
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
