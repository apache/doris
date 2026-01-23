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

suite("test_sql_digest_generation", "nonConcurrent") {
    sql "set global enable_audit_plugin = true"

    sql "drop table if exists audit_log_behavior"
    sql """
        CREATE TABLE `audit_log_behavior` (
          `id` bigint,
          `name` varchar(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql "insert into audit_log_behavior values (1, 'test')"
    sql "insert into audit_log_behavior values (2, 'test')"

    setFeConfigTemporary([sql_digest_generation_threshold_ms:0]) {
        Thread.sleep(10000)
        sql """call flush_audit_log()"""

        def prevSqlDigestCountResult = sql """SELECT COUNT(*) FROM __internal_schema.audit_log WHERE sql_digest != '';"""
        Long prevSqlDigestCount = Long.valueOf(prevSqlDigestCountResult[0][0])
        logger.info("prev sql_digest count: " + prevSqlDigestCount)

        sql "select * from audit_log_behavior"

        Thread.sleep(10000)
        sql """call flush_audit_log()"""

        def nowSqlDigestCountResult = sql """SELECT COUNT(*) FROM __internal_schema.audit_log WHERE sql_digest != '';"""
        Long nowSqlDigestCount = Long.valueOf(nowSqlDigestCountResult[0][0])
        logger.info("now sql_digest count: " + nowSqlDigestCount)

        assertTrue(nowSqlDigestCount > prevSqlDigestCount, "Count of sql_digest did not increase")
    }

    Thread.sleep(10000)
    sql """call flush_audit_log()"""

    setFeConfigTemporary([sql_digest_generation_threshold_ms:100000]) {
        sql """call flush_audit_log()"""
        Thread.sleep(10000)

        def prevSqlDigestCountResult = sql """SELECT COUNT(*) FROM __internal_schema.audit_log WHERE sql_digest != '';"""
        Long prevSqlDigestCount = Long.valueOf(prevSqlDigestCountResult[0][0])
        logger.info("prev sql_digest count: " + prevSqlDigestCount)

        sql "select * from audit_log_behavior"

        Thread.sleep(10000)
        sql """call flush_audit_log()"""

        def nowSqlDigestCountResult = sql """SELECT COUNT(*) FROM __internal_schema.audit_log WHERE sql_digest != '';"""
        Long nowSqlDigestCount = Long.valueOf(nowSqlDigestCountResult[0][0])
        logger.info("now sql_digest count: " + nowSqlDigestCount)

        assertTrue(nowSqlDigestCount == prevSqlDigestCount, "Count of sql_digest changed")
    }

    Thread.sleep(10000)
    sql """call flush_audit_log()"""
}
