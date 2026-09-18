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

suite("hbo_persist_internal_db_test", "nonConcurrent") {
    // Persistence of pinned hbo statistics into __internal_schema.hbo_statistics is gated by
    // Config.hbo_persist_pinned_to_internal_db (default false, hot-mutable). Flip it on for this
    // suite and restore the previous value afterwards so other suites keep the default
    // in-memory-only behavior. The in-memory pinned entry is authoritative: SET writes through
    // synchronously and DELETE removes the row again.
    def prevPersist = (sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'hbo_persist_pinned_to_internal_db'; """)[0][1].toString()
    // a pinned entry must carry the struct info its fingerprint was computed from, so the pair
    // below is self consistent (sha256 of the canonical struct info)
    def structCanonical = "S{internal.hbo_test.hbo_persist_t,v1}"
    def fingerprint = "543c7cbc00025dcfb7ead462e174640e63a791b63ce70d2d62b5f76a33208e19"
    def tableName = "__internal_schema.hbo_statistics"
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_persist_pinned_to_internal_db" = "true"); """
        try {
            // SET persists the pinned entry (including its stats type) into the internal table
            sql """ HBO SET STATISTICS VALUE=123456 TYPE=EXACT FINGERPRINT='${fingerprint}' STRUCT='${structCanonical}'; """
            qt_set_persisted """ SELECT fingerprint, row_count, stats_type, literal_mode, struct_info
                FROM ${tableName} WHERE fingerprint = '${fingerprint}'; """

            // DELETE removes the row from the internal table
            sql """ HBO DELETE STATISTICS FINGERPRINT='${fingerprint}'; """
            qt_delete_cleared """ SELECT fingerprint, row_count, stats_type, literal_mode, struct_info
                FROM ${tableName} WHERE fingerprint = '${fingerprint}'; """

            // a join expansion entry is the same kind of entry as a pinned row count - it lives in
            // the same table and is distinguished by stats_type, but its value column carries the
            // fan-out factor and its key is the join condition fingerprint
            def condCanonical = "JE{EqualTo(col(internal.hbo_test.x.a),col(internal.hbo_test.y.a))}"
            def condFingerprint = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(condCanonical.getBytes("UTF-8")).encodeHex().toString()
            sql """ HBO SET STATISTICS VALUE=1000 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}' STRUCT='${condCanonical}'; """
            qt_set_expansion """ SELECT stats_type, expansion, struct_info FROM ${tableName}
                WHERE fingerprint = '${condFingerprint}'; """

            // the unified list reports it with its own type and the factor as its value
            def expansionRows = sql """ HBO SHOW PINNED STATISTICS LIKE 'JE{%'; """
            assertEquals(1, expansionRows.size(), expansionRows.toString())
            assertEquals("join_expansion", expansionRows[0][3].toString())
            assertEquals("1000x", expansionRows[0][4].toString())
            assertEquals(condCanonical, expansionRows[0][5].toString())
            // no granularity and no table version state for a condition keyed entry
            assertEquals("-", expansionRows[0][2].toString())
            assertEquals("-", expansionRows[0][6].toString())

            // a stale clean up never removes an expansion entry: it carries no table version, so
            // even OLDER_THAN (which removes unresolvable entries) has to leave it alone
            sql """ HBO DELETE STALE STATISTICS; """
            sql """ HBO DELETE STALE STATISTICS OLDER_THAN 0; """
            qt_expansion_kept """ SELECT stats_type, expansion FROM ${tableName}
                WHERE fingerprint = '${condFingerprint}'; """

            sql """ HBO DELETE STATISTICS FINGERPRINT='${condFingerprint}'; """
            qt_expansion_deleted """ SELECT stats_type, expansion FROM ${tableName}
                WHERE fingerprint = '${condFingerprint}'; """
        } finally {
            // cleanup runs while the config is still on, so a failure between SET and the DELETE
            // above cannot leave a persisted row behind
            sql """ HBO DELETE STATISTICS FINGERPRINT='${fingerprint}'; """
        }
    } finally {
        // restore the config last so a failing cleanup can not leak the hot config to later suites
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_persist_pinned_to_internal_db" = "${prevPersist}"); """
    }
}
