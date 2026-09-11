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
            sql """ HBO SET STATISTICS '${fingerprint}' = 123456 TYPE FILTER_SMALL STRUCT '${structCanonical}'; """
            qt_set_persisted """ SELECT fingerprint, row_count, stats_type, fingerprint_kind, struct_info
                FROM ${tableName} WHERE fingerprint = '${fingerprint}'; """

            // DELETE removes the row from the internal table
            sql """ HBO DELETE STATISTICS '${fingerprint}'; """
            qt_delete_cleared """ SELECT fingerprint, row_count, stats_type, fingerprint_kind, struct_info
                FROM ${tableName} WHERE fingerprint = '${fingerprint}'; """
        } finally {
            // cleanup runs while the config is still on, so a failure between SET and the DELETE
            // above cannot leave a persisted row behind
            sql """ HBO DELETE STATISTICS '${fingerprint}'; """
        }
    } finally {
        // restore the config last so a failing cleanup can not leak the hot config to later suites
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_persist_pinned_to_internal_db" = "${prevPersist}"); """
    }
}
