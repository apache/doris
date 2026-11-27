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

suite("test_iceberg_google_unicode_audit_masking", "p0,nonConcurrent") {
    String catalogName = "iceberg_google_audit_" + UUID.randomUUID().toString().replace("-", "")
    String prefix = "/* " + ("😀" * 8) + " */ "
    String json = '{"type":"service_account","private_key":"fixture-' + ("z" * 32) + '😀' + ("z" * 32) + '"}'
    def auditStatement = { String operation, String statementType ->
        for (int attempt = 0; attempt < 60; attempt++) {
            sql "CALL flush_audit_log()"
            def rows = sql """SELECT stmt FROM __internal_schema.audit_log
                WHERE stmt_type='${statementType}' AND lower(stmt) LIKE '%${operation} ${catalogName}%'
                ORDER BY time DESC LIMIT 1"""
            if (!rows.isEmpty()) {
                return rows[0][0].toString()
            }
            sleep(1000)
        }
        throw new IllegalStateException("Audit statement was not persisted for " + operation)
    }
    def fullyMasked = { String statement ->
        assertTrue(statement.contains('"gcp.auth.credentials-json" = "*XXX"'))
        // An offset bug can mask the full secret while still leaving its trailing characters behind.
        assertFalse(statement.contains("zzzz"))
        assertFalse(statement.contains("private_key"))
    }

    setGlobalVarTemporary([enable_audit_plugin: true], {
        try {
            sql """${prefix}CREATE CATALOG ${catalogName} PROPERTIES (
                "type"="iceberg", "iceberg.catalog.type"="rest", "iceberg.rest.uri"="http://127.0.0.1:1",
                "iceberg.rest.security.type"="google", "gcp.auth.credentials-json"='${json}')"""
            fullyMasked(auditStatement("create catalog", "OTHER"))
            sql """${prefix}ALTER CATALOG ${catalogName} SET PROPERTIES (
                "gcp.auth.credentials-json"='${json.replace("fixture-", "updated-")}')"""
            String altered = auditStatement("alter catalog", "ALTER")
            fullyMasked(altered)
            assertTrue(altered ==~ /(?s).*\(\s*"gcp\.auth\.credentials-json"\s*=\s*"\*XXX"\s*\)\s*;?\s*/)
        } finally {
            sql "DROP CATALOG IF EXISTS ${catalogName}"
        }
    })
}
