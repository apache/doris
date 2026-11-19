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

suite("test_external_catalog_credential_masking", "p0,nonConcurrent") {
    String credentialCatalog = "audit_iceberg_credential_masking"
    String tokenCatalog = "audit_iceberg_token_masking"
    String jdbcCatalog = "audit_iceberg_jdbc_password_masking"
    // Low-entropy placeholders keep this negative masking test distinct from real credentials.
    String accessKey = "aaaaaaaaaaaaaaaaaaaa"
    String secretKey = "bbbbbbbbbbbbbbbbbbbb"
    String oauthCredential = "cccccccccccccccccccc"
    String oauthToken = "dddddddddddddddddddd"
    String jdbcPassword = "eeeeeeeeeeeeeeeeeeee"
    String alteredJdbcPassword = "ffffffffffffffffffff"

    // The audit log files CREATE CATALOG under stmt_type OTHER and ALTER CATALOG under ALTER.
    def waitForAuditStatement = { String catalogName, String statement = "create catalog", String stmtType = "OTHER" ->
        String query = """
            select stmt
            from __internal_schema.audit_log
            where stmt_type = '${stmtType}'
              and lower(stmt) like '%${statement} ${catalogName}%'
              and instr(stmt, '*XXX') > 0
            order by time asc
            limit 1
        """
        int retry = 60
        def rows = sql query
        while (rows.isEmpty()) {
            if (retry-- < 0) {
                throw new RuntimeException("audit statement for ${catalogName} was not found")
            }
            sleep(1000)
            sql """call flush_audit_log()"""
            rows = sql query
        }
        return rows[0][0].toString()
    }

    setGlobalVarTemporary([enable_audit_plugin: true], {
        try {
            sql """drop catalog if exists ${credentialCatalog}"""
            sql """drop catalog if exists ${tokenCatalog}"""
            sql """drop catalog if exists ${jdbcCatalog}"""
            sql """truncate table __internal_schema.audit_log"""

            sql """
                create catalog ${credentialCatalog} properties (
                    'type' = 'iceberg',
                    'iceberg.catalog.type' = 'rest',
                    'uri' = 'http://127.0.0.1:1',
                    's3.access_key' = '${accessKey}',
                    's3.secret_key' = '${secretKey}',
                    's3.endpoint' = 'http://127.0.0.1:1',
                    's3.region' = 'us-east-1',
                    'iceberg.rest.security.type' = 'oauth2',
                    'iceberg.rest.oauth2.credential' = '${oauthCredential}',
                    'iceberg.rest.oauth2.server-uri' = 'http://127.0.0.1:1/oauth/tokens'
                )
            """
            sql """
                create catalog ${tokenCatalog} properties (
                    'type' = 'iceberg',
                    'iceberg.catalog.type' = 'rest',
                    'uri' = 'http://127.0.0.1:1',
                    'iceberg.rest.security.type' = 'oauth2',
                    'iceberg.rest.oauth2.token' = '${oauthToken}'
                )
            """
            // iceberg.jdbc.password is on no list fe-core keeps: only the iceberg connector knows it, and
            // declares it on its security surface. The catalog is never initialized, so nothing connects.
            sql """
                create catalog ${jdbcCatalog} properties (
                    'type' = 'iceberg',
                    'iceberg.catalog.type' = 'jdbc',
                    'uri' = 'jdbc:mysql://127.0.0.1:1/iceberg',
                    'iceberg.jdbc.catalog_name' = 'audit_masking',
                    'iceberg.jdbc.user' = 'iceberg',
                    'iceberg.jdbc.password' = '${jdbcPassword}',
                    'warehouse' = 's3://audit-masking/warehouse',
                    's3.access_key' = '${accessKey}',
                    's3.secret_key' = '${secretKey}',
                    's3.endpoint' = 'http://127.0.0.1:1',
                    's3.region' = 'us-east-1'
                )
            """
            String shownJdbc = sql("show create catalog ${jdbcCatalog}")[0][1].toString()
            assertFalse(shownJdbc.contains(jdbcPassword))
            assertTrue(shownJdbc.contains('"iceberg.jdbc.password" = "*XXX"'))
            assertTrue(shownJdbc.contains('"iceberg.jdbc.user" = "iceberg"'))

            // ALTER CATALOG sets the same secrets, so its statement text must be masked in the audit log too.
            sql """
                alter catalog ${jdbcCatalog} set properties (
                    'iceberg.jdbc.password' = '${alteredJdbcPassword}',
                    's3.secret_key' = '${secretKey}'
                )
            """
            shownJdbc = sql("show create catalog ${jdbcCatalog}")[0][1].toString()
            assertFalse(shownJdbc.contains(alteredJdbcPassword))
            assertTrue(shownJdbc.contains('"iceberg.jdbc.password" = "*XXX"'))
            sql """call flush_audit_log()"""

            // Audit statements are independently persisted after parsing, so they must use the
            // same sensitive-key masking contract as SHOW CREATE and printable catalog metadata.
            String credentialStmt = waitForAuditStatement(credentialCatalog)
            assertFalse(credentialStmt.contains(accessKey))
            assertFalse(credentialStmt.contains(secretKey))
            assertFalse(credentialStmt.contains(oauthCredential))
            assertTrue(credentialStmt.contains('"s3.access_key" = "*XXX"'))
            assertTrue(credentialStmt.contains('"s3.secret_key" = "*XXX"'))
            assertTrue(credentialStmt.contains('"iceberg.rest.oauth2.credential" = "*XXX"'))

            String tokenStmt = waitForAuditStatement(tokenCatalog)
            assertFalse(tokenStmt.contains(oauthToken))
            assertTrue(tokenStmt.contains('"iceberg.rest.oauth2.token" = "*XXX"'))

            String jdbcCreateStmt = waitForAuditStatement(jdbcCatalog)
            assertFalse(jdbcCreateStmt.contains(jdbcPassword))
            assertTrue(jdbcCreateStmt.contains('"iceberg.jdbc.password" = "*XXX"'))

            String jdbcAlterStmt = waitForAuditStatement(jdbcCatalog, "alter catalog", "ALTER")
            assertFalse(jdbcAlterStmt.contains(alteredJdbcPassword))
            assertFalse(jdbcAlterStmt.contains(secretKey))
            assertTrue(jdbcAlterStmt.contains('"iceberg.jdbc.password" = "*XXX"'))
            assertTrue(jdbcAlterStmt.contains('"s3.secret_key" = "*XXX"'))
        } finally {
            sql """drop catalog if exists ${credentialCatalog}"""
            sql """drop catalog if exists ${tokenCatalog}"""
            sql """drop catalog if exists ${jdbcCatalog}"""
        }
    })
}
