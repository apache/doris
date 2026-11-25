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
    String maxComputeSessionToken = "gggggggggggggggggggg"
    String ozoneAccessKey = "hhhhhhhhhhhhhhhhhhhh"
    Map<String, String> policyProperties = [
        'jdbc_url': 'jdbc:fixture://db.example.com;password=fixture-jdbc',
        'hosts': 'https://fixture:fixture-userinfo@api.example.com/v1?token=fixture-url-token&region=east',
        'list.hosts': 'https://host1.example:9200/,https://fixture:fixture-list-password@host2.example:9200/',
        'token': 'fixture-native-token',
        'credential': 'fixture-native-credential',
        'fs.s3a.secret.key': 'fixture-hadoop-secret',
        'fs.azure.account.key.fixture.blob.core.windows.net': 'fixture-account-key',
        'custom.password': 'fixture-custom-password',
        'signed.endpoint': 'https://api.example.com/v1?X-Amz-Signature=fixture-signature',
        'safe.endpoint': 'host.example.com:9000',
        'driver_url': '/opt/example/libdriver.so',
        'file.uri': 'file:/data/example/catalog.db?token=fixture-file-token&mode=ro',
        'fs.defaultFS': 'viewfs://masking-cluster',
        'gs.uri': 'gs://example_bucket/warehouse?token=fixture-gcs-token',
        'hdfs.uri': 'hdfs:///data.csv',
        'viewfs.uri': 'viewfs:///warehouse',
        'viewfs.named.uri': 'viewfs://mount_table/warehouse'
    ]
    Map<String, String> azureLocations = [
        'azure.abfs.uri': 'abfs://warehouse@account.dfs.core.windows.net/tables',
        'azure.abfss.uri': 'abfss://warehouse@account.dfs.core.windows.net/tables',
        'azure.wasb.uri': 'wasb://warehouse@account.blob.core.windows.net/tables',
        'azure.wasbs.uri': 'wasbs://warehouse@account.blob.core.windows.net/tables'
    ]
    azureLocations.each { key, location ->
        policyProperties[key] = "${location}?token=fixture-azure-token&region=east"
    }
    String policySql = policyProperties.collect { key, value -> "'${key}' = '${value}'" }.join(',\n')
    List<String> policySecrets = ['fixture-jdbc', 'fixture-userinfo', 'fixture-url-token', 'fixture-native-token',
        'fixture-native-credential', 'fixture-hadoop-secret', 'fixture-account-key', 'fixture-custom-password',
        'fixture-signature', 'fixture-list-password', 'fixture-azure-token', 'fixture-file-token', 'fixture-gcs-token']

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
                    'mc.session_token' = '${maxComputeSessionToken}',
                    'ozone.access_key' = '${ozoneAccessKey}',
                    'iceberg.rest.security.type' = 'oauth2',
                    'iceberg.rest.oauth2.credential' = '${oauthCredential}',
                    'iceberg.rest.oauth2.server-uri' = 'http://127.0.0.1:1/oauth/tokens',
                    ${policySql}
                )
            """
            // Masking is global across catalog properties, even when a key belongs to another loaded plugin.
            String shownCredentials = sql("show create catalog ${credentialCatalog}")[0][1].toString()
            assertFalse(shownCredentials.contains(maxComputeSessionToken))
            assertFalse(shownCredentials.contains(ozoneAccessKey))
            assertTrue(shownCredentials.contains('"mc.session_token" = "*XXX"'))
            assertTrue(shownCredentials.contains('"ozone.access_key" = "*XXX"'))
            def shownProperties = sql("show catalog ${credentialCatalog}").collectEntries { row ->
                [(row[0].toString()): row[1].toString()]
            }
            assertEquals('*XXX', shownProperties['mc.session_token'])
            assertEquals('*XXX', shownProperties['ozone.access_key'])
            policySecrets.each { secret -> assertFalse(shownCredentials.contains(secret)) }
            ['jdbc_url', 'token', 'credential', 'fs.s3a.secret.key',
                'fs.azure.account.key.fixture.blob.core.windows.net', 'custom.password', 'signed.endpoint',
                'list.hosts'].each { key ->
                assertEquals('*XXX', shownProperties[key])
            }
            assertEquals('https://api.example.com/v1?token=*XXX&region=east', shownProperties['hosts'])
            assertEquals('host.example.com:9000', shownProperties['safe.endpoint'])
            assertEquals('/opt/example/libdriver.so', shownProperties['driver_url'])
            assertEquals('file:/data/example/catalog.db?token=*XXX&mode=ro', shownProperties['file.uri'])
            assertEquals('viewfs://masking-cluster', shownProperties['fs.defaultFS'])
            assertEquals('gs://example_bucket/warehouse?token=*XXX', shownProperties['gs.uri'])
            assertEquals('hdfs:///data.csv', shownProperties['hdfs.uri'])
            assertEquals('viewfs:///warehouse', shownProperties['viewfs.uri'])
            assertEquals('viewfs://mount_table/warehouse', shownProperties['viewfs.named.uri'])
            azureLocations.each { key, location ->
                String rendered = "${location}?token=*XXX&region=east"
                assertEquals(rendered, shownProperties[key])
                assertTrue(shownCredentials.contains(rendered))
            }
            def tvfProperties = sql("""select `Property`, `Value` from catalogs()
                    where CatalogName = '${credentialCatalog}' order by `Property`""").collectEntries { row ->
                [(row[0].toString()): row[1].toString()]
            }
            assertEquals(shownProperties, tvfProperties)
            sql """
                create catalog ${tokenCatalog} properties (
                    'type' = 'iceberg',
                    'iceberg.catalog.type' = 'rest',
                    'uri' = 'http://127.0.0.1:1',
                    'iceberg.rest.security.type' = 'oauth2',
                    'iceberg.rest.oauth2.token' = '${oauthToken}'
                )
            """
            // Keep both embedded and separate JDBC passwords covered through actual catalog commands.
            // The catalog is never initialized, so nothing connects.
            sql """
                create catalog ${jdbcCatalog} properties (
                    'type' = 'iceberg',
                    'iceberg.catalog.type' = 'jdbc',
                    'uri' = 'jdbc:mysql://127.0.0.1:1/iceberg?password=${jdbcPassword}',
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
            assertTrue(shownJdbc.contains('"uri" = "*XXX"'))
            assertFalse(shownJdbc.contains('jdbc:mysql:'))

            // ALTER CATALOG sets the same secrets, so its statement text must be masked in the audit log too.
            sql """
                alter catalog ${jdbcCatalog} set properties (
                    'uri' = 'jdbc:mysql://127.0.0.1:1/iceberg?password=${alteredJdbcPassword}',
                    'iceberg.jdbc.password' = '${alteredJdbcPassword}',
                    's3.secret_key' = '${secretKey}',
                    'mc.session_token' = '${maxComputeSessionToken}',
                    'ozone.access_key' = '${ozoneAccessKey}'
                )
            """
            shownJdbc = sql("show create catalog ${jdbcCatalog}")[0][1].toString()
            assertFalse(shownJdbc.contains(alteredJdbcPassword))
            assertTrue(shownJdbc.contains('"iceberg.jdbc.password" = "*XXX"'))
            assertFalse(shownJdbc.contains(maxComputeSessionToken))
            assertFalse(shownJdbc.contains(ozoneAccessKey))
            assertTrue(shownJdbc.contains('"mc.session_token" = "*XXX"'))
            assertTrue(shownJdbc.contains('"ozone.access_key" = "*XXX"'))
            sql """call flush_audit_log()"""

            // Audit statements are independently persisted after parsing, so they must use the
            // same sensitive-key masking contract as SHOW CREATE and printable catalog metadata.
            String credentialStmt = waitForAuditStatement(credentialCatalog)
            assertFalse(credentialStmt.contains(accessKey))
            assertFalse(credentialStmt.contains(secretKey))
            assertFalse(credentialStmt.contains(oauthCredential))
            assertFalse(credentialStmt.contains(maxComputeSessionToken))
            assertFalse(credentialStmt.contains(ozoneAccessKey))
            assertTrue(credentialStmt.contains('"s3.access_key" = "*XXX"'))
            assertTrue(credentialStmt.contains('"s3.secret_key" = "*XXX"'))
            assertTrue(credentialStmt.contains('"iceberg.rest.oauth2.credential" = "*XXX"'))
            assertTrue(credentialStmt.contains('"mc.session_token" = "*XXX"'))
            assertTrue(credentialStmt.contains('"ozone.access_key" = "*XXX"'))
            policySecrets.each { secret -> assertFalse(credentialStmt.contains(secret)) }
            assertTrue(credentialStmt.contains('https://api.example.com/v1?token=*XXX&region=east'))
            assertTrue(credentialStmt.contains('/opt/example/libdriver.so'))
            assertTrue(credentialStmt.contains('file:/data/example/catalog.db?token=*XXX&mode=ro'))
            assertTrue(credentialStmt.contains('viewfs://masking-cluster'))
            assertTrue(credentialStmt.contains('gs://example_bucket/warehouse?token=*XXX'))
            assertTrue(credentialStmt.contains('hdfs:///data.csv'))
            assertTrue(credentialStmt.contains('viewfs:///warehouse'))
            assertTrue(credentialStmt.contains('viewfs://mount_table/warehouse'))
            azureLocations.each { key, location ->
                assertTrue(credentialStmt.contains("${location}?token=*XXX&region=east"))
            }

            String tokenStmt = waitForAuditStatement(tokenCatalog)
            assertFalse(tokenStmt.contains(oauthToken))
            assertTrue(tokenStmt.contains('"iceberg.rest.oauth2.token" = "*XXX"'))

            String jdbcCreateStmt = waitForAuditStatement(jdbcCatalog)
            assertFalse(jdbcCreateStmt.contains(jdbcPassword))
            assertTrue(jdbcCreateStmt.contains('"iceberg.jdbc.password" = "*XXX"'))
            assertTrue(jdbcCreateStmt.contains('"uri" = "*XXX"'))
            assertFalse(jdbcCreateStmt.contains('jdbc:mysql:'))

            String jdbcAlterStmt = waitForAuditStatement(jdbcCatalog, "alter catalog", "ALTER")
            assertFalse(jdbcAlterStmt.contains(alteredJdbcPassword))
            assertFalse(jdbcAlterStmt.contains(secretKey))
            assertFalse(jdbcAlterStmt.contains(maxComputeSessionToken))
            assertFalse(jdbcAlterStmt.contains(ozoneAccessKey))
            assertTrue(jdbcAlterStmt.contains('"iceberg.jdbc.password" = "*XXX"'))
            assertTrue(jdbcAlterStmt.contains('"s3.secret_key" = "*XXX"'))
            assertTrue(jdbcAlterStmt.contains('"mc.session_token" = "*XXX"'))
            assertTrue(jdbcAlterStmt.contains('"ozone.access_key" = "*XXX"'))
            assertTrue(jdbcAlterStmt.contains('"uri" = "*XXX"'))
            assertFalse(jdbcAlterStmt.contains('jdbc:mysql:'))
        } finally {
            sql """drop catalog if exists ${credentialCatalog}"""
            sql """drop catalog if exists ${tokenCatalog}"""
            sql """drop catalog if exists ${jdbcCatalog}"""
        }
    })
}
