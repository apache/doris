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

import groovy.json.JsonOutput

suite("test_iceberg_google_credential_validation", "p0") {
    String catalogName = "iceberg_google_types_" + UUID.randomUUID().toString().replace("-", "")
    String baseProperties = '"type"="iceberg", "iceberg.catalog.type"="rest", ' +
        '"iceberg.rest.uri"="http://127.0.0.1:1", "iceberg.rest.security.type"="google"'
    List<String> fields = ["client_id", "client_email", "private_key", "private_key_id", "project_id",
        "token_uri", "quota_project_id", "universe_domain"]
    List badValues = [7, true, [], [:], null]
    def rejected = { String statement ->
        Exception failure = null
        try {
            sql statement
        } catch (Exception error) {
            failure = error
        }
        assertNotNull(failure)
        assertTrue(failure.message.contains("gcp.auth.credentials-json fields must contain strings"))
        assertFalse(failure.message.contains("fixture-secret-tail"))
    }
    def credential = { String field, Object value ->
        Map json = [type: "service_account", private_key: "fixture-secret-tail", ignored_marker: "fixture-secret-tail"]
        json[field] = value
        return JsonOutput.toJson(json)
    }

    try {
        // Invalid credentials must fail before any lazy SDK/catalog initialization or metadata publication.
        fields.each { field ->
            badValues.each { value ->
                rejected("""CREATE CATALOG ${catalogName} PROPERTIES (${baseProperties},
                    "gcp.auth.credentials-json"='${credential(field, value)}')""")
                assertTrue(sql("SHOW CATALOGS").every { row -> row[1].toString() != catalogName })
            }
        }
        sql """CREATE CATALOG ${catalogName} PROPERTIES (${baseProperties},
            "gcp.auth.credentials-json"='{"type":"service_account"}')"""
        def original = sql("SHOW CREATE CATALOG ${catalogName}")
        fields.each { field ->
            badValues.each { value ->
                rejected("""ALTER CATALOG ${catalogName} SET PROPERTIES (
                    "gcp.auth.credentials-json"='${credential(field, value)}')""")
                assertEquals(original, sql("SHOW CREATE CATALOG ${catalogName}"))
            }
        }
    } finally {
        sql "DROP CATALOG IF EXISTS ${catalogName}"
    }
}
