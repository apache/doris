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

suite("test_iceberg_google_credential_fields", "p0,external") {
    def valid = [type: "authorized_user", client_id: "fixture-client",
            client_secret: "fixture-secret", refresh_token: "fixture-refresh"]
    def createCatalog = { String catalog, String credentials ->
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest',
            'uri' = 'https://catalog.example.invalid', 'warehouse' = 'fixture-warehouse',
            'iceberg.rest.security.type' = 'google',
            'gcp.auth.credentials-json' = '${credentials}'
        )"""
    }
    sql "DROP CATALOG IF EXISTS test_iceberg_google_credential_fields"
    sql "DROP CATALOG IF EXISTS test_iceberg_google_credential_fields_invalid"
    createCatalog("test_iceberg_google_credential_fields", JsonOutput.toJson(valid))
    ["refresh_token", "client_secret", "account"].each { String field ->
        [null, 7, true, [], [:]].each { Object value ->
            Map invalid = new LinkedHashMap(valid)
            invalid[field] = value
            String credentials = JsonOutput.toJson(invalid)
            // CREATE and ALTER must reject malformed credentials before any catalog or OAuth request.
            String error = "gcp.auth.credentials-json ${field} must contain a string"
            test {
                sql """CREATE CATALOG test_iceberg_google_credential_fields_invalid PROPERTIES (
                    'type' = 'iceberg', 'iceberg.catalog.type' = 'rest',
                    'uri' = 'https://catalog.example.invalid', 'warehouse' = 'fixture-warehouse',
                    'iceberg.rest.security.type' = 'google',
                    'gcp.auth.credentials-json' = '${credentials}')"""
                exception error
            }
            test {
                sql """ALTER CATALOG test_iceberg_google_credential_fields SET PROPERTIES (
                    'gcp.auth.credentials-json' = '${credentials}')"""
                exception error
            }
        }
    }
}
