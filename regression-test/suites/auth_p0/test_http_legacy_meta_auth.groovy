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

import org.junit.Assert;

// Covers the LEGACY metadata controller, MetaInfoAction, served at /api/meta/**.
// Note the path: the sibling test_http_meta_* suites use /rest/v2/api/meta/**, which is a
// different class (MetaInfoActionV2). Before this suite the legacy controller had no regression
// coverage at all.
//
// What it pins down:
//  1. The endpoints authenticate. No credential -> rejected, whatever the privileges would be.
//  2. They do NOT require global ADMIN. A least-privilege account gets a successful response --
//     this is what a Doris-to-Doris external catalog (RemoteDorisRestClient) relies on, since it
//     calls exactly these routes with the catalog's configured user.
//  3. The response is privilege-filtered per object, and the filter is actually applied to what
//     is returned. getAllDatabases used to compute a SHOW-filtered list and then return the
//     unfiltered one, so a non-admin saw every database on the cluster.
suite("test_http_legacy_meta_auth", "p0,auth,nonConcurrent") {
    String suiteName = "test_http_legacy_meta_auth"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String hiddenTableName = "${suiteName}_hidden_table"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'

    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """DROP TABLE IF EXISTS `${tableName}`"""
    sql """DROP TABLE IF EXISTS `${hiddenTableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` int,
          `k2` int
        ) ENGINE=OLAP
        DISTRIBUTED BY random BUCKETS auto
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        CREATE TABLE `${hiddenTableName}` (
          `k1` int,
          `k2` int
        ) ENGINE=OLAP
        DISTRIBUTED BY random BUCKETS auto
        PROPERTIES ('replication_num' = '1') ;
        """

    try {
        def legacyGet = { uriPath, user_name, password, check_func ->
            httpTest {
                if (user_name != null) {
                    basicAuthorization "${user_name}", "${password}"
                }
                endpoint "${context.config.feHttpAddress}"
                uri uriPath
                op "get"
                check check_func
            }
        }

        String dbsUri = "/api/meta/namespaces/default_cluster/databases"
        String tblsUri = "/api/meta/namespaces/default_cluster/databases/${dbName}/tables"

        // 1. Authentication is required. Anonymous callers are rejected.
        legacyGet.call(dbsUri, null, null) {
            respCode, body ->
                log.info("legacy databases (anonymous) respCode:${respCode} body:${body}")
                assertTrue(respCode == 401 || "${body}".contains("401")
                        || "${body}".contains("Unauthorized") || "${body}".contains("Need auth"))
        }

        // 2. A valid but non-admin account is accepted -- no global ADMIN is demanded.
        // 3. ... and sees nothing it has no SHOW privilege on.
        legacyGet.call(dbsUri, user, pwd) {
            respCode, body ->
                log.info("legacy databases (no grants) respCode:${respCode} body:${body}")
                assertEquals(200, respCode)
                assertFalse("${body}".contains("Unauthorized"))
                assertFalse("${body}".contains("Admin_priv"))
                assertFalse("${body}".contains("${dbName}"))
        }

        sql """grant select_priv on ${dbName}.${tableName} to ${user}"""

        // The grant on one table makes the database visible...
        legacyGet.call(dbsUri, user, pwd) {
            respCode, body ->
                log.info("legacy databases (after grant) respCode:${respCode} body:${body}")
                assertEquals(200, respCode)
                assertTrue("${body}".contains("${dbName}"))
        }

        // ... but only the granted table inside it. The other table stays hidden.
        legacyGet.call(tblsUri, user, pwd) {
            respCode, body ->
                log.info("legacy tables (after grant) respCode:${respCode} body:${body}")
                assertEquals(200, respCode)
                assertTrue("${body}".contains("${tableName}"))
                assertFalse("${body}".contains("${hiddenTableName}"))
        }

        // The schema route authorizes per table: granted table succeeds, ungranted one does not.
        legacyGet.call("${tblsUri}/${tableName}/schema", user, pwd) {
            respCode, body ->
                log.info("legacy schema (granted) respCode:${respCode} body:${body}")
                assertEquals(200, respCode)
                assertTrue("${body}".contains("k1"))
        }

        legacyGet.call("${tblsUri}/${hiddenTableName}/schema", user, pwd) {
            respCode, body ->
                log.info("legacy schema (ungranted) respCode:${respCode} body:${body}")
                assertTrue("${body}".contains("401") || "${body}".contains("Access denied"))
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS `${tableName}`")
        try_sql("DROP TABLE IF EXISTS `${hiddenTableName}`")
        try_sql("DROP USER ${user}")
    }
}
