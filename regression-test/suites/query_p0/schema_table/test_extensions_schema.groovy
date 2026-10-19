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

import org.junit.Assert

suite("test_extensions_schema", "p0") {
    // Schema check: fixed five columns.
    def schema = sql "DESC information_schema.extensions"
    def columnNames = schema.collect { it[0] }
    Assert.assertEquals(
            ["EXTENSION_NAME", "EXTENSION_TYPE", "EXTENSION_VERSION", "SOURCE", "DESCRIPTION"], columnNames)

    // Loaded extensions must be listed. Do not require BUILTIN
    // rows: the packaged cluster deploys filesystem/connector providers as
    // directory extensions (source = EXTERNAL), so a correct inventory may contain
    // no BUILTIN row at all.
    def rows = sql """
        SELECT EXTENSION_NAME, EXTENSION_TYPE, SOURCE, EXTENSION_VERSION
        FROM information_schema.extensions
        ORDER BY EXTENSION_TYPE, EXTENSION_NAME
    """
    Assert.assertTrue("expect at least one extension row", rows.size() > 0)
    rows.each { row ->
        Assert.assertTrue(row[2] == "BUILTIN" || row[2] == "EXTERNAL")
        // Unknown versions must surface as SQL NULL, never as an empty string.
        Assert.assertTrue("EXTENSION_VERSION must be NULL or non-empty, got ''",
                row[3] == null || row[3].toString().length() > 0)
    }

    // (type, name) is the primary key: no duplicates may appear.
    def keys = rows.collect { "${it[1]}|${it[0]}".toString() }
    Assert.assertEquals(keys.size(), keys.unique(false).size())

    // Filter by family works.
    def fsRows = sql """
        SELECT EXTENSION_NAME FROM information_schema.extensions WHERE EXTENSION_TYPE = 'FILESYSTEM'
    """
    Assert.assertTrue("expect built-in filesystem providers", fsRows.size() > 0)

    // The inventory is not ADMIN-gated: a plain user sees the same rows.
    String user = "test_extensions_schema_user"
    String pwd = "C123_567p"
    try_sql("DROP USER IF EXISTS ${user}")
    sql "CREATE USER ${user} IDENTIFIED BY '${pwd}'"
    // The JDBC URL carries a default database; without a grant on it the
    // connection itself is refused before the query runs.
    sql "GRANT SELECT_PRIV ON regression_test TO ${user}"
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        Assert.assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}"""
    }
    try {
        connect(user, pwd, context.config.jdbcUrl) {
            def result = sql """
                SELECT EXTENSION_NAME, EXTENSION_TYPE, SOURCE, EXTENSION_VERSION
                FROM information_schema.extensions
                ORDER BY EXTENSION_TYPE, EXTENSION_NAME
            """
            Assert.assertEquals("a non-admin user must see the full inventory",
                    rows.size(), result.size())
        }
    } finally {
        try_sql("DROP USER IF EXISTS ${user}")
    }
}
