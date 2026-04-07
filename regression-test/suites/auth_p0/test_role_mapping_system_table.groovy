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

suite("test_role_mapping_system_table", "p0,auth") {
    String suiteName = "test_role_mapping_system_table"
    String mappingName = "test_role_mapping_system_table_mapping"
    String integrationName = "test_role_mapping_system_table_ldap"
    String readerRole = "test_role_mapping_reader"
    String financeReaderRole = "test_role_mapping_fin_reader"
    String financeWriterRole = "test_role_mapping_fin_writer"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    def jdbcUrlTokens = context.config.jdbcUrl.split('/')
    String jdbcUrlWithoutSchema = jdbcUrlTokens[0] + "//" + jdbcUrlTokens[2] + "/?"
    String expectedRules = (
            """RULE (USING CEL 'has_group("analyst")' GRANT ROLE ${readerRole}); """
            + """RULE (USING CEL 'attr("department") == "finance"' """
            + """GRANT ROLE ${financeReaderRole}, ${financeWriterRole})"""
    )

    try_sql("DROP ROLE MAPPING IF EXISTS ${mappingName}")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    try_sql("DROP USER ${user}")
    try_sql("DROP ROLE IF EXISTS ${financeWriterRole}")
    try_sql("DROP ROLE IF EXISTS ${financeReaderRole}")
    try_sql("DROP ROLE IF EXISTS ${readerRole}")

    try {
        sql """CREATE ROLE ${readerRole}"""
        sql """CREATE ROLE ${financeReaderRole}"""
        sql """CREATE ROLE ${financeWriterRole}"""

        sql """
            CREATE AUTHENTICATION INTEGRATION ${integrationName}
            PROPERTIES (
                'type'='ldap',
                'ldap.server'='ldap://127.0.0.1:389'
            )
            COMMENT 'role mapping auth'
        """

        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

        if (isCloudMode()) {
            def clusters = sql " SHOW CLUSTERS; "
            assertTrue(!clusters.isEmpty())
            def validCluster = clusters[0][0]
            sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}"""
        }

        sql """GRANT SELECT_PRIV ON internal.information_schema.* TO ${user}"""

        sql """
            CREATE ROLE MAPPING ${mappingName}
            ON AUTHENTICATION INTEGRATION ${integrationName}
            RULE ( USING CEL 'has_group("analyst")' GRANT ROLE ${readerRole} ),
            RULE (
                USING CEL 'attr("department") == "finance"'
                GRANT ROLE ${financeReaderRole}, ${financeWriterRole}
            )
            COMMENT 'role mapping comment'
        """

        def result = sql """
            SELECT
                NAME,
                INTEGRATION_NAME,
                RULES,
                COMMENT,
                CREATE_USER,
                CREATE_TIME,
                ALTER_USER,
                MODIFY_TIME
            FROM information_schema.role_mappings
            WHERE NAME = '${mappingName}'
            ORDER BY NAME
        """

        assertEquals(1, result.size())
        assertEquals(8, result[0].size())
        assertEquals(mappingName, result[0][0])
        assertEquals(integrationName, result[0][1])
        assertEquals(expectedRules, result[0][2])
        assertEquals("role mapping comment", result[0][3])
        assertTrue(result[0][4] != null && result[0][4].length() > 0)
        assertTrue(result[0][5] != null && result[0][5].length() > 0)
        assertTrue(result[0][6] != null && result[0][6].length() > 0)
        assertEquals(result[0][5], result[0][7])

        connect(user, "${pwd}", jdbcUrlWithoutSchema) {
            test {
                sql """
                    SELECT NAME
                    FROM information_schema.role_mappings
                    WHERE NAME = '${mappingName}'
                    ORDER BY NAME
                """
                exception "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation"
            }
        }
    } finally {
        try_sql("DROP ROLE MAPPING IF EXISTS ${mappingName}")
        try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
        try_sql("DROP USER ${user}")
        try_sql("DROP ROLE IF EXISTS ${financeWriterRole}")
        try_sql("DROP ROLE IF EXISTS ${financeReaderRole}")
        try_sql("DROP ROLE IF EXISTS ${readerRole}")
    }
}
