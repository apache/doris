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

suite("test_authentication_integration_auth", "p0,auth") {
    String suiteName = "test_authentication_integration_auth"
    String integrationName = "${suiteName}_ldap"

    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")

    try {
        test {
            sql """
                CREATE AUTHENTICATION INTEGRATION ${integrationName}
                 PROPERTIES ('ldap.server'='ldap://127.0.0.1:389')
            """
            exception "Property 'type' is required"
        }

        sql """
            CREATE AUTHENTICATION INTEGRATION ${integrationName}
             PROPERTIES (
                'type'='ldap',
                'ldap.server'='ldap://127.0.0.1:389',
                'ldap.admin_password'='123456',
                'secret.endpoint'='secret_create_value'
            )
            COMMENT 'for regression test'
        """

        test {
            sql """
                CREATE AUTHENTICATION INTEGRATION ${integrationName}
                 PROPERTIES ('type'='ldap', 'ldap.server'='ldap://127.0.0.1:1389')
            """
            exception "already exists"
        }

        test {
            sql """
                ALTER AUTHENTICATION INTEGRATION ${integrationName}
                SET PROPERTIES ('type'='oidc')
            """
            exception "does not allow modifying property 'type'"
        }


        sql """
            ALTER AUTHENTICATION INTEGRATION ${integrationName}
            SET PROPERTIES (
                'ldap.server'='ldap://127.0.0.1:1389',
                'ldap.admin_password'='abcdef',
                'secret.endpoint'='secret_alter_value'
            )
        """

        sql """ALTER AUTHENTICATION INTEGRATION ${integrationName} SET COMMENT 'updated comment'"""

        def result = sql """
            SELECT
                NAME,
                TYPE,
                PROPERTIES,
                COMMENT,
                CREATE_USER,
                CREATE_TIME,
                ALTER_USER,
                MODIFY_TIME
            FROM information_schema.authentication_integrations
            WHERE NAME = '${integrationName}'
            ORDER BY NAME
        """
        assertEquals(1, result.size())
        assertEquals(8, result[0].size())
        assertEquals(integrationName, result[0][0])
        assertEquals("ldap", result[0][1])
        assertTrue(result[0][2].contains("\"ldap.server\" = \"ldap://127.0.0.1:1389\""))
        assertTrue(!result[0][2].contains("\"ldap.server\" = \"ldap://127.0.0.1:2389\""))
        assertTrue(result[0][2].contains("\"ldap.admin_password\" = \"*XXX\""))
        assertTrue(result[0][2].contains("\"secret.endpoint\" = \"*XXX\""))
        assertTrue(!result[0][2].contains("abcdef"))
        assertTrue(!result[0][2].contains("secret_alter_value"))
        assertTrue(!result[0][2].contains("plugin.initialize_immediately"))
        assertEquals("updated comment", result[0][3])
        assertTrue(result[0][4] != null && result[0][4].length() > 0)
        assertTrue(result[0][5] != null && result[0][5].length() > 0)
        assertTrue(result[0][6] != null && result[0][6].length() > 0)
        assertTrue(result[0][7] != null && result[0][7].length() > 0)

        test {
            sql """DROP AUTHENTICATION INTEGRATION ${integrationName}_not_exist"""
            exception "does not exist"
        }

        sql """DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}_not_exist"""
    } finally {
        try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    }
}
