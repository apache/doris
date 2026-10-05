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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.jdbc.JdbcDbType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link JdbcDB2ConnectorClient}, focusing on the internal schema
 * filtering fix (P1-6). DB2 returns schema names in uppercase; the filter set
 * must match after {@code toLowerCase()} normalisation.
 */
public class JdbcDB2ConnectorClientTest {

    private JdbcDB2ConnectorClient createClient() {
        return new JdbcDB2ConnectorClient(
                "test_catalog",
                JdbcDbType.DB2,
                "jdbc:db2://localhost:50000/SAMPLE",
                false,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false,
                false);
    }

    @Test
    void testFilterInternalDatabasesAreLowerCase() {
        JdbcDB2ConnectorClient client = createClient();
        Set<String> internals = client.getFilterInternalDatabases();
        for (String name : internals) {
            Assertions.assertEquals(name.toLowerCase(), name,
                    "Internal database name must be lowercase: " + name);
        }
    }

    @Test
    void testFilterInternalDatabasesContainsExpectedEntries() {
        JdbcDB2ConnectorClient client = createClient();
        Set<String> internals = client.getFilterInternalDatabases();
        List<String> expected = Arrays.asList(
                "nullid", "sqlj", "syscat", "sysfun", "sysibm",
                "sysibmadm", "sysibminternal", "sysibmts",
                "sysproc", "syspublic", "sysstat", "systools");
        for (String e : expected) {
            Assertions.assertTrue(internals.contains(e),
                    "Missing expected internal database: " + e);
        }
    }

    @Test
    void testInternalSchemasFilteredByFilterDatabaseNames() {
        JdbcDB2ConnectorClient client = createClient();
        // DB2 returns schema names in UPPERCASE from JDBC metadata
        List<String> dbNames = new ArrayList<>(Arrays.asList(
                "NULLID", "SYSCAT", "SYSIBM", "MYSCHEMA", "APP_DATA"));

        List<String> filtered = client.filterDatabaseNames(dbNames);
        Assertions.assertEquals(Arrays.asList("MYSCHEMA", "APP_DATA"), filtered,
                "System schemas should be filtered out, user schemas retained");
    }

    @Test
    void testMixedCaseSchemaNamesFiltered() {
        JdbcDB2ConnectorClient client = createClient();
        // Edge case: mixed-case schema names should still be filtered
        List<String> dbNames = new ArrayList<>(Arrays.asList(
                "SysCat", "sysibm", "SYSTOOLS", "MyApp"));

        List<String> filtered = client.filterDatabaseNames(dbNames);
        Assertions.assertEquals(Collections.singletonList("MyApp"), filtered,
                "Filtering should be case-insensitive");
    }

    @Test
    void testUserSchemaPassesThrough() {
        JdbcDB2ConnectorClient client = createClient();
        List<String> dbNames = new ArrayList<>(Arrays.asList(
                "PROD_DB", "ANALYTICS", "STAGING"));

        List<String> filtered = client.filterDatabaseNames(dbNames);
        Assertions.assertEquals(Arrays.asList("PROD_DB", "ANALYTICS", "STAGING"), filtered,
                "Non-system schemas should pass through unchanged");
    }
}
