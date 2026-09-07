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

package org.apache.doris.datasource.jdbc.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;

public class JdbcClickHouseClientTest {

    @Test
    public void testDatabaseTermFollowsDriverMetadata() throws Exception {
        DatabaseMetaData databaseMetaData = Mockito.mock(DatabaseMetaData.class);

        Mockito.when(databaseMetaData.supportsCatalogsInDataManipulation()).thenReturn(false);
        Assertions.assertFalse(JdbcClickHouseClient.isDatabaseTermCatalog(databaseMetaData, "0.9.8"));

        Mockito.when(databaseMetaData.supportsCatalogsInDataManipulation()).thenReturn(true);
        Assertions.assertTrue(JdbcClickHouseClient.isDatabaseTermCatalog(databaseMetaData, "0.7.1"));

        Assertions.assertFalse(JdbcClickHouseClient.isDatabaseTermCatalog(databaseMetaData, "0.4.2"));
    }

    @Test
    public void testClickHouseSpecificTableTypesAreVisible() {
        JdbcClickHouseClient client = Mockito.mock(JdbcClickHouseClient.class, Answers.CALLS_REAL_METHODS);

        Assertions.assertArrayEquals(
                new String[] {"TABLE", "VIEW", "SYSTEM TABLE", "REMOTE TABLE", "MATERIALIZED VIEW"},
                client.getTableTypes());
    }

    @Test
    public void testIsNewClickHouseDriver() {
        try {
            Method method = JdbcClickHouseClient.class.getDeclaredMethod("isNewClickHouseDriver", String.class);
            method.setAccessible(true);

            // Valid test cases
            Assertions.assertTrue((boolean) method.invoke(null, "0.5.0")); // Major version 0, Minor version 5
            Assertions.assertTrue((boolean) method.invoke(null, "1.0.0")); // Major version 1
            Assertions.assertTrue((boolean) method.invoke(null, "0.6.3 (revision: a6a8a22)")); // Major version 0, Minor version 6
            Assertions.assertFalse((boolean) method.invoke(null, "0.4.2 (revision: 1513b27)")); // Major version 0, Minor version 4

            // Invalid version formats
            try {
                method.invoke(null, "invalid.version"); // Invalid version format
                Assertions.fail("Expected JdbcClientException for invalid version 'invalid.version'");
            } catch (Exception e) {
                Assertions.assertTrue(e.getCause() instanceof JdbcClientException);
                Assertions.assertTrue(e.getCause().getMessage().contains("Invalid clickhouse driver version format"));
            }

            try {
                method.invoke(null, ""); // Empty version
                Assertions.fail("Expected JdbcClientException for empty version");
            } catch (Exception e) {
                Assertions.assertTrue(e.getCause() instanceof JdbcClientException);
                Assertions.assertTrue(e.getCause().getMessage().contains("Invalid clickhouse driver version format"));
            }

            try {
                method.invoke(null, (Object) null); // Null version
                Assertions.fail("Expected JdbcClientException for null version");
            } catch (Exception e) {
                Assertions.assertTrue(e.getCause() instanceof JdbcClientException);
                Assertions.assertTrue(e.getCause().getMessage().contains("Driver version cannot be null"));
            }
        } catch (Exception e) {
            Assertions.fail("Exception occurred while testing isNewClickHouseDriver: " + e.getMessage());
        }
    }
}
