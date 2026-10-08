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

package org.apache.doris.datasource;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.jni.toolkit.jdbc.JdbcExceptionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class JdbcCatalogDiagnosticsTest {
    @Test
    void catalogStatusPreservesSafeConnectorAndLegacyDiagnosticsThroughCacheWrappers() {
        SQLException raw = new SQLException("denied raw=test-password", "42000", 1142);
        String safe = JdbcExceptionUtils.format("load databases", raw, "test-password");
        JdbcClientException legacy = new JdbcClientException(safe);
        legacy.initCause(raw);
        Throwable[] wrappers = {new DorisConnectorException(safe, raw), legacy};
        for (Throwable wrapper : wrappers) {
            String status = ExternalCatalog.initErrorMessage("jdbc", new RuntimeException("cache failed", wrapper));
            Assertions.assertTrue(status.contains("remote_sqlstate=42000"));
            Assertions.assertTrue(status.contains("remote_vendor_error_code=1142"));
            Assertions.assertFalse(status.contains("test-password"));
        }
    }

    @Test
    void nonSqlJdbcFailuresKeepSanitizedContext() {
        RuntimeException raw = new RuntimeException("driver failure raw=test-password");
        String safe = JdbcExceptionUtils.format("open JDBC client", raw, "test-password");
        Assertions.assertEquals(safe, ExternalCatalog.initErrorMessage("jdbc", new DorisConnectorException(safe, raw)));
        JdbcClientException legacy = new JdbcClientException(safe);
        legacy.initCause(raw);
        Assertions.assertEquals(safe, ExternalCatalog.initErrorMessage("jdbc", legacy));
    }

    @Test
    void connectorWithoutMessageStillReportsItsRootCause() {
        Assertions.assertEquals("IllegalStateException: root failure", ExternalCatalog.initErrorMessage("jdbc",
                new DorisConnectorException(null, new IllegalStateException("root failure"))));
    }

    @Test
    void nonJdbcConnectorStillReportsItsSpecificRootCause() {
        Assertions.assertEquals("IllegalStateException: remote IO failure", ExternalCatalog.initErrorMessage("fluss",
                new DorisConnectorException("listDatabases failed", new IllegalStateException("remote IO failure"))));
    }

    @Test
    void unrelatedCatalogErrorsKeepTheirExistingRootCauseMessage() {
        Assertions.assertEquals("IllegalStateException: root failure", ExternalCatalog.initErrorMessage("jdbc",
                new RuntimeException("wrapper", new IllegalStateException("root failure"))));
    }
}
