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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests that {@link PluginDrivenExternalCatalog} records a deferred metadata-load failure into
 * {@code errorMsg} so it shows up in {@code show catalogs}.
 *
 * <p>Plugin connectors connect lazily (initLocalObjectsImpl only constructs the connector), so the
 * first metastore round-trip happens inside the meta-cache loader — outside makeSureInitialized()'s
 * try/catch, which is the only other writer of {@code errorMsg}. Without capturing it there, a
 * broken catalog would show an empty error even though {@code show databases} throws. This encodes
 * WHY the capture exists: the error must be user-visible in {@code show catalogs}.</p>
 */
public class PluginDrivenExternalCatalogErrorMsgTest {

    @Test
    public void listDatabaseNamesCapturesErrorMsgOnDeferredFailure() {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata meta = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(meta);
        // Mirrors the real iceberg failure surfaced by the show_catalogs_error_msg regression:
        // a bad REST port (181812) rejected only when the connector actually connects.
        Mockito.when(meta.listDatabaseNames(Mockito.any()))
                .thenThrow(new RuntimeException("port out of range:181812 is out of range"));

        TestErrorCatalog catalog = new TestErrorCatalog(connector);
        Assertions.assertTrue(catalog.getErrorMsg().isEmpty(), "errorMsg starts empty");

        // listDatabaseNames() is the meta-cache loader's db-name source; the failure must both
        // propagate (so `show databases` reports it) AND be captured into errorMsg.
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                catalog::listDatabaseNames);
        Assertions.assertTrue(ex.getMessage().contains("181812 is out of range"), ex.getMessage());
        Assertions.assertTrue(catalog.getErrorMsg().contains("181812 is out of range"),
                "errorMsg should capture the deferred failure, was: " + catalog.getErrorMsg());
    }

    @Test
    public void listDatabaseNamesLeavesErrorMsgEmptyOnSuccess() {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata meta = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(meta);
        Mockito.when(meta.listDatabaseNames(Mockito.any()))
                .thenReturn(Collections.singletonList("db1"));

        TestErrorCatalog catalog = new TestErrorCatalog(connector);
        List<String> dbs = catalog.listDatabaseNames();
        Assertions.assertEquals(Collections.singletonList("db1"), dbs);
        Assertions.assertTrue(catalog.getErrorMsg().isEmpty(),
                "errorMsg must stay empty when the metadata load succeeds");
    }

    /**
     * Minimal subclass that keeps the real {@link PluginDrivenExternalCatalog#listDatabaseNames()}
     * (the method under test) but stubs out the pieces that need a full Doris environment.
     */
    private static class TestErrorCatalog extends PluginDrivenExternalCatalog {
        TestErrorCatalog(Connector connector) {
            super(1L, "err-catalog", null, testProps(), "", connector);
            this.initialized = true;
        }

        @Override
        protected Connector createConnectorFromProperties() {
            return null;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // Connector is already injected via the constructor; nothing to build.
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return Mockito.mock(ConnectorSession.class);
        }

        private static Map<String, String> testProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "test");
            return props;
        }
    }
}
