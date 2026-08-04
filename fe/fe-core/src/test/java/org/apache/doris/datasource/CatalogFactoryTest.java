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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;

public class CatalogFactoryTest {

    @Test
    public void testCloseCatalogWhenCreateValidationFails() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = Mockito.spy(new PluginDrivenExternalCatalog(
                1L, "failed_catalog", null, new HashMap<>(), "", connector));
        Mockito.doThrow(new DdlException("validation failed")).when(catalog).checkWhenCreating();

        DdlException exception = Assert.assertThrows(
                DdlException.class, () -> CatalogFactory.finishCatalogCreation(catalog, false));

        Assert.assertTrue(exception.getMessage().endsWith("validation failed"));
        Mockito.verify(connector).close();
    }

    @Test
    public void testPreserveValidationFailureWhenCleanupFails() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.doThrow(new RuntimeException("cleanup failed")).when(connector).close();
        PluginDrivenExternalCatalog catalog = new PluginDrivenExternalCatalog(
                2L, "failed_catalog", null, new HashMap<>(), "", connector) {
            @Override
            public void checkWhenCreating() throws DdlException {
                throw new DdlException("primary validation failure");
            }
        };

        DdlException exception = Assert.assertThrows(
                DdlException.class, () -> CatalogFactory.finishCatalogCreation(catalog, false));

        Assert.assertTrue(exception.getMessage().endsWith("primary validation failure"));
    }

    @Test
    public void testRuntimeConnectorFailureDoesNotSkipContextCleanupOrRetryConnector() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        DefaultConnectorContext connectorContext = Mockito.mock(DefaultConnectorContext.class);
        Mockito.doThrow(new RuntimeException("connector cleanup failed")).when(connector).close();
        TestablePluginCatalog catalog = new TestablePluginCatalog(connector);
        Deencapsulation.setField(catalog, "connectorContext", connectorContext);

        catalog.closeResourcesForTest();
        catalog.closeResourcesForTest();

        Mockito.verify(connector).close();
        Mockito.verify(connectorContext).close();
    }

    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        TestablePluginCatalog(Connector connector) {
            super(3L, "registered_catalog", null, new HashMap<>(), "", connector);
        }

        void closeResourcesForTest() {
            closeResources();
        }
    }
}
