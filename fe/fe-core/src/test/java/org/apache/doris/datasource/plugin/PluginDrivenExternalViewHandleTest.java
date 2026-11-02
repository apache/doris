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

package org.apache.doris.datasource.plugin;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalDatabase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

public class PluginDrivenExternalViewHandleTest {
    @Test
    public void viewDoesNotProbeTableEndpoint() {
        Fixture f = new Fixture(true);
        Mockito.when(f.metadata.viewExists(f.session, "remote_db", "remote_object")).thenReturn(true);
        Mockito.when(f.metadata.getTableHandle(f.session, "remote_db", "remote_object"))
                .thenThrow(new DorisConnectorException("Object is not an Iceberg compatible table"));

        Assertions.assertTrue(f.table.resolveConnectorTableHandle(f.session, f.metadata).isEmpty());

        Mockito.verify(f.metadata, Mockito.never()).getTableHandle(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verifyNoInteractions(f.invalidate);
    }

    @Test
    public void deletedViewStillInvalidatesLookup() {
        Fixture f = new Fixture(true);
        // The locally cached view classification can survive an out-of-band view deletion.
        Mockito.when(f.metadata.viewExists(f.session, "remote_db", "remote_object")).thenReturn(false);
        Mockito.when(f.metadata.getTableHandle(f.session, "remote_db", "remote_object"))
                .thenThrow(new DorisConnectorException("Object is not an Iceberg compatible table"));

        Assertions.assertTrue(f.table.resolveConnectorTableHandle(f.session, f.metadata).isEmpty());

        Mockito.verify(f.invalidate).run();
        Mockito.verify(f.metadata, Mockito.never()).getTableHandle(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void ordinaryTableStillResolvesHandle() {
        Fixture f = new Fixture(false);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(f.metadata.getTableHandle(f.session, "remote_db", "remote_object"))
                .thenReturn(Optional.of(handle));

        Assertions.assertSame(handle, f.table.resolveConnectorTableHandle(f.session, f.metadata).orElseThrow());

        Mockito.verify(f.metadata, Mockito.never()).viewExists(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verifyNoInteractions(f.invalidate);
    }

    @Test
    public void ordinaryTableLoadFailureStillPropagates() {
        Fixture f = new Fixture(false);
        DorisConnectorException failure = new DorisConnectorException("Table metadata request failed");
        Mockito.when(f.metadata.getTableHandle(f.session, "remote_db", "remote_object")).thenThrow(failure);

        Assertions.assertSame(failure, Assertions.assertThrows(DorisConnectorException.class,
                () -> f.table.resolveConnectorTableHandle(f.session, f.metadata)));

        Mockito.verifyNoInteractions(f.invalidate);
    }

    private static class Fixture {
        final ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        final ConnectorSession session = Mockito.mock(ConnectorSession.class);
        final Runnable invalidate = Mockito.mock(Runnable.class);
        final PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);

        Fixture(boolean view) {
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getCapabilities()).thenReturn(Set.of(ConnectorCapability.SUPPORTS_VIEW));
            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(catalog.getConnector()).thenReturn(connector);
            ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
            Mockito.when(database.getRemoteName()).thenReturn("remote_db");
            Mockito.when(database.observeTableLookup(Mockito.eq(table), Mockito.any()))
                    .thenReturn(Optional.of(invalidate));
            Deencapsulation.setField(table, "catalog", catalog);
            Deencapsulation.setField(table, "db", database);
            Deencapsulation.setField(table, "remoteName", "remote_object");
            Mockito.doReturn(view).when(table).isView();
        }
    }
}
