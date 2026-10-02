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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.write.ConnectorRowChangeStyle;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

class ConnectorCopyOnWriteSnapshotTest {
    @Test
    void snapshotPinsTheSameHandleAndNumericVersionForCountAndWrite() {
        Fixture fixture = new Fixture();
        ConnectorTableHandle pinned = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(7L).build();
        Mockito.when(fixture.metadata.beginQuerySnapshot(fixture.session, fixture.handle))
                .thenReturn(Optional.of(snapshot));
        Mockito.when(fixture.metadata.applySnapshot(fixture.session, fixture.handle, snapshot)).thenReturn(pinned);

        ConnectorCopyOnWriteUtils.CopyOnWriteSnapshot captured =
                ConnectorCopyOnWriteUtils.captureSnapshot(fixture.table, "UPDATE");

        Assertions.assertSame(pinned, captured.handle);
        Assertions.assertEquals(7L, captured.version);
        Mockito.verify(fixture.metadata).applySnapshot(fixture.session, fixture.handle, snapshot);
    }

    @Test
    void copyOnWriteRejectsMissingOrNonNumericSnapshot() {
        Fixture fixture = new Fixture();
        Mockito.when(fixture.metadata.beginQuerySnapshot(fixture.session, fixture.handle))
                .thenReturn(Optional.empty());
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorCopyOnWriteUtils.captureSnapshot(fixture.table, "UPDATE"));
        Mockito.when(fixture.metadata.beginQuerySnapshot(fixture.session, fixture.handle))
                .thenReturn(Optional.of(ConnectorMvccSnapshot.builder().snapshotId(-1L).build()));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorCopyOnWriteUtils.captureSnapshot(fixture.table, "MERGE"));
        Mockito.verify(fixture.metadata, Mockito.never()).applySnapshot(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    void snapshotCallbacksUseTheConnectorClassLoaderAndRestoreTheCaller() {
        Fixture fixture = new Fixture();
        ConnectorTableHandle pinned = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(7L).build();
        ClassLoader pluginLoader = fixture.connector.getClass().getClassLoader();
        Mockito.when(fixture.metadata.beginQuerySnapshot(fixture.session, fixture.handle))
                .thenAnswer(invocation -> {
                    Assertions.assertSame(pluginLoader, Thread.currentThread().getContextClassLoader());
                    return Optional.of(snapshot);
                });
        Mockito.when(fixture.metadata.applySnapshot(fixture.session, fixture.handle, snapshot))
                .thenAnswer(invocation -> {
                    Assertions.assertSame(pluginLoader, Thread.currentThread().getContextClassLoader());
                    return pinned;
                });
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader caller = new ClassLoader(previous) {};
        try {
            Thread.currentThread().setContextClassLoader(caller);
            Assertions.assertSame(pinned,
                    ConnectorCopyOnWriteUtils.captureSnapshot(fixture.table, "UPDATE").handle);
            Assertions.assertSame(caller, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void positionDeleteDmlKeepsItsRegistryTransform() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.connectorSupportedWriteOperations())
                .thenReturn(Set.of(WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE));
        Mockito.when(table.getConnectorRowChangeStyle()).thenReturn(ConnectorRowChangeStyle.POSITION_DELETE);

        Assertions.assertFalse(table.connectorSupportsCopyOnWriteDml());
        Assertions.assertInstanceOf(PositionDeleteRowLevelDmlTransform.class,
                RowLevelDmlRegistry.find(table).orElseThrow());
    }

    private static final class Fixture {
        final Connector connector = Mockito.mock(Connector.class);
        final ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        final ConnectorSession session = ConnectorSessionBuilder.create().build();
        final ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        final PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);

        private Fixture() {
            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getConnector()).thenReturn(connector);
            Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
            Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
            Mockito.when(table.resolveConnectorTableHandle(session, metadata)).thenReturn(Optional.of(handle));
        }
    }
}
