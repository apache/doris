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

package org.apache.doris.tablefunction;

import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Tests for the partitions() TVF dispatch to a {@link PluginDrivenExternalCatalog}
 * added by P4-T06c (MetadataGenerator.dealPluginDrivenCatalog).
 *
 * <p><b>Why:</b> after the MaxCompute SPI cutover, a {@code max_compute} catalog is a
 * {@link PluginDrivenExternalCatalog}, so the old {@code instanceof MaxComputeExternalCatalog}
 * branch no longer matches and the partitions() TVF would fall through to
 * "not support catalog". These tests lock in that the new branch routes partition
 * listing through the table's guarded handle resolver and retained metadata read, and emits one
 * single-string-column row per partition, matching the legacy dealMaxComputeCatalog shape.</p>
 */
public class MetadataGeneratorPluginDrivenTest {

    private TFetchSchemaTableDataResult invokeDeal(PluginDrivenExternalCatalog catalog, ExternalTable table)
            throws Exception {
        Method m = MetadataGenerator.class.getDeclaredMethod("dealPluginDrivenCatalog",
                PluginDrivenExternalCatalog.class, ExternalTable.class);
        m.setAccessible(true);
        return (TFetchSchemaTableDataResult) m.invoke(null, catalog, table);
    }

    @Test
    public void testRoutesThroughGuardedTableAndBuildsRows() throws Exception {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        Mockito.when(session.getStatementScope()).thenReturn(scope);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);

        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.buildCrossStatementSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);

        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);

        Mockito.when(table.resolveConnectorTableHandle(session, metadata)).thenReturn(Optional.of(handle));
        PluginDrivenExternalTable.MetadataRead read = Mockito.mock(PluginDrivenExternalTable.MetadataRead.class);
        Mockito.when(table.observeConnectorMetadata(session, metadata)).thenReturn(read);
        Mockito.when(read.execute(Mockito.any())).thenAnswer(call ->
                ((java.util.function.Supplier<?>) call.getArgument(0)).get());
        Mockito.when(metadata.listPartitionNames(session, handle))
                .thenReturn(Arrays.asList("pt=1", "pt=2"));

        TFetchSchemaTableDataResult result = invokeDeal(catalog, table);

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        List<TRow> rows = result.getDataBatch();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("pt=1", rows.get(0).getColumnValue().get(0).getStringVal());
        Assertions.assertEquals("pt=2", rows.get(1).getColumnValue().get(0).getStringVal());
        Mockito.verify(table).resolveConnectorTableHandle(session, metadata);
        Mockito.verify(read).execute(Mockito.any());
        Mockito.verify(metadata).close();
    }

    @Test
    public void testAbsentHandleYieldsEmptyOkResult() throws Exception {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        Mockito.when(session.getStatementScope()).thenReturn(scope);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);

        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.buildCrossStatementSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);

        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.resolveConnectorTableHandle(session, metadata)).thenReturn(Optional.empty());

        TFetchSchemaTableDataResult result = invokeDeal(catalog, table);

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(Collections.emptyList(), result.getDataBatch());
        Mockito.verify(metadata, Mockito.never()).listPartitionNames(Mockito.any(), Mockito.any());
        Mockito.verify(metadata).close();
    }
}
