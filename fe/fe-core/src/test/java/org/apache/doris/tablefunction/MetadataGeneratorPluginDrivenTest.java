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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
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
 * listing through the connector SPI (using remote names) and emits one
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
    public void testRoutesToSpiWithRemoteNamesAndBuildsRows() throws Exception {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);

        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);

        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db");
        Mockito.when(table.getRemoteName()).thenReturn("remote_tbl");

        // The SPI must be queried with the REMOTE db/table names, not the local Doris names.
        Mockito.when(metadata.getTableHandle(session, "remote_db", "remote_tbl"))
                .thenReturn(Optional.of(handle));
        Mockito.when(metadata.listPartitionNames(session, handle))
                .thenReturn(Arrays.asList("pt=1", "pt=2"));

        TFetchSchemaTableDataResult result = invokeDeal(catalog, table);

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        List<TRow> rows = result.getDataBatch();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("pt=1", rows.get(0).getColumnValue().get(0).getStringVal());
        Assertions.assertEquals("pt=2", rows.get(1).getColumnValue().get(0).getStringVal());
        Mockito.verify(metadata).getTableHandle(session, "remote_db", "remote_tbl");
    }

    @Test
    public void testAbsentHandleYieldsEmptyOkResult() throws Exception {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);

        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);

        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db");
        Mockito.when(table.getRemoteName()).thenReturn("remote_tbl");
        Mockito.when(metadata.getTableHandle(session, "remote_db", "remote_tbl"))
                .thenReturn(Optional.empty());

        TFetchSchemaTableDataResult result = invokeDeal(catalog, table);

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(Collections.emptyList(), result.getDataBatch());
        Mockito.verify(metadata, Mockito.never()).listPartitionNames(Mockito.any(), Mockito.any());
    }
}
