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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.qe.ShowResultSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

/**
 * Tests for SHOW PARTITIONS dispatch to a {@link PluginDrivenExternalCatalog} added by
 * P4-T06c (ShowPartitionsCommand.handleShowPluginDrivenTablePartitions).
 *
 * <p><b>Why:</b> after the MaxCompute SPI cutover, a {@code max_compute} catalog is a
 * {@link PluginDrivenExternalCatalog}. The legacy handler keyed on
 * {@code instanceof MaxComputeExternalCatalog} no longer matches, so SHOW PARTITIONS
 * must route through the connector SPI instead. This test locks in that the new handler
 * resolves the table handle using the REMOTE db/table names and emits one row per
 * partition returned by {@code listPartitionNames}.</p>
 */
public class ShowPartitionsCommandPluginDrivenTest {

    @Test
    public void testHandlerRoutesToSpiWithRemoteNames() throws Exception {
        TableNameInfo tableName = Mockito.mock(TableNameInfo.class);
        Mockito.when(tableName.getDb()).thenReturn("db");
        Mockito.when(tableName.getTbl()).thenReturn("t");

        ShowPartitionsCommand command = new ShowPartitionsCommand(tableName, null, null, -1L, -1L, false);

        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);

        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db");
        Mockito.when(table.getRemoteName()).thenReturn("remote_tbl");

        // Resolution chain: catalog.getDbOrAnalysisException(db).getTableOrAnalysisException(t) -> table.
        // doReturn avoids generic-type checks on the default interface methods.
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("db");
        Mockito.doReturn(table).when(db).getTableOrAnalysisException("t");
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(session, "remote_db", "remote_tbl"))
                .thenReturn(Optional.of(handle));
        Mockito.when(metadata.listPartitionNames(session, handle))
                .thenReturn(Arrays.asList("pt=2", "pt=1"));

        setField(command, "catalog", catalog);

        Method m = ShowPartitionsCommand.class.getDeclaredMethod("handleShowPluginDrivenTablePartitions");
        m.setAccessible(true);
        ShowResultSet rs = (ShowResultSet) m.invoke(command);

        List<List<String>> rows = rs.getResultRows();
        Assertions.assertEquals(2, rows.size());
        // sorted ascending by partition name
        Assertions.assertEquals("pt=1", rows.get(0).get(0));
        Assertions.assertEquals("pt=2", rows.get(1).get(0));
        Mockito.verify(metadata).getTableHandle(session, "remote_db", "remote_tbl");
    }

    @Test
    public void testHandlerEmitsFiveColumnsWhenConnectorDeclaresPartitionStats() throws Exception {
        TableNameInfo tableName = Mockito.mock(TableNameInfo.class);
        Mockito.when(tableName.getDb()).thenReturn("db");
        Mockito.when(tableName.getTbl()).thenReturn("t");

        ShowPartitionsCommand command = new ShowPartitionsCommand(tableName, null, null, -1L, -1L, false);

        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);

        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        // Must be a PluginDrivenExternalTable: the 5-col path casts the table to read its partition
        // columns for the PartitionKey column.
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db");
        Mockito.when(table.getRemoteName()).thenReturn("remote_tbl");
        Mockito.when(table.getPartitionColumns()).thenReturn(Arrays.asList(
                new Column("dt", PrimitiveType.INT), new Column("region", PrimitiveType.INT)));

        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("db");
        Mockito.doReturn(table).when(db).getTableOrAnalysisException("t");
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        // The capability that flips SHOW PARTITIONS to the 5-column rich result (D-045).
        Mockito.when(connector.getCapabilities())
                .thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_PARTITION_STATS));
        Mockito.when(metadata.getTableHandle(session, "remote_db", "remote_tbl"))
                .thenReturn(Optional.of(handle));
        Mockito.when(metadata.listPartitions(Mockito.eq(session), Mockito.eq(handle), Mockito.any()))
                .thenReturn(Collections.singletonList(new ConnectorPartitionInfo(
                        "dt=2024/region=cn", Collections.emptyMap(), Collections.emptyMap(),
                        /*rowCount*/ 42L, /*sizeBytes*/ 1024L, /*lastModifiedMillis*/ 1700000000000L,
                        /*fileCount*/ 7L)));

        setField(command, "catalog", catalog);

        Method m = ShowPartitionsCommand.class.getDeclaredMethod("handleShowPluginDrivenTablePartitions");
        m.setAccessible(true);
        ShowResultSet rs = (ShowResultSet) m.invoke(command);

        List<List<String>> rows = rs.getResultRows();
        Assertions.assertEquals(1, rows.size());
        List<String> row = rows.get(0);
        // WHY (D-045): a connector declaring SUPPORTS_PARTITION_STATS yields the legacy paimon
        // 5-column result: Partition / PartitionKey / RecordCount / FileSizeInBytes / FileCount.
        // MUTATION: gating on instanceof PaimonExternalCatalog (false here) or dropping the capability
        // branch -> 1-col fallback -> red.
        Assertions.assertEquals(5, row.size(),
                "SUPPORTS_PARTITION_STATS must yield the 5-column rich result, not the 1-col fallback");
        Assertions.assertEquals("dt=2024/region=cn", row.get(0));
        // PartitionKey = table partition-column names comma-joined, identical on every row.
        // MUTATION: deriving it from per-partition getPartitionValues() instead of the table's
        // partition columns -> red.
        Assertions.assertEquals("dt,region", row.get(1));
        // RecordCount<-getRowCount, FileSizeInBytes<-getSizeBytes, FileCount<-getFileCount.
        // MUTATION: swapping these getters / dropping fileCount -> red.
        Assertions.assertEquals("42", row.get(2));
        Assertions.assertEquals("1024", row.get(3));
        Assertions.assertEquals("7", row.get(4));
        // getMetaData() MUST agree on the column count or ShowResultSet headers/rows diverge.
        Assertions.assertEquals(5, command.getMetaData().getColumnCount(),
                "getMetaData must produce 5 headers to match the 5-col rows (same capability gate)");
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = ShowPartitionsCommand.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
