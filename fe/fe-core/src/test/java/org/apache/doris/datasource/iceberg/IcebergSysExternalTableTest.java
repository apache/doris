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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class IcebergSysExternalTableTest {
    @Test
    public void testStaticMetadataTablesDoNotSupportSnapshotSelection() {
        IcebergExternalTable sourceTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(sourceTable.getId()).thenReturn(1L);
        Mockito.when(sourceTable.getName()).thenReturn("table");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("table");
        Mockito.when(sourceTable.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));
        Mockito.when(sourceTable.getDatabase()).thenReturn(Mockito.mock(IcebergExternalDatabase.class));

        for (MetadataTableType type : new MetadataTableType[] {
                MetadataTableType.HISTORY,
                MetadataTableType.SNAPSHOTS,
                MetadataTableType.REFS,
                MetadataTableType.METADATA_LOG_ENTRIES}) {
            IcebergSysExternalTable table = new IcebergSysExternalTable(sourceTable, type.name());
            Assertions.assertFalse(table.supportsSnapshotSelection(), type.name());
        }

        IcebergSysExternalTable dataFiles = new IcebergSysExternalTable(
                sourceTable, MetadataTableType.DATA_FILES.name());
        Assertions.assertTrue(dataFiles.supportsSnapshotSelection());
    }

    @Test
    public void testMetadataSchemaReloadsAfterSourceEvolution() {
        IcebergExternalTable sourceTable = Mockito.mock(IcebergExternalTable.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(sourceTable.getId()).thenReturn(1L);
        Mockito.when(sourceTable.getName()).thenReturn("table");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("table");
        Mockito.when(sourceTable.getCatalog()).thenReturn(catalog);
        Mockito.when(sourceTable.getDatabase()).thenReturn(Mockito.mock(IcebergExternalDatabase.class));
        Table firstGeneration = Mockito.mock(Table.class);
        Table evolvedGeneration = Mockito.mock(Table.class);
        Mockito.when(firstGeneration.schema()).thenReturn(new Schema(
                Types.NestedField.required(1, "file_path", Types.StringType.get())));
        Mockito.when(evolvedGeneration.schema()).thenReturn(new Schema(
                Types.NestedField.required(1, "file_path", Types.StringType.get()),
                Types.NestedField.optional(2, "evolved_partition", Types.StringType.get())));
        IcebergSysExternalTable sysTable = Mockito.spy(new IcebergSysExternalTable(
                sourceTable, MetadataTableType.PARTITIONS.name()));
        Mockito.doReturn(firstGeneration, evolvedGeneration).when(sysTable).getSysIcebergTable();

        Assertions.assertEquals(1, sysTable.getFullSchema().size());
        Assertions.assertEquals(2, sysTable.getFullSchema().size());
        Mockito.verify(sysTable, Mockito.times(2)).getSysIcebergTable();
    }

    @Test
    public void testSnapshotSelectableSchemaFollowsRelationSnapshot() {
        IcebergExternalTable sourceTable = Mockito.mock(IcebergExternalTable.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(sourceTable.getId()).thenReturn(1L);
        Mockito.when(sourceTable.getName()).thenReturn("table");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("table");
        Mockito.when(sourceTable.getCatalog()).thenReturn(catalog);
        Mockito.when(sourceTable.getDatabase()).thenReturn(Mockito.mock(IcebergExternalDatabase.class));
        Table frozenGeneration = Mockito.mock(Table.class);
        Table latestGeneration = Mockito.mock(Table.class);
        IcebergSnapshotCacheValue snapshotValue = Mockito.mock(IcebergSnapshotCacheValue.class);
        Mockito.when(snapshotValue.getIcebergTable()).thenReturn(Optional.of(frozenGeneration));
        Optional<MvccSnapshot> relationSnapshot = Optional.of(new IcebergMvccSnapshot(snapshotValue));

        try (MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class);
                MockedStatic<IcebergUtils> icebergUtils = Mockito.mockStatic(IcebergUtils.class)) {
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(sourceTable)).thenReturn(relationSnapshot);
            icebergUtils.when(() -> IcebergUtils.getQueryScopedIcebergTable(sourceTable))
                    .thenReturn(latestGeneration);

            // $partitions is snapshot-selectable: analysis must see the generation the scan uses.
            IcebergSysExternalTable partitions = new IcebergSysExternalTable(
                    sourceTable, MetadataTableType.PARTITIONS.name());
            Assertions.assertSame(frozenGeneration, partitions.resolveBaseTable());

            // $snapshots ignores a selected snapshot and keeps reading the latest generation.
            IcebergSysExternalTable snapshots = new IcebergSysExternalTable(
                    sourceTable, MetadataTableType.SNAPSHOTS.name());
            Assertions.assertSame(latestGeneration, snapshots.resolveBaseTable());

            // Without a bound relation snapshot the latest generation is used.
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(sourceTable)).thenReturn(Optional.empty());
            Assertions.assertSame(latestGeneration, partitions.resolveBaseTable());
        }
    }
}
