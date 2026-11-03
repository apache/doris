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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.datasource.mvcc.MvccReadSelector;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.qe.cache.SqlCacheVersion;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

class IcebergSqlCacheVersionTest {
    private SqlCacheVersion version(String uuid, String metadataFile, long snapshotId, long schemaId) {
        IcebergSnapshotCacheValue value = Mockito.mock(IcebergSnapshotCacheValue.class);
        Table table = Mockito.mock(Table.class, Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        TableOperations operations = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(((HasTableOperations) table).operations()).thenReturn(operations);
        Mockito.when(operations.current()).thenReturn(metadata);
        Mockito.when(metadata.uuid()).thenReturn(uuid);
        Mockito.when(metadata.metadataFileLocation()).thenReturn(metadataFile);
        Mockito.when(metadata.lastUpdatedMillis()).thenReturn(1234L);
        Mockito.when(value.getIcebergTable()).thenReturn(Optional.of(table));
        Mockito.when(value.getSnapshot()).thenReturn(new IcebergSnapshot(snapshotId, schemaId));
        Optional<SqlCacheVersion> result = IcebergExternalTable.sqlCacheVersionOf(value);
        Mockito.verify(value, Mockito.never()).getPartitionInfo();
        return result.orElse(null);
    }

    @Test
    void versionCoversDataSchemaRecreationAndMetadataChangesWithoutPartitionReads() {
        SqlCacheVersion original = version("uuid", "metadata-1", 11, 3);
        Assertions.assertEquals(1234L, original.getUpdateTimeMillis());
        Assertions.assertEquals(original.getVersion(), version("uuid", "metadata-1", 11, 3).getVersion());
        Assertions.assertNotEquals(original.getVersion(), version("uuid", "metadata-1", 12, 3).getVersion());
        Assertions.assertNotEquals(original.getVersion(), version("uuid", "metadata-1", 11, 4).getVersion());
        Assertions.assertNotEquals(original.getVersion(), version("recreated", "metadata-1", 11, 3).getVersion());
        Assertions.assertNotEquals(original.getVersion(), version("uuid", "metadata-2", 11, 3).getVersion());
    }

    @Test
    void missingPhysicalIdentityDisablesCaching() {
        Assertions.assertNull(version(null, "metadata-1", 11, 3));
        Assertions.assertNull(version("uuid", null, 11, 3));
    }

    @Test
    void cachedSelectorsKeepOriginalValuesAndDoNotShareResolvedCopies() {
        TableSnapshot input = TableSnapshot.versionOf("11");
        MvccReadSelector version = MvccReadSelector.of(Optional.of(input), Optional.empty());
        input.setValue("12");
        version.getTableSnapshot().get().setValue("13");
        Assertions.assertEquals("11", version.getTableSnapshot().get().getValue());
        TableScanParams branch = new TableScanParams(TableScanParams.BRANCH, Collections.emptyMap(),
                Collections.singletonList("daily"));
        MvccReadSelector reference = MvccReadSelector.of(Optional.empty(), Optional.of(branch));
        reference.getScanParams().get().reuseResolvedMapParams(Collections.singletonMap("snapshot", "11"));
        Assertions.assertFalse(reference.getScanParams().get().getResolvedMapParams().isPresent());
        Assertions.assertNotEquals(version, reference);
        Assertions.assertNotEquals(MvccReadSelector.LATEST, reference);
    }

    @Test
    void oneTableRetainsLatestSnapshotAndBranchDependenciesIndependently() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        IcebergExternalDatabase database = Mockito.mock(IcebergExternalDatabase.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(catalog.getName()).thenReturn("ice");
        Mockito.when(catalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getSqlCacheVersion(Mockito.any())).thenReturn(
                Optional.of(new SqlCacheVersion("latest", 1)),
                Optional.of(new SqlCacheVersion("historical", 2)),
                Optional.of(new SqlCacheVersion("branch", 3)));
        SqlCacheContext context = new SqlCacheContext(null);
        context.addUsedTable(table, Optional.empty(), MvccReadSelector.LATEST);
        context.addUsedTable(table, Optional.empty(),
                MvccReadSelector.of(Optional.of(TableSnapshot.versionOf("11")), Optional.empty()));
        context.addUsedTable(table, Optional.empty(), MvccReadSelector.of(Optional.empty(), Optional.of(
                new TableScanParams(TableScanParams.BRANCH, Collections.emptyMap(),
                        Collections.singletonList("daily")))));
        Assertions.assertEquals(3, context.getUsedTables().size());
        Assertions.assertFalse(context.hasUnsupportedTables());
    }
}
