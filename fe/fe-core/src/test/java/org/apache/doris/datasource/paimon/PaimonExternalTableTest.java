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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PaimonExternalTableTest {

    @Test
    public void testSelectorFreeOptionsPreserveStatementSnapshot() {
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        Optional<MvccSnapshot> statementSnapshot = Optional.of(snapshot);
        Table statementTable = Mockito.mock(Table.class);
        Table statementCopy = Mockito.mock(Table.class);
        Table baseTable = Mockito.mock(Table.class);
        Table baseCopy = Mockito.mock(Table.class);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", "true"),
                Collections.emptyList());

        Mockito.doReturn(statementTable).when(externalTable).getPaimonTable(statementSnapshot);
        Mockito.when(statementTable.copy(scanParams.getMapParams())).thenReturn(statementCopy);
        Mockito.when(baseTable.copy(scanParams.getMapParams())).thenReturn(baseCopy);

        try (MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class);
                MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable)).thenReturn(statementSnapshot);
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(baseTable);

            Assert.assertSame(statementCopy, externalTable.getPaimonTable(scanParams));
        }
    }

    @Test
    public void testModeOnlyLatestUsesStatementSnapshotAcrossPhases() {
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        Optional<MvccSnapshot> statementSnapshot = Optional.of(snapshot);
        Table statementTable = Mockito.mock(Table.class);
        Table pinnedCopy = Mockito.mock(Table.class);
        Table baseTable = Mockito.mock(Table.class);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.mode", "latest"),
                Collections.emptyList());

        Mockito.doReturn(statementTable).when(externalTable).getPaimonTable(statementSnapshot);
        Mockito.when(statementTable.options()).thenReturn(ImmutableMap.of("scan.snapshot-id", "7"));
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(pinnedCopy);

        try (MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class);
                MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable)).thenReturn(statementSnapshot);
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(baseTable);

            Assert.assertSame(pinnedCopy, externalTable.getPaimonTable(scanParams));
            Assert.assertSame(pinnedCopy, externalTable.getPaimonTable(scanParams));
        }

        Mockito.verify(baseTable, Mockito.times(2)).copy(ArgumentMatchers.argThat(options ->
                "7".equals(options.get("scan.snapshot-id"))
                        && options.containsKey("scan.mode")
                        && options.get("scan.mode") == null));
    }

    @Test
    public void testBranchSnapshotUsesEffectiveTableSchema() {
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable externalTable = new PaimonExternalTable(
                1L, "local_table", "remote_table", catalog, database);
        FileStoreTable branchTable = Mockito.mock(FileStoreTable.class);
        TableSchema branchSchema = new TableSchema(3L,
                Collections.singletonList(new DataField(1, "branch_column", DataTypes.INT())),
                1, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), "");
        Mockito.when(branchTable.schema()).thenReturn(branchSchema);
        Mockito.when(branchTable.schemaManager()).thenThrow(
                new AssertionError("branch schema must not be looked up through the base namespace"));
        PaimonSnapshotCacheValue cacheValue = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(7L, 3L, branchTable), true);

        List<Column> schema = externalTable.getFullSchema(
                Optional.of(new PaimonMvccSnapshot(cacheValue)));

        Assert.assertEquals(1, schema.size());
        Assert.assertEquals("branch_column", schema.get(0).getName());
    }
}
