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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonMvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class PaimonSourceTest {

    @Test
    public void testUsesRelationSnapshotInsteadOfStatementCurrentSnapshot() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        Table branchTable = Mockito.mock(Table.class);
        desc.setTable(externalTable);
        Mockito.when(externalTable.getPaimonTable(Optional.of(relationSnapshot))).thenReturn(branchTable);

        PaimonSource source = new PaimonSource(desc, Optional.of(relationSnapshot));

        Assert.assertSame(branchTable, source.getPaimonTable());
    }

    @Test
    public void testBehavioralOptionsStayOnRelationTable() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        Table relationTable = Mockito.mock(Table.class);
        Table processedTable = Mockito.mock(Table.class);
        desc.setTable(externalTable);
        Mockito.when(externalTable.getPaimonTable(Optional.of(relationSnapshot))).thenReturn(relationTable);
        Mockito.when(relationTable.copy(ImmutableMap.of("scan.plan-sort-partition", "true")))
                .thenReturn(processedTable);
        PaimonSource source = new PaimonSource(desc, Optional.of(relationSnapshot));
        TableScanParams params = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", "true"), java.util.Collections.emptyList());

        Assert.assertSame(processedTable, source.getPaimonTable(params));
        Mockito.verify(externalTable, Mockito.never()).getPaimonTable(params);
    }

    @Test
    public void testSystemOptionsStayOnRelationBoundDataTable() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        FileStoreTable boundDataTable = Mockito.mock(FileStoreTable.class);
        Table rawSystemTable = Mockito.mock(Table.class);
        Table processedSystemTable = Mockito.mock(Table.class);
        FileStoreTable serializedSystemSource = Mockito.mock(FileStoreTable.class);
        MvccSnapshot relationSnapshot = new PaimonMvccSnapshot(new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(7, 11, boundDataTable)));
        desc.setTable(systemTable);
        Mockito.when(systemTable.getBoundDataTable(Optional.of(relationSnapshot))).thenReturn(boundDataTable);
        Mockito.when(systemTable.getRawSysPaimonTable(boundDataTable)).thenReturn(rawSystemTable);
        TableScanParams params = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", "true"), java.util.Collections.emptyList());
        Mockito.when(systemTable.getSysPaimonTable(boundDataTable, params)).thenReturn(processedSystemTable);
        Mockito.when(systemTable.runtimeSafeDataTable(
                boundDataTable, params, java.util.Collections.emptyMap())).thenReturn(serializedSystemSource);

        PaimonSource source = new PaimonSource(desc, Optional.of(relationSnapshot));

        Assert.assertSame(rawSystemTable, source.getPaimonTable());
        Assert.assertSame(processedSystemTable, source.getPaimonTable(params));
        Assert.assertSame(serializedSystemSource,
                source.runtimeSafeSystemDataTable(params, java.util.Collections.emptyMap()));
    }
}
