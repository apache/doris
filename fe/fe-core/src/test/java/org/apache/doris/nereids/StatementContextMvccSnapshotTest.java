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

package org.apache.doris.nereids;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class StatementContextMvccSnapshotTest {
    @SuppressWarnings("unchecked")
    private static MvccTable mockTable() {
        MvccTable table = Mockito.mock(MvccTable.class);
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        return table;
    }

    private static TableScanParams options(String value) {
        return new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", value), ImmutableList.of());
    }

    @Test
    public void aliasesWithDifferentRelationOptionsPinSeparateSnapshots() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
        MvccSnapshot first = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot second = Mockito.mock(MvccSnapshot.class);
        TableScanParams enabled = options("true");
        TableScanParams disabled = options("false");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled))).thenReturn(first);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(disabled))).thenReturn(second);

        context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
        context.loadSnapshots(table, Optional.empty(), Optional.of(disabled));

        Assertions.assertSame(first,
                context.getSnapshot(table, Optional.empty(), Optional.of(options("true"))).orElse(null));
        Assertions.assertSame(second,
                context.getSnapshot(table, Optional.empty(), Optional.of(options("false"))).orElse(null));
        Assertions.assertFalse(context.getSnapshot(table).isPresent());
    }

    @Test
    public void optionAndPlainAliasesAreOrderIndependent() {
        MvccTable table = mockTable();
        MvccSnapshot plain = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot option = Mockito.mock(MvccSnapshot.class);
        TableScanParams enabled = options("true");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenReturn(plain);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled))).thenReturn(option);

        for (boolean optionFirst : new boolean[] {true, false}) {
            StatementContext context = new StatementContext(new ConnectContext(), null);
            if (optionFirst) {
                context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
                context.loadSnapshots(table, Optional.empty(), Optional.empty());
            } else {
                context.loadSnapshots(table, Optional.empty(), Optional.empty());
                context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
            }
            Assertions.assertSame(plain,
                    context.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null));
            Assertions.assertSame(option,
                    context.getSnapshot(table, Optional.empty(), Optional.of(options("true"))).orElse(null));
        }
    }
}
