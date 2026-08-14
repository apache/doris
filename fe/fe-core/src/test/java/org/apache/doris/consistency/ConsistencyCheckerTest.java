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

package org.apache.doris.consistency;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConsistencyCheckerTest {
    private static final long DB_ID = 1L;
    private static final long NORMAL_TABLE_ID = 2L;
    private static final long ROW_TTL_TABLE_ID = 3L;
    private static final long PARTITION_ID = 4L;
    private static final long INDEX_ID = 5L;
    private static final long NORMAL_TABLET_ID = 6L;
    private static final long ROW_TTL_TABLET_ID = 7L;

    @Test
    public void testManualCheckRejectsWholeBatchAndPendingJobSkipsRowTtl() {
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable normalTable = Mockito.mock(OlapTable.class);
        OlapTable rowTtlTable = Mockito.mock(OlapTable.class);
        TabletMeta normalTabletMeta = tabletMeta(NORMAL_TABLE_ID);
        TabletMeta rowTtlTabletMeta = tabletMeta(ROW_TTL_TABLE_ID);

        Mockito.when(invertedIndex.getTabletMeta(NORMAL_TABLET_ID)).thenReturn(normalTabletMeta);
        Mockito.when(invertedIndex.getTabletMeta(ROW_TTL_TABLET_ID)).thenReturn(rowTtlTabletMeta);
        Mockito.when(catalog.getDbNullable(DB_ID)).thenReturn(db);
        Mockito.when(db.getTableNullable(NORMAL_TABLE_ID)).thenReturn(normalTable);
        Mockito.when(db.getTableNullable(ROW_TTL_TABLE_ID)).thenReturn(rowTtlTable);
        Mockito.when(rowTtlTable.hasRowTtl()).thenReturn(true);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            ConsistencyChecker checker = new ConsistencyChecker();
            DdlException exception = Assert.assertThrows(DdlException.class,
                    () -> checker.addTabletsToCheck(
                            Arrays.asList(NORMAL_TABLET_ID, ROW_TTL_TABLET_ID)));
            Assert.assertTrue(exception.getMessage().contains(Long.toString(ROW_TTL_TABLET_ID)));

            Map<Long, CheckConsistencyJob> jobs = Deencapsulation.getField(checker, "jobs");
            Assert.assertTrue(jobs.isEmpty());
            Assert.assertFalse(new CheckConsistencyJob(ROW_TTL_TABLET_ID).sendTasks());
        }
    }

    @Test
    public void testAutomaticSelectionSkipsRowTtlTable() {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable rowTtlTable = Mockito.mock(OlapTable.class);

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbIds()).thenReturn(Collections.singletonList(DB_ID));
        Mockito.when(catalog.getDbNullable(DB_ID)).thenReturn(db);
        Mockito.when(db.getTables()).thenReturn(Collections.singletonList((Table) rowTtlTable));
        Mockito.when(rowTtlTable.isManagedTable()).thenReturn(true);
        Mockito.when(rowTtlTable.hasRowTtl()).thenReturn(true);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            ConsistencyChecker checker = new ConsistencyChecker();
            List<Long> chosenTablets = Deencapsulation.invoke(checker, "chooseTablets");
            Assert.assertTrue(chosenTablets.isEmpty());
            Mockito.verify(rowTtlTable, Mockito.never()).getAllPartitions();
        }
    }

    private static TabletMeta tabletMeta(long tableId) {
        return new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, 1, TStorageMedium.HDD);
    }
}
