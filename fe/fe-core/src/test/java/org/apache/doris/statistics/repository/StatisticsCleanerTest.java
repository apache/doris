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

package org.apache.doris.statistics.repository;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.TableStatsDeletionLog;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.repository.StatisticsCleaner.ExpiredStats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class StatisticsCleanerTest {

    @Test
    void testFindExpiredStats() {
        StatisticsCleaner cleaner = new StatisticsCleaner();
        ExpiredStats stat = new ExpiredStats();
        OlapTable olapTable = new OlapTable();
        for (int i = 0; i <= Config.max_allowed_in_element_num_of_delete; i++) {
            stat.expiredCatalog.add((long) i);
        }
        long expiredStats = cleaner.findExpiredStats(null, stat, 1, true);
        Assertions.assertEquals(expiredStats, 1);

        try (MockedStatic<StatisticsRepository> mockedRep = Mockito.mockStatic(StatisticsRepository.class)) {
            mockedRep.when(() -> StatisticsRepository.fetchStatsFullName(
                    ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyBoolean()))
                    .thenReturn(Lists.newArrayList());
            stat.expiredCatalog.clear();
            expiredStats = cleaner.findExpiredStats(olapTable, stat, 0, true);
            Assertions.assertEquals(expiredStats, StatisticConstants.FETCH_LIMIT);
        }
    }

    /**
     * The record of a table which is in the recycle bin is removed under the table write lock, which the
     * recovery of that table and the load that recreates the record take as well. The deletion therefore
     * cannot be journaled after such a load recreated the record, which would leave the master with a record
     * that its followers removed.
     */
    @Test
    void testRecycledTableStatsAreRemovedUnderTheTableLock() {
        StatisticsCleaner cleaner = new StatisticsCleaner();
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        InternalCatalog catalog = new InternalCatalog();
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(40001L);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        List<String> calls = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            calls.add("writeLock");
            return null;
        }).when(table).writeLock();
        Mockito.doAnswer(invocation -> {
            calls.add("removeTableStats");
            return null;
        }).when(analysisManager).removeTableStats(40001L);
        Mockito.doAnswer(invocation -> {
            calls.add("logDeleteTableStats");
            return null;
        }).when(editLog).logDeleteTableStats(Mockito.any(TableStatsDeletionLog.class));
        Mockito.doAnswer(invocation -> {
            calls.add("writeUnlock");
            return null;
        }).when(table).writeUnlock();

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            cleaner.removeRecycledTableStats(table, analysisManager, catalog);

            Assertions.assertEquals(ImmutableList.of("writeLock", "removeTableStats", "logDeleteTableStats",
                    "writeUnlock"), calls);
        }
    }

    /**
     * A table which was recovered from the recycle bin after its record was found orphaned keeps its
     * statistics: the catalog is checked again under the table lock, so the record is not removed just
     * because it was orphaned when the cleaner looked at it.
     */
    @Test
    void testTableRecoveredBeforeTheRemovalKeepsItsStats() {
        StatisticsCleaner cleaner = new StatisticsCleaner();
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        InternalCatalog catalog = new InternalCatalog();
        Database db = new Database(20001L, "test_recovered_table");
        catalog.unprotectCreateDb(db);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(40002L);
        Mockito.when(table.getName()).thenReturn("t_recovered");
        db.registerTable(table);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            cleaner.removeRecycledTableStats(table, analysisManager, catalog);

            Mockito.verify(analysisManager, Mockito.never()).removeTableStats(Mockito.anyLong());
            Mockito.verify(editLog, Mockito.never()).logDeleteTableStats(Mockito.any(TableStatsDeletionLog.class));
        }
    }
}
