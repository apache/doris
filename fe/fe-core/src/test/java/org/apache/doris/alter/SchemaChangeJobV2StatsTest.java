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

package org.apache.doris.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.statistics.analysis.AnalysisManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SchemaChangeJobV2StatsTest {

    /**
     * The statistics record of a table is removed when the schema change finished, together with the journal
     * entry of FINISHED. A load which becomes visible in between updates or recreates the record, and a follower
     * replays "FINISHED, then VISIBLE", so the removal has to happen under the table write lock which the load
     * takes as well.
     */
    @Test
    public void testRemoveTableStatsOnFinishedRequiresTheTableWriteLock() {
        Env env = Mockito.mock(Env.class);
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        Mockito.when(env.getAnalysisManager()).thenReturn(analysisManager);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(10001L);
        Mockito.when(table.getName()).thenReturn("t");
        SchemaChangeJobV2 job = new SchemaChangeJobV2("sql", 1L, 2L, 10001L, "t", 10000L);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

            Mockito.when(table.isWriteLockHeldByCurrentThread()).thenReturn(false);
            Assertions.assertThrows(IllegalStateException.class, () -> job.removeTableStatsOnFinished(table));
            Mockito.verify(analysisManager, Mockito.never()).removeTableStats(Mockito.anyLong());

            Mockito.when(table.isWriteLockHeldByCurrentThread()).thenReturn(true);
            job.removeTableStatsOnFinished(table);
            Mockito.verify(analysisManager).removeTableStats(10001L);
        }
    }
}
