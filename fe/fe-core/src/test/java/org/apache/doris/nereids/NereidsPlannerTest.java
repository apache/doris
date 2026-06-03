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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.jobs.executor.TableCollectAndHookInitializer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NereidsPlannerTest {

    @Test
    public void testCollectAndLockTableRecordsPreloadTimeWhenExecuted() {
        SummaryProfile summaryProfile = Mockito.mock(SummaryProfile.class);
        NereidsPlanner planner = createPlanner(StatementContext.ExternalMetadataPreloadResult.executed(2, 1),
                summaryProfile);

        // Record the dedicated preload counter only when the external preload phase is executed.
        planner.collectAndLockTable(false);

        Mockito.verify(summaryProfile).addNereidsPreloadExternalMetadataTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableStartTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableFinishTime(Mockito.anyLong());
    }

    @Test
    public void testCollectAndLockTableSkipsPreloadCounterWhenNotExecuted() {
        SummaryProfile summaryProfile = Mockito.mock(SummaryProfile.class);
        NereidsPlanner planner = createPlanner(
                StatementContext.ExternalMetadataPreloadResult.skipped(1, "skip preload"), summaryProfile);

        // Keep the dedicated preload counter untouched when preload is skipped before table locking.
        planner.collectAndLockTable(false);

        Mockito.verify(summaryProfile, Mockito.never()).addNereidsPreloadExternalMetadataTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableStartTime(Mockito.anyLong());
        Mockito.verify(summaryProfile).setNereidsLockTableFinishTime(Mockito.anyLong());
    }

    private NereidsPlanner createPlanner(StatementContext.ExternalMetadataPreloadResult preloadResult,
            SummaryProfile summaryProfile) {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        StatementContext statementContext = Mockito.spy(
                new StatementContext(connectContext, new OriginStatement("select 1", 0)));
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        TableCollectAndHookInitializer tableCollector = Mockito.mock(TableCollectAndHookInitializer.class);

        // Mock the planner entry point so the test only exercises the preload/profile control flow.
        Mockito.when(connectContext.getExecutor()).thenReturn(executor);
        Mockito.when(executor.getSummaryProfile()).thenReturn(summaryProfile);
        Mockito.when(cascadesContext.newTableCollector(true)).thenReturn(tableCollector);
        Mockito.doReturn(preloadResult).when(statementContext).preloadExternalTablesBeforeLock();
        Mockito.doNothing().when(statementContext).lock();

        NereidsPlanner planner = new NereidsPlanner(statementContext);
        Deencapsulation.setField(planner, "cascadesContext", cascadesContext);
        return planner;
    }
}
