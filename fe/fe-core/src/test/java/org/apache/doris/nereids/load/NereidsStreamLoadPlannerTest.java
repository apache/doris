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

package org.apache.doris.nereids.load;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TQueryGlobals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;

class NereidsStreamLoadPlannerTest {

    @Test
    void testQueryGlobalsUseTaskStatementStartTime() {
        Instant statementStartTime = Instant.parse("2026-06-12T03:04:05.123456Z");
        NereidsLoadTaskInfo taskInfo = Mockito.mock(NereidsLoadTaskInfo.class);
        Mockito.when(taskInfo.getStatementStartTime()).thenReturn(statementStartTime);
        Mockito.when(taskInfo.getTimezone()).thenReturn("PST");
        Mockito.when(taskInfo.getMaxFilterRatio()).thenReturn(0.1);

        TQueryGlobals queryGlobals = new NereidsStreamLoadPlanner(null, null, taskInfo).createQueryGlobals();

        Assertions.assertEquals(statementStartTime.toEpochMilli(), queryGlobals.getTimestampMs());
        Assertions.assertEquals(statementStartTime.getNano(), queryGlobals.getNanoSeconds());
        Assertions.assertEquals("America/Los_Angeles", queryGlobals.getTimeZone());
    }

    @Test
    void testLoadPlanCollectorUsesTaskStatementStartTime() {
        Instant statementStartTime = Instant.parse("2026-06-12T23:59:59.999999Z");
        NereidsLoadTaskInfo taskInfo = Mockito.mock(NereidsLoadTaskInfo.class);
        Mockito.when(taskInfo.getStatementStartTime()).thenReturn(statementStartTime);
        Mockito.when(taskInfo.getTimezone()).thenReturn("+08:00");
        NereidsLoadPlanInfoCollector collector = new NereidsLoadPlanInfoCollector(
                null, taskInfo, null, 0, null, null, null, null);

        Assertions.assertEquals(statementStartTime,
                collector.createTaskStatementContext().getStatementStartTime());
        Assertions.assertEquals("+08:00", collector.createTaskStatementContext().getStatementTimeZone().getId());
    }

    @Test
    void testBrokerLoadStatementContextIsRestored() {
        ConnectContext connectContext = new ConnectContext();
        Instant oldStatementStartTime = Instant.parse("2026-06-12T00:00:00Z");
        StatementContext originalStatementContext = new StatementContext(connectContext, null, oldStatementStartTime);
        connectContext.setStatementContext(originalStatementContext);
        NereidsBrokerLoadTask taskInfo = new NereidsBrokerLoadTask(1L, 1, 1, false, false, false, null,
                "+08:00");

        StatementContext savedStatementContext = NereidsLoadingTaskPlanner.installBrokerLoadStatementContext(
                connectContext, taskInfo);

        Assertions.assertSame(originalStatementContext, savedStatementContext);
        Assertions.assertNotSame(originalStatementContext, connectContext.getStatementContext());
        Assertions.assertEquals(taskInfo.getStatementStartTime(),
                connectContext.getStatementContext().getStatementStartTime());
        Assertions.assertEquals("+08:00", connectContext.getStatementContext().getStatementTimeZone().getId());

        NereidsStreamLoadPlanner.restoreStatementContext(connectContext, savedStatementContext);

        Assertions.assertSame(originalStatementContext, connectContext.getStatementContext());
    }

    @Test
    void testStreamLoadStatementContextIsRestored() {
        ConnectContext connectContext = new ConnectContext();
        StatementContext originalStatementContext = new StatementContext(connectContext, null,
                Instant.parse("2026-06-12T00:00:00Z"));
        connectContext.setStatementContext(originalStatementContext);
        Instant taskStatementStartTime = Instant.parse("2026-06-12T03:04:05.123456Z");

        StatementContext savedStatementContext = NereidsStreamLoadPlanner.installTaskStatementContext(
                connectContext, taskStatementStartTime, "PST");

        Assertions.assertSame(originalStatementContext, savedStatementContext);
        Assertions.assertNotSame(originalStatementContext, connectContext.getStatementContext());
        Assertions.assertEquals(taskStatementStartTime,
                connectContext.getStatementContext().getStatementStartTime());
        Assertions.assertEquals("America/Los_Angeles",
                connectContext.getStatementContext().getStatementTimeZone().getId());

        NereidsStreamLoadPlanner.restoreStatementContext(connectContext, savedStatementContext);

        Assertions.assertSame(originalStatementContext, connectContext.getStatementContext());
    }
}
