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

package org.apache.doris.qe;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StmtExecutorReplanRetryTest {

    /**
     * A query failing with an error it is planned again on (no alive backend in the compute group, say) is run again
     * by queryRetry, which plans the statement again in the same StatementContext. What the failed attempt's planning
     * started for a plan no coordinator dispatched is stopped before the next attempt plans, rather than left
     * generating splits beside the new plan's until the statement ends.
     */
    @Test
    public void testAReplanStopsWhatTheFailedAttemptsPlanningStarted() throws Exception {
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = Mockito.spy(new StmtExecutor(ctx, "select * from ice.db.big"));
        SplitAssignment undispatched = Mockito.mock(SplitAssignment.class);
        Mockito.doAnswer(invocation -> {
            // A batch scan of the plan started generating its splits while planned; the attempt failed before a
            // coordinator took them over.
            ctx.getStatementContext().addSplitAssignmentStartedWhilePlanning(undispatched);
            throw new UserException(SystemInfoService.ERROR_E230 + " injected replan error");
        }).doAnswer(invocation -> {
            Mockito.verify(undispatched).stopIfNotDispatched();
            return null;
        }).when(executor).execute(Mockito.any(TUniqueId.class));

        executor.queryRetry(new TUniqueId(1, 1));

        Mockito.verify(executor, Mockito.times(2)).execute(Mockito.any(TUniqueId.class));
        Mockito.verify(undispatched, Mockito.times(1)).stopIfNotDispatched();
    }
}
