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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecuteCommandTest {

    @Test
    public void testResolvedScanOptionsAreResetForEveryExecute() throws Exception {
        String sql = "select * from p@options('scan.mode'='latest')";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        UnboundRelation relation = logicalPlan.<UnboundRelation>collectToList(
                UnboundRelation.class::isInstance).get(0);
        TableScanParams scanParams = relation.getScanParams();
        AtomicInteger snapshotId = new AtomicInteger();

        Assertions.assertEquals("1", resolveNextSnapshot(scanParams, snapshotId));

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        Assertions.assertEquals("2", resolveNextSnapshot(scanParams, snapshotId));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        Assertions.assertEquals("3", resolveNextSnapshot(scanParams, snapshotId));
        Mockito.verify(executor, Mockito.times(2)).execute();
    }

    @Test
    public void testExecutePreparedCommandWithoutPlanChildren() throws Exception {
        String sql = "show variables";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);

        Mockito.verify(executor).execute();
    }

    private String resolveNextSnapshot(TableScanParams scanParams, AtomicInteger snapshotId) {
        return scanParams.getOrResolveMapParams(ignored -> ImmutableMap.of(
                "scan.snapshot-id", String.valueOf(snapshotId.incrementAndGet())))
                .get("scan.snapshot-id");
    }

    @Test
    public void testReapplySetVarHintForCachedExecution() throws Exception {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setTimeZone("+00:00");
        LogicalPlan plan = new LogicalSelectHint<>(
                ImmutableList.of(new SelectHintSetVar("SET_VAR",
                        ImmutableMap.of(SessionVariable.TIME_ZONE, Optional.of("+8:00")))),
                new UnboundOneRowRelation(new RelationId(1), ImmutableList.of()));

        StatementContext firstExecution = new StatementContext(
                connectContext, new OriginStatement("select 1", 0));
        ExecuteCommand.applySetVarHints(plan, firstExecution);
        Assertions.assertEquals("+08:00", sessionVariable.getTimeZone());
        Assertions.assertEquals("+08:00", firstExecution.getStatementTimeZone().getId());

        VariableMgr.revertSessionValue(sessionVariable);
        sessionVariable.clearSessionOriginValue();
        sessionVariable.setIsSingleSetVar(false);
        Assertions.assertEquals("+00:00", sessionVariable.getTimeZone());

        StatementContext cachedExecution = new StatementContext(
                connectContext, new OriginStatement("select 1", 0));
        ExecuteCommand.applySetVarHints(plan, cachedExecution);
        Assertions.assertEquals("+08:00", sessionVariable.getTimeZone());
        Assertions.assertEquals("+08:00", cachedExecution.getStatementTimeZone().getId());
    }
}
