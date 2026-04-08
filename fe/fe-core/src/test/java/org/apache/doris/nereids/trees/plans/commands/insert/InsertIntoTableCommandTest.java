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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.collections.Sets;

import java.util.Optional;
import java.util.function.Predicate;

class InsertIntoTableCommandTest {
    @Mock
    private StmtExecutor stmtExecutor;

    @Mock
    private RemoteDorisExternalTable remoteDorisExternalTable;

    @Mock
    private DataSink dataSink;

    @Mock
    private PlanFragment planFragment;

    @Mock
    private OlapTable olapTable;

    @Mock
    private LogicalPlan logicalPlan;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testSelectInsertExecutorFactoryForRemoteTableWithArrowflightlException() {
        InsertIntoTableCommand command = new InsertIntoTableCommand(
                PlanType.INSERT_INTO_TABLE_COMMAND,
                logicalPlan,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty()
        );

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getConnectType()).thenReturn(ConnectContext.ConnectType.MYSQL);
        Mockito.when(ctx.getMysqlChannel()).thenReturn(null);
        Mockito.when(ctx.queryId()).thenReturn(new TUniqueId());

        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        PhysicalPlan physicalPlan = Mockito.mock(PhysicalPlan.class);
        PhysicalSink physicalSink = Mockito.mock(PhysicalSink.class);
        Mockito.when(physicalPlan.children()).thenReturn(Lists.newArrayList(physicalSink));
        Mockito.when(planner.getPhysicalPlan()).thenReturn(physicalPlan);
        Mockito.when(physicalPlan.collect(ArgumentMatchers.any(Predicate.class)))
                .thenReturn(Sets.newSet(physicalSink));
        Mockito.when(planner.getFragments()).thenReturn(Lists.newArrayList(planFragment));
        Mockito.when(planFragment.getSink()).thenReturn(dataSink);
        Mockito.when(remoteDorisExternalTable.getOlapTable()).thenReturn(olapTable);

        Mockito.when(remoteDorisExternalTable.useArrowFlight()).thenReturn(true);

        Assertions.assertThrows(AnalysisException.class, () -> {
            command.selectInsertExecutorFactory(planner, ctx, stmtExecutor, remoteDorisExternalTable);
        }, "insert remote doris only support when catalog use_arrow_flight is false");
    }

    @Test
    void testSelectInsertExecutorFactoryForRemoteTableWithTxnModelException() {
        InsertIntoTableCommand command = new InsertIntoTableCommand(
                PlanType.INSERT_INTO_TABLE_COMMAND,
                logicalPlan,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty()
        );

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.isTxnModel()).thenReturn(true);
        Mockito.when(ctx.getConnectType()).thenReturn(ConnectContext.ConnectType.MYSQL);
        Mockito.when(ctx.getMysqlChannel()).thenReturn(null);
        Mockito.when(ctx.queryId()).thenReturn(new TUniqueId());

        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        PhysicalPlan physicalPlan = Mockito.mock(PhysicalPlan.class);
        PhysicalSink physicalSink = Mockito.mock(PhysicalSink.class);
        Mockito.when(physicalPlan.children()).thenReturn(Lists.newArrayList(physicalSink));
        Mockito.when(planner.getPhysicalPlan()).thenReturn(physicalPlan);
        Mockito.when(physicalPlan.collect(ArgumentMatchers.any(Predicate.class)))
                .thenReturn(Sets.newSet(physicalSink));
        Mockito.when(planner.getFragments()).thenReturn(Lists.newArrayList(planFragment));
        Mockito.when(planFragment.getSink()).thenReturn(dataSink);
        Mockito.when(remoteDorisExternalTable.getOlapTable()).thenReturn(olapTable);
        Assertions.assertThrows(AnalysisException.class, () -> {
            command.selectInsertExecutorFactory(planner, ctx, stmtExecutor, remoteDorisExternalTable);
        }, "remote olap table do not support txn model");
    }

    @Test
    void testSelectInsertExecutorFactoryForRemoteTableWithGroupCommitException() {
        InsertIntoTableCommand command = new InsertIntoTableCommand(
                PlanType.INSERT_INTO_TABLE_COMMAND,
                new LogicalPlanBuilder(logicalPlan).build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty()
        );

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.isTxnModel()).thenReturn(false);
        Mockito.when(ctx.isGroupCommit()).thenReturn(true);
        Mockito.when(ctx.getConnectType()).thenReturn(ConnectContext.ConnectType.MYSQL);
        Mockito.when(ctx.getMysqlChannel()).thenReturn(null);
        Mockito.when(ctx.queryId()).thenReturn(new TUniqueId());

        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        PhysicalPlan physicalPlan = Mockito.mock(PhysicalPlan.class);
        PhysicalSink physicalSink = Mockito.mock(PhysicalSink.class);
        Mockito.when(physicalPlan.children()).thenReturn(Lists.newArrayList(physicalSink));
        Mockito.when(planner.getPhysicalPlan()).thenReturn(physicalPlan);
        Mockito.when(physicalPlan.collect(ArgumentMatchers.any(Predicate.class)))
                .thenReturn(Sets.newSet(physicalSink));
        Mockito.when(planner.getFragments()).thenReturn(Lists.newArrayList(planFragment));
        Mockito.when(planFragment.getSink()).thenReturn(dataSink);
        Mockito.when(remoteDorisExternalTable.getOlapTable()).thenReturn(olapTable);
        Assertions.assertThrows(AnalysisException.class, () -> {
            command.selectInsertExecutorFactory(planner, ctx, stmtExecutor, remoteDorisExternalTable);
        }, "remote olap table do not support group commit");
    }
}
