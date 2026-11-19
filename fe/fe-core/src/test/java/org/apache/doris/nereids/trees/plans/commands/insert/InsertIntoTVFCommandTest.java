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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

class InsertIntoTVFCommandTest {
    @Test
    void testSuccessfulExecutionPublishesCoordinator() throws Exception {
        assertExecutionPublishesCoordinator(false);
    }

    @Test
    void testPartialDispatchFailurePublishesCoordinator() throws Exception {
        assertExecutionPublishesCoordinator(true);
    }

    private void assertExecutionPublishesCoordinator(boolean dispatchFails) throws Exception {
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, dispatchFails ? 2 : 1));
        context.getSessionVariable().parallelPipelineTaskNum = 1;
        StmtExecutor executor = Mockito.spy(new StmtExecutor(context, "insert into files select 1"));
        Mockito.doNothing().when(executor).checkBlockRules();
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.when(coordinator.getDispatchedBackendIds()).thenReturn(Set.of(10001L));
        Mockito.when(coordinator.getExecStatus()).thenReturn(Status.OK);
        Mockito.when(coordinator.getLoadCounters()).thenReturn(Collections.emptyMap());
        if (dispatchFails) {
            Mockito.doThrow(new UserException("fragment dispatch failed")).when(coordinator).exec();
        }

        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.ADMIN)).thenReturn(true);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.eq(context), Mockito.any(NereidsPlanner.class), Mockito.any()))
                .thenReturn(coordinator);
        InsertIntoTVFCommand command = new InsertIntoTVFCommand(
                Mockito.mock(LogicalPlan.class), Optional.empty(), Optional.empty());

        try (MockedStatic<Env> envs = Mockito.mockStatic(Env.class);
                MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class);
                MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class)) {
            envs.when(Env::getCurrentEnv).thenReturn(env);
            factories.when(EnvFactory::getInstance).thenReturn(factory);

            command.run(context, executor);

            Assertions.assertSame(coordinator, executor.getCoord());
            Assertions.assertEquals(Set.of(10001L), executor.getAuditStatisticsBackendIds());
            Assertions.assertEquals(dispatchFails ? MysqlStateType.ERR : MysqlStateType.OK,
                    context.getState().getStateType());
            Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(context.queryId()));
            Mockito.verify(coordinator).close();
        }
    }
}
