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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.load.routineload.kinesis.KinesisTaskInfo;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlanFragment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.UUID;

class RoutineLoadWorkloadGroupResolutionTest {
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void replanUsesJobOwnerAndExplicitOrGlobalWorkloadGroup(boolean kinesis, boolean explicit) throws Exception {
        boolean savedEnabled = Config.enable_workload_group;
        Config.enable_workload_group = true;
        ConnectContext.remove();
        ConnectContext caller = new ConnectContext();
        caller.setCurrentUserIdentity(UserIdentity.ADMIN);
        caller.getSessionVariable().setWorkloadGroup("unrelated_caller_group");
        UserIdentity owner = UserIdentity.createAnalyzedUserIdentWithIp("job_owner", "%");
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        RoutineLoadJob job = Mockito.mock(RoutineLoadJob.class);
        Mockito.when(job.getDbId()).thenReturn(10L);
        Mockito.when(job.getTableId()).thenReturn(20L);
        Mockito.when(job.getUserIdentity()).thenReturn(owner);
        Mockito.when(job.getCloudCluster()).thenReturn("job_cluster");
        Mockito.when(job.getWorkloadGroup()).thenReturn(explicit ? "job_group" : "");
        Mockito.when(job.toNereidsRoutineLoadTaskInfo()).thenReturn(Mockito.mock(NereidsRoutineLoadTaskInfo.class));
        Mockito.when(catalog.getDbOrMetaException(10L)).thenReturn(db);
        Mockito.when(db.getTableOrMetaException(20L, Table.TableType.OLAP)).thenReturn(table);
        TPipelineFragmentParams plan = new TPipelineFragmentParams().setFragment(new TPlanFragment()
                .setOutputSink(new TDataSink().setOlapTableSink(new TOlapTableSink())));
        Mockito.when(job.plan(Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(plan);
        WorkloadGroupMgr groups = Mockito.mock(WorkloadGroupMgr.class);
        WorkloadGroup group = Mockito.mock(WorkloadGroup.class);
        TPipelineWorkloadGroup thriftGroup = new TPipelineWorkloadGroup();
        Mockito.when(group.toThrift()).thenReturn(thriftGroup);
        String expectedGroup = explicit ? "job_group" : "global_group";
        Mockito.when(groups.getWorkloadGroup(owner, expectedGroup, "job_cluster"))
                .thenReturn(Collections.singletonList(group));
        Mockito.when(env.getWorkloadGroupMgr()).thenReturn(groups);

        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(caller);
                MockedStatic<Env> environment = Mockito.mockStatic(Env.class);
                MockedStatic<VariableMgr> variables = Mockito.mockStatic(VariableMgr.class);
                MockedStatic<StreamLoadHandler> handler = Mockito.mockStatic(StreamLoadHandler.class);
                MockedConstruction<NereidsStreamLoadPlanner> planners =
                        Mockito.mockConstruction(NereidsStreamLoadPlanner.class);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            environment.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            variables.when(VariableMgr::getDefaultWorkloadGroup).thenReturn("global_group");
            RoutineLoadTaskInfo task = kinesis
                    ? new KinesisTaskInfo(UUID.randomUUID(), 1L, 1000L, Collections.emptyMap(), false, 0L, false)
                    : new KafkaTaskInfo(UUID.randomUUID(), 1L, 1000L, Collections.emptyMap(), false, 0L, false);
            Assertions.assertSame(plan, Deencapsulation.invoke(task, "rePlan", job));
            Assertions.assertEquals(Collections.singletonList(thriftGroup), plan.getWorkloadGroups());
            Mockito.verify(groups).getWorkloadGroup(owner, expectedGroup, "job_cluster");
            variables.verify(VariableMgr::getDefaultWorkloadGroup, Mockito.times(explicit ? 0 : 1));
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertEquals("unrelated_caller_group", caller.getSessionVariable().getWorkloadGroup());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            Config.enable_workload_group = savedEnabled;
        }
    }
}
