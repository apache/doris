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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class WorkloadSchedPolicyContextLifecycleTest {
    private boolean savedRunningUnitTest;
    private String savedDeployMode;
    private String savedCloudUniqueId;
    private Env env;
    private ComputeGroupMgr computeGroups;
    private WorkloadGroupMgr workloadGroups;
    private MockedStatic<Env> envMock;

    @BeforeEach
    void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        FeConstants.runningUnitTest = false;
        Config.deploy_mode = "local";
        Config.cloud_unique_id = "";
        env = Mockito.mock(Env.class);
        computeGroups = Mockito.mock(ComputeGroupMgr.class);
        workloadGroups = Mockito.mock(WorkloadGroupMgr.class);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroups);
        Mockito.when(env.getWorkloadGroupMgr()).thenReturn(workloadGroups);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        ConnectContext.remove();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        envMock.close();
        FeConstants.runningUnitTest = savedRunningUnitTest;
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
    }

    @Test
    void computeGroupLookupFailureReleasesUnboundSession() throws Exception {
        UserException failure = new UserException("missing compute group");
        Mockito.when(computeGroups.getComputeGroupByName(Mockito.anyString())).thenThrow(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(UserException.class,
                () -> new WorkloadSchedPolicyMgr().checkProperties(
                        Collections.singletonMap(WorkloadSchedPolicy.WORKLOAD_GROUP, "group"), new ArrayList<>())));
        ArgumentCaptor<String> session = ArgumentCaptor.forClass(String.class);
        Mockito.verify(env).registerSessionInfo(session.capture());
        Mockito.verify(env).unregisterSessionInfo(session.getValue());
        Assertions.assertNull(ConnectContext.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void workloadGroupLookupClosesPrivateContextAndPreservesCaller(boolean fail) throws Exception {
        Mockito.when(computeGroups.getComputeGroupByName(Mockito.anyString()))
                .thenReturn(Mockito.mock(ComputeGroup.class));
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        AtomicReference<ConnectContext> requestContext = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        WorkloadGroup workloadGroup = Mockito.mock(WorkloadGroup.class);
        Mockito.when(workloadGroup.getId()).thenReturn(7L);
        UserException failure = new UserException("workload group unavailable");
        Mockito.when(workloadGroups.getWorkloadGroup(Mockito.any())).thenAnswer(call -> {
            Assertions.assertSame(caller, ConnectContext.get());
            ConnectContext context = call.getArgument(0);
            requestContext.set(context);
            context.setStatementContext(statement);
            if (fail) {
                throw failure;
            }
            return Collections.singletonList(workloadGroup);
        });
        try {
            List<Long> groups = new ArrayList<>();
            WorkloadSchedPolicyMgr manager = new WorkloadSchedPolicyMgr();
            if (fail) {
                Assertions.assertSame(failure, Assertions.assertThrows(UserException.class,
                        () -> manager.checkProperties(
                                Collections.singletonMap(WorkloadSchedPolicy.WORKLOAD_GROUP, "group"), groups)));
            } else {
                manager.checkProperties(Collections.singletonMap(WorkloadSchedPolicy.WORKLOAD_GROUP, "group"), groups);
                Assertions.assertEquals(Collections.singletonList(7L), groups);
            }
            Assertions.assertTrue(requestContext.get().isClosingOrClosed());
            Mockito.verify(statement).close();
            Mockito.verify(env).unregisterSessionInfo(requestContext.get().getSessionId());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            caller.cleanup();
        }
    }
}
