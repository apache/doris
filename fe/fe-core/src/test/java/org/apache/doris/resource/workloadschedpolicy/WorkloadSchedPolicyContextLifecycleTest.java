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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
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
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    void computeGroupLookupFailureDoesNotCreateSession() throws Exception {
        UserException failure = new UserException("missing compute group");
        Mockito.when(computeGroups.getComputeGroupByName(Mockito.anyString())).thenThrow(failure);
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            Assertions.assertSame(failure, Assertions.assertThrows(UserException.class,
                    () -> new WorkloadSchedPolicyMgr().checkProperties(
                            Collections.singletonMap(WorkloadSchedPolicy.WORKLOAD_GROUP, "group"), new ArrayList<>())));
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Mockito.verifyNoInteractions(workloadGroups);
            Assertions.assertNull(ConnectContext.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void workloadGroupLookupUsesRootAndPreservesCaller(boolean fail) throws Exception {
        ComputeGroup computeGroup = Mockito.mock(ComputeGroup.class);
        Mockito.when(computeGroups.getComputeGroupByName(Mockito.anyString()))
                .thenReturn(computeGroup);
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        WorkloadGroup workloadGroup = Mockito.mock(WorkloadGroup.class);
        Mockito.when(workloadGroup.getId()).thenReturn(7L);
        UserException failure = new UserException("workload group unavailable");
        Mockito.when(workloadGroups.getWorkloadGroup(UserIdentity.ROOT, "group", computeGroup)).thenAnswer(call -> {
            Assertions.assertSame(caller, ConnectContext.get());
            if (fail) {
                throw failure;
            }
            return Collections.singletonList(workloadGroup);
        });
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
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
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Mockito.verify(workloadGroups).getWorkloadGroup(UserIdentity.ROOT, "group", computeGroup);
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            caller.cleanup();
        }
    }
}
