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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.AccessTranslation;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

class WorkloadGroupResolutionTest {
    private final UserIdentity owner = UserIdentity.createAnalyzedUserIdentWithIp("owner", "%");
    private final WorkloadGroupMgr groups = new WorkloadGroupMgr();
    private final Env env = Mockito.mock(Env.class);
    private final Auth auth = Mockito.mock(Auth.class);
    private final AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
    private final ComputeGroupMgr computeGroups = Mockito.mock(ComputeGroupMgr.class);
    private final ComputeGroup computeGroup = Mockito.mock(ComputeGroup.class);
    private final CloudSystemInfoService cloud = Mockito.mock(CloudSystemInfoService.class);
    private final List<WorkloadGroup> result = Collections.singletonList(Mockito.mock(WorkloadGroup.class));
    private MockedStatic<Env> environment;
    private MockedStatic<Config> config;

    @BeforeEach
    void setUp() throws Exception {
        ConnectContext.remove();
        environment = Mockito.mockStatic(Env.class);
        environment.when(Env::getCurrentEnv).thenReturn(env);
        environment.when(Env::getCurrentSystemInfo).thenReturn(cloud);
        config = Mockito.mockStatic(Config.class);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroups);
        Mockito.when(env.getWorkloadGroupMgr()).thenReturn(groups);
        Mockito.when(auth.getComputeGroup(owner.getQualifiedUser())).thenReturn(computeGroup);
        Mockito.when(computeGroups.getComputeGroupByName(Mockito.anyString())).thenReturn(computeGroup);
        Mockito.when(computeGroup.getWorkloadGroup(Mockito.anyString(), Mockito.same(groups))).thenReturn(result);
        Mockito.when(access.decide(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.same(AccessContext.NONE)))
                .thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        config.close();
        environment.close();
    }

    @Test
    void explicitGroupUsesOwnerAndNoClientAccessContext() throws Exception {
        ConnectContext caller = new ConnectContext();
        caller.setCurrentUserIdentity(UserIdentity.ADMIN);
        caller.setRemoteIP("10.1.2.3");
        caller.setQueryId(new TUniqueId(1, 2));
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(caller);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            Assertions.assertSame(result, groups.getWorkloadGroup(owner, "job_wg", computeGroup));
            Mockito.verify(access).decide(owner, AuthorizedResource.workloadGroup("job_wg"),
                    AccessTranslation.requirementOf(PrivPredicate.USAGE), AccessContext.NONE);
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
            Assertions.assertEquals("10.1.2.3", caller.getRemoteIP());
            Assertions.assertEquals(new TUniqueId(1, 2), caller.queryId());
        }
    }

    @Test
    void evenRootMustPassWorkloadGroupAuthorizationBeforeLookup() throws Exception {
        Mockito.when(access.decide(Mockito.eq(UserIdentity.ROOT), Mockito.any(), Mockito.any(),
                Mockito.same(AccessContext.NONE))).thenReturn(false);
        AnalysisException failure = Assertions.assertThrows(AnalysisException.class,
                () -> groups.getWorkloadGroup(UserIdentity.ROOT, "job_wg", computeGroup));
        Assertions.assertTrue(failure.getMessage().contains("USAGE/ADMIN"));
        Assertions.assertTrue(failure.getMessage().contains("job_wg"));
        Mockito.verifyNoInteractions(computeGroup);
    }

    @Test
    void nonCloudDefaultsUseOwnerPropertiesAndIgnoreCloudCluster() throws Exception {
        Mockito.when(auth.getWorkloadGroup(owner.getQualifiedUser())).thenReturn("owner_default");
        groups.getWorkloadGroup(owner, "", "ignored_cloud_cluster");
        Mockito.verify(computeGroup).getWorkloadGroup("owner_default", groups);
        Mockito.when(auth.getWorkloadGroup(owner.getQualifiedUser())).thenReturn("");
        groups.getWorkloadGroup(owner, null, "ignored_cloud_cluster");
        Mockito.verify(computeGroup).getWorkloadGroup("normal", groups);
        Mockito.verifyNoInteractions(computeGroups);

        Mockito.when(auth.getComputeGroup(owner.getQualifiedUser())).thenReturn(ComputeGroup.INVALID_COMPUTE_GROUP);
        UserException failure = Assertions.assertThrows(UserException.class,
                () -> groups.getWorkloadGroup(owner, "missing", "ignored_cloud_cluster"));
        Assertions.assertTrue(failure.getMessage().contains(ComputeGroup.INVALID_COMPUTE_GROUP_NAME));
        Mockito.verify(computeGroups, Mockito.never()).getAllBackendComputeGroup();
    }

    @Test
    void routineLoadValidationUsesExplicitCloudNamespaceWithoutSession() throws Exception {
        config.when(Config::isCloudMode).thenReturn(true);
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            CreateRoutineLoadInfo.checkWorkloadGroup(owner, "job_wg", "declared");
            Mockito.verify(computeGroups).getComputeGroupByName("declared");
            Mockito.verify(computeGroup).getWorkloadGroup("job_wg", groups);
            Mockito.verifyNoInteractions(cloud);
            Assertions.assertTrue(contexts.constructed().isEmpty());
        }
        Mockito.when(computeGroup.getWorkloadGroup("job_wg", groups)).thenReturn(Collections.emptyList());
        Assertions.assertEquals("Can not find workload group job_wg", Assertions.assertThrows(UserException.class,
                () -> CreateRoutineLoadInfo.checkWorkloadGroup(owner, "job_wg", "declared")).getDetailMessage());
    }

    @Test
    void cloudDefaultAndMissingDefaultKeepTheirLookupBehavior() throws Exception {
        config.when(Config::isCloudMode).thenReturn(true);
        Mockito.when(cloud.getCloudClusterNames()).thenReturn(Collections.singletonList("owner_default"));
        Mockito.when(auth.getDefaultCloudCluster(owner.getQualifiedUser())).thenReturn("owner_default");
        groups.getWorkloadGroup(owner, "job_wg", "");
        Mockito.verify(computeGroups).getComputeGroupByName("owner_default");
        Mockito.when(auth.getDefaultCloudCluster(owner.getQualifiedUser())).thenReturn("deleted");
        UserException missing = new UserException("missing compute group deleted");
        Mockito.when(computeGroups.getComputeGroupByName("deleted")).thenThrow(missing);
        Assertions.assertSame(missing, Assertions.assertThrows(UserException.class,
                () -> groups.getWorkloadGroup(owner, "job_wg", "")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cloudPolicyKeepsItsCallerAccessContextAndNoPermissionError(boolean allowed) throws Exception {
        config.when(Config::isCloudMode).thenReturn(true);
        Mockito.when(cloud.getCloudClusterNames()).thenReturn(Collections.singletonList("available"));
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isAlive()).thenReturn(true);
        Mockito.when(cloud.getBackendsByClusterName("available")).thenReturn(Collections.singletonList(backend));
        ConnectContext caller = new ConnectContext();
        caller.setCurrentUserIdentity(UserIdentity.ADMIN);
        Mockito.when(access.checkCloudPriv(owner, "available", PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER))
                .thenAnswer(invocation -> {
                    Assertions.assertSame(caller, ConnectContext.get());
                    return allowed;
                });
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(caller);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            if (allowed) {
                Assertions.assertSame(result, groups.getWorkloadGroup(owner, "job_wg", ""));
                Mockito.verify(computeGroups).getComputeGroupByName("available");
            } else {
                ComputeGroupException failure = Assertions.assertThrows(ComputeGroupException.class,
                        () -> groups.getWorkloadGroup(owner, "job_wg", ""));
                Assertions.assertEquals("the user is not granted permission to the compute group",
                        failure.getDetailMessage());
                Assertions.assertEquals(
                        ComputeGroupException.FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP,
                        failure.getFailedType());
                Mockito.verifyNoInteractions(computeGroups);
            }
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertSame(caller, ConnectContext.get());
            Mockito.verify(access).decide(owner, AuthorizedResource.workloadGroup("job_wg"),
                    AccessTranslation.requirementOf(PrivPredicate.USAGE), AccessContext.NONE);
        }
    }
}
