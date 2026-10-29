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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

class BackendDistributedPlanWorkerManagerCloudTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cloudSqlAndLoadUseOnlyTheAuthorizedComputeGroup(boolean loadJob) throws Exception {
        String oldCloudId = Config.cloud_unique_id;
        ConnectContext previous = ConnectContext.get();
        Config.cloud_unique_id = "test_cloud";
        CloudEnv env = Mockito.mock(CloudEnv.class);
        Mockito.when(env.getAuth()).thenReturn(Mockito.mock(Auth.class));
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(InternalCatalog.INTERNAL_CATALOG_ID);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(context.getCloudCluster()).thenReturn("authorized_group");
        ComputeGroup group = Mockito.mock(ComputeGroup.class);
        Mockito.when(group.getName()).thenReturn("authorized_group");
        Mockito.when(context.getComputeGroup()).thenReturn(group);
        Backend backend = new Backend(10001L, "127.0.0.1", 9050);
        backend.setBePort(9060);
        backend.setAlive(true);
        Mockito.when(group.getBackendList()).thenReturn(List.of(backend));
        // Load planning reads the job's thread context; SQL planning uses its supplied context.
        Mockito.doCallRealMethod().when(context).setThreadLocalInfo();
        context.setThreadLocalInfo();
        try (MockedStatic<Env> envAccess = Mockito.mockStatic(Env.class)) {
            envAccess.when(Env::getCurrentEnv).thenReturn(env);
            envAccess.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            BackendDistributedPlanWorkerManager manager = new BackendDistributedPlanWorkerManager(
                    context, false, loadJob);
            Assertions.assertEquals(10001L,
                    manager.randomAvailableWorker(InternalCatalog.INTERNAL_CATALOG_ID).id());
            Mockito.verify(env).checkCloudClusterPriv("authorized_group");

            Mockito.doThrow(new DdlException("compute group access denied"))
                    .when(env).checkCloudClusterPriv("authorized_group");
            Assertions.assertThrows(UserException.class,
                    () -> new BackendDistributedPlanWorkerManager(context, false, loadJob));

            Mockito.doNothing().when(env).checkCloudClusterPriv("authorized_group");
            Mockito.when(group.getBackendList()).thenReturn(List.of());
            Assertions.assertThrows(UserException.class,
                    () -> new BackendDistributedPlanWorkerManager(context, false, loadJob));
        } finally {
            Config.cloud_unique_id = oldCloudId;
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
