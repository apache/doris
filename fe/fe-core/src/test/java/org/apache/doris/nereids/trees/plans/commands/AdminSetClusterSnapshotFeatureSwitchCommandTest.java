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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AdminSetClusterSnapshotFeatureSwitchCommandTest {
    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessControllerManager;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    private String originalMinPrivilege;

    @BeforeEach
    public void setUp() {
        originalMinPrivilege = Config.cluster_snapshot_min_privilege;
        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
    }

    @AfterEach
    public void tearDown() {
        Config.cluster_snapshot_min_privilege = originalMinPrivilege;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    private void runBefore() throws Exception {
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        // Mock root user for privilege check
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        Config.deploy_mode = "";
        AdminSetClusterSnapshotFeatureSwitchCommand command = new AdminSetClusterSnapshotFeatureSwitchCommand(true);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "The sql is illegal in disk mode");

        Config.deploy_mode = "cloud";
        // switch on
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        // switch off
        AdminSetClusterSnapshotFeatureSwitchCommand command1 = new AdminSetClusterSnapshotFeatureSwitchCommand(false);
        Assertions.assertDoesNotThrow(() -> command1.validate(connectContext));
    }

    @Test
    public void testValidateNoPrivilegeRootMode() {
        // Test root mode (default): admin user should be denied
        Config.cluster_snapshot_min_privilege = "root";

        UserIdentity nonRootUser = new UserIdentity("admin", "%");
        nonRootUser.setIsAnalyzed();

        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonRootUser);
        Config.deploy_mode = "cloud";

        AdminSetClusterSnapshotFeatureSwitchCommand command = new AdminSetClusterSnapshotFeatureSwitchCommand(true);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (root privilege) privilege(s) for this operation");
    }

    @Test
    public void testValidateAdminModeWithAdminUser() {
        // Test admin mode: admin user with ADMIN privilege should be allowed
        Config.cluster_snapshot_min_privilege = "admin";

        UserIdentity adminUser = new UserIdentity("admin", "%");
        adminUser.setIsAnalyzed();

        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN))
                .thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(adminUser);
        Config.deploy_mode = "cloud";

        AdminSetClusterSnapshotFeatureSwitchCommand command = new AdminSetClusterSnapshotFeatureSwitchCommand(true);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testValidateAdminModeWithNormalUser() {
        // Test admin mode: normal user without ADMIN privilege should be denied
        Config.cluster_snapshot_min_privilege = "admin";

        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();

        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN))
                .thenReturn(false);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);
        Config.deploy_mode = "cloud";

        AdminSetClusterSnapshotFeatureSwitchCommand command = new AdminSetClusterSnapshotFeatureSwitchCommand(true);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation");
    }
}
