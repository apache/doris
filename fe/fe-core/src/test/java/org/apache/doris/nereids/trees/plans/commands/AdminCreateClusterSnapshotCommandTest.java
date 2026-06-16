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
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdminCreateClusterSnapshotCommandTest {
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

    @Test
    public void testValidateNormal() throws Exception {
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);

        Config.deploy_mode = "";
        AdminCreateClusterSnapshotCommand command = new AdminCreateClusterSnapshotCommand(new HashMap<>());
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "The sql is illegal in disk mode");

        Config.deploy_mode = "cloud";
        List<Pair<Map<String, String>, String>> properties = new ArrayList<>();
        // missing property
        properties.add(Pair.of(new HashMap<>(), "Property 'ttl' is required."));
        properties.add(Pair.of(ImmutableMap.of("label", "a"), "Property 'ttl' is required"));
        properties.add(Pair.of(ImmutableMap.of("ttl", "3600"), "Property 'label' is required."));
        // invalid value
        properties.add(Pair.of(ImmutableMap.of("ttl", "a", "label", "a"), "Invalid value"));
        properties.add(Pair.of(ImmutableMap.of("ttl", "0", "label", "a"), "Property 'ttl' must be positive"));
        properties.add(Pair.of(ImmutableMap.of("ttl", "3600", "label", ""), "Property 'label' cannot be empty"));
        // unknown property
        properties.add(Pair.of(ImmutableMap.of("ttl", "0", "a", "b"), "Unknown property"));
        // normal case
        properties.add(Pair.of(ImmutableMap.of("ttl", "3600", "label", "abc"), ""));

        for (Pair<Map<String, String>, String> entry : properties) {
            AdminCreateClusterSnapshotCommand command0 = new AdminCreateClusterSnapshotCommand(entry.first);
            String error = entry.second;
            if (error.isEmpty()) {
                command0.validate(connectContext);
            } else {
                Assertions.assertThrows(AnalysisException.class, () -> command0.validate(connectContext), error);
            }
        }
    }

    @Test
    public void testValidateNoPrivilegeRootMode() {
        // Test root mode (default): admin user should be denied
        Config.cluster_snapshot_min_privilege = "root";

        UserIdentity nonRootUser = new UserIdentity("admin", "%");
        nonRootUser.setIsAnalyzed();

        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonRootUser);

        Config.deploy_mode = "cloud";

        Map<String, String> properties = new HashMap<>();
        AdminCreateClusterSnapshotCommand command = new AdminCreateClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (root privilege) privilege(s) for this operation");
    }

    @Test
    public void testValidateAdminModeWithAdminUser() {
        // Test admin mode: admin user with ADMIN privilege should be allowed
        Config.cluster_snapshot_min_privilege = "admin";

        UserIdentity adminUser = new UserIdentity("admin", "%");
        adminUser.setIsAnalyzed();

        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(adminUser);

        Config.deploy_mode = "cloud";

        Map<String, String> properties = ImmutableMap.of("ttl", "3600", "label", "test");
        AdminCreateClusterSnapshotCommand command = new AdminCreateClusterSnapshotCommand(properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testValidateAdminModeWithNormalUser() {
        // Test admin mode: normal user without ADMIN privilege should be denied
        Config.cluster_snapshot_min_privilege = "admin";

        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();

        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(false);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);

        Config.deploy_mode = "cloud";

        Map<String, String> properties = new HashMap<>();
        AdminCreateClusterSnapshotCommand command = new AdminCreateClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation");
    }
}
