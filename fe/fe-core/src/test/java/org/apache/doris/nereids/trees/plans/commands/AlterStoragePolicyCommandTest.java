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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AlterStoragePolicyCommandTest {

    private static final String policyName = "test_policy";

    private Env env;
    private ConnectContext ctx;
    private AccessControllerManager accessManager;
    private PolicyMgr policyMgr;
    private StoragePolicy mockPolicy;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        ctx = Mockito.mock(ConnectContext.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        policyMgr = Mockito.mock(PolicyMgr.class);
        mockPolicy = Mockito.mock(StoragePolicy.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getPolicyMgr()).thenReturn(policyMgr);
        Mockito.when(accessManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        Mockito.when(ctx.getState()).thenReturn(new QueryState());
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    public void testValidate_success() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("cooldown_ttl", "86400");

        Mockito.when(policyMgr.findPolicy(policyName, PolicyTypeEnum.ROW)).thenReturn(Optional.empty());
        Mockito.when(policyMgr.getCopiedPoliciesByType(PolicyTypeEnum.STORAGE))
                .thenReturn(Collections.singletonList(mockPolicy));
        Mockito.when(mockPolicy.getPolicyName()).thenReturn(policyName);

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, props);
        Assertions.assertDoesNotThrow(() -> command.doRun(ctx, null));
    }

    @Test
    public void testValidate_unknownPolicy() {
        Map<String, String> props = new HashMap<>();
        props.put("some_key", "value");

        Mockito.when(policyMgr.findPolicy(policyName, PolicyTypeEnum.ROW)).thenReturn(Optional.empty());
        Mockito.when(policyMgr.getCopiedPoliciesByType(PolicyTypeEnum.STORAGE))
                .thenReturn(new ArrayList<>());

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, props);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }

    @Test
    public void testValidate_nullProps() {
        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, null);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }

    @Test
    public void testValidate_rejectStorageResourceChange() {
        Map<String, String> props = new HashMap<>();
        props.put("storage_resource", "s3_resource");

        Mockito.when(policyMgr.findPolicy(policyName, PolicyTypeEnum.ROW)).thenReturn(Optional.empty());
        Mockito.when(policyMgr.getCopiedPoliciesByType(PolicyTypeEnum.STORAGE))
                .thenReturn(Collections.singletonList(mockPolicy));
        Mockito.when(mockPolicy.getPolicyName()).thenReturn(policyName);

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, props);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }
}
