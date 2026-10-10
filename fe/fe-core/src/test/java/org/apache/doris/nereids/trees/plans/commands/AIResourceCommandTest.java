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
import org.apache.doris.catalog.AIResource;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class AIResourceCommandTest {
    private static final String RESOURCE_NAME = "root_ai_resource";

    private ConnectContext connectContext;
    private AIResource aiResource;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> connectContextMockedStatic;

    @BeforeEach
    public void setUp() {
        Env env = Mockito.mock(Env.class);
        ResourceMgr resourceMgr = Mockito.mock(ResourceMgr.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        connectContext = Mockito.mock(ConnectContext.class);
        aiResource = Mockito.mock(AIResource.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        connectContextMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        connectContextMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getResourceMgr()).thenReturn(resourceMgr);
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(accessManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(resourceMgr.getResource(RESOURCE_NAME)).thenReturn(aiResource);
    }

    @AfterEach
    public void tearDown() {
        connectContextMockedStatic.close();
        envMockedStatic.close();
    }

    @Test
    public void testAdminCannotAlterOrDropRootCreatedAIResource() {
        Mockito.when(aiResource.isCreatedByRoot()).thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ADMIN);

        AlterResourceCommand alterCommand = new AlterResourceCommand(
                RESOURCE_NAME, Collections.singletonMap("ai.temperature", "0.8"));
        AnalysisException alterException = Assertions.assertThrows(
                AnalysisException.class, () -> alterCommand.doRun(connectContext, null));
        Assertions.assertTrue(alterException.getMessage()
                .contains("Only root user can modify root-created AI resource"));

        DropResourceCommand dropCommand = new DropResourceCommand(false, RESOURCE_NAME);
        AnalysisException dropException = Assertions.assertThrows(
                AnalysisException.class, () -> dropCommand.doRun(connectContext, null));
        Assertions.assertTrue(dropException.getMessage()
                .contains("Only root user can modify root-created AI resource"));
    }

    @Test
    public void testRootCanAlterAndDropRootCreatedAIResource() {
        Mockito.when(aiResource.isCreatedByRoot()).thenReturn(true);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);

        AlterResourceCommand alterCommand = new AlterResourceCommand(
                RESOURCE_NAME, Collections.singletonMap("ai.temperature", "0.8"));
        Assertions.assertDoesNotThrow(() -> alterCommand.doRun(connectContext, null));

        DropResourceCommand dropCommand = new DropResourceCommand(false, RESOURCE_NAME);
        Assertions.assertDoesNotThrow(() -> dropCommand.doRun(connectContext, null));
    }

    @Test
    public void testAdminCanAlterAndDropLegacyAIResource() {
        Mockito.when(aiResource.isCreatedByRoot()).thenReturn(false);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ADMIN);

        AlterResourceCommand alterCommand = new AlterResourceCommand(
                RESOURCE_NAME, Collections.singletonMap("ai.temperature", "0.8"));
        Assertions.assertDoesNotThrow(() -> alterCommand.doRun(connectContext, null));

        DropResourceCommand dropCommand = new DropResourceCommand(false, RESOURCE_NAME);
        Assertions.assertDoesNotThrow(() -> dropCommand.doRun(connectContext, null));
    }
}
