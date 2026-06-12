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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AdminCopyTabletCommandTest extends TestWithFeService {
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Map<String, String> properties = new HashMap<>();
        properties.put("version", "0");
        AdminCopyTabletCommand command = new AdminCopyTabletCommand(100, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());

        Map<String, String> properties2 = new HashMap<>();
        properties2.put("backend_id", "10");
        AdminCopyTabletCommand command2 = new AdminCopyTabletCommand(100, properties2);
        Assertions.assertDoesNotThrow(() -> command2.validate());

        Map<String, String> properties3 = new HashMap<>();
        properties3.put("expiration_minutes", "10");
        AdminCopyTabletCommand command3 = new AdminCopyTabletCommand(100, properties3);
        Assertions.assertDoesNotThrow(() -> command3.validate());
    }

    @Test
    public void testNoPriviledge() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Map<String, String> properties = new HashMap<>();
        properties.put("version", "0");
        AdminCopyTabletCommand command = new AdminCopyTabletCommand(100, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(),
                "Access denied; you need (at least one of) the (Admin_priv) privilege(s) for this operation");
    }
}
