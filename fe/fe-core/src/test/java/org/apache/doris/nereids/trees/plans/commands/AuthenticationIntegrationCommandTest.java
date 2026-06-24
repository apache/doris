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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.AuthenticationIntegrationMgr;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class AuthenticationIntegrationCommandTest extends TestWithFeService {

    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    private static Map<String, String> map(String... kvs) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            result.put(kvs[i], kvs[i + 1]);
        }
        return result;
    }

    private static Set<String> set(String... keys) {
        Set<String> result = new LinkedHashSet<>();
        Collections.addAll(result, keys);
        return result;
    }

    @Test
    public void testCreateCommandRunAndDenied() throws Exception {
        runBefore();
        UserIdentity currentUser = UserIdentity.createAnalyzedUserIdentWithIp("admin", "%");
        connectContext.setCurrentUserIdentity(currentUser);

        CreateAuthenticationIntegrationCommand createCommand =
                new CreateAuthenticationIntegrationCommand("corp_ldap", false,
                        map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), "comment");

        // Grant access
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        // Mock AuthenticationIntegrationMgr
        AuthenticationIntegrationMgr mockAuthMgr = Mockito.mock(AuthenticationIntegrationMgr.class);
        Deencapsulation.setField(env, "authenticationIntegrationMgr", mockAuthMgr);

        Assertions.assertDoesNotThrow(() -> createCommand.run(connectContext, null));
        Assertions.assertEquals(StmtType.CREATE, createCommand.stmtType());
        Assertions.assertTrue(createCommand.needAuditEncryption());

        Mockito.verify(mockAuthMgr, Mockito.times(1)).createAuthenticationIntegration(
                Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyMap(),
                Mockito.anyString(), Mockito.anyString());

        // Deny access
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));

        Assertions.assertThrows(AnalysisException.class, () -> createCommand.run(connectContext, null));
    }

    @Test
    public void testAlterCommandRun() throws Exception {
        runBefore();
        UserIdentity currentUser = UserIdentity.createAnalyzedUserIdentWithIp("admin", "%");
        connectContext.setCurrentUserIdentity(currentUser);

        AlterAuthenticationIntegrationCommand setPropertiesCommand =
                AlterAuthenticationIntegrationCommand.forSetProperties(
                        "corp_ldap", map("ldap.server", "ldap://127.0.0.1:1389"));
        AlterAuthenticationIntegrationCommand unsetPropertiesCommand =
                AlterAuthenticationIntegrationCommand.forUnsetProperties(
                        "corp_ldap", set("ldap.server"));
        AlterAuthenticationIntegrationCommand setCommentCommand =
                AlterAuthenticationIntegrationCommand.forSetComment("corp_ldap", "new comment");

        // Grant access
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        // Mock AuthenticationIntegrationMgr
        AuthenticationIntegrationMgr mockAuthMgr = Mockito.mock(AuthenticationIntegrationMgr.class);
        Deencapsulation.setField(env, "authenticationIntegrationMgr", mockAuthMgr);

        Assertions.assertDoesNotThrow(() -> setPropertiesCommand.doRun(connectContext, null));
        Assertions.assertDoesNotThrow(() -> unsetPropertiesCommand.doRun(connectContext, null));
        Assertions.assertDoesNotThrow(() -> setCommentCommand.doRun(connectContext, null));
        Assertions.assertTrue(setPropertiesCommand.needAuditEncryption());
        Assertions.assertTrue(unsetPropertiesCommand.needAuditEncryption());
        Assertions.assertTrue(setCommentCommand.needAuditEncryption());

        Mockito.verify(mockAuthMgr, Mockito.times(1)).alterAuthenticationIntegrationProperties(
                Mockito.anyString(), Mockito.anyMap(), Mockito.anyString());
        Mockito.verify(mockAuthMgr, Mockito.times(1)).alterAuthenticationIntegrationUnsetProperties(
                Mockito.anyString(), Mockito.anySet(), Mockito.anyString());
        Mockito.verify(mockAuthMgr, Mockito.times(1)).alterAuthenticationIntegrationComment(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testAlterCommandDenied() throws Exception {
        runBefore();
        AlterAuthenticationIntegrationCommand setPropertiesCommand =
                AlterAuthenticationIntegrationCommand.forSetProperties(
                        "corp_ldap", map("ldap.server", "ldap://127.0.0.1:1389"));

        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Assertions.assertThrows(AnalysisException.class, () -> setPropertiesCommand.doRun(connectContext, null));
    }

    @Test
    public void testDropCommandRunAndDenied() throws Exception {
        runBefore();
        DropAuthenticationIntegrationCommand dropCommand =
                new DropAuthenticationIntegrationCommand(true, "corp_ldap");

        // Grant access
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        // Mock AuthenticationIntegrationMgr
        AuthenticationIntegrationMgr mockAuthMgr = Mockito.mock(AuthenticationIntegrationMgr.class);
        Deencapsulation.setField(env, "authenticationIntegrationMgr", mockAuthMgr);

        Assertions.assertDoesNotThrow(() -> dropCommand.doRun(connectContext, null));

        Mockito.verify(mockAuthMgr, Mockito.times(1)).dropAuthenticationIntegration(
                Mockito.anyString(), Mockito.anyBoolean());

        // Deny access
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));

        Assertions.assertThrows(AnalysisException.class, () -> dropCommand.doRun(connectContext, null));
    }
}
