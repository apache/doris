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
import org.apache.doris.authentication.AuthenticationIntegrationMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class AuthenticationIntegrationCommandTest {

    @Mocked
    private Env env;

    @Mocked
    private AccessControllerManager accessManager;

    @Mocked
    private AuthenticationIntegrationMgr authenticationIntegrationMgr;

    @Mocked
    private ConnectContext connectContext;

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
        CreateAuthenticationIntegrationCommand createCommand =
                new CreateAuthenticationIntegrationCommand("corp_ldap", false,
                        map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), "comment");

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;

                env.getAuthenticationIntegrationMgr();
                minTimes = 0;
                result = authenticationIntegrationMgr;

                authenticationIntegrationMgr.createAuthenticationIntegration(
                        anyString, anyBoolean, (Map<String, String>) any, anyString);
                times = 1;
            }
        };

        Assertions.assertDoesNotThrow(() -> createCommand.run(connectContext, null));
        Assertions.assertEquals(StmtType.CREATE, createCommand.stmtType());
        Assertions.assertTrue(createCommand.needAuditEncryption());

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = false;
            }
        };

        Assertions.assertThrows(AnalysisException.class, () -> createCommand.run(connectContext, null));
    }

    @Test
    public void testAlterCommandRun() throws Exception {
        AlterAuthenticationIntegrationCommand setPropertiesCommand =
                AlterAuthenticationIntegrationCommand.forSetProperties(
                        "corp_ldap", map("ldap.server", "ldap://127.0.0.1:1389"));
        AlterAuthenticationIntegrationCommand unsetPropertiesCommand =
                AlterAuthenticationIntegrationCommand.forUnsetProperties(
                        "corp_ldap", set("ldap.server"));
        AlterAuthenticationIntegrationCommand setCommentCommand =
                AlterAuthenticationIntegrationCommand.forSetComment("corp_ldap", "new comment");

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                env.getAuthenticationIntegrationMgr();
                minTimes = 0;
                result = authenticationIntegrationMgr;

                authenticationIntegrationMgr.alterAuthenticationIntegrationProperties(
                        anyString, (Map<String, String>) any);
                times = 1;

                authenticationIntegrationMgr.alterAuthenticationIntegrationUnsetProperties(
                        anyString, (Set<String>) any);
                times = 1;

                authenticationIntegrationMgr.alterAuthenticationIntegrationComment(anyString, anyString);
                times = 1;
            }
        };

        Assertions.assertDoesNotThrow(() -> setPropertiesCommand.doRun(connectContext, null));
        Assertions.assertDoesNotThrow(() -> unsetPropertiesCommand.doRun(connectContext, null));
        Assertions.assertDoesNotThrow(() -> setCommentCommand.doRun(connectContext, null));
        Assertions.assertTrue(setPropertiesCommand.needAuditEncryption());
        Assertions.assertTrue(unsetPropertiesCommand.needAuditEncryption());
        Assertions.assertTrue(setCommentCommand.needAuditEncryption());
    }

    @Test
    public void testAlterCommandDenied() {
        AlterAuthenticationIntegrationCommand setPropertiesCommand =
                AlterAuthenticationIntegrationCommand.forSetProperties(
                        "corp_ldap", map("ldap.server", "ldap://127.0.0.1:1389"));

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = false;
            }
        };

        Assertions.assertThrows(AnalysisException.class, () -> setPropertiesCommand.doRun(connectContext, null));
    }

    @Test
    public void testDropCommandRunAndDenied() throws Exception {
        DropAuthenticationIntegrationCommand dropCommand =
                new DropAuthenticationIntegrationCommand(true, "corp_ldap");

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;

                env.getAuthenticationIntegrationMgr();
                minTimes = 0;
                result = authenticationIntegrationMgr;

                authenticationIntegrationMgr.dropAuthenticationIntegration(anyString, anyBoolean);
                times = 1;
            }
        };

        Assertions.assertDoesNotThrow(() -> dropCommand.doRun(connectContext, null));

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = false;
            }
        };

        Assertions.assertThrows(AnalysisException.class, () -> dropCommand.doRun(connectContext, null));
    }
}
