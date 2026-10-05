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

package org.apache.doris.authentication;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleMappingCommand;
import org.apache.doris.persist.DropRoleMappingOperationLog;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

class RoleMappingMgrTest {
    private static final String CREATE_USER = "creator";

    private Env env;
    private EditLog editLog;
    private Auth auth;
    private AuthenticationIntegrationMgr authenticationIntegrationMgr;
    private MockedStatic<Env> envMockedStatic;

    @BeforeEach
    void setUp() {
        env = Mockito.mock(Env.class);
        editLog = Mockito.mock(EditLog.class);
        auth = Mockito.mock(Auth.class);
        authenticationIntegrationMgr = Mockito.mock(AuthenticationIntegrationMgr.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getAuthenticationIntegrationMgr()).thenReturn(authenticationIntegrationMgr);
    }

    @AfterEach
    void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    @Test
    void testCreateDropAndLookupFlow() throws Exception {
        RoleMappingMgr mgr = new RoleMappingMgr();
        Mockito.when(authenticationIntegrationMgr.getAuthenticationIntegration(Mockito.anyString()))
                .thenAnswer(invocation -> integrationMetaUnchecked(invocation.getArgument(0)));
        Mockito.when(auth.doesRoleExist(Mockito.anyString())).thenReturn(true);

        mgr.createRoleMapping("corp_mapping", false, "corp_oidc", Arrays.asList(
                rule("true", "analyst", "auditor"),
                rule("true", "reports_reader")), "oidc", CREATE_USER);

        RoleMappingMeta created = mgr.getRoleMapping("corp_mapping");
        Assertions.assertNotNull(created);
        Assertions.assertEquals("corp_oidc", created.getIntegrationName());
        Assertions.assertEquals("oidc", created.getComment());
        Assertions.assertEquals(CREATE_USER, created.getCreateUser());
        Assertions.assertTrue(mgr.hasRoleMapping("corp_oidc"));
        Assertions.assertEquals("corp_mapping", mgr.getRoleMappingByIntegration("corp_oidc").getName());

        ArgumentCaptor<RoleMappingMeta> createCaptor = ArgumentCaptor.forClass(RoleMappingMeta.class);
        Mockito.verify(editLog).logCreateRoleMapping(createCaptor.capture());
        Assertions.assertEquals("corp_mapping", createCaptor.getValue().getName());
        Assertions.assertEquals(2, createCaptor.getValue().getRules().size());

        mgr.dropRoleMapping("corp_mapping", false);
        Assertions.assertNull(mgr.getRoleMapping("corp_mapping"));
        Assertions.assertFalse(mgr.hasRoleMapping("corp_oidc"));
        Mockito.verify(editLog).logDropRoleMapping(Mockito.any(DropRoleMappingOperationLog.class));
    }

    @Test
    void testCreateRejectsDuplicateAndIfNotExistsIsIdempotent() throws Exception {
        RoleMappingMgr mgr = new RoleMappingMgr();
        Mockito.when(authenticationIntegrationMgr.getAuthenticationIntegration(Mockito.anyString()))
                .thenAnswer(invocation -> integrationMetaUnchecked(invocation.getArgument(0)));
        Mockito.when(auth.doesRoleExist(Mockito.anyString())).thenReturn(true);

        mgr.createRoleMapping("corp_mapping", false, "corp_oidc", Arrays.asList(rule("true", "analyst")),
                null, CREATE_USER);

        Assertions.assertThrows(DdlException.class,
                () -> mgr.createRoleMapping("corp_mapping", false, "corp_oidc",
                        Arrays.asList(rule("true", "analyst")), null, CREATE_USER));
        Assertions.assertDoesNotThrow(
                () -> mgr.createRoleMapping("corp_mapping", true, "corp_oidc",
                        Arrays.asList(rule("true", "analyst")), null, CREATE_USER));

        Mockito.verify(editLog, Mockito.times(1)).logCreateRoleMapping(Mockito.any(RoleMappingMeta.class));
    }

    @Test
    void testCreateRejectsMissingIntegrationSecondBindingMissingRoleAndInvalidCondition() throws Exception {
        RoleMappingMgr mgr = new RoleMappingMgr();
        Mockito.when(authenticationIntegrationMgr.getAuthenticationIntegration(Mockito.anyString()))
                .thenAnswer(invocation -> {
                    String name = invocation.getArgument(0);
                    if ("missing_oidc".equals(name)) {
                        return null;
                    }
                    return integrationMetaUnchecked(name);
                });
        Mockito.when(auth.doesRoleExist("analyst")).thenReturn(true);
        Mockito.when(auth.doesRoleExist("ghost")).thenReturn(false);

        Assertions.assertThrows(DdlException.class,
                () -> mgr.createRoleMapping("missing_integration", false, "missing_oidc",
                        Arrays.asList(rule("true", "analyst")), null, CREATE_USER));

        mgr.createRoleMapping("corp_mapping", false, "corp_oidc", Arrays.asList(rule("true", "analyst")),
                null, CREATE_USER);

        Assertions.assertThrows(DdlException.class,
                () -> mgr.createRoleMapping("second_mapping", false, "corp_oidc",
                        Arrays.asList(rule("true", "analyst")), null, CREATE_USER));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.createRoleMapping("missing_role", false, "corp_oidc_2",
                        Arrays.asList(rule("true", "ghost")), null, CREATE_USER));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.createRoleMapping("bad_condition", false, "corp_oidc_3",
                        Arrays.asList(rule("unknown_helper()", "analyst")), null, CREATE_USER));
    }

    @Test
    void testReplayAndWriteReadRoundTrip() throws IOException {
        RoleMappingMgr mgr = new RoleMappingMgr();
        RoleMappingMeta meta = new RoleMappingMeta(
                "corp_mapping",
                "corp_oidc",
                Arrays.asList(new RoleMappingMeta.RuleMeta("true", set("analyst", "auditor"))),
                "comment",
                CREATE_USER,
                1L,
                CREATE_USER,
                1L);

        mgr.replayCreateRoleMapping(meta);
        Assertions.assertEquals("corp_mapping", mgr.getRoleMappingByIntegration("corp_oidc").getName());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            mgr.write(dos);
        }

        RoleMappingMgr read;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            read = RoleMappingMgr.read(dis);
        }

        Assertions.assertNotNull(read.getRoleMapping("corp_mapping"));
        Assertions.assertEquals("corp_mapping", read.getRoleMappingByIntegration("corp_oidc").getName());

        read.replayDropRoleMapping(new DropRoleMappingOperationLog("corp_mapping"));
        Assertions.assertNull(read.getRoleMapping("corp_mapping"));
        Assertions.assertFalse(read.hasRoleMapping("corp_oidc"));
    }

    @Test
    void testReplayDoesNotRunCreateTimeValidation() {
        RoleMappingMgr mgr = new RoleMappingMgr();
        RoleMappingMeta meta = new RoleMappingMeta(
                "broken_mapping",
                "missing_integration",
                Arrays.asList(new RoleMappingMeta.RuleMeta("unknown_helper()", set("ghost"))),
                null,
                CREATE_USER,
                1L,
                CREATE_USER,
                1L);

        mgr.replayCreateRoleMapping(meta);

        Assertions.assertEquals("broken_mapping", mgr.getRoleMapping("broken_mapping").getName());
        Assertions.assertEquals("broken_mapping",
                mgr.getRoleMappingByIntegration("missing_integration").getName());
        Mockito.verifyNoInteractions(authenticationIntegrationMgr);
        Mockito.verifyNoInteractions(auth);
        Mockito.verifyNoInteractions(editLog);
    }

    private static AuthenticationIntegrationMeta integrationMetaUnchecked(String name) {
        try {
            return integrationMeta(name);
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private static AuthenticationIntegrationMeta integrationMeta(String name) throws DdlException {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "oidc");
        properties.put("issuer", "issuer-" + name);
        return AuthenticationIntegrationMeta.fromCreateSql(name, properties, null, CREATE_USER);
    }

    private static CreateRoleMappingCommand.RoleMappingRule rule(String condition, String... roles) {
        return new CreateRoleMappingCommand.RoleMappingRule(condition, set(roles));
    }

    private static Set<String> set(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
