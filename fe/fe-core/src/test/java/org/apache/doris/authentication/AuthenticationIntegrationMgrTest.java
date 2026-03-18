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
import org.apache.doris.persist.DropAuthenticationIntegrationOperationLog;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class AuthenticationIntegrationMgrTest {
    private static final String CREATE_USER = "creator";
    private static final String ALTER_USER = "modifier";
    private Env env;
    private EditLog editLog;
    private AuthenticationIntegrationRuntime runtime;
    private AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration prepared;
    private MockedStatic<Env> envMockedStatic;

    @BeforeEach
    void setUp() {
        env = Mockito.mock(Env.class);
        editLog = Mockito.mock(EditLog.class);
        runtime = Mockito.mock(AuthenticationIntegrationRuntime.class);
        prepared = Mockito.mock(AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getAuthenticationIntegrationRuntime()).thenReturn(runtime);
    }

    @AfterEach
    void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    @Test
    void testCreateAlterDropFlow() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        Map<String, String> createProperties = new LinkedHashMap<>();
        createProperties.put("type", "ldap");
        createProperties.put("ldap.server", "ldap://127.0.0.1:389");
        createProperties.put("ldap.admin_password", "123456");

        mgr.createAuthenticationIntegration("corp_ldap", false, createProperties, "comment", CREATE_USER);
        AuthenticationIntegrationMeta created = mgr.getAuthenticationIntegrations().get("corp_ldap");
        Assertions.assertNotNull(created);
        Assertions.assertEquals("ldap", created.getType());
        Assertions.assertEquals(CREATE_USER, created.getCreateUser());
        Assertions.assertEquals(CREATE_USER, created.getAlterUser());
        Assertions.assertEquals("ldap://127.0.0.1:389", created.getProperties().get("ldap.server"));

        mgr.alterAuthenticationIntegrationProperties(
                "corp_ldap", map("ldap.server", "ldap://127.0.0.1:1389"), ALTER_USER);
        Assertions.assertEquals(ALTER_USER,
                mgr.getAuthenticationIntegrations().get("corp_ldap").getAlterUser());
        Assertions.assertEquals("ldap://127.0.0.1:1389",
                mgr.getAuthenticationIntegrations().get("corp_ldap").getProperties().get("ldap.server"));

        mgr.alterAuthenticationIntegrationUnsetProperties("corp_ldap", set("ldap.admin_password"), ALTER_USER);
        Assertions.assertFalse(mgr.getAuthenticationIntegrations()
                .get("corp_ldap").getProperties().containsKey("ldap.admin_password"));

        mgr.alterAuthenticationIntegrationComment("corp_ldap", "new comment", ALTER_USER);
        Assertions.assertEquals("new comment", mgr.getAuthenticationIntegrations().get("corp_ldap").getComment());

        mgr.dropAuthenticationIntegration("corp_ldap", false);
        Assertions.assertTrue(mgr.getAuthenticationIntegrations().isEmpty());

        Mockito.verify(runtime, Mockito.times(2)).markAuthenticationIntegrationDirty("corp_ldap");
        Mockito.verify(runtime).removeAuthenticationIntegration("corp_ldap");
        Mockito.verifyNoInteractions(editLog);
    }

    @Test
    void testCreateFailureWhenPrepareFails() throws Exception {
        Mockito.when(runtime.prepareAuthenticationIntegration(Mockito.any()))
                .thenThrow(new AuthenticationException("prepare failed"));

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                mgr.createAuthenticationIntegration("corp_ldap", false,
                        map("type", "ldap", "plugin.initialize_immediately", "true"), null, CREATE_USER));

        Assertions.assertEquals("prepare failed", exception.getDetailMessage());
        Assertions.assertTrue(mgr.getAuthenticationIntegrations().isEmpty());
        Mockito.verify(runtime, Mockito.never()).activatePreparedAuthenticationIntegration(prepared);
        Mockito.verifyNoInteractions(editLog);
    }

    @Test
    void testCreateDuplicateAndDropIfExists() throws Exception {
        Mockito.when(runtime.prepareAuthenticationIntegration(Mockito.any())).thenReturn(prepared);

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.createAuthenticationIntegration("corp_ldap", false, map(
                "type", "ldap",
                "ldap.server", "ldap://127.0.0.1:389"), null, CREATE_USER);

        Assertions.assertThrows(DdlException.class,
                () -> mgr.createAuthenticationIntegration("corp_ldap", false, map("type", "ldap"), null, CREATE_USER));
        Assertions.assertDoesNotThrow(
                () -> mgr.createAuthenticationIntegration("corp_ldap", true, map("type", "ldap"), null, CREATE_USER));

        Assertions.assertDoesNotThrow(() -> mgr.dropAuthenticationIntegration("not_exist", true));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.dropAuthenticationIntegration("not_exist", false));
    }

    @Test
    void testCreatePreparesOutsideWriteLockAndActivatesInsideWriteLock() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        ReentrantReadWriteLock lock = getLock(mgr);
        Mockito.doAnswer(invocation -> {
            Assertions.assertFalse(lock.isWriteLockedByCurrentThread());
            return prepared;
        }).when(runtime).prepareAuthenticationIntegration(Mockito.any());
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(lock.isWriteLockedByCurrentThread());
            return null;
        }).when(runtime).activatePreparedAuthenticationIntegration(prepared);

        mgr.createAuthenticationIntegration("corp_ldap", false,
                map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389",
                        "plugin.initialize_immediately", "true"), null, CREATE_USER);
    }

    @Test
    void testAlterPreparesOutsideWriteLockAndActivatesInsideWriteLock() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.replayCreateAuthenticationIntegration(AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), null, CREATE_USER));
        ReentrantReadWriteLock lock = getLock(mgr);
        Mockito.doAnswer(invocation -> {
            Assertions.assertFalse(lock.isWriteLockedByCurrentThread());
            return prepared;
        }).when(runtime).prepareAuthenticationIntegration(Mockito.any());
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(lock.isWriteLockedByCurrentThread());
            return null;
        }).when(runtime).activatePreparedAuthenticationIntegration(prepared);

        mgr.alterAuthenticationIntegrationProperties("corp_ldap",
                map("ldap.server", "ldap://127.0.0.1:1389",
                        "plugin.initialize_immediately", "true"), ALTER_USER);
    }

    @Test
    void testCreateDiscardPreparedAndRollbackMetadataWhenActivateFails() throws Exception {
        Mockito.when(runtime.prepareAuthenticationIntegration(Mockito.any())).thenReturn(prepared);
        Mockito.doThrow(new RuntimeException("activate failed"))
                .when(runtime).activatePreparedAuthenticationIntegration(prepared);

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> mgr.createAuthenticationIntegration("corp_ldap", false,
                        map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389",
                                "plugin.initialize_immediately", "true"), null, CREATE_USER));

        Assertions.assertEquals("activate failed", exception.getMessage());
        Assertions.assertTrue(mgr.getAuthenticationIntegrations().isEmpty());
        Mockito.verify(runtime).discardPreparedAuthenticationIntegration(prepared);
    }

    @Test
    void testAlterDiscardPreparedAndRestoreMetadataWhenActivateFails() throws Exception {
        Mockito.when(runtime.prepareAuthenticationIntegration(Mockito.any())).thenReturn(prepared);
        Mockito.doThrow(new RuntimeException("activate failed"))
                .when(runtime).activatePreparedAuthenticationIntegration(prepared);

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.replayCreateAuthenticationIntegration(AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), null, CREATE_USER));

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> mgr.alterAuthenticationIntegrationProperties("corp_ldap",
                        map("ldap.server", "ldap://127.0.0.1:1389",
                                "plugin.initialize_immediately", "true"), ALTER_USER));

        Assertions.assertEquals("activate failed", exception.getMessage());
        Assertions.assertEquals("ldap://127.0.0.1:389",
                mgr.getAuthenticationIntegrations().get("corp_ldap").getProperties().get("ldap.server"));
        Mockito.verify(runtime).discardPreparedAuthenticationIntegration(prepared);
    }

    @Test
    void testCreateWithoutImmediateInitDoesNotPrepareAndDoesNotPersistTransientProperty() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();

        mgr.createAuthenticationIntegration("corp_ldap", false, map(
                "type", "ldap",
                "ldap.server", "ldap://127.0.0.1:389",
                "plugin.initialize_immediately", "false"), null, CREATE_USER);

        AuthenticationIntegrationMeta created = mgr.getAuthenticationIntegrations().get("corp_ldap");
        Assertions.assertNotNull(created);
        Assertions.assertEquals("ldap://127.0.0.1:389", created.getProperties().get("ldap.server"));
        Assertions.assertFalse(created.getProperties().containsKey("plugin.initialize_immediately"));
        Mockito.verify(runtime, Mockito.never()).prepareAuthenticationIntegration(Mockito.any());
        Mockito.verify(runtime, Mockito.never()).activatePreparedAuthenticationIntegration(Mockito.any());
    }

    @Test
    void testAlterWithoutImmediateInitMarksDirtyAndDoesNotPrepare() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.replayCreateAuthenticationIntegration(AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), null, CREATE_USER));

        mgr.alterAuthenticationIntegrationProperties("corp_ldap", map(
                "ldap.server", "ldap://127.0.0.1:1389",
                "plugin.initialize_immediately", "false"), ALTER_USER);

        AuthenticationIntegrationMeta altered = mgr.getAuthenticationIntegrations().get("corp_ldap");
        Assertions.assertEquals("ldap://127.0.0.1:1389", altered.getProperties().get("ldap.server"));
        Assertions.assertFalse(altered.getProperties().containsKey("plugin.initialize_immediately"));
        Mockito.verify(runtime).markAuthenticationIntegrationDirty("corp_ldap");
        Mockito.verify(runtime, Mockito.never()).prepareAuthenticationIntegration(Mockito.any());
        Mockito.verify(runtime, Mockito.never()).activatePreparedAuthenticationIntegration(Mockito.any());
    }

    @Test
    void testAlterWithImmediateInitOnlyUsesCurrentMetadata() throws Exception {
        Mockito.when(runtime.prepareAuthenticationIntegration(Mockito.any())).thenReturn(prepared);

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.replayCreateAuthenticationIntegration(AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), null, CREATE_USER));

        mgr.alterAuthenticationIntegrationProperties("corp_ldap",
                map("plugin.initialize_immediately", "true"), ALTER_USER);

        AuthenticationIntegrationMeta current = mgr.getAuthenticationIntegrations().get("corp_ldap");
        Assertions.assertEquals(CREATE_USER, current.getAlterUser());
        Assertions.assertEquals("ldap://127.0.0.1:389", current.getProperties().get("ldap.server"));
        Mockito.verify(runtime).prepareAuthenticationIntegration(Mockito.any());
        Mockito.verify(runtime).activatePreparedAuthenticationIntegration(prepared);
        Mockito.verify(runtime, Mockito.never()).markAuthenticationIntegrationDirty("corp_ldap");
    }

    @Test
    void testCreateRejectsInvalidImmediateInitValue() {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> mgr.createAuthenticationIntegration("corp_ldap", false, map(
                        "type", "ldap",
                        "plugin.initialize_immediately", "abc"), null, CREATE_USER));

        Assertions.assertTrue(exception.getDetailMessage().contains("plugin.initialize_immediately"));
    }

    @Test
    void testAlterRejectsInvalidImmediateInitValue() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.replayCreateAuthenticationIntegration(AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://127.0.0.1:389"), null, CREATE_USER));

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationProperties("corp_ldap", map(
                        "ldap.server", "ldap://127.0.0.1:1389",
                        "plugin.initialize_immediately", "abc"), ALTER_USER));

        Assertions.assertTrue(exception.getDetailMessage().contains("plugin.initialize_immediately"));
    }

    @Test
    void testAlterNotExistThrows() {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationProperties("not_exist", map("k", "v"), ALTER_USER));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationUnsetProperties("not_exist", set("k"), ALTER_USER));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationComment("not_exist", "comment", ALTER_USER));
    }

    @Test
    void testReplayAndGetUnmodifiableView() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();

        AuthenticationIntegrationMeta meta1 = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://old"), null, CREATE_USER);
        AuthenticationIntegrationMeta meta2 = meta1.withAlterProperties(map("ldap.server", "ldap://new"), ALTER_USER);

        mgr.replayCreateAuthenticationIntegration(meta1);
        mgr.replayAlterAuthenticationIntegration(meta2);

        Map<String, AuthenticationIntegrationMeta> copy = mgr.getAuthenticationIntegrations();
        Assertions.assertEquals(1, copy.size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> copy.put("x", meta1));

        mgr.replayDropAuthenticationIntegration(new DropAuthenticationIntegrationOperationLog("corp_ldap"));
        Assertions.assertTrue(mgr.getAuthenticationIntegrations().isEmpty());
    }

    @Test
    void testWriteReadRoundTrip() throws IOException, DdlException {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        AuthenticationIntegrationMeta meta = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map(
                        "type", "ldap",
                        "ldap.server", "ldap://127.0.0.1:389"),
                "comment",
                CREATE_USER);
        mgr.replayCreateAuthenticationIntegration(meta);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            mgr.write(dos);
        }

        AuthenticationIntegrationMgr read;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            read = AuthenticationIntegrationMgr.read(dis);
        }

        Assertions.assertEquals(1, read.getAuthenticationIntegrations().size());
        AuthenticationIntegrationMeta readMeta = read.getAuthenticationIntegrations().get("corp_ldap");
        Assertions.assertNotNull(readMeta);
        Assertions.assertEquals("ldap", readMeta.getType());
        Assertions.assertEquals("ldap://127.0.0.1:389", readMeta.getProperties().get("ldap.server"));
        Assertions.assertEquals("comment", readMeta.getComment());
        Assertions.assertEquals(CREATE_USER, readMeta.getCreateUser());
        Assertions.assertEquals(CREATE_USER, readMeta.getAlterUser());
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

    private static ReentrantReadWriteLock getLock(AuthenticationIntegrationMgr mgr) throws Exception {
        Field field = AuthenticationIntegrationMgr.class.getDeclaredField("lock");
        field.setAccessible(true);
        return (ReentrantReadWriteLock) field.get(mgr);
    }
}
