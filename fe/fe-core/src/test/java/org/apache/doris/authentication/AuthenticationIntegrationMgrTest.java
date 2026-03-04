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

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class AuthenticationIntegrationMgrTest {

    @Mocked
    private Env env;

    @Mocked
    private EditLog editLog;

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
    public void testCreateAlterDropFlow() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logCreateAuthenticationIntegration((AuthenticationIntegrationMeta) any);
                minTimes = 0;

                editLog.logAlterAuthenticationIntegration((AuthenticationIntegrationMeta) any);
                minTimes = 0;

                editLog.logDropAuthenticationIntegration((DropAuthenticationIntegrationOperationLog) any);
                minTimes = 0;
            }
        };

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        Map<String, String> createProperties = new LinkedHashMap<>();
        createProperties.put("type", "ldap");
        createProperties.put("ldap.server", "ldap://127.0.0.1:389");
        createProperties.put("ldap.admin_password", "123456");

        mgr.createAuthenticationIntegration("corp_ldap", false, createProperties, "comment");
        AuthenticationIntegrationMeta created = mgr.getAuthenticationIntegrations().get("corp_ldap");
        Assertions.assertNotNull(created);
        Assertions.assertEquals("ldap", created.getType());
        Assertions.assertEquals("ldap://127.0.0.1:389", created.getProperties().get("ldap.server"));

        mgr.alterAuthenticationIntegrationProperties("corp_ldap", map("ldap.server", "ldap://127.0.0.1:1389"));
        Assertions.assertEquals("ldap://127.0.0.1:1389",
                mgr.getAuthenticationIntegrations().get("corp_ldap").getProperties().get("ldap.server"));

        mgr.alterAuthenticationIntegrationUnsetProperties("corp_ldap", set("ldap.admin_password"));
        Assertions.assertFalse(mgr.getAuthenticationIntegrations()
                .get("corp_ldap").getProperties().containsKey("ldap.admin_password"));

        mgr.alterAuthenticationIntegrationComment("corp_ldap", "new comment");
        Assertions.assertEquals("new comment", mgr.getAuthenticationIntegrations().get("corp_ldap").getComment());

        mgr.dropAuthenticationIntegration("corp_ldap", false);
        Assertions.assertTrue(mgr.getAuthenticationIntegrations().isEmpty());
    }

    @Test
    public void testCreateDuplicateAndDropIfExists() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logCreateAuthenticationIntegration((AuthenticationIntegrationMeta) any);
                minTimes = 0;

                editLog.logDropAuthenticationIntegration((DropAuthenticationIntegrationOperationLog) any);
                minTimes = 0;
            }
        };

        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        mgr.createAuthenticationIntegration("corp_ldap", false, map(
                "type", "ldap",
                "ldap.server", "ldap://127.0.0.1:389"), null);

        Assertions.assertThrows(DdlException.class,
                () -> mgr.createAuthenticationIntegration("corp_ldap", false, map("type", "ldap"), null));
        Assertions.assertDoesNotThrow(
                () -> mgr.createAuthenticationIntegration("corp_ldap", true, map("type", "ldap"), null));

        Assertions.assertDoesNotThrow(() -> mgr.dropAuthenticationIntegration("not_exist", true));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.dropAuthenticationIntegration("not_exist", false));
    }

    @Test
    public void testAlterNotExistThrows() {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationProperties("not_exist", map("k", "v")));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationUnsetProperties("not_exist", set("k")));
        Assertions.assertThrows(DdlException.class,
                () -> mgr.alterAuthenticationIntegrationComment("not_exist", "comment"));
    }

    @Test
    public void testReplayAndGetUnmodifiableView() throws Exception {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();

        AuthenticationIntegrationMeta meta1 = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map("type", "ldap", "ldap.server", "ldap://old"), null);
        AuthenticationIntegrationMeta meta2 = meta1.withAlterProperties(map("ldap.server", "ldap://new"));

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
    public void testWriteReadRoundTrip() throws IOException, DdlException {
        AuthenticationIntegrationMgr mgr = new AuthenticationIntegrationMgr();
        AuthenticationIntegrationMeta meta = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap", map(
                        "type", "ldap",
                        "ldap.server", "ldap://127.0.0.1:389"),
                "comment");
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
    }
}
