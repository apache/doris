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

import org.apache.doris.common.DdlException;

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

public class AuthenticationIntegrationMetaTest {

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
    public void testFromCreateSqlSuccessAndTypeFiltered() throws Exception {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("TYPE", "ldap");
        properties.put("ldap.server", "ldap://127.0.0.1:389");
        properties.put("ldap.admin_dn", "cn=admin,dc=example,dc=com");

        AuthenticationIntegrationMeta meta =
                AuthenticationIntegrationMeta.fromCreateSql("corp_ldap", properties, "ldap integration");

        Assertions.assertEquals("corp_ldap", meta.getName());
        Assertions.assertEquals("ldap", meta.getType());
        Assertions.assertEquals("ldap integration", meta.getComment());
        Assertions.assertEquals(2, meta.getProperties().size());
        Assertions.assertEquals("ldap://127.0.0.1:389", meta.getProperties().get("ldap.server"));
        Assertions.assertFalse(meta.getProperties().containsKey("type"));
        Assertions.assertFalse(meta.getProperties().containsKey("TYPE"));

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> meta.getProperties().put("x", "y"));

        Map<String, String> sqlProperties = meta.toSqlPropertiesView();
        Assertions.assertEquals("ldap", sqlProperties.get("type"));
        Assertions.assertEquals("cn=admin,dc=example,dc=com", sqlProperties.get("ldap.admin_dn"));
    }

    @Test
    public void testFromCreateSqlRequireType() {
        Assertions.assertThrows(DdlException.class,
                () -> AuthenticationIntegrationMeta.fromCreateSql("i1", null, null));
        Assertions.assertThrows(DdlException.class,
                () -> AuthenticationIntegrationMeta.fromCreateSql("i1", Collections.emptyMap(), null));
        Assertions.assertThrows(DdlException.class,
                () -> AuthenticationIntegrationMeta.fromCreateSql("i1", map("k", "v"), null));
        Assertions.assertThrows(DdlException.class,
                () -> AuthenticationIntegrationMeta.fromCreateSql("i1", map("type", ""), null));
    }

    @Test
    public void testFromCreateSqlRejectDuplicatedTypeIgnoreCase() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "ldap");
        properties.put("TYPE", "oidc");

        Assertions.assertThrows(DdlException.class,
                () -> AuthenticationIntegrationMeta.fromCreateSql("i1", properties, null));
    }

    @Test
    public void testWithAlterProperties() throws Exception {
        AuthenticationIntegrationMeta meta = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap",
                map("type", "ldap",
                        "ldap.server", "ldap://old",
                        "ldap.base_dn", "dc=example,dc=com"),
                "old comment");

        AuthenticationIntegrationMeta altered = meta.withAlterProperties(map(
                "ldap.server", "ldap://new",
                "ldap.user_filter", "(uid={login})"));

        Assertions.assertEquals("ldap", altered.getType());
        Assertions.assertEquals("old comment", altered.getComment());
        Assertions.assertEquals("ldap://new", altered.getProperties().get("ldap.server"));
        Assertions.assertEquals("(uid={login})", altered.getProperties().get("ldap.user_filter"));

        Assertions.assertThrows(DdlException.class,
                () -> meta.withAlterProperties(Collections.emptyMap()));
        Assertions.assertThrows(DdlException.class,
                () -> meta.withAlterProperties(map("TYPE", "oidc")));
    }

    @Test
    public void testWithUnsetProperties() throws Exception {
        AuthenticationIntegrationMeta meta = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap",
                map("type", "ldap",
                        "ldap.server", "ldap://old",
                        "ldap.base_dn", "dc=example,dc=com"),
                "old comment");

        AuthenticationIntegrationMeta altered = meta.withUnsetProperties(set("ldap.base_dn"));
        Assertions.assertEquals("ldap", altered.getType());
        Assertions.assertFalse(altered.getProperties().containsKey("ldap.base_dn"));
        Assertions.assertEquals("ldap://old", altered.getProperties().get("ldap.server"));

        Assertions.assertThrows(DdlException.class,
                () -> meta.withUnsetProperties(Collections.emptySet()));
        Assertions.assertThrows(DdlException.class,
                () -> meta.withUnsetProperties(set("TYPE")));
    }

    @Test
    public void testWriteReadRoundTrip() throws IOException, DdlException {
        AuthenticationIntegrationMeta meta = AuthenticationIntegrationMeta.fromCreateSql(
                "corp_ldap",
                map("type", "ldap",
                        "ldap.server", "ldap://127.0.0.1:389",
                        "ldap.admin_password", "123456"),
                "comment");

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            meta.write(dos);
        }

        AuthenticationIntegrationMeta read;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            read = AuthenticationIntegrationMeta.read(dis);
        }

        Assertions.assertEquals(meta.getName(), read.getName());
        Assertions.assertEquals(meta.getType(), read.getType());
        Assertions.assertEquals(meta.getComment(), read.getComment());
        Assertions.assertEquals(meta.getProperties(), read.getProperties());
    }
}
