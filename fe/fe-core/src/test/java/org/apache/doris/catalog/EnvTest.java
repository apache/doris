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

package org.apache.doris.catalog;

import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.persist.meta.MetaHeader;

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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class EnvTest {

    private MockedStatic<MetaContext> mockedMetaContext;

    @BeforeEach
    public void setUp() {
        MetaContext metaContext = new MetaContext();
        mockedMetaContext = Mockito.mockStatic(MetaContext.class);
        mockedMetaContext.when(MetaContext::get).thenReturn(metaContext);
    }

    @AfterEach
    public void tearDown() {
        if (mockedMetaContext != null) {
            mockedMetaContext.close();
        }
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit, String metaDir) {
        File imageFile = new File(metaDir + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(metaDir + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(metaDir + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(metaDir + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testSaveLoadHeaderUses141WithoutRowTtl() throws Exception {
        Env env = new Env(false);
        // A context used to read an old image must not downgrade newly saved metadata.
        MetaContext.get().setMetaVersion(FeMetaVersion.VERSION_140);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        long savedChecksum;
        try (CountingDataOutputStream output = new CountingDataOutputStream(bytes)) {
            savedChecksum = env.saveHeader(output, 123L, 0);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assertions.assertEquals(FeMetaVersion.VERSION_141, input.readInt());
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assertions.assertEquals(savedChecksum, env.loadHeader(input, MetaHeader.EMPTY_HEADER, 0));
            Assertions.assertEquals(FeMetaVersion.VERSION_141, MetaContext.get().getMetaVersion());
        }
    }

    @Test
    public void testLoad140ImageBeforeUpgrading() throws Exception {
        Env env = new Env(false);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(FeMetaVersion.VERSION_140);
            output.writeLong(123L);
            output.writeLong(456L);
            output.writeBoolean(true);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assertions.assertEquals(FeMetaVersion.VERSION_140 ^ 123L ^ 456L,
                    env.loadHeader(input, MetaHeader.EMPTY_HEADER, 0));
            Assertions.assertEquals(FeMetaVersion.VERSION_140, MetaContext.get().getMetaVersion());
        }
        bytes.reset();
        try (CountingDataOutputStream output = new CountingDataOutputStream(bytes)) {
            env.saveHeader(output, 123L, 0);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assertions.assertEquals(FeMetaVersion.VERSION_141, input.readInt());
        }
    }

    @Test
    public void testRejectImageAboveCurrentVersion() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(FeMetaVersion.VERSION_CURRENT + 1);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            IOException exception = Assertions.assertThrows(IOException.class,
                    () -> Env.getCurrentEnv().loadHeaderCOR1(input, 0));
            Assertions.assertTrue(exception.getMessage().contains("FE current version 141"));
        }
    }

    @Test
    public void testJournalVersionWithoutThreadContextUses141() {
        mockedMetaContext.when(MetaContext::get).thenReturn(null);
        Assertions.assertEquals(FeMetaVersion.VERSION_141, Env.getCurrentEnvJournalVersion());
    }

    @Test
    public void testSetLdapDefaultRolesConfigRefreshesLdapCache() throws Exception {
        Env env = Mockito.spy(new Env(false));
        Auth auth = Mockito.mock(Auth.class);
        LdapManager ldapManager = Mockito.mock(LdapManager.class);
        Mockito.doReturn(auth).when(env).getAuth();
        Mockito.when(auth.getLdapManager()).thenReturn(ldapManager);

        Map<String, Field> oldConfFields = ConfigBase.confFields;
        Field oldLdapDefaultRolesField = ConfigBase.ldapConfFields.put("ldap_default_roles",
                LdapConfig.class.getField("ldap_default_roles"));
        String[] oldLdapDefaultRoles = LdapConfig.ldap_default_roles;
        try {
            ConfigBase.confFields = new HashMap<>();

            env.setMutableConfigWithCallback("ldap_default_roles", "role1,role2");

            Assertions.assertArrayEquals(new String[] {"role1", "role2"}, LdapConfig.ldap_default_roles);
            Mockito.verify(ldapManager).refresh(true, null);
        } finally {
            ConfigBase.confFields = oldConfFields;
            if (oldLdapDefaultRolesField == null) {
                ConfigBase.ldapConfFields.remove("ldap_default_roles");
            } else {
                ConfigBase.ldapConfFields.put("ldap_default_roles", oldLdapDefaultRolesField);
            }
            LdapConfig.ldap_default_roles = oldLdapDefaultRoles;
        }
    }
}
