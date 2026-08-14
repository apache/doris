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
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.meta.MetaHeader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EnvTest {

    private MockedStatic<MetaContext> mockedMetaContext;

    @Before
    public void setUp() {
        MetaContext metaContext = new MetaContext();
        mockedMetaContext = Mockito.mockStatic(MetaContext.class);
        mockedMetaContext.when(MetaContext::get).thenReturn(metaContext);
    }

    @After
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
    public void testSaveLoadHeader() throws Exception {
        String dir = "testLoadHeader";
        mkdir(dir);
        Env env = Env.getCurrentEnv();
        Field metaContextField = Env.class.getDeclaredField("metaContext");
        metaContextField.setAccessible(true);
        MetaContext sharedMetaContext = (MetaContext) metaContextField.get(env);
        int originalMetaVersion = env.getEffectiveMetaVersion();
        File file = new File(dir, "image");
        try {
            file.createNewFile();
            sharedMetaContext.setMetaVersion(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION);
            MetaContext temporaryThreadLocalContext = new MetaContext();
            temporaryThreadLocalContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
            temporaryThreadLocalContext.setThreadLocalInfo();
            mockedMetaContext.when(MetaContext::get).thenReturn(temporaryThreadLocalContext);
            long checksum1;
            try (CountingDataOutputStream dos =
                    new CountingDataOutputStream(new FileOutputStream(file))) {
                checksum1 = env.saveHeader(dos, new Random().nextLong(), 0);
            }
            try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
                Assert.assertEquals(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION, dis.readInt());
            }
            env.clear();

            try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                long checksum2 = env.loadHeader(dis, MetaHeader.EMPTY_HEADER, 0);
                Assert.assertEquals(checksum1, checksum2);
            }
        } finally {
            sharedMetaContext.setMetaVersion(originalMetaVersion);
            sharedMetaContext.setThreadLocalInfo();
            mockedMetaContext.when(MetaContext::get).thenReturn(sharedMetaContext);
            deleteDir(dir);
        }
    }

    @Test
    public void testConcurrentRowTtlActivationWritesBarrierOnce() throws Exception {
        Env activationEnv = new Env(false);
        Field feTypeField = Env.class.getDeclaredField("feType");
        feTypeField.setAccessible(true);
        feTypeField.set(activationEnv, FrontendNodeType.MASTER);
        Field metaContextField = Env.class.getDeclaredField("metaContext");
        metaContextField.setAccessible(true);
        MetaContext activationContext = (MetaContext) metaContextField.get(activationEnv);
        activationContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);

        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.doAnswer(invocation -> {
            Assert.assertEquals(FeMetaVersion.VERSION_CURRENT, activationContext.getMetaVersion());
            return null;
        }).when(editLog).logMetaVersion(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION);
        activationEnv.setEditLog(editLog);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            Future<?> first = executor.submit(() -> {
                activationEnv.activateRowTtlMetaVersion();
                return null;
            });
            Future<?> second = executor.submit(() -> {
                activationEnv.activateRowTtlMetaVersion();
                return null;
            });
            Future<?> third = executor.submit(() -> {
                activationEnv.activateRowTtlMetaVersion();
                return null;
            });
            first.get();
            second.get();
            third.get();
        } finally {
            executor.shutdownNow();
        }

        Mockito.verify(editLog, Mockito.times(1))
                .logMetaVersion(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION);
        Assert.assertEquals(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION,
                activationEnv.getEffectiveMetaVersion());
    }

    @Test
    public void testReplayMetaVersionUpdatesEnvSharedContext() {
        Env replayEnv = new Env(false);
        MetaContext.get().setMetaVersion(FeMetaVersion.VERSION_CURRENT);

        replayEnv.setMetaVersionForReplay(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION);

        Assert.assertEquals(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION,
                replayEnv.getEffectiveMetaVersion());
        Assert.assertEquals(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION,
                MetaContext.get().getMetaVersion());
    }

    @Test
    public void testRejectImageAboveMaximumSupportedVersion() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(FeMetaVersion.VERSION_MAX_SUPPORTED + 1);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            IOException exception = Assert.assertThrows(IOException.class,
                    () -> Env.getCurrentEnv().loadHeaderCOR1(input, 0));
            Assert.assertTrue(exception.getMessage().contains("maximum supported version"));
        }
    }

    @Test
    public void testOldFrontendRejectsRowTtlActivationImageVersion() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(FeMetaVersion.VERSION_ROW_TTL_ACTIVATION);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            IOException exception = Assert.assertThrows(IOException.class,
                    () -> Env.getCurrentEnv().loadHeaderCOR1(
                            input, 0, FeMetaVersion.VERSION_CURRENT));
            Assert.assertTrue(exception.getMessage().contains(
                    "maximum supported version " + FeMetaVersion.VERSION_CURRENT));
        }
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

            Assert.assertArrayEquals(new String[] {"role1", "role2"}, LdapConfig.ldap_default_roles);
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
