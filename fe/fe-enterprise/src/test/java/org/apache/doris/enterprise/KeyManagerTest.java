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

package org.apache.doris.enterprise;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.encryption.EncryptionKey;
import org.apache.doris.encryption.EncryptionKey.Algorithm;
import org.apache.doris.encryption.EncryptionKey.KeyType;
import org.apache.doris.encryption.KeyManagerStore;
import org.apache.doris.encryption.RootKeyInfo;
import org.apache.doris.encryption.RootKeyInfo.RootKeyType;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.AdminRotateTdeRootKeyCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KeyManagerTest {
    @Test
    public void testSetRootKeyByConfig() {
        Config.doris_tde_key_endpoint = "xxx";
        Config.doris_tde_key_region = "xxx";
        Config.doris_tde_key_provider = "aws_kms";
        Config.doris_tde_key_id = "";
        KeyManager manager = new KeyManager();
        IllegalArgumentException exception1 = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    manager.setRootKeyByConfig();
                }
        );

        Assert.assertTrue(exception1.getMessage().contains("some of the doris_tde-related configurations are empty"));

        Config.doris_tde_key_endpoint = "xxx";
        Config.doris_tde_key_region = "xxx";
        Config.doris_tde_key_provider = "xxx_kms";
        Config.doris_tde_key_id = "key id";
        IllegalArgumentException exception2 = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    manager.setRootKeyByConfig();
                }
        );

        Assert.assertTrue(exception2.getMessage().contains("doris_tde_key_provider"));
    }

    @Test
    public void testRotateRootKey(@Mocked Env env, @Mocked EditLog editLog, @Mocked ConnectContext ctx,
            @Mocked AccessControllerManager accessMgr) {
        KeyManager manager = new KeyManager();
        RootKeyProvider provider = new MockedRootKeyProvider();

        RootKeyInfo rootKeyInfo = new RootKeyInfo();
        rootKeyInfo.sk = "mocked_sk";
        rootKeyInfo.ak = "mocked_ak";
        rootKeyInfo.region = "mocked_region";
        rootKeyInfo.endpoint = "mocked_region";
        rootKeyInfo.type = RootKeyType.AWS_KMS;
        rootKeyInfo.cmkId = "mocked_key_id";
        rootKeyInfo.algorithm = Algorithm.AES256;

        List<EncryptionKey> masterKeys = new ArrayList<>();
        EncryptionKey masterKey = new EncryptionKey();
        masterKey.id = "1";
        masterKey.version = 1;
        masterKey.type = KeyType.MASTER_KEY;
        masterKey.ctime = 0;
        masterKey.mtime = 0;
        masterKey.ciphertext = "2";
        masterKey.plaintext = "1".getBytes(StandardCharsets.UTF_8);
        masterKey.algorithm = Algorithm.AES256;
        masterKey.crc = manager.computeCrc(masterKey.plaintext);
        masterKeys.add(masterKey);

        KeyManagerStore keyManagerStore = new KeyManagerStore();
        Deencapsulation.setField(keyManagerStore, "rootKeyInfo", rootKeyInfo);
        Deencapsulation.setField(keyManagerStore, "masterKeys", masterKeys);

        Deencapsulation.setField(manager, "rootKeyProvider", provider);
        Deencapsulation.setField(manager, "store", keyManagerStore);

        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }

            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }

            {
                accessMgr.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };

        HashMap<String, String> properties = new HashMap<>();
        try {
            manager.rotateRootKey(properties);

            Assert.assertTrue(masterKey.ciphertext.endsWith("MQ=="));
            Assert.assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), masterKey.plaintext);
            Assert.assertNotEquals(0, masterKey.mtime);
            masterKey.mtime = 0;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        properties.put(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_PROVIDER, "aws_kms");
        properties.put(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ID, "new_key_id");
        properties.put(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_REGION, "new_region");
        properties.put(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ENDPOINT, "new_endpoint");
        try {
            manager.rotateRootKey(properties);

            Assert.assertTrue(masterKey.ciphertext.endsWith("Mg=="));
            Assert.assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), masterKey.plaintext);
            Assert.assertNotEquals(0, masterKey.mtime);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        properties.put("redundant", "redundant");
        try {
            manager.rotateRootKey(properties);
            Assert.fail();
        } catch (Exception e) {
            // do nothing
        }

        properties.clear();
        properties.put(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_PROVIDER, "local");
        try {
            manager.rotateRootKey(properties);
            Assert.fail();
        } catch (Exception e) {
            // do nothing
        }

        new Expectations() {
            {
                accessMgr.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                minTimes = 1;
                result = false;
            }
        };

        try {
            manager.rotateRootKey(properties);
            Assert.fail();
        } catch (Exception e) {
            // do nothing
        }
    }

    @Test
    public void testRotateMasterKeys(@Mocked Env env, @Mocked EditLog editLog) {
        KeyManager manager = new KeyManager();
        RootKeyProvider provider = new MockedRootKeyProvider();

        RootKeyInfo rootKeyInfo = new RootKeyInfo();
        rootKeyInfo.sk = "mocked_sk";
        rootKeyInfo.ak = "mocked_ak";
        rootKeyInfo.region = "mocked_region";
        rootKeyInfo.endpoint = "mocked_region";
        rootKeyInfo.type = RootKeyType.AWS_KMS;
        rootKeyInfo.cmkId = "mocked_key_id";
        rootKeyInfo.algorithm = Algorithm.AES256;

        List<EncryptionKey> masterKeys = new ArrayList<>();
        EncryptionKey masterKey = new EncryptionKey();
        List<Algorithm> algorithms = Lists.newArrayList(Algorithm.AES256, Algorithm.SM4);
        for (Algorithm algorithm : algorithms) {
            masterKey.id = "1";
            masterKey.version = 1;
            masterKey.type = KeyType.MASTER_KEY;
            masterKey.ctime = 0;
            masterKey.mtime = 0;
            masterKey.ciphertext = "2";
            masterKey.plaintext = "1".getBytes(StandardCharsets.UTF_8);
            masterKey.algorithm = algorithm;
            masterKey.crc = manager.computeCrc(masterKey.plaintext);
            masterKeys.add(masterKey);
        }


        KeyManagerStore keyManagerStore = new KeyManagerStore();
        Deencapsulation.setField(keyManagerStore, "rootKeyInfo", rootKeyInfo);
        Deencapsulation.setField(keyManagerStore, "masterKeys", masterKeys);

        Deencapsulation.setField(manager, "rootKeyProvider", provider);
        Deencapsulation.setField(manager, "store", keyManagerStore);

        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        manager.rotateMasterKeys();

        List<EncryptionKey> allMasterKeys = manager.getAllMasterKeys();
        Assert.assertEquals(4, allMasterKeys.size());
        Assert.assertEquals(2, allMasterKeys.get(2).version);
        Assert.assertEquals(2, allMasterKeys.get(3).version);

        manager.rotateMasterKeys();
        Assert.assertEquals(4, allMasterKeys.size());
        Assert.assertEquals(2, allMasterKeys.get(2).version);
        Assert.assertEquals(2, allMasterKeys.get(3).version);
    }
}
