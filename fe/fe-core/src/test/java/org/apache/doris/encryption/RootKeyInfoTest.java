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

package org.apache.doris.encryption;

import org.apache.doris.encryption.EncryptionKey.Algorithm;
import org.apache.doris.encryption.EncryptionKey.KeyType;
import org.apache.doris.encryption.RootKeyInfo.RootKeyType;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class RootKeyInfoTest {
    @Test
    public void testDeserializeGcpAndAzureKmsTypes() {
        RootKeyInfo gcpInfo = GsonUtils.GSON.fromJson("{\"type\":\"GCP_KMS\"}", RootKeyInfo.class);
        Assertions.assertEquals(RootKeyInfo.RootKeyType.GCP_KMS, gcpInfo.type);

        RootKeyInfo azureInfo = GsonUtils.GSON.fromJson("{\"type\":\"AZURE_KMS\"}", RootKeyInfo.class);
        Assertions.assertEquals(RootKeyInfo.RootKeyType.AZURE_KMS, azureInfo.type);
    }

    @Test
    public void testLocalRootKeyInfoSerialization() {
        RootKeyInfo rootKeyInfo = createLocalRootKeyInfo();

        String json = GsonUtils.GSON.toJson(rootKeyInfo);
        Assertions.assertTrue(json.contains("rootKeyFilePath"));
        Assertions.assertTrue(json.contains("rootKeyHash"));
        Assertions.assertFalse(json.contains("rootKeyBase64"));
        Assertions.assertFalse(json.contains(rootKeyInfo.rootKeyBase64));

        RootKeyInfo restored = GsonUtils.GSON.fromJson(json, RootKeyInfo.class);
        Assertions.assertEquals(RootKeyType.LOCAL, restored.type);
        Assertions.assertEquals(rootKeyInfo.rootKeyFilePath, restored.rootKeyFilePath);
        Assertions.assertEquals(rootKeyInfo.rootKeyHash, restored.rootKeyHash);
        Assertions.assertNull(restored.rootKeyBase64);
    }

    @Test
    public void testLocalRootKeyHashVerification() {
        byte[] rootKey = "local-root-key".getBytes(StandardCharsets.UTF_8);
        RootKeyInfo rootKeyInfo = new RootKeyInfo();

        Assertions.assertTrue(rootKeyInfo.verifyRootKey(rootKey));

        rootKeyInfo.setRootKeyHashFromKey(rootKey);
        Assertions.assertTrue(rootKeyInfo.verifyRootKey(rootKey));
        Assertions.assertFalse(rootKeyInfo.verifyRootKey("different-key".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testCopyLocalRootKeyInfo() {
        RootKeyInfo rootKeyInfo = createLocalRootKeyInfo();

        RootKeyInfo copied = new RootKeyInfo(rootKeyInfo);

        Assertions.assertEquals(rootKeyInfo.rootKeyFilePath, copied.rootKeyFilePath);
        Assertions.assertEquals(rootKeyInfo.rootKeyHash, copied.rootKeyHash);
        Assertions.assertEquals(rootKeyInfo.rootKeyBase64, copied.rootKeyBase64);
    }

    @Test
    public void testKeyManagerStoreSerializationKeepsLocalRootKeyReference() throws Exception {
        RootKeyInfo rootKeyInfo = createLocalRootKeyInfo();

        EncryptionKey masterKey = new EncryptionKey();
        masterKey.id = "1";
        masterKey.version = 1;
        masterKey.parentId = "local";
        masterKey.parentVersion = 1;
        masterKey.type = KeyType.MASTER_KEY;
        masterKey.algorithm = Algorithm.AES256;
        masterKey.ciphertext = "ciphertext";
        masterKey.crc = 1234;

        KeyManagerStore store = new KeyManagerStore();
        store.setRootKeyInfo(rootKeyInfo);
        store.addMasterKey(masterKey);

        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        store.write(new DataOutputStream(byteOutput));

        KeyManagerStore restored = KeyManagerStore.read(new DataInputStream(
                new ByteArrayInputStream(byteOutput.toByteArray())));

        Assertions.assertEquals(RootKeyType.LOCAL, restored.getRootKeyInfo().type);
        Assertions.assertEquals(rootKeyInfo.rootKeyFilePath, restored.getRootKeyInfo().rootKeyFilePath);
        Assertions.assertEquals(rootKeyInfo.rootKeyHash, restored.getRootKeyInfo().rootKeyHash);
        Assertions.assertNull(restored.getRootKeyInfo().rootKeyBase64);
        Assertions.assertEquals(1, restored.getMasterKeys().size());
        Assertions.assertEquals(masterKey.ciphertext, restored.getMasterKeys().get(0).ciphertext);
    }

    private RootKeyInfo createLocalRootKeyInfo() {
        RootKeyInfo rootKeyInfo = new RootKeyInfo();
        rootKeyInfo.type = RootKeyType.LOCAL;
        rootKeyInfo.cmkId = "local";
        rootKeyInfo.rootKeyFilePath = "/opt/apache-doris/fe/conf/doris_tde_root_key";
        rootKeyInfo.rootKeyHash = "hash";
        rootKeyInfo.rootKeyBase64 = Base64.getEncoder().encodeToString(
                "secret".getBytes(StandardCharsets.UTF_8));
        return rootKeyInfo;
    }
}
