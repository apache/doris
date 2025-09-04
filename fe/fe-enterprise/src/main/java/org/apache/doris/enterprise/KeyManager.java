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
import org.apache.doris.encryption.DataKeyMaterial;
import org.apache.doris.encryption.EncryptionKey;
import org.apache.doris.encryption.KeyManagerInterface;
import org.apache.doris.encryption.KeyManagerStore;
import org.apache.doris.encryption.RootKeyInfo;
import org.apache.doris.persist.KeyOperationInfo;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.zip.CRC32;

public class KeyManager implements KeyManagerInterface {
    private static final int AES256_KEY_LENGTH = 32;

    private static final int SM4_KEY_LENGTH = 16;

    private static final Logger LOG = LogManager.getLogger(KeyManager.class);

    private RootKeyProvider rootKeyProvider;

    private KeyManagerStore store;

    public void setRootKey(RootKeyInfo rootKeyInfo) throws RuntimeException {
        if (rootKeyInfo.type == RootKeyInfo.RootKeyType.AWS_KMS) {
            rootKeyProvider = new AwsKmsRootKeyProvider();
        } else if (rootKeyInfo.type == RootKeyInfo.RootKeyType.LOCAL) {
            rootKeyProvider = new LocalRootKeyProvider();
        } else {
            LOG.warn("not invalid root key type {}", rootKeyInfo.type);
            throw new RuntimeException("not invalid root key type" + rootKeyInfo.type);
        }

        try {
            rootKeyProvider.init(rootKeyInfo);
            rootKeyProvider.describeKey();
        } catch (Exception e) {
            throw new RuntimeException("failed to set root key", e);
        }

        KeyOperationInfo opInfo = new KeyOperationInfo();
        opInfo.setRootKeyInfo(rootKeyInfo);
        EncryptionKey aes256MasterKey = generateMasterKey(EncryptionKey.Algorithm.AES256, rootKeyInfo);
        opInfo.addMasterKey(aes256MasterKey);
        EncryptionKey sm4MasterKey = generateMasterKey(EncryptionKey.Algorithm.SM4, rootKeyInfo);
        opInfo.addMasterKey(sm4MasterKey);
        opInfo.setOpType(KeyOperationInfo.KeyOPType.SET_ROOT_KEY);

        // Decryption isn’t required; it’s just to check that encryption and decryption work properly.
        decryptMasterKey();

        // write edit log
        Env.getCurrentEnv().getEditLog().logOperateKey(opInfo);

        store = Env.getCurrentEnv().getKeyManagerStore();
        store.addMasterKey(sm4MasterKey);
        store.addMasterKey(aes256MasterKey);
        store.setRootKeyInfo(rootKeyInfo);
    }

    public void setRootKeyByConfig() {
        if (Config.doris_tde_key_id.isEmpty() && Config.doris_tde_key_endpoint.isEmpty()
                && Config.doris_tde_key_provider.isEmpty() && Config.doris_tde_key_region.isEmpty()) {
            LOG.info("all doris_tde-related configurations are all empty");
            return;
        }

        if (Config.doris_tde_key_id.isEmpty() || Config.doris_tde_key_endpoint.isEmpty()
                || Config.doris_tde_key_provider.isEmpty() || Config.doris_tde_key_region.isEmpty()) {
            LOG.warn("some of the doris_tde-related configurations are empty");
            throw new IllegalArgumentException("some of the doris_tde-related configurations are empty. "
                    + "Please either set all doris_tde-related conf to correct values or leave them all unset");
        }

        RootKeyInfo rootKeyInfo = new RootKeyInfo();
        try {
            rootKeyInfo.type = RootKeyInfo.RootKeyType.valueOf(Config.doris_tde_key_provider.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("Config.doris_tde_key_provider = " + Config.doris_tde_key_provider
                    + " is not supported ");
        }

        LOG.info("Setting RootKey with provider={}, cmkId={}, region={}, endpoint={}",
                rootKeyInfo.type, rootKeyInfo.cmkId, rootKeyInfo.region, rootKeyInfo.endpoint);
        rootKeyInfo.cmkId = Config.doris_tde_key_id;
        rootKeyInfo.region = Config.doris_tde_key_region;
        rootKeyInfo.endpoint = Config.doris_tde_key_endpoint;

        setRootKey(rootKeyInfo);
        LOG.info("RootKey has been successfully set");
    }

    public void setLocalRootKey() {
        String password = System.getenv("DORIS_TDE_ROOT_KEY_PASSWORD");

        RootKeyInfo rootKeyInfo = new RootKeyInfo();
        rootKeyInfo.type = RootKeyInfo.RootKeyType.LOCAL;
        rootKeyInfo.password = password;

        setRootKey(rootKeyInfo);
    }

    public void init() {
        store = Env.getCurrentEnv().getKeyManagerStore();
        RootKeyInfo rootKeyInfo = store.getRootKeyInfo();
        if (rootKeyInfo == null) {
            try {
                setRootKeyByConfig();
            } catch (Exception e) {
                LOG.info("failed to set root key by config: ", e);
            }
            return;
        }

        if (rootKeyInfo.type == RootKeyInfo.RootKeyType.AWS_KMS) {
            rootKeyProvider = new AwsKmsRootKeyProvider();
        } else {
            throw new RuntimeException("not support root key type" + rootKeyInfo.type);
        }

        try {
            rootKeyProvider.init(rootKeyInfo);
            rootKeyProvider.describeKey();
            decryptMasterKey();
        } catch (Exception e) {
            LOG.info("failed to init root key or decrypt master key", e);
        }

    }

    @Override
    public void replayKeyOperation(KeyOperationInfo keyOpInfo) {
        store = Env.getCurrentEnv().getKeyManagerStore();
        store.setRootKeyInfo(keyOpInfo.getRootKeyInfo());
        for (EncryptionKey key : keyOpInfo.getMasterKeys()) {
            store.addMasterKey(key);
        }
    }

    public long computeCrc(byte[] plaintext) {
        CRC32 crc32 = new CRC32();
        crc32.update(plaintext);
        return crc32.getValue();
    }

    private EncryptionKey generateMasterKey(EncryptionKey.Algorithm algorithm, RootKeyInfo rootKeyInfo) {
        int keyLength = -1;
        if (algorithm == EncryptionKey.Algorithm.AES256) {
            keyLength = AES256_KEY_LENGTH;
        } else if (algorithm == EncryptionKey.Algorithm.SM4) {
            keyLength = SM4_KEY_LENGTH;
        } else {
            throw new RuntimeException("invalid encryption algorithm: " + algorithm.toString());
        }

        DataKeyMaterial material = rootKeyProvider.generateSymmetricDataKey(keyLength);
        EncryptionKey masterKey = new EncryptionKey();
        masterKey.id = Long.toString(Env.getCurrentEnv().getNextId());
        masterKey.version = 1;
        masterKey.parentId = rootKeyInfo.cmkId;
        masterKey.parentVersion = 1;
        masterKey.algorithm = algorithm;
        masterKey.type = EncryptionKey.KeyType.MASTER_KEY;
        masterKey.plaintext = material.plaintext;
        masterKey.ciphertext = Base64.getEncoder().encodeToString(material.ciphertext);
        masterKey.ctime = System.currentTimeMillis();
        masterKey.mtime = System.currentTimeMillis();
        masterKey.crc = computeCrc(masterKey.plaintext);
        LOG.info("Generated master key successfully. id={}, algorithm={}, keyLength={}, ciphertextLength={}",
                masterKey.id, algorithm, keyLength, material.ciphertext.length);

        return masterKey;
    }

    public List<EncryptionKey> getAllMasterKeys() {
        return store.getMasterKeys();
    }

    private void decryptMasterKey() {
        List<EncryptionKey> masterKey = store.getMasterKeys();
        for (EncryptionKey versionKey : masterKey) {
            Preconditions.checkArgument(versionKey.plaintext == null || versionKey.plaintext.length == 0);
            byte[] plaintext = rootKeyProvider.decrypt(Base64.getDecoder().decode(versionKey.ciphertext));
            if (versionKey.plaintext != null && versionKey.plaintext.length != 0) {
                if (!Arrays.equals(versionKey.plaintext, plaintext)) {
                    LOG.error("The decrypted plaintext {} does not match th original {}", plaintext,
                            versionKey.plaintext);
                    throw new RuntimeException("The decrypted plaintext " + Arrays.toString(plaintext)
                            + "does not match the original" + Arrays.toString(versionKey.plaintext));
                }
            }
            versionKey.plaintext = plaintext;
            Preconditions.checkArgument(versionKey.crc == computeCrc(versionKey.plaintext));
            LOG.info("Successfully decrypted the plaintext of {}, version {}", versionKey.id, versionKey.version);
        }
    }
}
