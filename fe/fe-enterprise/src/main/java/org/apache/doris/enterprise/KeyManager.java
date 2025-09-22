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
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.encryption.DataKeyMaterial;
import org.apache.doris.encryption.EncryptionKey;
import org.apache.doris.encryption.EncryptionKey.Algorithm;
import org.apache.doris.encryption.KeyManagerInterface;
import org.apache.doris.encryption.KeyManagerStore;
import org.apache.doris.encryption.RootKeyInfo;
import org.apache.doris.encryption.RootKeyInfo.RootKeyType;
import org.apache.doris.nereids.trees.plans.commands.AdminRotateTdeRootKeyCommand;
import org.apache.doris.persist.KeyOperationInfo;
import org.apache.doris.persist.KeyOperationInfo.KeyOPType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

public class KeyManager extends MasterDaemon implements KeyManagerInterface {
    private static final int AES256_KEY_LENGTH = 32;

    private static final int SM4_KEY_LENGTH = 16;

    private static final Logger LOG = LogManager.getLogger(KeyManager.class);

    private RootKeyProvider rootKeyProvider;

    private KeyManagerStore store;

    public KeyManager() {
        super(KeyManager.class.getSimpleName(), Config.doris_tde_check_rotate_master_key_interval_ms);
    }

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
        // decryptMasterKey();

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

        rootKeyInfo.cmkId = Config.doris_tde_key_id;
        rootKeyInfo.region = Config.doris_tde_key_region;
        rootKeyInfo.endpoint = Config.doris_tde_key_endpoint;
        LOG.info("Setting RootKey with provider={}, cmkId={}, region={}, endpoint={}",
                rootKeyInfo.type, rootKeyInfo.cmkId, rootKeyInfo.region, rootKeyInfo.endpoint);

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
        doInit();
        start();
    }

    private void doInit() {
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
            decryptMasterKey();
        } catch (Exception e) {
            LOG.info("failed to init root key or decrypt master key", e);
        }
    }

    @Override
    public void replayKeyOperation(KeyOperationInfo keyOpInfo) {
        store = Env.getCurrentEnv().getKeyManagerStore();
        store.setRootKeyInfo(keyOpInfo.getRootKeyInfo());
        store.clearMasterKeys();
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

    @Override
    public void rotateRootKey(Map<String, String> properties) {
        if (properties == null) {
            properties = new HashMap<>();
        } else {
            properties = new HashMap<>(properties);
        }

        store.writeLock();
        try {
            RootKeyInfo rootKeyInfo = store.getRootKeyInfo();
            if (rootKeyInfo == null) {
                throw new IllegalStateException("TDE is not enabled");
            }
            RootKeyInfo newRootKeyInfo = new RootKeyInfo(rootKeyInfo);

            String keyProviderType = properties.remove(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_PROVIDER);
            if (keyProviderType != null) {
                newRootKeyInfo.type = RootKeyType.tryFrom(keyProviderType);
                // TODO(tsy): remove this branch after local key provider is supported
                if (newRootKeyInfo.type == RootKeyType.LOCAL) {
                    throw new IllegalArgumentException("Local key provider is currently unsupported");
                }
            }

            String kmsMasterKeyId = properties.remove(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ID);
            if (kmsMasterKeyId != null) {
                newRootKeyInfo.cmkId = kmsMasterKeyId;
            }

            String kmsEndpoint = properties.remove(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ENDPOINT);
            if (kmsEndpoint != null) {
                newRootKeyInfo.endpoint = kmsEndpoint;
            }

            String kmsRegion = properties.remove(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_REGION);
            if (kmsRegion != null) {
                newRootKeyInfo.region = kmsRegion;
            }

            String password = properties.remove(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_PASSWORD);
            if (newRootKeyInfo.type.equals(RootKeyType.LOCAL) && !Objects.equals(password, rootKeyInfo.password)) {
                if (password == null) {
                    throw new IllegalArgumentException("Missing required `"
                            + AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_PASSWORD + "` for local key provider");
                }
                String originalPasswd = properties.remove(AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ORIGINAL_PASSWORD);
                if (rootKeyInfo.type.equals(RootKeyType.LOCAL)) {
                    if (originalPasswd == null) {
                        throw new IllegalArgumentException("Missing required ` + "
                                + AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ORIGINAL_PASSWORD
                                + "` for local key provider");
                    }
                    if (!Objects.equals(originalPasswd, rootKeyInfo.password)) {
                        throw new IllegalArgumentException("Password in `"
                                + AdminRotateTdeRootKeyCommand.DORIS_TDE_KEY_ORIGINAL_PASSWORD + "` is incorrect");
                    }
                }
            }
            if (password != null) {
                if (!newRootKeyInfo.type.equals(RootKeyType.LOCAL)) {
                    throw new IllegalArgumentException("Password is only required for local key provider");
                }
                newRootKeyInfo.password = password;
            }
            if (!properties.isEmpty()) {
                throw new IllegalArgumentException("unknown properties: " + properties);
            }

            rootKeyProvider.init(newRootKeyInfo);
            store.setRootKeyInfo(newRootKeyInfo);

            KeyOperationInfo opInfo = new KeyOperationInfo();
            opInfo.setRootKeyInfo(newRootKeyInfo);
            List<EncryptionKey> masterKeys = store.getMasterKeys();
            for (EncryptionKey masterKey : masterKeys) {
                byte[] newCiphertext = rootKeyProvider.encrypt(masterKey.plaintext);
                masterKey.ciphertext = Base64.getEncoder().encodeToString(newCiphertext);
                masterKey.mtime = System.currentTimeMillis();
                opInfo.addMasterKey(masterKey);
            }
            opInfo.setOpType(KeyOPType.ROTATE_ROOT_KEY);
            Env.getCurrentEnv().getEditLog().logOperateKey(opInfo);
        } finally {
            store.writeUnlock();
        }
    }

    public void rotateMasterKeys() {
        store.writeLock();
        try {
            RootKeyInfo rootKeyInfo = store.getRootKeyInfo();
            List<EncryptionKey> masterKeys = store.getMasterKeys();
            if (masterKeys.isEmpty()) {
                return;
            }
            EncryptionKey latestKey = masterKeys.get(masterKeys.size() - 1);
            long now = System.currentTimeMillis();
            long rotationThreshold = latestKey.ctime + Config.doris_tde_rotate_master_key_interval_ms;
            LOG.info("Check timing for rotating master key");
            if (now < rotationThreshold) {
                return;
            }

            int latestVersion = latestKey.version;
            ++latestVersion;

            EncryptionKey aesKey = generateMasterKey(Algorithm.AES256, rootKeyInfo);
            EncryptionKey sm4Key = generateMasterKey(Algorithm.SM4, rootKeyInfo);
            aesKey.version = latestVersion;
            sm4Key.version = latestVersion;

            masterKeys.add(aesKey);
            masterKeys.add(sm4Key);

            KeyOperationInfo opInfo = new KeyOperationInfo();
            opInfo.setRootKeyInfo(rootKeyInfo);
            for (EncryptionKey masterKey : masterKeys) {
                opInfo.addMasterKey(masterKey);
            }
            opInfo.setOpType(KeyOPType.ROTATE_MASTER_KEYS);

            // Decryption isn’t required; it’s just to check that encryption and decryption work properly.
            // decryptMasterKey();

            // write edit log
            Env.getCurrentEnv().getEditLog().logOperateKey(opInfo);
        } finally {
            store.writeUnlock();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            rotateMasterKeys();
        } catch (Exception e) {
            LOG.warn("Rotate master key failed", e);
        }
    }
}
