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

import org.apache.doris.encryption.DataKeyMaterial;
import org.apache.doris.encryption.RootKeyInfo;

import java.nio.charset.StandardCharsets;
import java.security.spec.KeySpec;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class LocalRootKeyProvider implements RootKeyProvider {
    private static final int KEY_SIZE = 256;
    private static final int IV_SIZE = 16;
    private static final int ITERATION_COUNT = 65536;
    private static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA256";

    private SecretKey rootKey;

    private static final byte[] SALT = "fixed_salt_12345".getBytes(StandardCharsets.UTF_8);

    @Override
    public void init(RootKeyInfo info) {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2_ALGORITHM);
            KeySpec spec = new PBEKeySpec(info.password.toCharArray(), SALT, ITERATION_COUNT, KEY_SIZE);
            byte[] keyBytes = factory.generateSecret(spec).getEncoded();
            this.rootKey = new SecretKeySpec(keyBytes, "AES");
        } catch (Exception e) {
            throw new RuntimeException("Failed to init LocalRootKeyProvider", e);
        }
    }

    @Override
    public void describeKey() {
        System.out.println("LocalRootKeyProvider using password-derived AES-256 root key (CTR mode).");
    }

    @Override
    public byte[] decrypt(byte[] ciphertext) {
        try {
            byte[] iv = new byte[IV_SIZE];
            System.arraycopy(ciphertext, 0, iv, 0, IV_SIZE);
            byte[] actualCiphertext = new byte[ciphertext.length - IV_SIZE];
            System.arraycopy(ciphertext, IV_SIZE, actualCiphertext, 0, actualCiphertext.length);

            Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, rootKey, new IvParameterSpec(iv));
            return cipher.doFinal(actualCiphertext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt", e);
        }
    }

    @Override
    public DataKeyMaterial generateSymmetricDataKey(int length) {
        return null;
    }

    //@Override
    //public DataKeyMaterial generateSymmetricDataKey() {
    //    try {
    //        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    //        keyGen.init(KEY_SIZE);
    //        SecretKey dataKey = keyGen.generateKey();
    //        byte[] plaintextKey = dataKey.getEncoded();

    //        byte[] iv = new byte[IV_SIZE];
    //        new SecureRandom().nextBytes(iv);

    //        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
    //        cipher.init(Cipher.ENCRYPT_MODE, rootKey, new IvParameterSpec(iv));
    //        byte[] ciphertextKey = cipher.doFinal(plaintextKey);

    //        byte[] ciphertext = new byte[IV_SIZE + ciphertextKey.length];
    //        System.arraycopy(iv, 0, ciphertext, 0, IV_SIZE);
    //        System.arraycopy(ciphertextKey, 0, ciphertext, IV_SIZE, ciphertextKey.length);

    //        return new DataKeyMaterial(plaintextKey, ciphertext);
    //    } catch (Exception e) {
    //        throw new RuntimeException("Failed to generate symmetric data key", e);
    //    }
    //}
}
