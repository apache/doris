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

package org.apache.doris.common.util;

import com.google.common.base.Strings;

import org.apache.commons.codec.binary.Base64;

import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class SymmetricEncryption {
    private static final String ALGORITHM = "AES/CFB/PKCS5Padding";
    private static final String AES = "AES";

    private static Cipher getCipher(int cipherMode, byte[] key, byte[] iv) throws InvalidAlgorithmParameterException,
            InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        SecretKeySpec secretKey = new SecretKeySpec(key, AES);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        cipher.init(cipherMode, secretKey, ivSpec);
        return cipher;
    }

    private static byte[] generateRandomBytes() {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    public static byte[] generateKey() {
        return generateRandomBytes();
    }

    public static byte[] generateIv() {
        return generateRandomBytes();
    }

    // The key and iv are generated using the generateKey() and generateIv()
    public static String encrypt(String strToEncrypt, byte[] key, byte[] iv) {
        if (strToEncrypt == null || key == null || iv == null) {
            return null;
        }
        try {
            Cipher cipher = getCipher(Cipher.ENCRYPT_MODE, key, iv);
            return Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    // key, iv are used in encryption.
    public static String decrypt(String strToDecrypt, byte[] key, byte[] iv) {
        if (Strings.isNullOrEmpty(strToDecrypt) || key == null || iv == null) {
            return null;
        }
        try {
            Cipher cipher = getCipher(Cipher.DECRYPT_MODE, key, iv);
            return new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
