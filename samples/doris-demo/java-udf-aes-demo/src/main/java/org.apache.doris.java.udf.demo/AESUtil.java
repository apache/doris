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

package org.apache.doris.udf.demo;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;

import java.security.SecureRandom;


/**
 * AES encryption and decryption tool class
 *
 * @author zhangfeng
 */
public class AESUtil {
    private static final String defaultCharset = "UTF-8";
    private static final String KEY_AES = "AES";

    /**
     * AES encryption function method
     *
     * @param content
     * @param secret
     * @return
     */
    public static String encrypt(String content, String secret) {
        return doAES(content, secret, Cipher.ENCRYPT_MODE);
    }

    /**
     * AES decryption function method
     *
     * @param content
     * @param secret
     * @return
     */
    public static String decrypt(String content, String secret) {
        return doAES(content, secret, Cipher.DECRYPT_MODE);
    }

    /**
     * encryption and decryption
     *
     * @param content
     * @param secret
     * @param mode
     * @return
     */
    private static String doAES(String content, String secret, int mode) {
        try {
            if (StringUtils.isBlank(content) || StringUtils.isBlank(secret)) {
                return null;
            }
            //Determine whether to encrypt or decrypt
            boolean encrypt = mode == Cipher.ENCRYPT_MODE;
            byte[] data;

            //1.Construct a key generator, specified as the AES algorithm, case-insensitive
            KeyGenerator kgen = KeyGenerator.getInstance(KEY_AES);
            SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
            //2. Initialize the key generator according to the ecnodeRules rules
            //Generate a 128-bit random source, based on the incoming byte array
            secureRandom.setSeed(secret.getBytes());
            //Generate a 128-bit random source, based on the incoming byte array
            kgen.init(128, secureRandom);
            //3.generate the original symmetric key
            SecretKey secretKey = kgen.generateKey();
            //4.Get the byte array of the original symmetric key
            byte[] enCodeFormat = secretKey.getEncoded();
            //5.Generate AES key from byte array
            SecretKeySpec keySpec = new SecretKeySpec(enCodeFormat, KEY_AES);
            //6.According to the specified algorithm AES self-generated cipher
            Cipher cipher = Cipher.getInstance(KEY_AES);
            //7.Initialize the cipher, the first parameter is encryption (Encrypt_mode) or decryption (Decrypt_mode) operation,
            // the second parameter is the KEY used
            cipher.init(mode, keySpec);

            if (encrypt) {
                data = content.getBytes(defaultCharset);
            } else {
                data = parseHexStr2Byte(content);
            }
            byte[] result = cipher.doFinal(data);
            if (encrypt) {
                //convert binary to hexadecimal
                return parseByte2HexStr(result);
            } else {
                return new String(result, defaultCharset);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    /**
     * convert binary to hexadecimal
     *
     * @param buf
     * @return
     */
    public static String parseByte2HexStr(byte buf[]) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buf.length; i++) {
            String hex = Integer.toHexString(buf[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            sb.append(hex.toUpperCase());
        }
        return sb.toString();
    }

    /**
     * Convert hexadecimal to binary
     *
     * @param hexStr
     * @return
     */
    public static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1) {
            return null;
        }
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
            result[i] = (byte) (high * 16 + low);
        }
        return result;
    }

}