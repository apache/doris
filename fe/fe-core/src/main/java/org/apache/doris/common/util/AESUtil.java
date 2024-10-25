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

import org.apache.doris.thrift.TBDPUserInfo;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

public class AESUtil {

    private static final Logger LOG = LogManager.getLogger(AESUtil.class);

    public static final String KEY_ALGORITHM = "AES";

    private static String SECRET_KEY_URL = "";

    private static String SECRET_KEY_TOKEN = "";

    private static ThreadLocal<Map<String, Cipher>> decryptCipherMap = new ThreadLocal<>();

    private static Map<String, SecretKeySpec> serviceToSecretKeyMap = Maps.newHashMap();

    public static byte[] decodeBase64(String key) {
        return Base64.getDecoder().decode(key);
    }

    public static void init(String propFile) throws IOException {
        Properties props = new Properties();
        FileReader reader = null;
        SECRET_KEY_URL = System.getenv("SECRET_KEY_URL");
        SECRET_KEY_TOKEN = System.getenv("SECRET_KEY_TOKEN");

        try {
            reader = new FileReader(propFile);
            props.load(reader);
        } catch (Exception e) {
            LOG.warn("parse lakehouse_auth.conf file failed", e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        initServicePublicKeyCertificate(props);
        initServicePublicKeyCertificateFromUrl(SECRET_KEY_URL);
    }

    public static void initServicePublicKeyCertificate(Properties properties) {
        for (String key : properties.stringPropertyNames()) {
            String secretKeyStr = properties.getProperty(key);
            byte[] secretKeyBytes = secretKeyStr.getBytes(StandardCharsets.UTF_8);
            SecretKeySpec secretKey = new SecretKeySpec(secretKeyBytes, KEY_ALGORITHM);
            serviceToSecretKeyMap.put(key, secretKey);
        }
    }

    public static void initServicePublicKeyCertificateFromUrl(String secretKeyUrl) {
        HttpURLConnection connection = null;
        try {
            URL serviceUrl = new URL(secretKeyUrl);
            connection = (HttpURLConnection) serviceUrl.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", "Bearer " + SECRET_KEY_TOKEN);

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream in = connection.getInputStream()) {
                    Properties properties = new Properties();
                    properties.load(in);
                    initServicePublicKeyCertificate(properties);
                    LOG.info("Init secret key certificate from url: {} successfully", secretKeyUrl);
                }
            } else {
                LOG.warn("Failed to load secret key certificate from url: {}. Response Code: {}",
                        secretKeyUrl, responseCode);
            }
        } catch (IOException e) {
            LOG.warn("Failed to load secret key certificate from url: {}", secretKeyUrl, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public static SecretKeySpec getSecretKey(String serviceName)  throws NoSuchAlgorithmException {
        SecretKeySpec secretKey = serviceToSecretKeyMap.get(serviceName);
        if (secretKey == null) {
            LOG.info("Secret key for service '{}' not found locally. Attempting to fetch from remote store.",
                    serviceName);
            initServicePublicKeyCertificateFromUrl(SECRET_KEY_URL);
            secretKey = serviceToSecretKeyMap.get(serviceName);
            if (secretKey == null) {
                throw new NoSuchAlgorithmException("unable to get secret key certificate for " + serviceName);
            }
        }
        return secretKey;
    }

    public static TBDPUserInfo decrypt(String serviceName, String encryptedText)
            throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, TException,
            IllegalBlockSizeException, BadPaddingException {
        Map<String, Cipher> localDecryptCipherMap = decryptCipherMap.get();
        if (localDecryptCipherMap == null) {
            localDecryptCipherMap = Maps.newHashMap();
            decryptCipherMap.set(localDecryptCipherMap);
        }
        Cipher decryptCipher = localDecryptCipherMap.get(serviceName);
        if (decryptCipher == null) {
            decryptCipher = Cipher.getInstance(KEY_ALGORITHM);
            decryptCipher.init(Cipher.DECRYPT_MODE, getSecretKey(serviceName));
            localDecryptCipherMap.put(serviceName, decryptCipher);
        }
        byte[] decryptedBytes = decryptCipher.doFinal(Base64.getDecoder().decode(
            encryptedText.replace(" ", "+")));
        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
        TBDPUserInfo userInfo = new TBDPUserInfo();
        deserializer.deserialize(userInfo, decryptedBytes);
        return userInfo;
    }
}
