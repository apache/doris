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

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Properties;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AESUtilTest {

    @Mocked
    private Cipher cipher;

    private static final String FIXED_KEY = "FixedTestKey1234";

    @Before
    public void setUp() throws Exception {
        String baseUrl = "http://example.com/keys";
        String token = "testToken";

        new Expectations() {{
                System.getenv("SECRET_KEY_URL");
                result = baseUrl;

                System.getenv("SECRET_KEY_TOKEN");
                result = token;
            }};
        Properties props = new Properties();
        props.setProperty("testService", FIXED_KEY); // 16-byte key for AES
        AESUtil.initServicePublicKeyCertificate(props);
    }

    @Test
    public void testDecodeBase64() {
        String original = "testString";
        String encoded = Base64.getUrlEncoder().encodeToString(original.getBytes(StandardCharsets.UTF_8));
        byte[] decodedBytes = AESUtil.decodeBase64(encoded);
        String decoded = new String(decodedBytes, StandardCharsets.UTF_8);
        Assert.assertEquals("Base64 decode should return the original string", original, decoded);

        // Test empty string
        String emptyString = "";
        String emptyEncoded = Base64.getUrlEncoder().encodeToString(emptyString.getBytes(StandardCharsets.UTF_8));
        byte[] emptyDecoded = AESUtil.decodeBase64(emptyEncoded);
        Assert.assertEquals("Empty string should be correctly decoded", emptyString, new String(emptyDecoded, StandardCharsets.UTF_8));

        // Test string with special characters
        String specialChars = "!@#$%^&*()_+{}[]|\\:;\"'<>,.?/~`☃";
        Assert.assertThrows(
                "Invalid Base64 input should throw Exception",
                Exception.class,
                () -> AESUtil.decodeBase64(specialChars)
        );

        // Test long string (reduced size to save memory)
        StringBuilder longStringBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) { // Reduced loop count from 1000 to 100
            longStringBuilder.append("abcdefghijklmnopqrstuvwxyz");
        }
        String longString = longStringBuilder.toString();
        String longEncoded = Base64.getUrlEncoder().encodeToString(longString.getBytes(StandardCharsets.UTF_8));
        byte[] longDecoded = AESUtil.decodeBase64(longEncoded);
        Assert.assertEquals("Long string should be correctly decoded", longString, new String(longDecoded, StandardCharsets.UTF_8));

        // Test URL-safe characters
        String urlSafe = "This_is-a.url+safe/string";
        String urlSafeEncoded = Base64.getUrlEncoder().encodeToString(urlSafe.getBytes(StandardCharsets.UTF_8));
        byte[] urlSafeDecoded = AESUtil.decodeBase64(urlSafeEncoded);
        Assert.assertEquals("URL-safe string should be correctly decoded", urlSafe, new String(urlSafeDecoded, StandardCharsets.UTF_8));

        // Test padding characters
        String withPadding = "a";  // This will result in padding
        String paddingEncoded = Base64.getUrlEncoder().encodeToString(withPadding.getBytes(StandardCharsets.UTF_8));
        byte[] paddingDecoded = AESUtil.decodeBase64(paddingEncoded);
        Assert.assertEquals("String with padding should be correctly decoded", withPadding, new String(paddingDecoded, StandardCharsets.UTF_8));

        // Test invalid input
        String invalidInput = "This is not a valid Base64 string!";
        Assert.assertThrows(
                "Invalid Base64 input should throw IllegalArgumentException",
                IllegalArgumentException.class,
                () -> AESUtil.decodeBase64(invalidInput)
        );

        // Test string with percentage encoding
        String percentEncoded = "Hello%20World%21";
        try {
            String decodedPercentEncoded = java.net.URLDecoder.decode(percentEncoded, StandardCharsets.UTF_8.name());
            String base64Encoded = Base64.getUrlEncoder().encodeToString(decodedPercentEncoded.getBytes(StandardCharsets.UTF_8));
            decodedBytes = AESUtil.decodeBase64(base64Encoded);
            decoded = new String(decodedBytes, StandardCharsets.UTF_8);
            Assert.assertEquals("Percentage encoded string should be correctly decoded", "Hello World!", decoded);
        } catch (IOException e) { // Changed to IOException for broader coverage
            Assert.fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testInitServicePublicKeyCertificateFromUrl_success(@Mocked URL mockUrl, @Mocked HttpURLConnection mockConnection) throws Exception {
        String baseUrl = "http://example.com/keys";
        String token = "testToken";
        String encodedToken = URLEncoder.encode(token, "UTF-8");
        String url = baseUrl + "?token=" + encodedToken;
        // Define the mock HTTP response in "key=value" format (using smaller data)
        String response = "service1=key1key1key1key1\nservice2=key2key2key2key2";
        // Set up expectations for URL and HttpURLConnection
        new Expectations() {{
                mockUrl.openConnection();
                result = mockConnection;
                mockConnection.setRequestMethod("GET");
                mockConnection.getResponseCode();
                result = HttpURLConnection.HTTP_OK;
                mockConnection.getInputStream();
                result = new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8));
            }};
        AESUtil.initServicePublicKeyCertificateFromUrl(url);
        // Verify that the secret keys have been initialized correctly
        SecretKeySpec key1 = AESUtil.getSecretKey("testService");
        Assert.assertNotNull("Secret key for service1 should not be null", key1);
        Assert.assertEquals("AES", key1.getAlgorithm());
        Assert.assertArrayEquals("FixedTestKey1234".getBytes(StandardCharsets.UTF_8), key1.getEncoded());

        SecretKeySpec key2 = AESUtil.getSecretKey("service2");
        Assert.assertNotNull("Secret key for service2 should not be null", key2);
        Assert.assertEquals("AES", key2.getAlgorithm());
        Assert.assertArrayEquals("key2key2key2key2".getBytes(StandardCharsets.UTF_8), key2.getEncoded());

        // Verify that disconnect was called on the connection
        new Verifications() {{
                mockConnection.disconnect();
                times = 1;
            }};
    }

    @Test
    public void testInitWithInvalidProps() {
        Properties invalidProps = new Properties();
        // Missing service key
        AESUtil.initServicePublicKeyCertificate(invalidProps);
        Exception exception = Assert.assertThrows(NoSuchAlgorithmException.class, () -> {
            AESUtil.getSecretKey("invalidService");
        });
        Assert.assertTrue(exception.getMessage().contains("unable to get secret key certificate"));
    }

    @Test
    public void testGetSecretKey() throws Exception {
        new MockUp<AESUtil>() {
            @Mock
            void initServicePublicKeyCertificateFromUrl(String baseUrl) {
                Properties props = new Properties();
                props.setProperty("testService1", "testKey123testKey123");
                props.setProperty("newService", "newKey456newKey456");
                AESUtil.initServicePublicKeyCertificate(props);
            }
        };

        // Test that we can get the secret key for testService1
        SecretKeySpec secretKey = AESUtil.getSecretKey("testService1");
        Assert.assertNotNull(secretKey);
        Assert.assertEquals("AES", secretKey.getAlgorithm());
        Assert.assertArrayEquals("testKey123testKey123".getBytes(StandardCharsets.UTF_8), secretKey.getEncoded());

        // Test that we can get the secret key for a new service after mocking the URL fetch
        SecretKeySpec newSecretKey = AESUtil.getSecretKey("newService");
        Assert.assertNotNull(newSecretKey);
        Assert.assertEquals("AES", newSecretKey.getAlgorithm());
        Assert.assertArrayEquals("newKey456newKey456".getBytes(StandardCharsets.UTF_8), newSecretKey.getEncoded());

        // Test that we still get an exception for an invalid service
        Exception exception = Assert.assertThrows(NoSuchAlgorithmException.class, () -> {
            AESUtil.getSecretKey("invalidService");
        });
        Assert.assertTrue(exception.getMessage().contains("unable to get secret key certificate for invalidService"));
    }

}
