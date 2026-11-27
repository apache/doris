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

package org.apache.doris.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.Provider;
import java.util.HexFormat;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

class CosBouncyCastleTest {
    @ParameterizedTest
    @CsvSource({
            "1,user@example.com.,true",
            "6,https://example.com./,true",
            "1,user@example.com,true",
            "6,https://example.com/,true",
            "1,user@safe.example,false",
            "6,https://safe.example/,false"
    })
    void sdkProviderEnforcesExcludedHosts(int tag, String candidate, boolean excluded) throws Exception {
        String prefix = provider().getClass().getPackageName().replace(".jce.provider", "") + ".";
        Class<?> nameType = Class.forName(prefix + "asn1.x509.GeneralName");
        Class<?> subtreeType = Class.forName(prefix + "asn1.x509.GeneralSubtree");
        Class<?> validatorType = Class.forName(prefix + "jce.provider.PKIXNameConstraintValidator");
        Object validator = validatorType.getConstructor().newInstance();
        Object constraint = nameType.getConstructor(int.class, String.class).newInstance(tag, "example.com");
        Object subtree = subtreeType.getConstructor(nameType).newInstance(constraint);
        validatorType.getMethod("addExcludedSubtree", subtreeType).invoke(validator, subtree);
        Object name = nameType.getConstructor(int.class, String.class).newInstance(tag, candidate);
        if (excluded) {
            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> validatorType.getMethod("checkExcluded", nameType).invoke(validator, name));
            Assertions.assertEquals(prefix + "jce.provider.PKIXNameConstraintValidatorException",
                    failure.getCause().getClass().getName());
        } else {
            Assertions.assertDoesNotThrow(
                    () -> validatorType.getMethod("checkExcluded", nameType).invoke(validator, name));
        }
    }

    @Test
    void sdkProviderComputesSm3() throws Exception {
        byte[] digest = MessageDigest.getInstance("SM3", provider())
                .digest("abc".getBytes(StandardCharsets.US_ASCII));
        Assertions.assertEquals("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
                new BigInteger(1, digest).toString(16));
    }

    @Test
    void preservesAesKeyWrappingAndRejectsTampering() throws Exception {
        HexFormat hex = HexFormat.of();
        SecretKeySpec wrappingKey = new SecretKeySpec(hex.parseHex("000102030405060708090a0b0c0d0e0f"), "AES");
        byte[] keyBytes = hex.parseHex("00112233445566778899aabbccddeeff");
        Cipher cipher = Cipher.getInstance("AESWrap", provider());
        cipher.init(Cipher.WRAP_MODE, wrappingKey);
        // RFC 3394 section 4.1 fixes the expected bytes independently of the provider implementation.
        byte[] wrapped = cipher.wrap(new SecretKeySpec(keyBytes, "AES"));
        Assertions.assertArrayEquals(hex.parseHex("1fa68b0a8112b447aef34bd8fb5a7b829d3e862371d2cfe5"), wrapped);
        cipher.init(Cipher.UNWRAP_MODE, wrappingKey);
        Assertions.assertArrayEquals(keyBytes, cipher.unwrap(wrapped, "AES", Cipher.SECRET_KEY).getEncoded());
        wrapped[0] ^= 1;
        Assertions.assertThrows(InvalidKeyException.class, () -> cipher.unwrap(wrapped, "AES", Cipher.SECRET_KEY));
    }

    private static Provider provider() throws ReflectiveOperationException {
        Class<?> runtime = Class.forName("com.qcloud.cos.internal.crypto.CryptoRuntime");
        Field providerName = runtime.getDeclaredField("BC_PROVIDER_FQCN");
        providerName.setAccessible(true);
        return (Provider) Class.forName((String) providerName.get(null)).getConstructor().newInstance();
    }
}
