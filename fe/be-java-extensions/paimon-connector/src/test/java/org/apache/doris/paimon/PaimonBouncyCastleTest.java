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

package org.apache.doris.paimon;

import org.apache.paimon.plugin.PluginLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Provider;

class PaimonBouncyCastleTest {
    @ParameterizedTest
    @CsvSource({
            "1,user@example.com.,true",
            "6,https://example.com./,true",
            "1,user@example.com,true",
            "6,https://example.com/,true",
            "1,user@safe.example,false",
            "6,https://safe.example/,false"
    })
    void embeddedProviderEnforcesExcludedHosts(int tag, String candidate, boolean excluded) throws Exception {
        // Paimon's component-only loader cannot inherit a fixed provider from the scanner classpath.
        try (URLClassLoader loader = (URLClassLoader) new PluginLoader("paimon-plugin-s3").submoduleClassLoader()) {
            Class<?> nameType = loader.loadClass("org.bouncycastle.asn1.x509.GeneralName");
            Class<?> subtreeType = loader.loadClass("org.bouncycastle.asn1.x509.GeneralSubtree");
            Class<?> validatorType = loader.loadClass("org.bouncycastle.jce.provider.PKIXNameConstraintValidator");
            Object validator = validatorType.getConstructor().newInstance();
            Object constraint = nameType.getConstructor(int.class, String.class).newInstance(tag, "example.com");
            Object subtree = subtreeType.getConstructor(nameType).newInstance(constraint);
            validatorType.getMethod("addExcludedSubtree", subtreeType).invoke(validator, subtree);
            Object name = nameType.getConstructor(int.class, String.class).newInstance(tag, candidate);
            Method check = validatorType.getMethod("checkExcluded", nameType);
            if (excluded) {
                InvocationTargetException failure = Assertions.assertThrows(
                        InvocationTargetException.class, () -> check.invoke(validator, name));
                Assertions.assertEquals("org.bouncycastle.jce.provider.PKIXNameConstraintValidatorException",
                        failure.getCause().getClass().getName());
            } else {
                Assertions.assertDoesNotThrow(() -> check.invoke(validator, name));
            }
        }
    }

    @Test
    void embeddedProviderRemainsIsolatedAndComputesSm3() throws Exception {
        try (URLClassLoader loader = (URLClassLoader) new PluginLoader("paimon-plugin-s3").submoduleClassLoader()) {
            Class<?> providerType = loader.loadClass("org.bouncycastle.jce.provider.BouncyCastleProvider");
            Assertions.assertSame(loader, providerType.getClassLoader());
            Assertions.assertNotSame(getClass().getClassLoader(), providerType.getClassLoader());
            Provider provider = (Provider) providerType.getConstructor().newInstance();
            byte[] digest = MessageDigest.getInstance("SM3", provider)
                    .digest("abc".getBytes(StandardCharsets.US_ASCII));
            Assertions.assertEquals("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
                    new BigInteger(1, digest).toString(16));
        }
    }
}
