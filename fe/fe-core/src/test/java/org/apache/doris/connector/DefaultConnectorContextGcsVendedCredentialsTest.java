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

package org.apache.doris.connector;

import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

class DefaultConnectorContextGcsVendedCredentialsTest {
    private static final String LOCATION = "gs://fixture-bucket/data.parquet";

    @ParameterizedTest
    @ValueSource(strings = {"gs.oauth2.token", "gcs.oauth2.token",
            "gs.oauth2.token-expires-at", "gcs.oauth2.token-expires-at"})
    void nullOAuthFieldCannotBecomeAnEmptyOverlayOrUseStaticStorage(String key) {
        AtomicInteger staticLookups = new AtomicInteger();
        DefaultConnectorContext context = context(staticLookups);
        Map<String, String> token = new HashMap<>();
        token.put(key, null);
        assertRejected(() -> context.vendStorageCredentials(token), key);
        assertRejected(() -> context.normalizeStorageUri(LOCATION, token), key);
        assertRejected(() -> context.getBackendFileType(LOCATION, token), key);
        UnaryOperator<String> normalizer = context.newStorageUriNormalizer(token);
        Assertions.assertNull(normalizer.apply(null));
        Assertions.assertEquals("", normalizer.apply(""));
        assertRejected(() -> normalizer.apply(LOCATION), key);
        assertRejected(() -> normalizer.apply(LOCATION), key);
        Assertions.assertEquals(0, staticLookups.get(), "invalid credentials must not select a base identity");
    }

    @Test
    void absentCredentialsRemainAnEmptyOverlay() {
        AtomicInteger staticLookups = new AtomicInteger();
        DefaultConnectorContext context = context(staticLookups);
        Assertions.assertTrue(context.vendStorageCredentials(null).isEmpty());
        Assertions.assertTrue(context.vendStorageCredentials(Collections.emptyMap()).isEmpty());
        Assertions.assertEquals(0, staticLookups.get());
    }

    @Test
    void flattenedSdkTokensAreValidatedAtTheirSelectedFileIoPath() {
        // A broader invalid scope may follow a valid narrow scope in the flattened map.
        // Only FileIO has the path needed to decide which token actually applies.
        Assertions.assertTrue(context(new AtomicInteger()).vendStorageCredentials(
                Map.of("gcs.oauth2.token", " ")).isEmpty());
    }

    @Test
    void validGcsHmacStillProducesBackendCredentials() {
        Map<String, String> overlay = context(new AtomicInteger()).vendStorageCredentials(Map.of(
                "gs.endpoint", "https://storage.googleapis.com", "gs.access_key", "fixture-key",
                "gs.secret_key", "fixture-secret"));
        Assertions.assertEquals("fixture-key", overlay.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("fixture-secret", overlay.get("AWS_SECRET_KEY"));
    }

    private static DefaultConnectorContext context(AtomicInteger staticLookups) {
        return new DefaultConnectorContext("fixture", 1L, () -> new ExecutionAuthenticator() {}, () -> {
            staticLookups.incrementAndGet();
            throw new AssertionError("static credentials were accessed");
        });
    }

    private static void assertRejected(Runnable call, String key) {
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class, call::run);
        Assertions.assertEquals("Invalid vended GCS credential: " + key, failure.getMessage());
        Assertions.assertNull(failure.getCause(), "credential input must not escape through a parser cause");
    }
}
