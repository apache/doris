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

package org.apache.doris.filesystem.gcs.auth;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class GcsOAuthTokenValidatorTest {
    @ParameterizedTest
    @ValueSource(strings = {"gs.oauth2.", "gcs.oauth2."})
    void validatesPresenceSyntaxAndExpirationWithoutLeakingInput(String prefix) {
        String key = prefix + "token";
        String expiry = prefix + "token-expires-at";
        for (String value : Arrays.asList(null, "", " ", "invalid secret token", "secret\nvalue", "token=middle")) {
            Map<String, String> properties = new HashMap<>();
            properties.put(key, value);
            assertRejected(properties, Map.of(), key);
            assertRejected(Map.of(key, "valid-token"), properties, key);
        }
        for (String value : Arrays.asList(null, "", " ", "secret-value", "9223372036854775808")) {
            Map<String, String> properties = new HashMap<>();
            properties.put(key, "valid-token");
            properties.put(expiry, value);
            assertRejected(properties, Map.of(), expiry);
        }
        GcsOAuthTokenValidator.validate(Map.of(expiry, "4102444800000"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"gs.oauth2.", "gcs.oauth2."})
    void preservesOpaqueTokensScopeOverridesAndSdkRefresh(String prefix) {
        String key = prefix + "token";
        for (String token : Arrays.asList("fixture-token", "opaque+value/with~symbols._-==")) {
            GcsOAuthTokenValidator.validate(Map.of(key, token));
            GcsOAuthTokenValidator.validate(Map.of(key, " "), Map.of(key, token));
            GcsOAuthTokenValidator.validate(Map.of(key, token, prefix + "token-expires-at", "0"));
            GcsOAuthTokenValidator.validate(Map.of(key, token, prefix + "token-expires-at", "4102444800000"));
        }
    }

    @Test
    void absentTokenAndOtherProvidersRemainUnchanged() {
        GcsOAuthTokenValidator.validate(Map.of());
        GcsOAuthTokenValidator.validate(Map.of("gs.access_key", "fixture-key", "gs.secret_key", "fixture-secret"));
        Map<String, String> unrelated = new HashMap<>();
        unrelated.put("s3.session_token", null);
        unrelated.put(null, "ignored");
        GcsOAuthTokenValidator.validate(unrelated);
    }

    private static void assertRejected(Map<String, String> properties, Map<String, String> overrides, String key) {
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> GcsOAuthTokenValidator.validate(properties, overrides));
        Assertions.assertEquals("Invalid vended GCS credential: " + key, failure.getMessage());
        Assertions.assertNull(failure.getCause());
    }
}
