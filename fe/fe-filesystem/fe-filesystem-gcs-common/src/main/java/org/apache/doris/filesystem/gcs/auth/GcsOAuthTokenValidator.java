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

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

/** Validates explicit OAuth fields without loading credentials or consuming SDK refresh state. */
public final class GcsOAuthTokenValidator {
    private static final String[][] FIELDS = {
            {"gs.oauth2.token", "gs.oauth2.token-expires-at"},
            {"gcs.oauth2.token", "gcs.oauth2.token-expires-at"}
    };
    // RFC 6750 bearer-token syntax; never trim a token into a different credential.
    private static final Pattern TOKEN = Pattern.compile("[a-zA-Z0-9\\-._~+/]+=*");

    private GcsOAuthTokenValidator() {
    }

    public static void validate(Map<String, String> properties) {
        validate(properties, Collections.emptyMap());
    }

    public static void validate(Map<String, String> properties, Map<String, String> overrides) {
        for (String[] fields : FIELDS) {
            String tokenKey = fields[0];
            String expiryKey = fields[1];
            boolean hasToken = properties.containsKey(tokenKey) || overrides.containsKey(tokenKey);
            if (hasToken) {
                String token = value(properties, overrides, tokenKey);
                if (token == null || !TOKEN.matcher(token).matches()) {
                    throw invalid(tokenKey);
                }
            }
            if (properties.containsKey(expiryKey) || overrides.containsKey(expiryKey)) {
                try {
                    // A past expiry remains valid input: the SDK may refresh it before the next request.
                    Long.parseLong(value(properties, overrides, expiryKey));
                } catch (NumberFormatException e) {
                    throw invalid(expiryKey);
                }
            }
        }
    }

    private static String value(Map<String, String> properties, Map<String, String> overrides, String key) {
        return overrides.containsKey(key) ? overrides.get(key) : properties.get(key);
    }

    private static IllegalArgumentException invalid(String key) {
        return new IllegalArgumentException("Invalid vended GCS credential: " + key);
    }
}
