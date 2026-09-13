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

package org.apache.doris.filesystem.azure;

import org.apache.doris.foundation.property.StoragePropertiesException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Iceberg's account-scoped SAS input, parsed before applying any catalog defaults. */
final class AzureVendedSas {
    private static final String TOKEN_PREFIX = "adls.sas-token.";
    private static final String EXPIRY_PREFIX = "adls.sas-token-expires-at-ms.";

    private final AzureAccountHost accountHost;
    private final AzureSasToken token;
    private final Map<String, String> properties;

    private AzureVendedSas(String accountHost, String token, String expiry, Map<String, String> properties) {
        this.accountHost = AzureAccountHost.parse(accountHost);
        Long expiryMs = null;
        if (expiry != null) {
            try {
                expiryMs = Long.parseLong(expiry.trim());
            } catch (NumberFormatException e) {
                throw new StoragePropertiesException("Invalid Azure ADLS SAS expiry value");
            }
        }
        this.token = AzureSasToken.of(token, expiryMs);
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    static Optional<AzureVendedSas> parse(Map<String, String> credentials) {
        String accountHost = null;
        String token = null;
        String expiry = null;
        Map<String, String> matched = new HashMap<>();
        for (Map.Entry<String, String> entry : credentials.entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            String lowerKey = key.toLowerCase(Locale.ROOT);
            boolean isToken = lowerKey.startsWith(TOKEN_PREFIX);
            boolean isExpiry = lowerKey.startsWith(EXPIRY_PREFIX);
            if (!isToken && !isExpiry) {
                continue;
            }
            String host = parseHost(key.substring(isToken ? TOKEN_PREFIX.length() : EXPIRY_PREFIX.length()));
            if (accountHost != null && !accountHost.equals(host)) {
                throw new StoragePropertiesException(
                        "Azure ADLS SAS token and expiry must refer to one account host");
            }
            accountHost = host;
            String value = entry.getValue();
            if (value == null || value.isBlank()) {
                throw new StoragePropertiesException("Azure ADLS SAS token and expiry must not be empty");
            }
            if (isToken) {
                if (token != null && !token.equals(value)) {
                    throw new StoragePropertiesException("Conflicting Azure ADLS SAS tokens for one account host");
                }
                token = value;
            } else {
                if (expiry != null && !expiry.equals(value)) {
                    throw new StoragePropertiesException("Conflicting Azure ADLS SAS expiry values");
                }
                expiry = value;
            }
            matched.put(key, value);
        }
        if (accountHost == null) {
            if (credentials.keySet().stream().anyMatch("adls.token"::equalsIgnoreCase)) {
                throw new StoragePropertiesException(
                        "Azure vended access tokens are not supported; an account-scoped SAS token is required");
            }
            return Optional.empty();
        }
        if (token == null) {
            throw new StoragePropertiesException("Azure ADLS SAS expiry was supplied without a SAS token");
        }
        return Optional.of(new AzureVendedSas(accountHost, token, expiry, matched));
    }

    private static String parseHost(String host) {
        try {
            // The property suffix is an account host, not a URL or an object path. Reject query,
            // userinfo and port syntax here without including the credential-bearing input.
            URI uri = new URI("https://" + host);
            if (uri.getHost() == null || !host.equalsIgnoreCase(uri.getHost())) {
                throw new StoragePropertiesException("Invalid Azure ADLS SAS account host");
            }
            // DFS and Blob address the same account; only canonicalize the grouping key.
            // Retain the original property names for Iceberg's exact-host credential lookup.
            return AzureAccountHost.parse(uri.getHost().toLowerCase(Locale.ROOT)).dfsHost();
        } catch (URISyntaxException e) {
            throw new StoragePropertiesException("Invalid Azure ADLS SAS account host");
        }
    }

    AzureAccountHost accountHost() {
        return accountHost;
    }

    AzureSasToken token() {
        return token;
    }

    Map<String, String> properties() {
        return properties;
    }
}
