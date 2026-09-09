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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/** Normalized Azure SAS token with optional, validated expiry information. */
public final class AzureSasToken {

    private final String value;
    private final Optional<Instant> expiresAt;

    private AzureSasToken(String value, Optional<Instant> expiresAt) {
        this.value = value;
        this.expiresAt = expiresAt;
    }

    /**
     * Removes only transport prefixes from a SAS token. The signed query contents are not
     * decoded or re-encoded.
     */
    public static AzureSasToken of(String raw, Long explicitExpiryMs) {
        if (raw == null || raw.isBlank()) {
            throw new StoragePropertiesException("Azure SAS token must not be empty");
        }
        String normalized = raw.trim();
        while (normalized.startsWith("?") || normalized.startsWith("&")) {
            normalized = normalized.substring(1);
        }
        if (normalized.isEmpty()) {
            throw new StoragePropertiesException("Azure SAS token must not be empty");
        }
        if (normalized.indexOf('\r') >= 0 || normalized.indexOf('\n') >= 0) {
            throw new StoragePropertiesException("Azure SAS token must not contain a line break");
        }

        Optional<Instant> explicitExpiry = Optional.ofNullable(explicitExpiryMs)
                .map(AzureSasToken::instantFromEpochMillis);
        Optional<Instant> tokenExpiry = findTokenExpiry(normalized);
        Optional<Instant> effectiveExpiry = minExpiry(explicitExpiry, tokenExpiry);
        return new AzureSasToken(normalized, effectiveExpiry);
    }

    public String value() {
        return value;
    }

    public Optional<Instant> expiresAt() {
        return expiresAt;
    }

    public boolean isExpired(Clock clock) {
        return expiresAt.map(expiry -> !expiry.isAfter(Instant.now(clock))).orElse(false);
    }

    private static Instant instantFromEpochMillis(Long expiryMs) {
        if (expiryMs <= 0) {
            throw new StoragePropertiesException("Azure SAS expiry must be a positive Unix timestamp");
        }
        try {
            return Instant.ofEpochMilli(expiryMs);
        } catch (DateTimeException | ArithmeticException e) {
            throw new StoragePropertiesException("Invalid Azure SAS expiry value", e);
        }
    }

    private static Optional<Instant> findTokenExpiry(String token) {
        for (String field : token.split("&", -1)) {
            int separator = field.indexOf('=');
            if (separator <= 0 || !field.substring(0, separator).equals("se")) {
                continue;
            }
            String encodedExpiry = field.substring(separator + 1);
            if (encodedExpiry.isEmpty()) {
                throw new StoragePropertiesException("Azure SAS credential has an empty expiry");
            }
            String decodedExpiry = URLDecoder.decode(encodedExpiry, StandardCharsets.UTF_8);
            try {
                return Optional.of(OffsetDateTime.parse(
                        decodedExpiry, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant());
            } catch (DateTimeParseException e) {
                try {
                    return Optional.of(Instant.parse(decodedExpiry));
                } catch (DateTimeParseException ignored) {
                    throw new StoragePropertiesException("Azure SAS credential has an invalid expiry", e);
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Instant> minExpiry(Optional<Instant> first, Optional<Instant> second) {
        if (first.isEmpty()) {
            return second;
        }
        if (second.isEmpty()) {
            return first;
        }
        return Optional.of(first.get().isBefore(second.get()) ? first.get() : second.get());
    }
}
