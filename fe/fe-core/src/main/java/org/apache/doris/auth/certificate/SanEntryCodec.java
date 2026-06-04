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

package org.apache.doris.auth.certificate;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class SanEntryCodec {
    private static final Map<String, String> CANONICAL_TYPES = ImmutableMap.<String, String>builder()
            .put("EMAIL", "email")
            .put("RFC822", "email")
            .put("DNS", "DNS")
            .put("URI", "URI")
            .put("IP", "IP Address")
            .put("IP ADDRESS", "IP Address")
            .build();

    private SanEntryCodec() {
    }

    public static List<String> parseAndNormalize(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> entries = new LinkedHashSet<>();
        for (String part : raw.split(",", -1)) {
            String normalized = normalizeEntry(part);
            if (normalized.isEmpty()) {
                throw invalidEntry(part);
            }
            entries.add(normalized);
        }
        return new ArrayList<>(entries);
    }

    public static boolean containsAll(Collection<String> requiredEntries, Collection<String> presentedEntries) {
        if (requiredEntries == null || presentedEntries == null
                || requiredEntries.isEmpty() || presentedEntries.isEmpty()) {
            return false;
        }
        Set<String> normalizedPresented = new LinkedHashSet<>();
        for (String presented : presentedEntries) {
            String normalized = normalizeEntry(presented);
            if (!normalized.isEmpty()) {
                normalizedPresented.add(normalized);
            }
        }
        for (String required : requiredEntries) {
            String normalizedRequired = normalizeEntry(required);
            if (normalizedRequired.isEmpty() || !normalizedPresented.contains(normalizedRequired)) {
                return false;
            }
        }
        return true;
    }

    public static String toSqlString(Collection<String> entries) {
        if (entries == null || entries.isEmpty()) {
            return "";
        }
        return String.join(", ", entries);
    }

    public static String normalizeEntry(String entry) {
        if (entry == null) {
            return "";
        }
        String trimmed = entry.trim();
        if (trimmed.isEmpty()) {
            return "";
        }

        int colon = trimmed.indexOf(':');
        if (colon <= 0 || colon == trimmed.length() - 1) {
            return "";
        }

        String rawType = trimmed.substring(0, colon).trim().toUpperCase(Locale.ROOT);
        String canonicalType = CANONICAL_TYPES.get(rawType);
        if (canonicalType == null) {
            return "";
        }

        String value = trimmed.substring(colon + 1).trim();
        if (value.isEmpty()) {
            return "";
        }
        while (value.endsWith(".")) {
            value = value.substring(0, value.length() - 1);
        }
        return canonicalType + ":" + value;
    }

    private static IllegalArgumentException invalidEntry(String rawEntry) {
        String entry = rawEntry == null ? "" : rawEntry.trim();
        if (entry.isEmpty()) {
            return new IllegalArgumentException("SAN value contains empty entry");
        }

        int colon = entry.indexOf(':');
        if (colon <= 0 || colon == entry.length() - 1) {
            return new IllegalArgumentException(
                    String.format("Invalid SAN entry format '%s', expected '<type>:<value>'", entry));
        }

        String rawType = entry.substring(0, colon).trim();
        if (rawType.isEmpty()) {
            return new IllegalArgumentException(
                    String.format("Invalid SAN entry format '%s', expected '<type>:<value>'", entry));
        }

        if (!CANONICAL_TYPES.containsKey(rawType.toUpperCase(Locale.ROOT))) {
            return new IllegalArgumentException(String.format(
                    "Unsupported SAN entry type '%s' in '%s'. Supported types are: email, DNS, URI, IP Address",
                    rawType, entry));
        }

        return new IllegalArgumentException(
                String.format("Invalid SAN entry format '%s', expected '<type>:<value>'", entry));
    }
}
