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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorSecuritySurface;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Sanitizes provider errors with the credentials captured by the failing operation. */
final class LanceErrorMessages {
    private LanceErrorMessages() {
    }

    private static final int MAX_PROVIDER_MESSAGE_BYTES = 1024;
    // Namespaces can vend options for any Lance provider, including pass-through Azure/GCS aliases.
    // Use the same case-insensitive credential markers as catalog masking, across the whole option map.
    private static final ConnectorSecuritySurface RUNTIME_SECURITY_SURFACE =
            ConnectorSecuritySurface.ofSensitiveKeyPrefix("", "key", "secret", "password", "token",
                    "credential", "authorization", "cookie", "sas", "service_account");

    /**
     * A connector error for {@code error}, sanitized. The cause is a stand-in carrying the sanitized text
     * only: the provider's own exception may name a credential, and the engine logs the causes it is
     * handed.
     */
    static DorisConnectorException failure(String prefix, Throwable error, String uri,
            Map<String, String> options, List<String> catalogSecrets) {
        String message = sanitize(error, uri, options, catalogSecrets);
        RuntimeException cause = error instanceof IllegalArgumentException
                ? new IllegalArgumentException(message) : new RuntimeException(message);
        return new DorisConnectorException(prefix + ": " + message, cause);
    }

    static String sanitize(Throwable throwable, String datasetUri,
            Map<String, String> runtimeStorageOptions, List<String> catalogSecrets) {
        String message = ExceptionUtils.getRootCauseMessage(throwable);
        Map<String, String> nonNullStorageOptions = runtimeStorageOptions == null
                ? Collections.emptyMap() : runtimeStorageOptions;
        List<String> sensitiveValues = new ArrayList<>(catalogSecrets);
        nonNullStorageOptions.forEach((key, value) -> {
            if (RUNTIME_SECURITY_SURFACE.sensitiveKeyPrefixes().stream().anyMatch(prefix -> prefix.covers(key))) {
                sensitiveValues.add(value);
            }
        });
        sensitiveValues.add(datasetUri);
        sensitiveValues.removeIf(StringUtils::isEmpty);
        sensitiveValues.sort((left, right) -> Integer.compare(right.length(), left.length()));
        for (String sensitiveValue : sensitiveValues) {
            message = message.replace(sensitiveValue, "***");
        }
        return truncateUtf8(removeControlCharacters(message), MAX_PROVIDER_MESSAGE_BYTES);
    }

    private static String removeControlCharacters(String value) {
        StringBuilder sanitized = new StringBuilder(value.length());
        value.codePoints().filter(codePoint -> !Character.isISOControl(codePoint))
                .forEach(sanitized::appendCodePoint);
        return sanitized.toString();
    }

    private static String truncateUtf8(String value, int maxBytes) {
        if (value.getBytes(StandardCharsets.UTF_8).length <= maxBytes) {
            return value;
        }
        int end = 0;
        int bytes = 0;
        while (end < value.length()) {
            int codePoint = value.codePointAt(end);
            int codePointBytes = new String(Character.toChars(codePoint))
                    .getBytes(StandardCharsets.UTF_8).length;
            if (bytes + codePointBytes > maxBytes) {
                break;
            }
            bytes += codePointBytes;
            end += Character.charCount(codePoint);
        }
        return value.substring(0, end);
    }

}
