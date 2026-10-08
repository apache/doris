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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.net.URI;
import java.util.Map;
import java.util.Set;

/** ServiceLoader entry point for the native Delta connector. */
public class DeltaConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return DeltaConnectorProperties.TYPE;
    }

    @Override
    public Set<String> acceptedCreateTableEngineNames() {
        return Set.of(DeltaConnectorProperties.TYPE);
    }

    @Override
    public DeltaConnector create(Map<String, String> properties, ConnectorContext context) {
        validateProperties(properties);
        return new DeltaConnector(properties, context);
    }

    @Override
    public void validateProperties(Map<String, String> properties) {
        validateBooleanProperty(properties, DeltaConnectorProperties.WRITE_ENABLED);
        validateBooleanProperty(properties, DeltaConnectorProperties.CREATE_ENABLED);
        validateBooleanProperty(properties, DeltaConnectorProperties.DROP_ENABLED);
        validateTimeoutProperties(properties);
        String catalogType = DeltaConnectorProperties.catalogType(properties);
        if (Boolean.parseBoolean(properties.getOrDefault(
                DeltaConnectorProperties.DROP_ENABLED, "false"))
                && !Boolean.parseBoolean(properties.getOrDefault(
                DeltaConnectorProperties.WRITE_ENABLED, "false"))) {
            throw new IllegalArgumentException("Delta catalog property '"
                    + DeltaConnectorProperties.DROP_ENABLED + "=true' requires '"
                    + DeltaConnectorProperties.WRITE_ENABLED + "=true'");
        }
        switch (catalogType) {
            case DeltaConnectorProperties.CATALOG_TYPE_PATH:
                if (Boolean.parseBoolean(properties.getOrDefault(
                        DeltaConnectorProperties.DROP_ENABLED, "false"))) {
                    throw new IllegalArgumentException(
                            "Native Delta path catalogs do not support DROP TABLE; "
                                    + "remove the catalog instead of deleting storage data");
                }
                validatePathProperties(properties);
                return;
            case DeltaConnectorProperties.CATALOG_TYPE_UNITY:
                validateUnityProperties(properties);
                return;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Delta catalog type '" + catalogType
                                + "'; expected 'path' or 'unity'");
        }
    }

    private static void validateTimeoutProperties(Map<String, String> properties) {
        DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_CONNECT_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_CONNECT_TIMEOUT_MS);
        DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_READ_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_READ_TIMEOUT_MS);
        DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_CREDENTIAL_MIN_LIFETIME_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_CREDENTIAL_MIN_LIFETIME_MS);
    }

    private static void validatePathProperties(Map<String, String> properties) {
        requireNonBlank(properties, DeltaConnectorProperties.TABLE_PATH);
        requireNonBlank(properties, DeltaConnectorProperties.DATABASE);
        requireNonBlank(properties, DeltaConnectorProperties.TABLE);
        URI tablePath = URI.create(properties.get(DeltaConnectorProperties.TABLE_PATH));
        if (!tablePath.isAbsolute()) {
            throw new IllegalArgumentException("Delta table path must be an absolute URI");
        }
    }

    private static void validateUnityProperties(Map<String, String> properties) {
        requireNonBlank(properties, DeltaConnectorProperties.UNITY_URI);
        requireNonBlank(properties, DeltaConnectorProperties.UNITY_CATALOG);
        String authType = properties.getOrDefault(
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "pat").trim().toLowerCase(
                        java.util.Locale.ROOT);
        switch (authType) {
            case "pat":
                requireNonBlank(properties, DeltaConnectorProperties.UNITY_TOKEN);
                break;
            case "oauth":
                requireNonBlank(properties, DeltaConnectorProperties.UNITY_OAUTH_URI);
                requireNonBlank(properties, DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID);
                requireNonBlank(properties, DeltaConnectorProperties.UNITY_OAUTH_CLIENT_SECRET);
                validateSecureHttpUri(properties.get(DeltaConnectorProperties.UNITY_OAUTH_URI),
                        DeltaConnectorProperties.UNITY_OAUTH_URI);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Unity authentication type '" + authType
                                + "'; expected 'pat' or 'oauth'");
        }
        URI unityUri = validateSecureHttpUri(properties.get(DeltaConnectorProperties.UNITY_URI),
                DeltaConnectorProperties.UNITY_URI);
        if (unityUri.getPath() != null && !unityUri.getPath().isEmpty()
                && !"/".equals(unityUri.getPath())) {
            throw new IllegalArgumentException(
                    "Unity Catalog URI must be the Databricks workspace root without an API path");
        }
        if (unityUri.getUserInfo() != null || unityUri.getQuery() != null
                || unityUri.getFragment() != null) {
            throw new IllegalArgumentException(
                    "Unity Catalog URI must not contain user info, query parameters, or a fragment");
        }
    }

    private static URI validateHttpUri(String value, String property) {
        URI uri = URI.create(value);
        if (!uri.isAbsolute()
                || (!("http".equalsIgnoreCase(uri.getScheme()))
                && !("https".equalsIgnoreCase(uri.getScheme())))) {
            throw new IllegalArgumentException(
                    "Unity property '" + property + "' must be an absolute HTTP or HTTPS URI");
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
            throw new IllegalArgumentException(
                    "Unity property '" + property + "' must include a host");
        }
        if (uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException(
                    "Unity property '" + property
                            + "' must not contain user info, query parameters, or a fragment");
        }
        return uri;
    }

    private static URI validateSecureHttpUri(String value, String property) {
        URI uri = validateHttpUri(value, property);
        if ("http".equalsIgnoreCase(uri.getScheme()) && !isLoopbackHost(uri.getHost())) {
            throw new IllegalArgumentException(
                    "Unity property '" + property
                            + "' must use HTTPS unless the host is loopback");
        }
        return uri;
    }

    private static boolean isLoopbackHost(String host) {
        String normalized = host.toLowerCase(java.util.Locale.ROOT);
        return "localhost".equals(normalized) || "::1".equals(normalized)
                || "[::1]".equals(normalized)
                || normalized.matches("127(?:\\.\\d{1,3}){3}");
    }

    private static void requireNonBlank(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required Delta catalog property '" + key + "'");
        }
    }

    private static void validateBooleanProperty(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value != null && !"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException(
                    "Delta catalog property '" + key + "' must be true or false");
        }
    }
}
