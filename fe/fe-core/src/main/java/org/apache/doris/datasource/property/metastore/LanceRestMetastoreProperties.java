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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.lance.namespace.LanceNamespace;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Properties for a Lance REST namespace catalog. */
public class LanceRestMetastoreProperties extends AbstractLanceProperties {
    public static final String REST_URI = "lance.rest.uri";
    public static final String REST_SECURITY_TYPE = "lance.rest.security.type";
    public static final String REST_BEARER_TOKEN = "lance.rest.bearer-token";
    public static final String REST_API_KEY = "lance.rest.api-key";
    public static final String REST_HEADER_PREFIX = "lance.rest.header.";

    private static final String REST_SECURITY_NONE = "none";
    private static final String REST_SECURITY_BEARER = "bearer";
    private static final String REST_SECURITY_API_KEY = "api_key";
    private static final Pattern HTTP_HEADER_NAME =
            Pattern.compile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$");

    @ConnectorProperty(
            names = {REST_URI},
            required = false,
            description = "The HTTP or HTTPS endpoint of the Lance REST namespace service."
    )
    private String restUri;

    @ConnectorProperty(
            names = {REST_SECURITY_TYPE},
            required = false,
            description = "REST authentication type: none, bearer, or api_key. Default: none."
    )
    private String securityType = REST_SECURITY_NONE;

    @ConnectorProperty(
            names = {REST_BEARER_TOKEN},
            required = false,
            sensitive = true,
            description = "Bearer token used when lance.rest.security.type is bearer."
    )
    private String bearerToken;

    @ConnectorProperty(
            names = {REST_API_KEY},
            required = false,
            sensitive = true,
            description = "API key used when lance.rest.security.type is api_key."
    )
    private String apiKey;

    public LanceRestMetastoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getLanceCatalogType() {
        return LANCE_REST;
    }

    @Override
    public LanceNamespace createNamespace(
            BufferAllocator allocator, Map<String, String> javaStorageOptions) {
        Map<String, String> namespaceProperties = new HashMap<>();
        namespaceProperties.put("uri", normalizedRestUri());
        namespaceProperties.put("delimiter", getNamespaceDelimiter());

        origProps.forEach((key, value) -> {
            if (key.startsWith(REST_HEADER_PREFIX)) {
                namespaceProperties.put(
                        "header." + key.substring(REST_HEADER_PREFIX.length()), value);
            }
        });
        if (REST_SECURITY_BEARER.equals(securityType)) {
            namespaceProperties.put("header.Authorization", "Bearer " + bearerToken);
        } else if (REST_SECURITY_API_KEY.equals(securityType)) {
            namespaceProperties.put("header.x-api-key", apiKey);
        }
        return LanceNamespace.connect("rest", namespaceProperties, allocator);
    }

    public String getRestUri() {
        return normalizedRestUri();
    }

    public String getSecurityType() {
        return securityType;
    }

    @Override
    protected void validateCatalogProperties() {
        restUri = origProps.get(REST_URI);
        securityType = origProps.getOrDefault(REST_SECURITY_TYPE, REST_SECURITY_NONE);
        bearerToken = origProps.get(REST_BEARER_TOKEN);
        apiKey = origProps.get(REST_API_KEY);
        if (origProps.containsKey(LanceFileSystemMetastoreProperties.WAREHOUSE)) {
            throw new IllegalArgumentException(
                    "Property 'warehouse' is not valid for Lance REST catalog");
        }
        if (StringUtils.isBlank(restUri)) {
            throw new IllegalArgumentException(
                    "Missing required property '" + REST_URI + "' for Lance REST catalog");
        }
        validateRestUri(restUri);

        securityType = securityType.trim().toLowerCase(Locale.ROOT);
        boolean bearerTokenConfigured = origProps.containsKey(REST_BEARER_TOKEN);
        boolean apiKeyConfigured = origProps.containsKey(REST_API_KEY);
        boolean hasBearerToken = StringUtils.isNotBlank(bearerToken);
        boolean hasApiKey = StringUtils.isNotBlank(apiKey);
        switch (securityType) {
            case REST_SECURITY_NONE:
                if (bearerTokenConfigured || apiKeyConfigured) {
                    throw new IllegalArgumentException("Lance REST security type 'none' cannot configure '"
                            + REST_BEARER_TOKEN + "' or '" + REST_API_KEY + "'");
                }
                break;
            case REST_SECURITY_BEARER:
                if (!hasBearerToken || apiKeyConfigured) {
                    throw new IllegalArgumentException("Lance REST security type 'bearer' requires only '"
                            + REST_BEARER_TOKEN + "'");
                }
                validateCredentialHeaderValue(REST_BEARER_TOKEN, bearerToken);
                break;
            case REST_SECURITY_API_KEY:
                if (!hasApiKey || bearerTokenConfigured) {
                    throw new IllegalArgumentException("Lance REST security type 'api_key' requires only '"
                            + REST_API_KEY + "'");
                }
                validateCredentialHeaderValue(REST_API_KEY, apiKey);
                break;
            default:
                throw new IllegalArgumentException("Property '" + REST_SECURITY_TYPE
                        + "' must be 'none', 'bearer', or 'api_key'");
        }

        Set<String> supportedKeys = new HashSet<>(Arrays.asList(
                REST_URI, REST_SECURITY_TYPE, REST_BEARER_TOKEN, REST_API_KEY));
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("lance.rest.") || supportedKeys.contains(key)) {
                continue;
            }
            if (!key.startsWith(REST_HEADER_PREFIX)) {
                throw new IllegalArgumentException("Unsupported Lance REST property '" + key + "'");
            }
            validateRestHeader(key.substring(REST_HEADER_PREFIX.length()), entry.getValue());
        }
    }

    private static void validateRestHeader(String headerName, String headerValue) {
        if (StringUtils.isBlank(headerName) || !HTTP_HEADER_NAME.matcher(headerName).matches()) {
            throw new IllegalArgumentException("Invalid HTTP header name in property '"
                    + REST_HEADER_PREFIX + headerName + "'");
        }
        if ("authorization".equalsIgnoreCase(headerName)
                || "x-api-key".equalsIgnoreCase(headerName)) {
            throw new IllegalArgumentException("Authentication header '" + headerName
                    + "' must be configured through '" + REST_SECURITY_TYPE + "'");
        }
        if (headerValue == null || headerValue.indexOf('\r') >= 0 || headerValue.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("Invalid HTTP header value in property '"
                    + REST_HEADER_PREFIX + headerName + "'");
        }
    }

    private static void validateCredentialHeaderValue(String propertyName, String value) {
        if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(
                    "Invalid HTTP credential value in property '" + propertyName + "'");
        }
    }

    private static void validateRestUri(String value) {
        final URI uri;
        try {
            uri = URI.create(value.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid Lance REST URI in property '" + REST_URI + "'", e);
        }
        String scheme = uri.getScheme();
        if (scheme == null
                || (!("http".equalsIgnoreCase(scheme)) && !("https".equalsIgnoreCase(scheme)))) {
            throw new IllegalArgumentException(
                    "Property '" + REST_URI + "' must use http or https");
        }
        if (StringUtils.isBlank(uri.getRawAuthority()) || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException("Property '" + REST_URI
                    + "' must contain an authority and cannot contain user-info, query, or fragment");
        }
    }

    private String normalizedRestUri() {
        String uri = restUri.trim();
        while (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length() - 1);
        }
        return uri;
    }
}
