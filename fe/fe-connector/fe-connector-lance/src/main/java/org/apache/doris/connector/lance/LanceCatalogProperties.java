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
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a Lance catalog, bound and checked.
 *
 * <p>The keys and their rules are the ones Lance catalogs have had since 4.1, unchanged: images written by
 * those versions persist the raw property map, and this class must accept it as it is. Settings that are
 * one-per-FE rather than one-per-catalog live in the plugin's {@code lance.conf} ({@link LanceConf}).
 *
 * <p>{@link #of(Map)} touches neither storage nor any {@code org.lance.*} type: it runs at CREATE, again on
 * the merged candidate at ALTER, and every time the connector is rebuilt, including while an FE replays the
 * edit log. Unknown keys are accepted, always: the same map carries engine and storage keys, and ALTER
 * CATALOG can overwrite a key but never remove one. The {@code lance.rest.} keys are the exception: a REST
 * catalog accepts only the ones it knows, and a filesystem catalog none.
 */
public final class LanceCatalogProperties {

    public static final String CATALOG_TYPE = "lance.catalog.type";
    public static final String FILESYSTEM = "filesystem";
    public static final String REST = "rest";

    public static final String WAREHOUSE = "warehouse";

    public static final String NAMESPACE_PARENT = "lance.namespace.parent";
    public static final String NAMESPACE_DELIMITER = "lance.namespace.delimiter";
    public static final String ROOT_DATABASE = "lance.namespace.root_database";
    public static final String DEFAULT_DELIMITER = "$";
    public static final String DEFAULT_ROOT_DATABASE = "default";

    /** Every REST-only key starts with this; a filesystem catalog rejects all of them. */
    public static final String REST_PREFIX = "lance.rest.";
    /** The namespace server the FE sends every namespace request to. */
    public static final String REST_URI = "lance.rest.uri";
    public static final String REST_SECURITY_TYPE = "lance.rest.security.type";
    public static final String REST_BEARER_TOKEN = "lance.rest.bearer-token";
    public static final String REST_API_KEY = "lance.rest.api-key";
    /** {@code lance.rest.header.<name>} sends the HTTP header {@code <name>} with every REST request. */
    public static final String REST_HEADER_PREFIX = "lance.rest.header.";

    private static final String SECURITY_NONE = "none";
    private static final String SECURITY_BEARER = "bearer";
    private static final String SECURITY_API_KEY = "api_key";
    private static final Set<String> REST_KEYS = Set.of(REST_URI, REST_SECURITY_TYPE, REST_BEARER_TOKEN,
            REST_API_KEY);
    /** An RFC 9110 token. */
    private static final Pattern HTTP_HEADER_NAME = Pattern.compile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$");

    /**
     * What the engine's CREATE / ALTER CATALOG gates and the masking of catalog properties need to know: the
     * REST server and S3-compatible storage endpoints the FE connects to, and the secrets. An extra REST
     * header value is sent as written, so the header
     * namespace is declared with the words a credential-bearing header name carries, a cookie included.
     */
    static final ConnectorSecuritySurface SECURITY_SURFACE = ConnectorSecuritySurface.ofSsrfEndpoints(
            REST_URI, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint",
            "glue.endpoint", "aws.glue.endpoint", "oss.endpoint", "OSS_ENDPOINT",
            "dlf.endpoint", "dlf.catalog.endpoint", "fs.oss.endpoint", "cos.endpoint",
            "COS_ENDPOINT", "obs.endpoint", "OBS_ENDPOINT", "uri")
            .withSensitiveKeys(ConnectorPropertiesUtils.getSensitiveKeys(LanceCatalogProperties.class))
            .withSensitiveKeyPrefix(REST_HEADER_PREFIX,
                    "key", "secret", "password", "token", "credential", "authorization", "cookie");

    @ConnectorProperty(names = {CATALOG_TYPE}, required = false,
            description = "The Lance namespace implementation: filesystem or rest. Default: filesystem.")
    private String catalogType = FILESYSTEM;

    @ConnectorProperty(names = {WAREHOUSE}, required = false,
            description = "The local, file, S3, or OSS warehouse containing Lance datasets.")
    private String warehouse;

    @ConnectorProperty(names = {NAMESPACE_PARENT}, required = false,
            description = "The optional Lance namespace under which Doris databases are exposed.")
    private String namespaceParent = "";

    @ConnectorProperty(names = {NAMESPACE_DELIMITER}, required = false,
            description = "The delimiter used in an escaped parent namespace. Default: $.")
    private String namespaceDelimiter = DEFAULT_DELIMITER;

    @ConnectorProperty(names = {ROOT_DATABASE}, required = false,
            description = "The Doris database name representing the catalog's root namespace. Default: default.")
    private String rootDatabase = DEFAULT_ROOT_DATABASE;

    @ConnectorProperty(names = {REST_URI}, required = false,
            description = "The HTTP or HTTPS endpoint of the Lance REST namespace service.")
    private String restUri;

    @ConnectorProperty(names = {REST_SECURITY_TYPE}, required = false,
            description = "REST authentication type: none, bearer, or api_key. Default: none.")
    private String securityType = SECURITY_NONE;

    @ConnectorProperty(names = {REST_BEARER_TOKEN}, required = false, sensitive = true,
            description = "Bearer token used when lance.rest.security.type is bearer.")
    private String bearerToken;

    @ConnectorProperty(names = {REST_API_KEY}, required = false, sensitive = true,
            description = "API key used when lance.rest.security.type is api_key.")
    private String apiKey;

    private final Map<String, String> raw;
    private final Map<String, String> restHeaders = new HashMap<>();
    private List<String> parentNamespace;

    private LanceCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static LanceCatalogProperties of(Map<String, String> properties) {
        LanceCatalogProperties p = new LanceCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        // The binder trims and treats a blank value as unset. These keys keep their value as written: the
        // type is matched trimmed and lower-cased, the warehouse is handed to Lance as is, namespace
        // whitespace and explicit empty values are meaningful to the escaping rules, and the REST URI and
        // credentials are checked as written, so they must be validated rather than silently replaced by
        // defaults.
        p.catalogType = properties.getOrDefault(CATALOG_TYPE, FILESYSTEM).trim().toLowerCase(Locale.ROOT);
        p.warehouse = properties.get(WAREHOUSE);
        p.namespaceParent = properties.getOrDefault(NAMESPACE_PARENT, "");
        p.namespaceDelimiter = properties.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER);
        p.rootDatabase = properties.getOrDefault(ROOT_DATABASE, DEFAULT_ROOT_DATABASE);
        p.restUri = properties.get(REST_URI);
        p.securityType = properties.getOrDefault(REST_SECURITY_TYPE, SECURITY_NONE).trim().toLowerCase(Locale.ROOT);
        p.bearerToken = properties.get(REST_BEARER_TOKEN);
        p.apiKey = properties.get(REST_API_KEY);
        if (!FILESYSTEM.equals(p.catalogType) && !REST.equals(p.catalogType)) {
            throw new IllegalArgumentException("Property '" + CATALOG_TYPE
                    + "' must be 'filesystem' or 'rest', but was '" + p.catalogType + "'");
        }
        p.validateCommonProperties();
        if (FILESYSTEM.equals(p.catalogType)) {
            p.validateFilesystemProperties();
        } else {
            p.validateRestProperties();
        }
        return p;
    }

    private void validateCommonProperties() {
        if (namespaceDelimiter.isEmpty() || namespaceDelimiter.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("Property '" + NAMESPACE_DELIMITER
                    + "' cannot be empty or contain the escape character '\\'");
        }
        if (StringUtils.isBlank(rootDatabase)) {
            throw new IllegalArgumentException("Property '" + ROOT_DATABASE + "' must be non-empty");
        }
        parentNamespace = Collections.unmodifiableList(
                LanceNamespaceName.parseParentNamespace(namespaceParent, namespaceDelimiter));
    }

    private void validateFilesystemProperties() {
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException(
                    "Missing required property 'warehouse' for Lance filesystem catalog");
        }
        validateWarehouse(warehouse);
        for (String key : raw.keySet()) {
            if (key.startsWith(REST_PREFIX)) {
                throw new IllegalArgumentException(
                        "Property '" + key + "' is not valid for Lance filesystem catalog");
            }
        }
    }

    private static void validateWarehouse(String warehouse) {
        final URI uri;
        try {
            uri = URI.create(warehouse);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Lance warehouse URI: " + warehouse, e);
        }
        if (uri.getScheme() == null) {
            Path path = Paths.get(warehouse);
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException(
                        "Local Lance warehouse must be an absolute path: " + warehouse);
            }
            return;
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"file".equals(scheme) && !"s3".equals(scheme) && !"oss".equals(scheme)) {
            throw new IllegalArgumentException("Unsupported Lance filesystem warehouse scheme '" + scheme
                    + "'; supported schemes are local/file, s3, and oss");
        }
        // An object-store root names its bucket in the authority. Lance reads that authority as the
        // bucket and fails deep inside the store when it is absent, so reject the no-authority form
        // here where the message can still name the property.
        if (("s3".equals(scheme) || "oss".equals(scheme)) && StringUtils.isBlank(uri.getAuthority())) {
            throw new IllegalArgumentException(
                    "Lance " + scheme + " warehouse must name a bucket, as in " + scheme
                            + "://bucket/path, but was: " + warehouse);
        }
    }

    private void validateRestProperties() {
        if (raw.containsKey(WAREHOUSE)) {
            throw new IllegalArgumentException("Property 'warehouse' is not valid for Lance REST catalog");
        }
        if (StringUtils.isBlank(restUri)) {
            throw new IllegalArgumentException(
                    "Missing required property '" + REST_URI + "' for Lance REST catalog");
        }
        validateRestUri(restUri);

        // ALTER merges property maps and cannot remove keys; an empty inactive credential clears it.
        boolean bearerTokenConfigured = StringUtils.isNotEmpty(bearerToken);
        boolean apiKeyConfigured = StringUtils.isNotEmpty(apiKey);
        switch (securityType) {
            case SECURITY_NONE:
                if (bearerTokenConfigured || apiKeyConfigured) {
                    throw new IllegalArgumentException("Lance REST security type 'none' cannot configure '"
                            + REST_BEARER_TOKEN + "' or '" + REST_API_KEY + "'");
                }
                break;
            case SECURITY_BEARER:
                if (StringUtils.isBlank(bearerToken) || apiKeyConfigured) {
                    throw new IllegalArgumentException("Lance REST security type 'bearer' requires only '"
                            + REST_BEARER_TOKEN + "'");
                }
                validateCredentialHeaderValue(REST_BEARER_TOKEN, bearerToken);
                break;
            case SECURITY_API_KEY:
                if (StringUtils.isBlank(apiKey) || bearerTokenConfigured) {
                    throw new IllegalArgumentException("Lance REST security type 'api_key' requires only '"
                            + REST_API_KEY + "'");
                }
                validateCredentialHeaderValue(REST_API_KEY, apiKey);
                break;
            default:
                throw new IllegalArgumentException("Property '" + REST_SECURITY_TYPE
                        + "' must be 'none', 'bearer', or 'api_key'");
        }

        for (Map.Entry<String, String> entry : raw.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(REST_PREFIX) || REST_KEYS.contains(key)) {
                continue;
            }
            if (!key.startsWith(REST_HEADER_PREFIX)) {
                throw new IllegalArgumentException("Unsupported Lance REST property '" + key + "'");
            }
            String headerName = key.substring(REST_HEADER_PREFIX.length());
            String headerValue = entry.getValue();
            validateRestHeader(headerName, headerValue);
            // ALTER preserves case-sensitive property keys, but HTTP header names are case-insensitive.
            headerName = headerName.toLowerCase(Locale.ROOT);
            String previous = restHeaders.putIfAbsent(headerName, headerValue);
            if (previous != null && !previous.equals(headerValue)) {
                throw new IllegalArgumentException("Conflicting values for HTTP header '" + headerName
                        + "' in Lance REST properties");
            }
        }
    }

    private static void validateRestUri(String value) {
        final URI uri;
        try {
            uri = URI.create(value.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Lance REST URI in property '" + REST_URI + "'", e);
        }
        String scheme = uri.getScheme();
        if (scheme == null || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("Property '" + REST_URI + "' must use http or https");
        }
        if (StringUtils.isBlank(uri.getRawAuthority()) || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException("Property '" + REST_URI
                    + "' must contain an authority and cannot contain user-info, query, or fragment");
        }
    }

    private static void validateRestHeader(String headerName, String headerValue) {
        if (StringUtils.isBlank(headerName) || !HTTP_HEADER_NAME.matcher(headerName).matches()) {
            throw new IllegalArgumentException("Invalid HTTP header name in property '"
                    + REST_HEADER_PREFIX + headerName + "'");
        }
        if ("authorization".equalsIgnoreCase(headerName) || "x-api-key".equalsIgnoreCase(headerName)) {
            throw new IllegalArgumentException("Authentication header '" + headerName
                    + "' must be configured through '" + REST_SECURITY_TYPE + "'");
        }
        if (headerValue == null || containsInvalidHttpHeaderCharacter(headerValue)) {
            throw new IllegalArgumentException("Invalid HTTP header value in property '"
                    + REST_HEADER_PREFIX + headerName + "'");
        }
    }

    private static void validateCredentialHeaderValue(String propertyName, String value) {
        if (containsInvalidHttpHeaderCharacter(value)) {
            throw new IllegalArgumentException("Invalid HTTP credential value in property '" + propertyName + "'");
        }
    }

    private static boolean containsInvalidHttpHeaderCharacter(String value) {
        // Match Lance's native HeaderValue parser: HTAB is legal, other C0 controls and DEL are not.
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if ((character < 0x20 && character != '\t') || character == 0x7f) {
                return true;
            }
        }
        return false;
    }

    /**
     * What {@code LanceNamespace.connect("rest", ...)} is given: the server, the namespace delimiter, and every
     * header to send, the credential header included. It contains the credentials: keep it internal.
     */
    Map<String, String> restNamespaceProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", StringUtils.stripEnd(restUri.trim(), "/"));
        properties.put("delimiter", namespaceDelimiter);
        restHeaders.forEach((name, value) -> properties.put("header." + name, value));
        if (SECURITY_BEARER.equals(securityType)) {
            properties.put("header.Authorization", "Bearer " + bearerToken);
        } else if (SECURITY_API_KEY.equals(securityType)) {
            properties.put("header.x-api-key", apiKey);
        }
        return properties;
    }

    /**
     * The values of the properties {@link #SECURITY_SURFACE} declares secret, to strip from provider errors:
     * what SHOW CREATE CATALOG masks is never echoed back in a message either.
     */
    List<String> secretValues() {
        List<String> secrets = new ArrayList<>();
        raw.forEach((key, value) -> {
            if (StringUtils.isNotEmpty(value) && isSecret(key)) {
                secrets.add(value);
            }
        });
        return secrets;
    }

    private static boolean isSecret(String key) {
        return SECURITY_SURFACE.sensitiveKeys().contains(key)
                || SECURITY_SURFACE.sensitiveKeyPrefixes().stream().anyMatch(prefix -> prefix.covers(key));
    }

    public String getCatalogType() {
        return catalogType;
    }

    public boolean isRest() {
        return REST.equals(catalogType);
    }

    /** The warehouse a filesystem catalog's directory namespace roots at; null for a REST catalog. */
    public String getWarehouse() {
        return warehouse;
    }

    public String getNamespaceDelimiter() {
        return namespaceDelimiter;
    }

    public String getRootDatabase() {
        return rootDatabase;
    }

    /** The configured parent namespace, already parsed and validated; empty when none is configured. */
    public List<String> getParentNamespace() {
        return parentNamespace;
    }

    /**
     * The catalog's properties as written, unmodifiable. It contains credentials: pass it only to code
     * that keeps it internal.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
