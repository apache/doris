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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

/** Read-only Lance Directory or REST Namespace catalog. */
public class LanceExternalCatalog extends ExternalCatalog {
    public static final String LANCE_CATALOG_TYPE = "lance.catalog.type";
    public static final String LANCE_FILESYSTEM = "filesystem";
    public static final String LANCE_REST = "rest";
    public static final String WAREHOUSE = "warehouse";
    public static final String NAMESPACE_PARENT = "lance.namespace.parent";
    public static final String NAMESPACE_DELIMITER = "lance.namespace.delimiter";
    public static final String ROOT_DATABASE = "lance.namespace.root_database";
    public static final String REST_URI = "lance.rest.uri";
    public static final String REST_SECURITY_TYPE = "lance.rest.security.type";
    public static final String REST_BEARER_TOKEN = "lance.rest.bearer-token";
    public static final String REST_API_KEY = "lance.rest.api-key";
    public static final String REST_HEADER_PREFIX = "lance.rest.header.";

    private static final String DEFAULT_DELIMITER = "$";
    private static final String DEFAULT_ROOT_DATABASE = "default";
    private static final String DATABASE_NAMESPACE_DELIMITER = ".";
    private static final String REST_SECURITY_NONE = "none";
    private static final String REST_SECURITY_BEARER = "bearer";
    private static final String REST_SECURITY_API_KEY = "api_key";
    private static final Pattern HTTP_HEADER_NAME = Pattern.compile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$");
    private static final int PAGE_SIZE = 1000;
    private static final long ALLOCATOR_LIMIT = 256L * 1024 * 1024;

    private transient LanceNamespace namespace;
    private transient BufferAllocator allocator;
    private transient List<String> parentNamespace = Collections.emptyList();
    private transient String catalogType;
    private transient String namespaceDelimiter;
    private transient String rootDatabase;
    private transient Map<String, String> javaStorageOptions = Collections.emptyMap();
    private transient Map<String, String> backendStorageOptions = Collections.emptyMap();
    private transient Object namespaceLock = new Object();

    public LanceExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.LANCE, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        try {
            namespaceLock = new Object();
            catalogType = normalizedCatalogType();
            namespaceDelimiter = catalogProperty.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER);
            rootDatabase = catalogProperty.getOrDefault(ROOT_DATABASE, DEFAULT_ROOT_DATABASE);
            parentNamespace = LanceNamespaceName.parseParent(
                    catalogProperty.getOrDefault(NAMESPACE_PARENT, ""), namespaceDelimiter);
            backendStorageOptions = catalogProperty.getBackendStorageProperties();
            javaStorageOptions = LanceStorageOptions.forJavaSdk(backendStorageOptions);

            allocator = new RootAllocator(ALLOCATOR_LIMIT);
            namespace = connectNamespace(catalogType, allocator, javaStorageOptions);
        } catch (Exception e) {
            closeLanceObjects();
            throw new RuntimeException("Failed to initialize Lance " + normalizedCatalogType() + " catalog '"
                    + getName()
                    + "': " + sanitizedRootCauseMessage(e), safeCause(e));
        }
    }

    @Override
    public void checkWhenCreating() throws DdlException {
        checkProperties();
        boolean testConnection = Boolean.parseBoolean(
                catalogProperty.getOrDefault(TEST_CONNECTION, String.valueOf(DEFAULT_TEST_CONNECTION)));
        if (!testConnection) {
            return;
        }

        Map<String, String> storageOptions = LanceStorageOptions.forJavaSdk(
                catalogProperty.getBackendStorageProperties());
        String delimiter = catalogProperty.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER);
        List<String> parent = LanceNamespaceName.parseParent(
                catalogProperty.getOrDefault(NAMESPACE_PARENT, ""), delimiter);
        String type = normalizedCatalogType();
        try (BufferAllocator testAllocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            LanceNamespace testNamespace = connectNamespace(type, testAllocator, storageOptions);
            try {
                testNamespace.listTables(new ListTablesRequest().id(parent).limit(1));
                testNamespace.listNamespaces(new ListNamespacesRequest().id(parent).limit(1));
            } finally {
                closeNamespace(testNamespace);
            }
        } catch (Exception e) {
            throw new DdlException("Lance " + type + " catalog connectivity test failed: "
                    + sanitizedRootCauseMessage(e), safeCause(e));
        }
    }

    private LanceNamespace connectNamespace(String type, BufferAllocator namespaceAllocator,
            Map<String, String> storageOptions) {
        if (LANCE_FILESYSTEM.equals(type)) {
            return connectDirectoryNamespace(namespaceAllocator, storageOptions);
        }
        if (LANCE_REST.equals(type)) {
            return connectRestNamespace(namespaceAllocator);
        }
        throw new IllegalArgumentException("Unsupported Lance catalog type '" + type + "'");
    }

    private LanceNamespace connectDirectoryNamespace(BufferAllocator namespaceAllocator,
            Map<String, String> storageOptions) {
        Map<String, String> namespaceProperties = new HashMap<>();
        namespaceProperties.put("root", catalogProperty.getOrDefault(WAREHOUSE, ""));
        storageOptions.forEach((key, value) -> namespaceProperties.put("storage." + key, value));
        return LanceNamespace.connect("dir", namespaceProperties, namespaceAllocator);
    }

    private LanceNamespace connectRestNamespace(BufferAllocator namespaceAllocator) {
        Map<String, String> namespaceProperties = new HashMap<>();
        namespaceProperties.put("uri", normalizedRestUri());
        namespaceProperties.put("delimiter",
                catalogProperty.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER));

        Map<String, String> properties = catalogProperty.getProperties();
        properties.forEach((key, value) -> {
            if (key.startsWith(REST_HEADER_PREFIX)) {
                namespaceProperties.put("header." + key.substring(REST_HEADER_PREFIX.length()), value);
            }
        });
        String securityType = properties.getOrDefault(REST_SECURITY_TYPE, REST_SECURITY_NONE)
                .trim().toLowerCase(Locale.ROOT);
        if (REST_SECURITY_BEARER.equals(securityType)) {
            namespaceProperties.put("header.Authorization", "Bearer " + properties.get(REST_BEARER_TOKEN));
        } else if (REST_SECURITY_API_KEY.equals(securityType)) {
            namespaceProperties.put("header.x-api-key", properties.get(REST_API_KEY));
        }
        return LanceNamespace.connect("rest", namespaceProperties, namespaceAllocator);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        Map<String, String> properties = catalogProperty.getProperties();
        String type = normalizedCatalogType();
        if (!LANCE_FILESYSTEM.equals(type) && !LANCE_REST.equals(type)) {
            throw new DdlException("Property '" + LANCE_CATALOG_TYPE
                    + "' must be 'filesystem' or 'rest', but was '" + type + "'");
        }
        validateCommonProperties(properties);
        if (LANCE_FILESYSTEM.equals(type)) {
            validateFilesystemProperties(properties);
        } else {
            validateRestProperties(properties);
        }
    }

    private void validateCommonProperties(Map<String, String> properties) throws DdlException {
        String delimiter = properties.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER);
        if (delimiter.isEmpty() || delimiter.indexOf('\\') >= 0) {
            throw new DdlException("Property '" + NAMESPACE_DELIMITER
                    + "' cannot be empty or contain the escape character '\\'");
        }
        String rootDb = properties.getOrDefault(ROOT_DATABASE, DEFAULT_ROOT_DATABASE);
        if (StringUtils.isBlank(rootDb)) {
            throw new DdlException("Property '" + ROOT_DATABASE
                    + "' must be non-empty");
        }
        LanceNamespaceName.parseParent(properties.getOrDefault(NAMESPACE_PARENT, ""), delimiter);
    }

    private void validateFilesystemProperties(Map<String, String> properties) throws DdlException {
        String warehouse = properties.get(WAREHOUSE);
        if (StringUtils.isBlank(warehouse)) {
            throw new DdlException("Missing required property 'warehouse' for Lance filesystem catalog");
        }
        validateWarehouse(warehouse);
        for (String key : properties.keySet()) {
            if (key.startsWith("lance.rest.")) {
                throw new DdlException("Property '" + key + "' is not valid for Lance filesystem catalog");
            }
        }
    }

    private void validateRestProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(WAREHOUSE)) {
            throw new DdlException("Property 'warehouse' is not valid for Lance REST catalog");
        }
        String restUri = properties.get(REST_URI);
        if (StringUtils.isBlank(restUri)) {
            throw new DdlException("Missing required property '" + REST_URI + "' for Lance REST catalog");
        }
        validateRestUri(restUri);

        String securityType = properties.getOrDefault(REST_SECURITY_TYPE, REST_SECURITY_NONE)
                .trim().toLowerCase(Locale.ROOT);
        boolean bearerTokenConfigured = properties.containsKey(REST_BEARER_TOKEN);
        boolean apiKeyConfigured = properties.containsKey(REST_API_KEY);
        boolean hasBearerToken = StringUtils.isNotBlank(properties.get(REST_BEARER_TOKEN));
        boolean hasApiKey = StringUtils.isNotBlank(properties.get(REST_API_KEY));
        switch (securityType) {
            case REST_SECURITY_NONE:
                if (bearerTokenConfigured || apiKeyConfigured) {
                    throw new DdlException("Lance REST security type 'none' cannot configure '"
                            + REST_BEARER_TOKEN + "' or '" + REST_API_KEY + "'");
                }
                break;
            case REST_SECURITY_BEARER:
                if (!hasBearerToken || apiKeyConfigured) {
                    throw new DdlException("Lance REST security type 'bearer' requires only '"
                            + REST_BEARER_TOKEN + "'");
                }
                validateCredentialHeaderValue(REST_BEARER_TOKEN, properties.get(REST_BEARER_TOKEN));
                break;
            case REST_SECURITY_API_KEY:
                if (!hasApiKey || bearerTokenConfigured) {
                    throw new DdlException("Lance REST security type 'api_key' requires only '"
                            + REST_API_KEY + "'");
                }
                validateCredentialHeaderValue(REST_API_KEY, properties.get(REST_API_KEY));
                break;
            default:
                throw new DdlException("Property '" + REST_SECURITY_TYPE
                        + "' must be 'none', 'bearer', or 'api_key'");
        }

        Set<String> supportedKeys = new HashSet<>();
        Collections.addAll(supportedKeys, REST_URI, REST_SECURITY_TYPE, REST_BEARER_TOKEN, REST_API_KEY);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("lance.rest.") || supportedKeys.contains(key)) {
                continue;
            }
            if (!key.startsWith(REST_HEADER_PREFIX)) {
                throw new DdlException("Unsupported Lance REST property '" + key + "'");
            }
            validateRestHeader(key.substring(REST_HEADER_PREFIX.length()), entry.getValue());
        }
    }

    private void validateRestHeader(String headerName, String headerValue) throws DdlException {
        if (StringUtils.isBlank(headerName) || !HTTP_HEADER_NAME.matcher(headerName).matches()) {
            throw new DdlException("Invalid HTTP header name in property '" + REST_HEADER_PREFIX
                    + headerName + "'");
        }
        if ("authorization".equalsIgnoreCase(headerName) || "x-api-key".equalsIgnoreCase(headerName)) {
            throw new DdlException("Authentication header '" + headerName + "' must be configured through '"
                    + REST_SECURITY_TYPE + "'");
        }
        if (headerValue == null || headerValue.indexOf('\r') >= 0 || headerValue.indexOf('\n') >= 0) {
            throw new DdlException("Invalid HTTP header value in property '" + REST_HEADER_PREFIX
                    + headerName + "'");
        }
    }

    private void validateCredentialHeaderValue(String propertyName, String value) throws DdlException {
        if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            throw new DdlException("Invalid HTTP credential value in property '" + propertyName + "'");
        }
    }

    private void validateRestUri(String value) throws DdlException {
        URI uri;
        try {
            uri = URI.create(value.trim());
        } catch (IllegalArgumentException e) {
            throw new DdlException("Invalid Lance REST URI in property '" + REST_URI + "'", e);
        }
        String scheme = uri.getScheme();
        if (scheme == null || (!("http".equalsIgnoreCase(scheme)) && !("https".equalsIgnoreCase(scheme)))) {
            throw new DdlException("Property '" + REST_URI + "' must use http or https");
        }
        if (StringUtils.isBlank(uri.getRawAuthority()) || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new DdlException("Property '" + REST_URI
                    + "' must contain an authority and cannot contain user-info, query, or fragment");
        }
    }

    private String normalizedCatalogType() {
        return catalogProperty.getOrDefault(LANCE_CATALOG_TYPE, LANCE_FILESYSTEM)
                .trim().toLowerCase(Locale.ROOT);
    }

    private String normalizedRestUri() {
        String uri = catalogProperty.getOrDefault(REST_URI, "").trim();
        while (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length() - 1);
        }
        return uri;
    }

    private void validateWarehouse(String warehouse) throws DdlException {
        URI uri;
        try {
            uri = URI.create(warehouse);
        } catch (IllegalArgumentException e) {
            throw new DdlException("Invalid Lance warehouse URI: " + warehouse, e);
        }
        if (uri.getScheme() == null) {
            Path path = Paths.get(warehouse);
            if (!path.isAbsolute()) {
                throw new DdlException("Local Lance warehouse must be an absolute path: " + warehouse);
            }
            return;
        }
        String scheme = uri.getScheme().toLowerCase();
        if (!"file".equals(scheme) && !"s3".equals(scheme)) {
            throw new DdlException("Unsupported Lance filesystem warehouse scheme '" + scheme
                    + "'; first phase supports local/file and s3");
        }
    }

    @Override
    protected List<String> listDatabaseNames() {
        makeSureInitialized();
        LinkedHashSet<String> databases = new LinkedHashSet<>();
        databases.add(rootDatabase);

        Queue<List<String>> queue = new ArrayDeque<>();
        queue.add(Collections.emptyList());
        Set<List<String>> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            List<String> relativeParent = queue.remove();
            if (!visited.add(relativeParent)) {
                continue;
            }
            for (String child : listChildNamespaces(fullNamespace(relativeParent))) {
                List<String> relativeChild = new ArrayList<>(relativeParent);
                relativeChild.add(child);
                databases.add(LanceNamespaceName.encode(
                        relativeChild, DATABASE_NAMESPACE_DELIMITER, rootDatabase));
                queue.add(relativeChild);
            }
        }
        return new ArrayList<>(databases);
    }

    private List<String> listChildNamespaces(List<String> namespaceId) {
        List<String> result = new ArrayList<>();
        String pageToken = null;
        Set<String> consumedTokens = new HashSet<>();
        do {
            ListNamespacesRequest request = new ListNamespacesRequest().id(namespaceId).limit(PAGE_SIZE);
            if (pageToken != null) {
                request.pageToken(pageToken);
            }
            ListNamespacesResponse response;
            synchronized (namespaceLock) {
                response = namespace.listNamespaces(request);
            }
            if (response.getNamespaces() != null) {
                result.addAll(response.getNamespaces());
            }
            pageToken = response.getPageToken();
        } while (StringUtils.isNotEmpty(pageToken) && consumedTokens.add(pageToken));
        return result;
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        makeSureInitialized();
        try {
            List<String> namespaceId = fullNamespace(
                    LanceNamespaceName.decode(dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase));
            List<String> result = new ArrayList<>();
            String pageToken = null;
            Set<String> consumedTokens = new HashSet<>();
            do {
                ListTablesRequest request = new ListTablesRequest().id(namespaceId).limit(PAGE_SIZE);
                if (pageToken != null) {
                    request.pageToken(pageToken);
                }
                ListTablesResponse response;
                synchronized (namespaceLock) {
                    response = namespace.listTables(request);
                }
                if (response.getTables() != null) {
                    result.addAll(response.getTables());
                }
                pageToken = response.getPageToken();
            } while (StringUtils.isNotEmpty(pageToken) && consumedTokens.add(pageToken));
            return result;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        try {
            describeTable(dbName, tblName);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName) {
        return loadTableMetadata(dbName, tableName, Optional.empty());
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName,
            Optional<TableSnapshot> tableSnapshot) {
        makeSureInitialized();
        DescribeTableResponse table = describeTable(dbName, tableName);
        if (Boolean.TRUE.equals(table.getManagedVersioning())) {
            throw new UnsupportedOperationException(
                    "Lance managed versioning is not supported by the current BE reader");
        }
        String datasetUri = StringUtils.firstNonBlank(table.getTableUri(), table.getLocation());
        if (datasetUri == null) {
            throw new RuntimeException("Lance namespace returned no table URI for " + dbName + "." + tableName);
        }

        Map<String, String> storageOptions = new HashMap<>(javaStorageOptions);
        if (table.getStorageOptions() != null) {
            storageOptions.putAll(table.getStorageOptions());
        }
        Map<String, String> tableBackendStorageOptions = LanceStorageOptions.forBackend(
                backendStorageOptions, table.getStorageOptions());
        try {
            if (tableSnapshot.isPresent()) {
                TableSnapshot snapshot = tableSnapshot.get();
                long version;
                if (snapshot.getType() == TableSnapshot.VersionType.VERSION) {
                    version = LanceSnapshotResolver.parseVersion(snapshot.getValue());
                } else {
                    long timestamp = TimeUtils.timeStringToLong(snapshot.getValue(), TimeUtils.getTimeZone());
                    if (timestamp < 0) {
                        throw new IllegalArgumentException(
                                "Cannot parse Lance FOR TIME AS OF value '" + snapshot.getValue() + "'");
                    }
                    version = LanceMetadataLoader.resolveVersionAtOrBefore(
                            datasetUri, storageOptions, timestamp, allocator);
                }
                return LanceMetadataLoader.load(datasetUri, storageOptions,
                        tableBackendStorageOptions, version, allocator);
            }
            return LanceMetadataLoader.load(datasetUri, storageOptions,
                    tableBackendStorageOptions, allocator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Lance table metadata for " + dbName + "." + tableName
                    + ": " + sanitizedRootCauseMessage(e), safeCause(e));
        }
    }

    private DescribeTableResponse describeTable(String dbName, String tableName) {
        try {
            List<String> tableId = fullNamespace(
                    LanceNamespaceName.decode(dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase));
            tableId.add(tableName);
            DescribeTableRequest request = new DescribeTableRequest().id(tableId).withTableUri(true)
                    .vendCredentials(LANCE_REST.equals(catalogType));
            synchronized (namespaceLock) {
                return namespace.describeTable(request);
            }
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> fullNamespace(List<String> relativeNamespace) {
        List<String> result = new ArrayList<>(parentNamespace.size() + relativeNamespace.size());
        result.addAll(parentNamespace);
        result.addAll(relativeNamespace);
        return result;
    }

    public Map<String, String> getBackendStorageOptions() {
        makeSureInitialized();
        return backendStorageOptions;
    }

    public String getLanceCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

    @Override
    public void onClose() {
        super.onClose();
        closeLanceObjects();
    }

    private void closeLanceObjects() {
        closeNamespace(namespace);
        namespace = null;
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception ignored) {
                // Best effort during catalog close or failed initialization.
            }
        }
        allocator = null;
    }

    private static void closeNamespace(LanceNamespace namespaceToClose) {
        if (namespaceToClose instanceof AutoCloseable) {
            try {
                ((AutoCloseable) namespaceToClose).close();
            } catch (Exception ignored) {
                // Best effort during catalog close or failed initialization.
            }
        }
    }

    private String sanitizedRootCauseMessage(Throwable throwable) {
        String message = ExceptionUtils.getRootCauseMessage(throwable);
        for (String sensitiveKey : new String[] {REST_BEARER_TOKEN, REST_API_KEY}) {
            String sensitiveValue = catalogProperty.getOrDefault(sensitiveKey, "");
            if (StringUtils.isNotEmpty(sensitiveValue)) {
                message = message.replace(sensitiveValue, "***");
            }
        }
        return message;
    }

    private Throwable safeCause(Throwable throwable) {
        if (StringUtils.isNotEmpty(catalogProperty.getOrDefault(REST_BEARER_TOKEN, ""))
                || StringUtils.isNotEmpty(catalogProperty.getOrDefault(REST_API_KEY, ""))) {
            return new RuntimeException(sanitizedRootCauseMessage(throwable));
        }
        return throwable;
    }
}
