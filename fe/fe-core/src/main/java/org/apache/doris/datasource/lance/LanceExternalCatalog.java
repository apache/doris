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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.AbstractLanceProperties;
import org.apache.doris.datasource.property.metastore.LanceFileSystemMetastoreProperties;
import org.apache.doris.datasource.property.metastore.LanceRestMetastoreProperties;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.TableExistsRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/** Read-only Lance Directory or REST Namespace catalog. */
public class LanceExternalCatalog extends ExternalCatalog {
    public static final String LANCE_CATALOG_TYPE = AbstractLanceProperties.LANCE_CATALOG_TYPE;
    public static final String LANCE_FILESYSTEM = AbstractLanceProperties.LANCE_FILESYSTEM;
    public static final String LANCE_REST = AbstractLanceProperties.LANCE_REST;
    public static final String WAREHOUSE = LanceFileSystemMetastoreProperties.WAREHOUSE;
    public static final String NAMESPACE_PARENT = AbstractLanceProperties.NAMESPACE_PARENT;
    public static final String NAMESPACE_DELIMITER = AbstractLanceProperties.NAMESPACE_DELIMITER;
    public static final String ROOT_DATABASE = AbstractLanceProperties.ROOT_DATABASE;
    public static final String REST_URI = LanceRestMetastoreProperties.REST_URI;
    public static final String REST_SECURITY_TYPE = LanceRestMetastoreProperties.REST_SECURITY_TYPE;
    public static final String REST_BEARER_TOKEN = LanceRestMetastoreProperties.REST_BEARER_TOKEN;
    public static final String REST_API_KEY = LanceRestMetastoreProperties.REST_API_KEY;
    public static final String REST_HEADER_PREFIX = LanceRestMetastoreProperties.REST_HEADER_PREFIX;

    private static final String DATABASE_NAMESPACE_DELIMITER = ".";
    private static final int PAGE_SIZE = 1000;
    private static final long ALLOCATOR_LIMIT = 256L * 1024 * 1024;
    private static final int MAX_PROVIDER_MESSAGE_BYTES = 1024;
    private static final String[] RUNTIME_SENSITIVE_OPTION_KEYS = {
            "aws_access_key_id", "aws_secret_access_key", "aws_session_token",
            "oss_access_key_id", "oss_secret_access_key", "oss_security_token",
            "access_key_id", "access_key_secret", "security_token"
    };

    private transient LanceNamespace namespace;
    private transient BufferAllocator allocator;
    private transient List<String> parentNamespace = Collections.emptyList();
    private transient String catalogType;
    private transient String rootDatabase;
    private transient Map<String, String> namespaceStorageOptions = Collections.emptyMap();
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
            AbstractLanceProperties properties = getLanceProperties();
            catalogType = properties.getLanceCatalogType();
            rootDatabase = properties.getRootDatabase();
            parentNamespace = LanceNamespaceName.parseParentNamespace(
                    properties.getNamespaceParent(), properties.getNamespaceDelimiter());
            namespaceStorageOptions = LanceStorageOptions.forUri(
                    properties.getNamespaceStorageUri(),
                    catalogProperty.getOrderedStoragePropertiesList());

            allocator = new RootAllocator(ALLOCATOR_LIMIT);
            namespace = properties.createNamespace(allocator, namespaceStorageOptions);
        } catch (Exception e) {
            closeLanceObjects();
            throw new RuntimeException("Failed to initialize Lance catalog '" + getName()
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

        AbstractLanceProperties properties = getLanceProperties();
        Map<String, String> storageOptions = LanceStorageOptions.forUri(
                properties.getNamespaceStorageUri(),
                catalogProperty.getOrderedStoragePropertiesList());
        List<String> parent = LanceNamespaceName.parseParentNamespace(
                properties.getNamespaceParent(), properties.getNamespaceDelimiter());
        String type = properties.getLanceCatalogType();
        try (BufferAllocator testAllocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            LanceNamespace testNamespace = properties.createNamespace(testAllocator, storageOptions);
            try {
                testNamespace.listTables(new ListTablesRequest().id(parent).limit(1));
                testNamespace.listNamespaces(new ListNamespacesRequest().id(parent).limit(1));
            } finally {
                closeNamespace(testNamespace);
            }
        } catch (Exception e) {
            // The catalog is not initialized yet, so the namespace options this test just built are
            // the only ones the sanitizer can see.
            String sanitizedMessage = sanitizedRootCauseMessage(
                    e, properties.getNamespaceStorageUri(), storageOptions);
            throw new DdlException("Lance " + type + " catalog connectivity test failed: "
                    + sanitizedMessage, sanitizedCause(e, sanitizedMessage));
        }
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        try {
            AbstractLanceProperties properties = getLanceProperties();
            LanceNamespaceName.parseParentNamespace(
                    properties.getNamespaceParent(), properties.getNamespaceDelimiter());
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private AbstractLanceProperties getLanceProperties() {
        return (AbstractLanceProperties) catalogProperty.getMetastoreProperties();
    }

    /**
     * Returns whether this catalog is configured to use the Lance REST namespace.
     *
     * <p>This deliberately reads only the normalized catalog properties and does not initialize
     * the namespace. Callers can therefore reject unsupported REST operations before resolving a
     * database or table, both of which may trigger remote metadata requests.
     */
    public boolean isRestCatalogConfigured() {
        return LANCE_REST.equals(getLanceProperties().getLanceCatalogType());
    }

    @Override
    protected List<String> listDatabaseNames() {
        makeSureInitialized();

        // The configured root database represents the empty relative Lance namespace.
        LinkedHashSet<String> databases = new LinkedHashSet<>();
        databases.add(rootDatabase);

        // Breadth-first traversal starts at the catalog's configured parent namespace.
        // Queue entries remain relative so they can be exposed as Doris database names.
        Queue<List<String>> queue = new ArrayDeque<>();
        queue.add(Collections.emptyList());
        Set<List<String>> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            List<String> relativeParent = queue.remove();
            if (!visited.add(relativeParent)) {
                continue;
            }

            // The Lance API expects a full namespace, including the configured parent.
            List<String> fullParentNamespace = buildFullNamespace(relativeParent);
            for (String child : listChildNamespaces(fullParentNamespace)) {
                List<String> relativeChild = new ArrayList<>(relativeParent);
                relativeChild.add(child);

                // Doris exposes each hierarchical relative namespace as one flat database name.
                databases.add(LanceNamespaceName.namespaceToDorisDatabaseName(
                        relativeChild, DATABASE_NAMESPACE_DELIMITER, rootDatabase));

                // Visit this child later to discover namespaces nested below it.
                queue.add(relativeChild);
            }
        }
        return new ArrayList<>(databases);
    }

    /**
     * Lists all direct child namespace names under the given full Lance namespace.
     *
     * <p>Each request asks for at most {@link #PAGE_SIZE} children. If Lance returns a
     * page token, this method keeps requesting subsequent pages until all children are collected.
     */
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
            List<String> relativeNamespace = LanceNamespaceName.dorisDatabaseNameToNamespace(
                    dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase);
            List<String> namespaceId = buildFullNamespace(relativeNamespace);
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
        makeSureInitialized();
        try {
            List<String> relativeNamespace = LanceNamespaceName.dorisDatabaseNameToNamespace(
                    dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase);
            List<String> tableId = buildFullNamespace(relativeNamespace);
            tableId.add(tblName);
            TableExistsRequest request = new TableExistsRequest().id(tableId);
            synchronized (namespaceLock) {
                namespace.tableExists(request);
            }
            return true;
        } catch (TableNotFoundException | NamespaceNotFoundException e) {
            return false;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName) {
        return loadTableMetadata(dbName, tableName, Optional.empty(), false);
    }

    public LanceTableMetadata loadTableMetadataForVectorSearch(String dbName, String tableName) {
        return loadTableMetadata(dbName, tableName, Optional.empty(), true);
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName,
            Optional<TableSnapshot> tableSnapshot) {
        return loadTableMetadata(dbName, tableName, tableSnapshot, false);
    }

    private LanceTableMetadata loadTableMetadata(String dbName, String tableName,
            Optional<TableSnapshot> tableSnapshot, boolean loadIndexSegments) {
        makeSureInitialized();
        ResolvedTableAccess tableAccess = resolveTableAccess(dbName, tableName);
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
                    version = LanceSnapshotResolver.getVersionAtOrBefore(
                            tableAccess.datasetUri, tableAccess.storageOptions, timestamp, allocator);
                }
                return LanceMetadataLoader.loadVersion(
                        tableAccess.datasetUri, tableAccess.storageOptions, version, allocator);
            }
            return loadIndexSegments
                    ? LanceMetadataLoader.loadLatestWithIndexSegments(tableAccess.datasetUri,
                            tableAccess.storageOptions, allocator)
                    : LanceMetadataLoader.loadLatest(
                            tableAccess.datasetUri, tableAccess.storageOptions, allocator);
        } catch (Exception e) {
            String sanitizedMessage = sanitizedRootCauseMessage(
                    e, tableAccess.datasetUri, tableAccess.storageOptions);
            throw new RuntimeException("Failed to load Lance table metadata for " + dbName + "." + tableName
                    + ": " + sanitizedMessage, sanitizedCause(e, sanitizedMessage));
        }
    }

    public List<LanceLogicalIndex> loadTableIndexMetadata(
            String dbName, String tableName) throws AnalysisException {
        if (isRestCatalogConfigured()) {
            throw new AnalysisException("SHOW INDEX is not supported for Lance REST catalogs");
        }
        try {
            makeSureInitialized();
        } catch (Exception e) {
            throw indexMetadataLoadFailure(
                    dbName, tableName, e, null, namespaceStorageOptions);
        }

        ResolvedTableAccess tableAccess = null;
        try {
            // Keep Directory namespace resolution on the caller while it owns the catalog's
            // shared namespace and allocator. Moving that shared owner into a timed task would
            // let catalog close release it after the caller returns but before the task ends.
            // The deadline below covers the Dataset/JNI index metadata read itself.
            tableAccess = resolveTableAccess(dbName, tableName);
            String datasetUri = tableAccess.datasetUri;
            Map<String, String> storageOptions = tableAccess.storageOptions;
            return LanceMetadataReadExecutor.execute(() -> {
                // The caller may return on deadline while JNI is still running. A task-owned
                // allocator prevents catalog close from releasing native resources prematurely.
                try (BufferAllocator readAllocator = new RootAllocator(ALLOCATOR_LIMIT)) {
                    return LanceIndexMetadataLoader.load(datasetUri, storageOptions, readAllocator);
                }
            });
        } catch (Exception e) {
            String datasetUri = tableAccess == null ? null : tableAccess.datasetUri;
            Map<String, String> runtimeStorageOptions = tableAccess == null
                    ? namespaceStorageOptions : tableAccess.storageOptions;
            throw indexMetadataLoadFailure(
                    dbName, tableName, e, datasetUri, runtimeStorageOptions);
        }
    }

    @VisibleForTesting
    RuntimeException indexMetadataLoadFailure(String dbName, String tableName,
            Throwable throwable, String datasetUri, Map<String, String> runtimeStorageOptions) {
        String sanitizedMessage = sanitizedRootCauseMessage(
                throwable, datasetUri, runtimeStorageOptions);
        return new RuntimeException("Failed to load Lance index metadata for " + dbName + "." + tableName
                + ": " + sanitizedMessage, sanitizedCause(throwable, sanitizedMessage));
    }

    private ResolvedTableAccess resolveTableAccess(String dbName, String tableName) {
        DescribeTableResponse table = describeTable(dbName, tableName);
        if (Boolean.TRUE.equals(table.getManagedVersioning())) {
            throw new UnsupportedOperationException(
                    "Lance managed versioning is not supported by the current BE reader");
        }
        String datasetUri = StringUtils.firstNonBlank(table.getTableUri(), table.getLocation());
        if (datasetUri == null) {
            throw new RuntimeException("Lance namespace returned no table URI for " + dbName + "." + tableName);
        }

        // One option map serves both readers: the FE opens the dataset through the Lance Java SDK
        // and the BE through lance-c, so neither can end up with credentials the other lacks. The
        // dataset URL picks the option vocabulary, the same way Lance picks a provider from it.
        Map<String, String> storageOptions = LanceStorageOptions.forVendedTable(datasetUri,
                catalogProperty.getOrderedStoragePropertiesList(), table.getStorageOptions());
        return new ResolvedTableAccess(datasetUri, storageOptions);
    }

    private DescribeTableResponse describeTable(String dbName, String tableName) {
        try {
            List<String> relativeNamespace = LanceNamespaceName.dorisDatabaseNameToNamespace(
                    dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase);
            List<String> tableId = buildFullNamespace(relativeNamespace);
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

    /**
     * Prepends the configured parent namespace to a namespace relative to this catalog.
     *
     * <p>For example, if {@code parentNamespace} is {@code [company, analytics]} and
     * {@code relativeNamespace} is {@code [sales, daily]}, this method returns
     * {@code [company, analytics, sales, daily]}. The returned list is a new mutable list;
     * neither input list is modified.
     */
    private List<String> buildFullNamespace(List<String> relativeNamespace) {
        List<String> result = new ArrayList<>(parentNamespace.size() + relativeNamespace.size());
        result.addAll(parentNamespace);
        result.addAll(relativeNamespace);
        return result;
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

    /**
     * Every provider-facing failure has at least the namespace's own storage options behind it, so
     * default to those rather than redacting only the REST secrets. Callers holding a resolved
     * table pass its dataset URI and options to the overload below instead.
     */
    private String sanitizedRootCauseMessage(Throwable throwable) {
        return sanitizedRootCauseMessage(throwable, null, namespaceStorageOptions);
    }

    @VisibleForTesting
    String sanitizedRootCauseMessage(Throwable throwable, String datasetUri,
            Map<String, String> runtimeStorageOptions) {
        String message = ExceptionUtils.getRootCauseMessage(throwable);
        Map<String, String> nonNullStorageOptions = runtimeStorageOptions == null
                ? Collections.emptyMap() : runtimeStorageOptions;
        List<String> sensitiveValues = new ArrayList<>();
        sensitiveValues.add(catalogProperty.getOrDefault(REST_BEARER_TOKEN, ""));
        sensitiveValues.add(catalogProperty.getOrDefault(REST_API_KEY, ""));
        // Lance also accepts these options scoped to one base, as base_<id>.oss_secret_access_key,
        // so match on the suffix rather than looking each bare key up exactly.
        nonNullStorageOptions.forEach((key, value) -> {
            if (key == null) {
                return;
            }
            for (String sensitiveKey : RUNTIME_SENSITIVE_OPTION_KEYS) {
                if (key.equals(sensitiveKey) || key.endsWith("." + sensitiveKey)) {
                    sensitiveValues.add(value);
                    return;
                }
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

    /**
     * Always hand back a rebuilt cause. Returning the original once left every catalog without REST
     * auth - a directory namespace on OSS, say - free to carry provider text holding its
     * credentials, which the message beside it had just redacted.
     */
    private Throwable safeCause(Throwable throwable) {
        return sanitizedCause(throwable, sanitizedRootCauseMessage(throwable));
    }

    private static Throwable sanitizedCause(Throwable throwable, String sanitizedMessage) {
        return throwable instanceof IllegalArgumentException
                ? new IllegalArgumentException(sanitizedMessage)
                : new RuntimeException(sanitizedMessage);
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

    private static final class ResolvedTableAccess {
        private final String datasetUri;
        private final Map<String, String> storageOptions;

        private ResolvedTableAccess(String datasetUri, Map<String, String> storageOptions) {
            this.datasetUri = datasetUri;
            this.storageOptions = Collections.unmodifiableMap(new HashMap<>(storageOptions));
        }
    }
}
