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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.storage.LanceStorageOptions;
import org.apache.doris.datasource.property.metastore.AbstractLanceProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.commons.lang3.StringUtils;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.errors.TableVersionNotFoundException;
import org.lance.namespace.model.AddColumnsEntry;
import org.lance.namespace.model.AlterColumnsEntry;
import org.lance.namespace.model.AlterTableAddColumnsRequest;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DescribeTableVersionRequest;
import org.lance.namespace.model.DescribeTableVersionResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.model.TableVersion;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Namespace requests and table access resolution. The catalog client owns the native resources
 * and holds a Lease throughout each call to this client.
 */
final class LanceNamespaceClient {
    private static final String DATABASE_NAMESPACE_DELIMITER = ".";
    private static final int PAGE_SIZE = 1000;
    private static final String LANCE_FILESYSTEM = AbstractLanceProperties.LANCE_FILESYSTEM;
    private static final String LANCE_REST = AbstractLanceProperties.LANCE_REST;

    private final LanceNamespace namespace;
    private final String catalogType;
    private final String rootDatabase;
    private final List<String> parentNamespace;
    private final List<StorageProperties> storageProperties;
    /**
     * Serializes the namespace and table listings and table existence checks. Table describes and
     * version requests, which a read makes whenever the table's access is not cached (always for a
     * managed table), do not take it: the REST and Directory namespaces are safe to call
     * concurrently (lance-jni calls them through a shared reference on its multi-threaded runtime),
     * and a stalled describe must not hold up every other table of the catalog.
     */
    private final Map<String, String> namespaceStorageOptions;
    private final Object namespaceLock = new Object();
    private final long tableAccessTtlNanos;
    private final Ticker ticker;
    private volatile Cache<List<String>, CachedTableAccess> tableAccessCache;

    LanceNamespaceClient(LanceNamespace namespace, String catalogType, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties) {
        this(namespace, catalogType, rootDatabase, parentNamespace, storageProperties,
                Collections.emptyMap(), AbstractLanceProperties.DEFAULT_TABLE_ACCESS_CACHE_TTL_SECONDS,
                Ticker.systemTicker());
    }

    LanceNamespaceClient(LanceNamespace namespace, String catalogType, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties,
            Map<String, String> namespaceStorageOptions) {
        this(namespace, catalogType, rootDatabase, parentNamespace, storageProperties,
                namespaceStorageOptions, AbstractLanceProperties.DEFAULT_TABLE_ACCESS_CACHE_TTL_SECONDS,
                Ticker.systemTicker());
    }

    LanceNamespaceClient(LanceNamespace namespace, String catalogType, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties,
            long tableAccessTtlSeconds, Ticker ticker) {
        this(namespace, catalogType, rootDatabase, parentNamespace, storageProperties,
                Collections.emptyMap(), tableAccessTtlSeconds, ticker);
    }

    LanceNamespaceClient(LanceNamespace namespace, String catalogType, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties,
            Map<String, String> namespaceStorageOptions, long tableAccessTtlSeconds, Ticker ticker) {
        this.namespace = namespace;
        this.catalogType = catalogType;
        this.rootDatabase = rootDatabase;
        this.parentNamespace = Collections.unmodifiableList(new ArrayList<>(parentNamespace));
        this.storageProperties = Collections.unmodifiableList(new ArrayList<>(storageProperties));
        this.namespaceStorageOptions = Collections.unmodifiableMap(
                new java.util.HashMap<>(namespaceStorageOptions));
        this.tableAccessTtlNanos = TimeUnit.SECONDS.toNanos(tableAccessTtlSeconds);
        this.ticker = ticker;
        this.tableAccessCache = newTableAccessCache();
    }

    List<String> listDatabaseNames() {
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

    boolean isRootDatabase(String dbName) {
        return rootDatabase.equals(dbName);
    }

    boolean databaseExists(String dbName) {
        if (isRootDatabase(dbName)) {
            return true;
        }
        try {
            NamespaceExistsRequest request = new NamespaceExistsRequest().id(buildNamespaceId(dbName));
            synchronized (namespaceLock) {
                namespace.namespaceExists(request);
            }
            return true;
        } catch (NamespaceNotFoundException e) {
            return false;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    void createDatabase(String dbName, Map<String, String> properties) {
        try {
            CreateNamespaceRequest request = new CreateNamespaceRequest()
                    .id(buildNamespaceId(dbName))
                    .mode("Create")
                    .properties(properties == null ? Collections.emptyMap() : properties);
            synchronized (namespaceLock) {
                namespace.createNamespace(request);
            }
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    void dropDatabase(String dbName, boolean ifExists, boolean force) {
        try {
            List<String> namespaceId = buildNamespaceId(dbName);
            synchronized (namespaceLock) {
                if (force) {
                    try {
                        dropNamespaceCascade(namespaceId, ifExists ? "Skip" : "Fail");
                    } catch (NamespaceNotFoundException e) {
                        if (!ifExists) {
                            throw e;
                        }
                    }
                    return;
                }
                namespace.dropNamespace(new DropNamespaceRequest()
                        .id(namespaceId)
                        .mode(ifExists ? "Skip" : "Fail")
                        .behavior("Restrict"));
            }
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private void dropNamespaceCascade(List<String> namespaceId, String mode) {
        for (String child : listChildNamespaces(namespaceId)) {
            List<String> childId = new ArrayList<>(namespaceId);
            childId.add(child);
            dropNamespaceCascade(childId, "Fail");
        }
        for (String table : listTableNames(namespaceId)) {
            List<String> tableId = new ArrayList<>(namespaceId);
            tableId.add(table);
            namespace.dropTable(new DropTableRequest().id(tableId));
        }
        namespace.dropNamespace(new DropNamespaceRequest()
                .id(namespaceId)
                .mode(mode)
                .behavior("Restrict"));
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
            if (StringUtils.isNotEmpty(pageToken) && !consumedTokens.add(pageToken)) {
                throw new IllegalStateException("Lance namespace repeated a pagination token");
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return result;
    }

    List<String> listTableNames(String dbName) {
        try {
            return listTableNames(buildNamespaceId(dbName));
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> listTableNames(List<String> namespaceId) {
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
            if (StringUtils.isNotEmpty(pageToken) && !consumedTokens.add(pageToken)) {
                throw new IllegalStateException("Lance namespace repeated a pagination token");
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return result;
    }

    boolean tableExists(String dbName, String tblName) {
        try {
            List<String> tableId = buildTableId(dbName, tblName);
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

    void createTable(String dbName, String tableName, Map<String, String> properties,
            byte[] arrowStream) {
        try {
            CreateTableRequest request = new CreateTableRequest()
                    .id(buildTableId(dbName, tableName))
                    .mode("Create")
                    .properties(properties == null ? Collections.emptyMap() : properties)
                    .storageOptions(namespaceStorageOptions);
            synchronized (namespaceLock) {
                namespace.createTable(request, arrowStream);
            }
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    void dropTable(String dbName, String tableName) {
        try {
            DropTableRequest request = new DropTableRequest().id(buildTableId(dbName, tableName));
            executeTableMutation(dbName, tableName, () -> namespace.dropTable(request));
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    void addColumns(String dbName, String tableName, List<AddColumnsEntry> columns) {
        try {
            AlterTableAddColumnsRequest request = new AlterTableAddColumnsRequest()
                    .id(buildTableId(dbName, tableName))
                    .newColumns(columns);
            executeTableMutation(dbName, tableName, () -> namespace.alterTableAddColumns(request));
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    void alterColumns(String dbName, String tableName, List<AlterColumnsEntry> alterations) {
        try {
            AlterTableAlterColumnsRequest request = new AlterTableAlterColumnsRequest()
                    .id(buildTableId(dbName, tableName))
                    .alterations(alterations);
            executeTableMutation(dbName, tableName, () -> namespace.alterTableAlterColumns(request));
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    void dropColumns(String dbName, String tableName, List<String> columns) {
        try {
            AlterTableDropColumnsRequest request = new AlterTableDropColumnsRequest()
                    .id(buildTableId(dbName, tableName))
                    .columns(columns);
            executeTableMutation(dbName, tableName, () -> namespace.alterTableDropColumns(request));
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeTableMutation(String dbName, String tableName, Runnable mutation)
            throws DdlException {
        List<String> namespaceId = buildNamespaceId(dbName);
        List<String> tableId = new ArrayList<>(namespaceId);
        tableId.add(tableName);
        synchronized (namespaceLock) {
            try {
                mutation.run();
                return;
            } catch (TableNotFoundException originalException) {
                if (!LANCE_FILESYSTEM.equals(catalogType) || !namespaceId.isEmpty()) {
                    throw originalException;
                }
                try {
                    namespace.tableExists(new TableExistsRequest().id(tableId));
                } catch (TableNotFoundException | NamespaceNotFoundException e) {
                    throw originalException;
                }
                try {
                    // DirectoryNamespace only accepts locations relative to its warehouse root.
                    namespace.registerTable(new RegisterTableRequest()
                            .id(tableId)
                            .location(tableName + ".lance")
                            .mode("Create"));
                } catch (TableAlreadyExistsException e) {
                    // Another mutation registered the external dataset first.
                }
                mutation.run();
            }
        }
    }

    LanceTableAccess resolveTableAccess(String dbName, String tableName) {
        List<String> tableId = tableAccessKey(dbName, tableName);
        if (tableAccessTtlNanos == 0) {
            return loadTableAccess(tableId).access;
        }
        // Cache hits avoid the catalog-wide namespace lock as well as filesystem or REST I/O.
        return tableAccessCache.get(tableId, this::loadTableAccess).access;
    }

    LanceTableAccess resolveTableAccessUncached(String dbName, String tableName) {
        return loadTableAccess(tableAccessKey(dbName, tableName)).access;
    }

    void invalidateTableAccessCache() {
        // Swap generations: a describe already in flight may finish for its caller, but must
        // never repopulate the cache used by reads admitted after an explicit refresh.
        tableAccessCache = newTableAccessCache();
    }

    private List<String> tableAccessKey(String dbName, String tableName) {
        try {
            return Collections.unmodifiableList(buildTableId(dbName, tableName));
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private CachedTableAccess loadTableAccess(List<String> tableId) {
        DescribeTableResponse table = describeTable(tableId);
        if (Boolean.TRUE.equals(table.getIsOnlyDeclared())) {
            throw new RuntimeException("Lance table is declared in the namespace but has no data yet");
        }
        String datasetUri = StringUtils.firstNonBlank(table.getTableUri(), table.getLocation());
        if (datasetUri == null) {
            throw new RuntimeException("Lance namespace returned no table URI for " + tableId);
        }

        LanceTableAccess access;
        boolean managed = Boolean.TRUE.equals(table.getManagedVersioning());
        if (managed) {
            // The namespace decides which versions exist; the FE and the BE both read one of them
            // by URI, which is all lance-c supports. The manifest paths the namespace records are
            // object-store paths under `location`, so `table_uri` must name the same place; it may
            // add a query, such as presigned credentials, which Doris then opens it with.
            if (StringUtils.isBlank(table.getLocation())) {
                throw new RuntimeException("Lance namespace returned no location for managed table " + tableId);
            }
            // An s3+ddb URI commits through DynamoDB, whose handler records and finalizes versions
            // itself, even on read; the namespace already decides the versions of a managed table.
            if (StringUtils.startsWithIgnoreCase(datasetUri, "s3+ddb:")) {
                throw new RuntimeException("Lance namespace returned an s3+ddb URI for managed table " + tableId
                        + ", whose versions the namespace manages");
            }
            if (StringUtils.isNotBlank(table.getTableUri())
                    && !withoutQuery(table.getTableUri()).equals(withoutQuery(table.getLocation()))) {
                throw new RuntimeException("Lance namespace returned a table_uri that differs from location for "
                        + "managed table " + tableId);
            }
            access = LanceTableAccess.managedByNamespace(datasetUri,
                    storageOptions(datasetUri, table.getStorageOptions()), tableId);
        } else {
            access = new LanceTableAccess(datasetUri, storageOptions(datasetUri, table.getStorageOptions()));
        }
        // A managed access is not cached: the version list the read asks for next is the
        // namespace's current one, and must be checked against the location the namespace
        // reports now, not against one it reported before moving the table.
        return new CachedTableAccess(access, managed ? 0 : tableAccessTtlNanos(datasetUri, table.getStorageOptions()));
    }

    /**
     * A location without its query and trailing slash. A fragment is kept: Lance ignores it, but
     * joins a branch directory after it, so it would move the branch onto the table root.
     */
    private static String withoutQuery(String uri) {
        return StringUtils.removeEnd(StringUtils.substringBefore(uri, "?"), "/");
    }

    /**
     * One option map serves both readers: the FE opens the dataset through the Lance Java SDK and
     * the BE through lance-c, so neither can end up with credentials the other lacks. The dataset
     * URL picks the option vocabulary, the same way Lance picks a provider from it.
     */
    private Map<String, String> storageOptions(String datasetUri, Map<String, String> vendedOptions) {
        return LanceStorageOptions.fromDorisAndVendedStorageOptions(datasetUri, storageProperties, vendedOptions);
    }

    /**
     * Every version the namespace records for a managed chain. No page size is requested: Lance's
     * Directory namespace applies a limit without returning a page token, which would silently
     * truncate the history, while a namespace that pages on its own still returns one.
     */
    List<TableVersion> listManagedVersions(LanceTableAccess access, Optional<String> branch) {
        List<TableVersion> result = new ArrayList<>();
        String pageToken = null;
        Set<String> consumedTokens = new HashSet<>();
        do {
            ListTableVersionsRequest request = new ListTableVersionsRequest().id(access.getNamespaceTableId());
            branch.ifPresent(request::branch);
            if (pageToken != null) {
                request.pageToken(pageToken);
            }
            ListTableVersionsResponse response = namespace.listTableVersions(request);
            if (response.getVersions() != null) {
                result.addAll(response.getVersions());
            }
            pageToken = response.getPageToken();
            if (StringUtils.isNotEmpty(pageToken) && !consumedTokens.add(pageToken)) {
                throw new IllegalStateException("Lance namespace repeated a pagination token");
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return result;
    }

    /**
     * The newest version the namespace records for a managed chain, asked for the way the Lance
     * SDK asks when it opens the latest version: newest first, one entry. Empty when the chain
     * records no version.
     */
    Optional<TableVersion> latestManagedVersion(LanceTableAccess access, Optional<String> branch) {
        String pageToken = null;
        Set<String> consumedTokens = new HashSet<>();
        do {
            ListTableVersionsRequest request = new ListTableVersionsRequest().id(access.getNamespaceTableId())
                    .descending(true).limit(1);
            branch.ifPresent(request::branch);
            if (pageToken != null) {
                request.pageToken(pageToken);
            }
            ListTableVersionsResponse response = namespace.listTableVersions(request);
            // The maximum rather than the first entry, in case a namespace ignores the limit. A
            // limit is an upper bound, so a page may be empty and the version on a later one.
            Optional<TableVersion> newest = response.getVersions() == null ? Optional.empty()
                    : response.getVersions().stream().filter(version -> version.getVersion() != null)
                            .max(Comparator.comparingLong(TableVersion::getVersion));
            if (newest.isPresent()) {
                return newest;
            }
            pageToken = response.getPageToken();
            if (StringUtils.isNotEmpty(pageToken) && !consumedTokens.add(pageToken)) {
                throw new IllegalStateException("Lance namespace repeated a pagination token");
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return Optional.empty();
    }

    /**
     * What the namespace records for one version of a managed chain.
     *
     * @throws TableVersionNotFoundException if the namespace does not record it
     */
    TableVersion describeManagedVersion(LanceTableAccess access, Optional<String> branch, long version) {
        DescribeTableVersionRequest request = new DescribeTableVersionRequest().id(access.getNamespaceTableId())
                .version(version);
        branch.ifPresent(request::branch);
        DescribeTableVersionResponse response = namespace.describeTableVersion(request);
        if (response.getVersion() == null || !Long.valueOf(version).equals(response.getVersion().getVersion())) {
            throw new IllegalStateException("Lance namespace described another version than " + version
                    + " of table " + access.getNamespaceTableId());
        }
        return response.getVersion();
    }

    private long tableAccessTtlNanos(String datasetUri, Map<String, String> vendedOptions) {
        // The BE cannot renew credentials during a scan. A fixed expiry margin cannot cover
        // arbitrary query durations, so preserve per-read vending even with a reported deadline.
        if (vendedOptions != null && !vendedOptions.isEmpty()) {
            return 0;
        }
        try {
            URI uri = new URI(datasetUri.trim());
            // Presigned/SAS credentials may live in the URI even when storage_options is empty.
            // Also check registry-based authorities, for which URI.getRawUserInfo() returns null.
            if (uri.isOpaque() || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (uri.getRawAuthority() != null && uri.getRawAuthority().contains("@"))) {
                return 0;
            }
            return tableAccessTtlNanos;
        } catch (URISyntaxException e) {
            // Unclassified locators remain usable but must not be assumed credential-free.
            return 0;
        }
    }

    DescribeTableResponse describeTable(String dbName, String tableName, boolean vendCredentials) {
        try {
            List<String> tableId = buildTableId(dbName, tableName);
            DescribeTableRequest request = new DescribeTableRequest().id(tableId).withTableUri(true)
                    .vendCredentials(vendCredentials && LANCE_REST.equals(catalogType));
            synchronized (namespaceLock) {
                return namespace.describeTable(request);
            }
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private Cache<List<String>, CachedTableAccess> newTableAccessCache() {
        return Caffeine.newBuilder().maximumSize(10_000).ticker(ticker)
                .expireAfter(new Expiry<List<String>, CachedTableAccess>() {
                    @Override
                    public long expireAfterCreate(List<String> key, CachedTableAccess value, long currentTime) {
                        return value.ttlNanos;
                    }

                    @Override
                    public long expireAfterUpdate(List<String> key, CachedTableAccess value,
                            long currentTime, long currentDuration) {
                        return value.ttlNanos;
                    }

                    @Override
                    public long expireAfterRead(List<String> key, CachedTableAccess value,
                            long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                }).build();
    }

    private static final class CachedTableAccess {
        private final LanceTableAccess access;
        private final long ttlNanos;

        private CachedTableAccess(LanceTableAccess access, long ttlNanos) {
            this.access = access;
            this.ttlNanos = ttlNanos;
        }
    }

    private DescribeTableResponse describeTable(List<String> tableId) {
        DescribeTableRequest request = new DescribeTableRequest().id(tableId).withTableUri(true)
                .vendCredentials(LANCE_REST.equals(catalogType));
        return namespace.describeTable(request);
    }

    private List<String> buildNamespaceId(String dbName) throws DdlException {
        List<String> relativeNamespace = LanceNamespaceName.dorisDatabaseNameToNamespace(
                dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase);
        return buildFullNamespace(relativeNamespace);
    }

    private List<String> buildTableId(String dbName, String tableName) throws DdlException {
        List<String> tableId = buildNamespaceId(dbName);
        tableId.add(tableName);
        return tableId;
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

}
