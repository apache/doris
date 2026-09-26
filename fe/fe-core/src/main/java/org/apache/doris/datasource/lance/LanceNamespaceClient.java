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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.model.TableVersion;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
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
    private static final String LANCE_REST = AbstractLanceProperties.LANCE_REST;

    private final LanceNamespace namespace;
    private final String catalogType;
    private final String rootDatabase;
    private final List<String> parentNamespace;
    private final List<StorageProperties> storageProperties;
    private final Object namespaceLock = new Object();
    private final long tableAccessTtlNanos;
    private final Ticker ticker;
    private volatile Cache<List<String>, CachedTableAccess> tableAccessCache;

    LanceNamespaceClient(LanceNamespace namespace, String catalogType, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties) {
        this(namespace, catalogType, rootDatabase, parentNamespace, storageProperties,
                AbstractLanceProperties.DEFAULT_TABLE_ACCESS_CACHE_TTL_SECONDS,
                Ticker.systemTicker());
    }

    LanceNamespaceClient(LanceNamespace namespace, String catalogType, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties,
            long tableAccessTtlSeconds, Ticker ticker) {
        this.namespace = namespace;
        this.catalogType = catalogType;
        this.rootDatabase = rootDatabase;
        this.parentNamespace = Collections.unmodifiableList(new ArrayList<>(parentNamespace));
        this.storageProperties = Collections.unmodifiableList(new ArrayList<>(storageProperties));
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
            List<String> namespaceId = buildNamespaceId(dbName);
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
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
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

    /** Drops one table's cached access, so its next read describes the table again. */
    void invalidateTableAccess(String dbName, String tableName) {
        tableAccessCache.invalidate(tableAccessKey(dbName, tableName));
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

        // One option map serves both readers: the FE opens the dataset through the Lance Java SDK
        // and the BE through lance-c, so neither can end up with credentials the other lacks. The
        // dataset URL picks the option vocabulary, the same way Lance picks a provider from it.
        Map<String, String> storageOptions = LanceStorageOptions.fromDorisAndVendedStorageOptions(datasetUri,
                storageProperties, table.getStorageOptions());
        LanceTableAccess access;
        if (Boolean.TRUE.equals(table.getManagedVersioning())) {
            // The namespace, not the dataset directory, records which manifest each version has.
            // The FE opens the dataset through the namespace so the SDK resolves the version there
            // and finalizes a still-staged manifest to its canonical path; the BE then opens the
            // same version by URI, which is all lance-c supports. The SDK opens `location` and
            // ignores `table_uri`, so both readers must agree on it.
            if (StringUtils.isBlank(table.getLocation())) {
                throw new RuntimeException("Lance namespace returned no location for managed table " + tableId);
            }
            if (StringUtils.isNotBlank(table.getTableUri()) && !StringUtils.removeEnd(table.getTableUri(), "/")
                    .equals(StringUtils.removeEnd(table.getLocation(), "/"))) {
                throw new RuntimeException("Lance namespace returned a table_uri that differs from location for "
                        + "managed table " + tableId);
            }
            access = LanceTableAccess.managedByNamespace(datasetUri, storageOptions,
                    LanceStorageOptions.forManagedSdkOpen(datasetUri, storageOptions, table.getStorageOptions()),
                    tableId);
        } else {
            access = new LanceTableAccess(datasetUri, storageOptions);
        }
        return new CachedTableAccess(access, tableAccessTtlNanos(datasetUri, table.getStorageOptions()));
    }

    /**
     * Opens a namespace-managed dataset through the namespace client. The SDK describes the table
     * through this Java client, outside {@code namespaceLock}, and then resolves versions with its
     * own native client (the native handle of a REST or Directory namespace), so the open is not
     * serialized with the requests this class issues itself. Both clients are safe to call
     * concurrently.
     *
     * <p>The session is passed to the builder explicitly: when the SDK opens through a namespace
     * client it rebuilds the read options and drops the session they carry, so the one inside
     * {@code readOptions} is ignored on this path.
     */
    Dataset openManagedDataset(BufferAllocator allocator, LanceTableAccess access, ReadOptions readOptions,
            Session session) {
        return Dataset.open().allocator(allocator).namespaceClient(namespace)
                .tableId(access.getNamespaceTableId()).readOptions(readOptions).session(session).build();
    }

    /**
     * Every version the namespace records for a managed chain. No page size is requested: Lance's
     * Directory namespace applies a limit without returning a page token, which would silently
     * truncate the history, while a namespace that pages on its own still returns one.
     */
    List<TableVersion> listManagedVersions(LanceTableAccess access) {
        List<TableVersion> result = new ArrayList<>();
        String pageToken = null;
        Set<String> consumedTokens = new HashSet<>();
        do {
            ListTableVersionsRequest request = new ListTableVersionsRequest().id(access.getNamespaceTableId());
            access.getBranch().ifPresent(request::branch);
            if (pageToken != null) {
                request.pageToken(pageToken);
            }
            ListTableVersionsResponse response;
            synchronized (namespaceLock) {
                response = namespace.listTableVersions(request);
            }
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
     * records no version, where the SDK would fall back to the newest manifest in storage.
     */
    OptionalLong latestManagedVersion(LanceTableAccess access, Optional<String> branch) {
        ListTableVersionsRequest request = new ListTableVersionsRequest().id(access.getNamespaceTableId())
                .descending(true).limit(1);
        branch.ifPresent(request::branch);
        ListTableVersionsResponse response;
        synchronized (namespaceLock) {
            response = namespace.listTableVersions(request);
        }
        // The maximum rather than the first entry, in case a namespace ignores the limit.
        return response.getVersions() == null ? OptionalLong.empty()
                : response.getVersions().stream().map(TableVersion::getVersion).filter(Objects::nonNull)
                        .mapToLong(Long::longValue).max();
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
        synchronized (namespaceLock) {
            return namespace.describeTable(request);
        }
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
