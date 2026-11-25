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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.commons.lang3.StringUtils;
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Namespace requests and table access resolution. The catalog client owns the native resources and holds
 * a lease throughout each call to this client.
 *
 * <p>Every metadata read starts from the table's access: the dataset URI and the storage options the namespace
 * describes. Those rarely change, while describing costs a REST request or a directory listing per read, so a
 * described access is reused for {@code lance.table_access_cache_ttl_seconds}, counted from the describe: a hit
 * never extends it, and any REFRESH drops them all with the generation this client belongs to. A table whose
 * options carry vended credentials, or whose URI may carry credentials, is described on every read: the BE
 * cannot renew credentials during a scan of any length, and no fixed margin covers every query.
 */
final class LanceNamespaceClient {
    /** Joins the components of a nested namespace into one Doris database name. */
    private static final String DATABASE_NAMESPACE_DELIMITER = ".";
    private static final int PAGE_SIZE = 1000;
    private static final int TABLE_ACCESS_CACHE_MAX_ENTRIES = 10_000;

    private final LanceNamespace namespace;
    /** Whether to ask the namespace for each table's storage credentials, as a REST namespace can vend them. */
    private final boolean vendCredentials;
    private final String rootDatabase;
    private final List<String> parentNamespace;
    private final List<StorageProperties> storageProperties;
    /**
     * Serializes the namespace and table listings and table existence checks. Table describes do not take
     * it: the namespaces are safe to call concurrently (lance-jni calls them through a shared reference on
     * its multi-threaded runtime), and a stalled describe must not hold up every other table of the
     * catalog.
     */
    private final Object namespaceLock = new Object();
    private final long tableAccessTtlNanos;
    private final Ticker ticker;
    /** Lives as long as this client: a refresh retires the client with its generation. */
    private final Cache<List<String>, CachedTableAccess> tableAccessCache;

    /**
     * @param tableAccessTtlSeconds how long a described access is reused; 0 describes the table on every read
     * @param ticker the clock the TTL is measured with
     */
    LanceNamespaceClient(LanceNamespace namespace, boolean vendCredentials, String rootDatabase,
            List<String> parentNamespace, List<StorageProperties> storageProperties, long tableAccessTtlSeconds,
            Ticker ticker) {
        this.namespace = namespace;
        this.vendCredentials = vendCredentials;
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
     * <p>Each request asks for at most {@link #PAGE_SIZE} children. If Lance returns a page token, this
     * method keeps requesting subsequent pages until all children are collected.
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
    }

    /**
     * Whether the namespace holds the table. Only a namespace that says so is a "no": a failed request
     * propagates, because a table reported missing over an authentication or transport failure would be
     * dropped from the engine's name cache.
     */
    boolean tableExists(String dbName, String tblName) {
        List<String> tableId = buildTableId(dbName, tblName);
        TableExistsRequest request = new TableExistsRequest().id(tableId);
        try {
            synchronized (namespaceLock) {
                namespace.tableExists(request);
            }
            return true;
        } catch (TableNotFoundException | NamespaceNotFoundException e) {
            return false;
        }
    }

    /**
     * The URI and options both the FE and the BE open the table with: a cached access while its TTL lasts,
     * otherwise a new describe. Concurrent misses of one table share one describe; a failed describe is not
     * cached.
     */
    LanceTableAccess resolveTableAccess(String dbName, String tableName) {
        List<String> tableId = Collections.unmodifiableList(buildTableId(dbName, tableName));
        if (tableAccessTtlNanos == 0) {
            return describeTableAccess(tableId, dbName, tableName).access;
        }
        // A hit takes neither the namespace lock nor any filesystem or REST I/O.
        return tableAccessCache.get(tableId, id -> describeTableAccess(id, dbName, tableName)).access;
    }

    private CachedTableAccess describeTableAccess(List<String> tableId, String dbName, String tableName) {
        DescribeTableResponse table = namespace.describeTable(
                new DescribeTableRequest().id(tableId).withTableUri(true).vendCredentials(vendCredentials));
        if (Boolean.TRUE.equals(table.getIsOnlyDeclared())) {
            throw new DorisConnectorException("Lance table is declared in the namespace but has no data yet");
        }
        // The namespace, not the dataset's _versions/ directory, decides which versions of such a table exist.
        // Opening it by URI reads the newest manifest in storage, which the namespace may not have published,
        // and is all this connector can do yet. Refused on every read: a refusal is never cached.
        if (Boolean.TRUE.equals(table.getManagedVersioning())) {
            throw new DorisConnectorException("Lance table " + dbName + "." + tableName + " has its versions"
                    + " managed by the namespace (managed_versioning), which is not supported yet");
        }
        String datasetUri = StringUtils.firstNonBlank(table.getTableUri(), table.getLocation());
        if (datasetUri == null) {
            throw new DorisConnectorException("Lance namespace returned no table URI for " + tableId);
        }
        // One option map serves both readers: the FE opens the dataset through the Lance Java SDK and the
        // BE through lance-c, so neither can end up with credentials the other lacks. The dataset URL picks
        // the option vocabulary, the same way Lance picks a provider from it.
        Map<String, String> vendedOptions = table.getStorageOptions();
        LanceTableAccess access = new LanceTableAccess(datasetUri,
                LanceStorageOptions.fromDorisAndVendedStorageOptions(datasetUri, storageProperties, vendedOptions),
                vendedOptions != null && !vendedOptions.isEmpty());
        return new CachedTableAccess(access, cacheTtlNanos(access));
    }

    /** How long a described access may be reused: 0 when it may carry credentials. */
    private long cacheTtlNanos(LanceTableAccess access) {
        // The BE cannot renew credentials during a scan. A fixed expiry margin cannot cover arbitrary query
        // durations, so vended options are described again for every read, even with a reported deadline.
        if (access.hasVendedOptions()) {
            return 0;
        }
        try {
            URI uri = new URI(access.getDatasetUri().trim());
            // Presigned or SAS credentials may live in the URI even when the storage options are empty. A
            // registry-based authority holds user info that URI.getRawUserInfo() does not report.
            if (uri.isOpaque() || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (uri.getRawAuthority() != null && uri.getRawAuthority().contains("@"))) {
                return 0;
            }
            return tableAccessTtlNanos;
        } catch (URISyntaxException e) {
            // A locator that cannot be classified stays usable, but is not assumed credential-free.
            return 0;
        }
    }

    private Cache<List<String>, CachedTableAccess> newTableAccessCache() {
        return Caffeine.newBuilder().maximumSize(TABLE_ACCESS_CACHE_MAX_ENTRIES).ticker(ticker)
                .expireAfter(new Expiry<List<String>, CachedTableAccess>() {
                    @Override
                    public long expireAfterCreate(List<String> key, CachedTableAccess value, long currentTime) {
                        return value.ttlNanos;
                    }

                    @Override
                    public long expireAfterUpdate(List<String> key, CachedTableAccess value, long currentTime,
                            long currentDuration) {
                        return value.ttlNanos;
                    }

                    @Override
                    public long expireAfterRead(List<String> key, CachedTableAccess value, long currentTime,
                            long currentDuration) {
                        return currentDuration;
                    }
                }).build();
    }

    /** A described access and how long it may be reused; a TTL of 0 expires it at once. */
    private static final class CachedTableAccess {
        private final LanceTableAccess access;
        private final long ttlNanos;

        private CachedTableAccess(LanceTableAccess access, long ttlNanos) {
            this.access = access;
            this.ttlNanos = ttlNanos;
        }
    }

    private List<String> buildNamespaceId(String dbName) {
        List<String> relativeNamespace;
        try {
            relativeNamespace = LanceNamespaceName.dorisDatabaseNameToNamespace(
                    dbName, DATABASE_NAMESPACE_DELIMITER, rootDatabase);
        } catch (IllegalArgumentException e) {
            throw new DorisConnectorException(e.getMessage(), e);
        }
        return buildFullNamespace(relativeNamespace);
    }

    private List<String> buildTableId(String dbName, String tableName) {
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
