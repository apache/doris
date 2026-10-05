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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.lance.index.LanceIndexInspection;
import org.apache.doris.datasource.lance.index.LanceIndexInspectionExecutor;
import org.apache.doris.datasource.lance.index.LancePhysicalIndexEntry;
import org.apache.doris.datasource.lance.index.LanceShowIndexInfo;
import org.apache.doris.datasource.lance.job.LanceIndexDatasetLocator;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceReadOptions;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceSnapshotResolver;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics.Stage;
import org.apache.doris.datasource.property.metastore.AbstractLanceProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Ref;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.TableBranchNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.errors.TableVersionNotFoundException;
import org.lance.namespace.model.TableVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.function.BiFunction;

/**
 * One catalog generation: Namespace access, snapshot reads and native resource lifetime.
 * Callers must hold a Lease, acquired while selecting the current generation in the catalog.
 */
final class LanceCatalogClient implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(LanceCatalogClient.class);
    private static final long METADATA_CACHE_SIZE_BYTES = 64L * 1024 * 1024;
    private static final long INDEX_CACHE_SIZE_BYTES = 128L * 1024 * 1024;

    private final LanceNamespace namespace;
    private final Session session;
    private final LanceNamespaceClient namespaceClient;
    private final Map<String, String> namespaceStorageOptions;
    private final BufferAllocator namespaceAllocator;
    private final List<String> catalogSecrets;
    private int activeOperations;
    private boolean retired;

    static LanceCatalogClient create(AbstractLanceProperties properties,
            List<StorageProperties> storageProperties, Map<String, String> namespaceOptions,
            List<String> catalogSecrets) throws DdlException {
        long limit = Config.lance_catalog_arrow_memory_limit_bytes;
        if (limit <= 0) {
            throw new IllegalArgumentException("lance_catalog_arrow_memory_limit_bytes must be positive");
        }
        BufferAllocator allocator = new RootAllocator(limit);
        LanceNamespace namespace = null;
        Session session = null;
        try {
            List<String> parent = LanceNamespaceName.parseParentNamespace(
                    properties.getNamespaceParent(), properties.getNamespaceDelimiter());
            namespace = properties.createNamespace(allocator, namespaceOptions);
            session = Session.builder().metadataCacheSizeBytes(METADATA_CACHE_SIZE_BYTES)
                    .indexCacheSizeBytes(INDEX_CACHE_SIZE_BYTES).build();
            return new LanceCatalogClient(namespace, allocator, session, properties.getLanceCatalogType(),
                    properties.getRootDatabase(), parent, storageProperties, namespaceOptions, catalogSecrets,
                    properties.getTableAccessCacheTtlSeconds());
        } catch (RuntimeException | Error e) {
            closeResource(namespace);
            closeResource(session);
            closeResource(allocator);
            throw e;
        }
    }

    LanceCatalogClient(LanceNamespace namespace, BufferAllocator allocator, Session session,
            String catalogType, String rootDatabase, List<String> parentNamespace,
            List<StorageProperties> storageProperties, Map<String, String> namespaceStorageOptions,
            List<String> catalogSecrets) {
        this(namespace, allocator, session, catalogType, rootDatabase, parentNamespace,
                storageProperties, namespaceStorageOptions, catalogSecrets,
                AbstractLanceProperties.DEFAULT_TABLE_ACCESS_CACHE_TTL_SECONDS);
    }

    LanceCatalogClient(LanceNamespace namespace, BufferAllocator allocator, Session session,
            String catalogType, String rootDatabase, List<String> parentNamespace,
            List<StorageProperties> storageProperties, Map<String, String> namespaceStorageOptions,
            List<String> catalogSecrets, int tableAccessCacheTtlSeconds) {
        this.catalogSecrets = Collections.unmodifiableList(new ArrayList<>(catalogSecrets));
        this.namespace = namespace;
        this.namespaceAllocator = allocator;
        this.session = session;
        this.namespaceClient = new LanceNamespaceClient(
                namespace, catalogType, rootDatabase, parentNamespace, storageProperties,
                tableAccessCacheTtlSeconds, Ticker.systemTicker());
        this.namespaceStorageOptions = Collections.unmodifiableMap(new HashMap<>(namespaceStorageOptions));
    }

    /** Pins this generation for one operation; the lock does not cover its SDK or JNI calls. */
    synchronized Lease acquire() {
        if (retired) {
            throw new IllegalStateException("Lance catalog resources have been closed");
        }
        activeOperations++;
        return new Lease(this);
    }

    /** Retires this generation immediately; its last active operation performs resource cleanup. */
    @Override
    public void close() {
        boolean release;
        synchronized (this) {
            if (retired) {
                return;
            }
            retired = true;
            release = activeOperations == 0;
        }
        if (release) {
            closeResources();
        }
    }

    private void release() {
        boolean release;
        synchronized (this) {
            activeOperations--;
            release = retired && activeOperations == 0;
        }
        if (release) {
            closeResources();
        }
    }

    private void closeResources() {
        closeResource(namespace);
        closeResource(session);
        closeResource(namespaceAllocator);
    }

    private static void closeResource(Object resource) {
        if (resource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) resource).close();
            } catch (Exception e) {
                // Provider exception messages may contain credentials.
                LOG.warn("Failed to close a Lance catalog resource ({})", resource.getClass().getSimpleName());
            }
        }
    }

    static final class Lease implements AutoCloseable {
        private final LanceCatalogClient client;
        private boolean closed;

        private Lease(LanceCatalogClient client) {
            this.client = client;
        }

        LanceCatalogClient client() {
            return client;
        }

        @Override
        public void close() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            client.release();
        }
    }

    void invalidateTableAccessCache() {
        namespaceClient.invalidateTableAccessCache();
    }

    List<String> listDatabaseNames() {
        return namespaceClient.listDatabaseNames();
    }

    List<String> listTableNames(String dbName) {
        return namespaceClient.listTableNames(dbName);
    }

    boolean tableExists(String dbName, String tableName) {
        return namespaceClient.tableExists(dbName, tableName);
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName) {
        return loadTableMetadata(dbName, tableName, Optional.empty());
    }

    public LanceTableMetadata loadTableMetadataForSearch(String dbName, String tableName) {
        return loadTableMetadataForSearch(dbName, tableName, LanceRefSelector.latest());
    }

    public LanceTableMetadata loadTableMetadataForSearch(String dbName, String tableName, LanceRefSelector selector) {
        LanceTableMetadata metadata = loadQueryMetadata(dbName, tableName, selector,
                LanceMetadataLoader.MetadataScope.WITH_INDEXES);
        if (!metadata.getIndexMetadataState().canPlanIndexSegments()) {
            throw new IllegalArgumentException("Lance SDK cannot provide field IDs required for search planning");
        }
        return metadata;
    }

    public LanceTableMetadata loadBasicTableMetadata(String dbName, String tableName) {
        return loadBasicTableMetadata(dbName, tableName, LanceRefSelector.latest());
    }

    public LanceTableMetadata loadBasicTableMetadata(String dbName, String tableName, LanceRefSelector selector) {
        return loadQueryMetadata(dbName, tableName, selector, LanceMetadataLoader.MetadataScope.BASIC);
    }

    public Schema loadTableSchema(String dbName, String tableName) {
        return readTableSnapshot(dbName, tableName, LanceRefSelector.latest(),
                (dataset, access, metrics) -> metrics.measure(Stage.SCHEMA, dataset::getSchema));
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName,
            Optional<TableSnapshot> tableSnapshot) {
        return loadTableMetadata(dbName, tableName, LanceRefSelector.snapshot(tableSnapshot));
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName, LanceRefSelector selector) {
        return loadQueryMetadata(dbName, tableName, selector, LanceMetadataLoader.MetadataScope.WITH_INDEXES);
    }

    private LanceTableMetadata loadQueryMetadata(String dbName, String tableName,
            LanceRefSelector selector, LanceMetadataLoader.MetadataScope mode) {
        return readTableSnapshot(dbName, tableName, selector,
                (dataset, access, metrics) -> LanceMetadataLoader.read(dataset, access, mode, metrics));
    }

    /**
     * Pins one resource generation, resolved table access, and the Dataset version for the whole read.
     *
     * <p>Every dataset is read by its URI and a version, as the BE reads it. The latest version of
     * the main chain in storage is opened once as a handle, and the other selectors are checkouts
     * from it, except an explicit version on the main chain and a managed table's branch (below).
     * A tag is resolved first to the chain and version it points at, so a tag created on a branch
     * selects that branch.
     *
     * <p>For a managed table the namespace decides which versions exist. "Latest" is the newest
     * version it records, never the newest manifest in storage, and every version a read selects
     * must be one it records, at the manifest path Doris reads ({@link LanceManifestPaths}). A
     * branch is read from its own directory, as the BE reads it, so it does not depend on the main
     * chain; the main handle only supplies tag files and the manifest listing that FOR TIME AS OF
     * on main takes commit times from.
     */
    private <T> T readTableSnapshot(String dbName, String tableName, LanceRefSelector selector,
            SnapshotReader<T> reader) {
        ReadState state = new ReadState(selector, dbName + "." + tableName);
        LanceMetadataMetrics metrics = LanceMetadataMetrics.startMetadataRead();
        try {
            T result;
            try (BufferAllocator allocator = namespaceAllocator.newChildAllocator(
                    "lance-metadata-read", 0, namespaceAllocator.getLimit())) {
                state.access = metrics.measure(Stage.TABLE_ACCESS,
                        () -> namespaceClient.resolveTableAccess(dbName, tableName));
                OptionalLong direct = directMainVersion(state, metrics);
                if (state.access.isManagedVersioning() && state.branch.isPresent() && !selector.getTag().isPresent()) {
                    result = readManagedBranch(allocator, state, reader, metrics);
                } else if (direct.isPresent() || isLatestMain(selector)) {
                    state.version = direct;
                    try (Dataset dataset = openDataset(allocator, state.access, direct, metrics)) {
                        result = reader.read(dataset, state.access, metrics);
                    }
                } else {
                    try (Dataset main = openDataset(allocator, state.access, OptionalLong.empty(), metrics)) {
                        result = readFromLatest(main, state, reader, metrics);
                    }
                }
            }
            metrics.succeeded();
            return result;
        } catch (LanceUserFacingException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (Exception e) {
            LanceTableAccess access = state.access;
            String uri = access == null ? null : access.getDatasetUri();
            Map<String, String> options = access == null ? namespaceStorageOptions : access.getStorageOptions();
            String what = state.displayName();
            // Lance's Directory namespace reports a branch it lacks as a missing table. The table
            // was described in this read, so a missing table from a branch's version request
            // means the branch.
            boolean namespaceLacksBranch = access != null && access.isManagedVersioning()
                    && ExceptionUtils.indexOfType(e, TableNotFoundException.class) >= 0;
            if (state.branch.isPresent() && !state.branchExists
                    && (namespaceLacksBranch || isBranchNotFound(e, state.branch.get()))) {
                throw new RuntimeException("Lance branch '" + state.branch.get() + "' of " + state.tableName
                        + state.selector.getTag().map(tag -> " (tag '" + tag + "')").orElse("")
                        + " was not found" + (namespaceLacksBranch || isNamespaceMiss(e) ? " in the namespace" : ""),
                        sanitizedCause(e, uri, options));
            }
            if (isVersionNotFound(e) && state.pinned != null
                    && state.pinned.manifest == LanceManifestPaths.Recorded.STAGED) {
                throw new RuntimeException(unreadableStaged(state.pinned, state), sanitizedCause(e, uri, options));
            }
            if (state.version.isPresent() && isVersionNotFound(e)) {
                throw new RuntimeException("Lance version " + state.version.getAsLong() + " of " + what
                        + state.selector.getTag().map(tag -> " (tag '" + tag + "')").orElse("")
                        + " was not found" + (isNamespaceMiss(e) ? " in the namespace" : ""),
                        sanitizedCause(e, uri, options));
            }
            throw LanceErrorMessages.failure("Failed to load Lance table metadata for " + what, e, uri, options,
                    catalogSecrets);
        } finally {
            metrics.close();
        }
    }

    /** What a read has resolved so far; the catch block reports errors against it. */
    private static final class ReadState {
        private final LanceRefSelector selector;
        private final String tableName;
        /** The table's access; a branch's access is derived from it with {@code onBranch}. */
        private LanceTableAccess access;
        private Optional<String> branch;
        /**
         * Set once the branch is known to exist: the namespace recorded versions for it, or its
         * latest version was checked out. Later failures are not reported as a missing branch.
         */
        private boolean branchExists;
        private OptionalLong version = OptionalLong.empty();
        /** The managed version this read opens next, as the namespace records it. */
        private Recorded pinned;
        /** The namespace's version list of the chain a FOR TIME AS OF reads ("" is main), fetched once. */
        private final Map<String, List<TableVersion>> namespaceVersions = new HashMap<>();

        private ReadState(LanceRefSelector selector, String tableName) {
            this.selector = selector;
            this.tableName = tableName;
            this.branch = selector.getBranch();
        }

        private String displayName() {
            return tableName + branch.map(name -> "@" + name).orElse("");
        }
    }

    /** A version of a managed chain the namespace records, and how it records its manifest. */
    private static final class Recorded {
        private final Optional<String> branch;
        private final long version;
        private final LanceManifestPaths.Recorded manifest;

        private Recorded(Optional<String> branch, long version, LanceManifestPaths.Recorded manifest) {
            this.branch = branch;
            this.version = version;
            this.manifest = manifest;
        }
    }

    private static boolean isLatestMain(LanceRefSelector selector) {
        return !selector.getTag().isPresent() && !selector.getBranch().isPresent()
                && !selector.getSnapshot().isPresent();
    }

    /**
     * The main-chain version a selector names without looking at the latest manifest: an explicit
     * version, or the latest version of a managed table, which the namespace records.
     */
    private OptionalLong directMainVersion(ReadState state, LanceMetadataMetrics metrics) {
        LanceRefSelector selector = state.selector;
        if (selector.getTag().isPresent() || selector.getBranch().isPresent()) {
            return OptionalLong.empty();
        }
        if (!selector.getSnapshot().isPresent()) {
            return state.access.isManagedVersioning()
                    ? OptionalLong.of(recordedHead(state, Optional.empty(), metrics))
                    : OptionalLong.empty();
        }
        TableSnapshot snapshot = selector.getSnapshot().get();
        if (snapshot.getType() != TableSnapshot.VersionType.VERSION) {
            return OptionalLong.empty();
        }
        state.version = OptionalLong.of(LanceSnapshotResolver.parseVersion(snapshot.getValue()));
        requireRecorded(state, Optional.empty(), state.version.getAsLong(), metrics);
        return state.version;
    }

    /** Resolves the selector against the open latest main chain and reads the selected snapshot. */
    private <T> T readFromLatest(Dataset main, ReadState state, SnapshotReader<T> reader, LanceMetadataMetrics metrics)
            throws Exception {
        LanceRefSelector selector = state.selector;
        if (selector.getTag().isPresent()) {
            // Only this tag's file is read, however many tags the table has. The SDK checks the tag
            // out on the branch of the version it points at.
            String tag = selector.getTag().get();
            state.version = OptionalLong.of(metrics.measure(Stage.VERSION_RESOLVE, () -> tagVersion(main, tag, state)));
            try (Dataset target = checkout(main, Ref.ofTag(tag), metrics)) {
                // The checkout reads the tag file again; the version it read is the one to check.
                state.version = OptionalLong.of(target.version());
                state.branch = branchOf(target.uri(), state.access.getDatasetUri());
                requireRecorded(state, state.branch, target.version(), metrics);
                return reader.read(target, accessOf(target, state), metrics);
            }
        }
        if (state.branch.isPresent()) {
            String branch = state.branch.get();
            // Check out the branch's latest version first even when a version is already known, so
            // a missing branch and a missing version inside an existing branch are told apart.
            try (Dataset latest = checkout(main, Ref.ofBranch(branch), metrics)) {
                state.branchExists = true;
                LanceTableAccess branchAccess = accessOf(latest, state);
                if (!state.version.isPresent() && selector.getSnapshot().isPresent()) {
                    state.version = resolveSnapshotVersion(latest, branchAccess, selector.getSnapshot().get(), state,
                            metrics);
                }
                if (!state.version.isPresent()) {
                    return reader.read(latest, branchAccess, metrics);
                }
                try (Dataset dataset = checkout(latest, Ref.ofBranch(branch, state.version.getAsLong()), metrics)) {
                    return reader.read(dataset, branchAccess, metrics);
                }
            }
        }
        // FOR TIME AS OF on the main chain, resolved from manifest commit times.
        state.version = resolveSnapshotVersion(main, state.access, selector.getSnapshot().get(), state, metrics);
        try (Dataset dataset = checkout(main, Ref.ofMain(state.version.getAsLong()), metrics)) {
            return reader.read(dataset, state.access, metrics);
        }
    }

    /**
     * Reads a branch of a managed table from the branch's directory, by URI and version as the BE
     * reads it. The namespace answers for the branch first, so a branch it lacks is reported as
     * missing, and the main chain need not be readable. FOR TIME AS OF lists the branch's
     * manifests from its newest version in storage and, as on main, selects among the versions the
     * namespace records.
     */
    private <T> T readManagedBranch(BufferAllocator allocator, ReadState state, SnapshotReader<T> reader,
            LanceMetadataMetrics metrics) throws Exception {
        String branch = state.branch.get();
        checkBranchName(branch);
        LanceTableAccess branchAccess = state.access.onBranch(branch,
                branchUri(state.access.getDatasetUri(), branch));
        Optional<TableSnapshot> snapshot = state.selector.getSnapshot();
        if (!snapshot.isPresent()) {
            state.version = OptionalLong.of(recordedHead(state, state.branch, metrics));
        } else if (snapshot.get().getType() == TableSnapshot.VersionType.VERSION) {
            state.version = OptionalLong.of(LanceSnapshotResolver.parseVersion(snapshot.get().getValue()));
            requireRecorded(state, state.branch, state.version.getAsLong(), metrics);
        } else {
            namespaceVersions(state, state.branch, metrics);
            state.branchExists = true;
            try (Dataset latest = openDataset(allocator, branchAccess, OptionalLong.empty(), metrics)) {
                state.version = resolveSnapshotVersion(latest, branchAccess, snapshot.get(), state, metrics);
            }
        }
        state.branchExists = true;
        try (Dataset dataset = openDataset(allocator, branchAccess, state.version, metrics)) {
            return reader.read(dataset, branchAccess, metrics);
        }
    }

    /**
     * Rejects a branch name that could leave the table's {@code tree/} directory or change the URI
     * {@link #branchUri} joins for a managed table, which Lance does not validate on that path;
     * the object store resolves dot segments, including percent-encoded ones. Other tables check a
     * branch out through Lance, which validates the name itself.
     *
     * <p>This never rejects a name Lance's {@code check_valid_branch} accepts, whichever Unicode
     * version either side uses: ASCII allows exactly Lance's letters, digits, '.', '-', '_' and
     * '/', and outside ASCII only whitespace and control characters, which are never
     * alphanumeric, are rejected. Lance cannot create a branch with a name it would reject, and the
     * namespace decides whether such a branch exists.
     */
    static void checkBranchName(String branch) {
        String reason = null;
        if (branch.isEmpty()) {
            reason = "it is empty";
        } else if (branch.startsWith("/") || branch.endsWith("/") || branch.contains("//")) {
            reason = "it starts or ends with '/' or contains an empty segment";
        } else if (branch.contains("..") || branch.endsWith(".lock")) {
            reason = "it contains '..' or ends with '.lock'";
        } else if (!branch.codePoints().allMatch(LanceCatalogClient::isBranchNameCharacter)) {
            reason = "only letters, digits, '.', '-', '_' and '/' between segments are allowed";
        }
        if (reason != null) {
            throw new LanceUserFacingException("Invalid Lance branch name '" + branch + "': " + reason);
        }
    }

    private static boolean isBranchNameCharacter(int codePoint) {
        if (codePoint < 0x80) {
            return (codePoint >= 'a' && codePoint <= 'z') || (codePoint >= 'A' && codePoint <= 'Z')
                    || (codePoint >= '0' && codePoint <= '9') || codePoint == '.' || codePoint == '-'
                    || codePoint == '_' || codePoint == '/';
        }
        return !Character.isWhitespace(codePoint) && !Character.isSpaceChar(codePoint)
                && !Character.isISOControl(codePoint);
    }

    /**
     * The URI of a branch's directory, joined as Lance's {@code BranchLocation} joins it:
     * {@code tree/<branch>} under the table root, before the URI's query string.
     */
    static String branchUri(String tableUri, String branch) {
        int query = tableUri.indexOf('?');
        String path = query < 0 ? tableUri : tableUri.substring(0, query);
        String joined = path + (path.endsWith("/") ? "" : "/") + "tree/" + StringUtils.stripStart(branch, "/");
        return query < 0 ? joined : joined + tableUri.substring(query);
    }

    private static long tagVersion(Dataset main, String tag, ReadState state) {
        try {
            return main.tags().getVersion(tag);
        } catch (RuntimeException e) {
            String rootMessage = ExceptionUtils.getRootCauseMessage(e);
            if (rootMessage != null && rootMessage.contains("tag " + tag + " does not exist")) {
                throw new LanceUserFacingException("Lance tag '" + tag + "' of " + state.tableName + " was not found");
            }
            throw e;
        }
    }

    /**
     * A dataset URI without its query and trailing slash. The query may carry credentials, which a
     * namespace can vend anew on every describe.
     */
    private static String location(String uri) {
        return StringUtils.removeEnd(StringUtils.substringBefore(uri, "?"), "/");
    }

    /**
     * The branch a dataset checked out from the table root is on, from its root directory: the
     * table root for main, {@code <root>/tree/<branch>} otherwise. Lance inserts the branch path
     * before a URI's query string, so the query is compared apart. A URI that is neither is an
     * error rather than main, which would hand the BE the wrong chain.
     */
    static Optional<String> branchOf(String checkedOutUri, String tableUri) {
        String root = location(tableUri);
        String uri = location(checkedOutUri);
        if (uri.equals(root)) {
            return Optional.empty();
        }
        String branchRoot = root + "/tree/";
        if (!uri.startsWith(branchRoot) || uri.length() == branchRoot.length()) {
            // The URIs may carry credentials in their query, so they stay out of the message.
            throw new IllegalStateException("Cannot tell which branch a Lance tag was checked out on");
        }
        return Optional.of(uri.substring(branchRoot.length()));
    }

    /**
     * The access for a dataset checked out from the table: the main chain keeps the table access,
     * and a branch takes the directory the SDK checked out, which is what the BE opens by URI.
     */
    private static LanceTableAccess accessOf(Dataset dataset, ReadState state) {
        return state.branch.isPresent() ? state.access.onBranch(state.branch.get(), dataset.uri()) : state.access;
    }

    /** A selector error whose message is user-facing as is, such as a tag that does not exist. */
    private static final class LanceUserFacingException extends RuntimeException {
        private LanceUserFacingException(String message) {
            super(message);
        }
    }

    /**
     * The newest version the namespace records for a managed chain, which the read then opens.
     * Doris asks for it itself: opening "latest" by URI would read the newest manifest in storage,
     * which the namespace may not have published.
     */
    private long recordedHead(ReadState state, Optional<String> branch, LanceMetadataMetrics metrics) {
        state.pinned = null;
        Optional<TableVersion> head = metrics.measure(Stage.VERSION_RESOLVE,
                () -> namespaceClient.latestManagedVersion(state.access, branch));
        if (!head.isPresent()) {
            throw new LanceUserFacingException("Lance namespace lists no versions for " + state.tableName
                    + branch.map(name -> "@" + name).orElse(""));
        }
        long version = head.get().getVersion();
        state.pinned = new Recorded(branch, version, LanceManifestPaths.check(state.access.getDatasetUri(), branch,
                version, head.get().getManifestPath(), state.tableName));
        return version;
    }

    /**
     * Requires the namespace of a managed table to record {@code version} of the chain on
     * {@code branch}, at the manifest path Doris reads; nothing for a storage-versioned table.
     */
    private void requireRecorded(ReadState state, Optional<String> branch, long version,
            LanceMetadataMetrics metrics) {
        if (!state.access.isManagedVersioning()) {
            return;
        }
        state.pinned = null;
        TableVersion recorded = metrics.measure(Stage.VERSION_RESOLVE,
                () -> namespaceClient.describeManagedVersion(state.access, branch, version));
        state.pinned = new Recorded(branch, version, LanceManifestPaths.check(state.access.getDatasetUri(), branch,
                version, recorded.getManifestPath(), state.tableName));
    }

    /**
     * The error for a version the namespace records at a staged manifest while its canonical
     * manifest, which Doris reads, does not exist. Either the commit reserved the version and was
     * not finalized, which a reader that uses the namespace would finish and Doris does not, or
     * the version was finalized and cleanup later removed it; the namespace's record does not tell
     * the two apart.
     */
    private static String unreadableStaged(Recorded pinned, ReadState state) {
        return "Lance version " + pinned.version + " of " + state.tableName
                + pinned.branch.map(name -> "@" + name).orElse("") + " cannot be read: " + stagedOnly();
    }

    private static String stagedOnly() {
        return "the namespace records it at a staged manifest, and its canonical manifest, which Doris reads,"
                + " does not exist (its commit was not finalized, or cleanup removed it)";
    }

    private RuntimeException sanitizedCause(Throwable error, String uri, Map<String, String> options) {
        return new RuntimeException(LanceErrorMessages.sanitize(error, uri, options, catalogSecrets));
    }

    /** Checks out a ref of an already open dataset; the SDK resolves the ref from the dataset directory. */
    private static Dataset checkout(Dataset dataset, Ref ref, LanceMetadataMetrics metrics) {
        return metrics.measure(Stage.VERSION_RESOLVE, () -> dataset.checkout(ref));
    }

    /**
     * Resolves a {@code FOR VERSION AS OF} / {@code FOR TIME AS OF} snapshot against the chain
     * {@code latest} is checked out on: the main chain, or a branch when {@code access} is a
     * branch access.
     */
    private OptionalLong resolveSnapshotVersion(Dataset latest, LanceTableAccess access, TableSnapshot snapshot,
            ReadState state, LanceMetadataMetrics metrics) {
        if (snapshot.getType() == TableSnapshot.VersionType.VERSION) {
            state.version = OptionalLong.of(LanceSnapshotResolver.parseVersion(snapshot.getValue()));
            requireRecorded(state, access.getBranch(), state.version.getAsLong(), metrics);
            return state.version;
        }
        long timestamp = parseTimeTravelTimestamp(snapshot.getValue());
        try {
            return OptionalLong.of(resolveVersionAtOrBefore(latest, access, timestamp, snapshot.getValue(), state,
                    metrics));
        } catch (LanceSnapshotResolver.NoVersionAtOrBeforeException e) {
            if (!access.getBranch().isPresent()) {
                throw new LanceUserFacingException("Lance table " + state.tableName + " has no version at or before '"
                        + snapshot.getValue() + "'");
            }
            // A branch's chain starts at the version it was created from and carries its own
            // commit times, so an earlier timestamp has nothing to select on the branch.
            throw new LanceUserFacingException("Lance branch '" + access.getBranch().get() + "' of "
                    + state.tableName + " has no version at or before '" + snapshot.getValue()
                    + "'; a branch only holds the versions from its creation on");
        }
    }

    /**
     * Whether a failure means the branch does not exist. A checkout reports "branch <name> does
     * not exist", or a missing manifest under the branch directory when nothing was ever
     * committed there; a namespace that reports the branch itself throws
     * {@link TableBranchNotFoundException}.
     */
    private static boolean isBranchNotFound(Throwable throwable, String branch) {
        if (ExceptionUtils.indexOfType(throwable, TableBranchNotFoundException.class) >= 0) {
            return true;
        }
        String rootMessage = ExceptionUtils.getRootCauseMessage(throwable);
        if (rootMessage == null) {
            return false;
        }
        String lower = rootMessage.toLowerCase(Locale.ROOT);
        String name = branch.toLowerCase(Locale.ROOT);
        return lower.contains("branch " + name + " does not exist")
                || (lower.contains("not found") && lower.contains("tree/" + name + "/"));
    }

    /** Whether a not-found came from the namespace client rather than storage. */
    private static boolean isNamespaceMiss(Throwable throwable) {
        return ExceptionUtils.indexOfType(throwable, TableVersionNotFoundException.class) >= 0
                || ExceptionUtils.indexOfType(throwable, TableBranchNotFoundException.class) >= 0;
    }

    /**
     * Every version the namespace records for the chain on {@code branch}, listed once per read.
     * The whole list is needed: FOR TIME AS OF selects among it, and neither the order a namespace
     * returns nor monotonic commit times can be relied on to stop early.
     */
    private List<TableVersion> namespaceVersions(ReadState state, Optional<String> branch,
            LanceMetadataMetrics metrics) {
        return state.namespaceVersions.computeIfAbsent(branch.orElse(""), chain -> {
            List<TableVersion> versions = metrics.measure(Stage.VERSION_RESOLVE,
                    () -> namespaceClient.listManagedVersions(state.access, branch));
            if (versions.isEmpty()) {
                throw new LanceUserFacingException("Lance namespace lists no versions for "
                        + state.tableName + (chain.isEmpty() ? "" : "@" + chain));
            }
            return versions;
        });
    }

    /**
     * Resolves {@code FOR TIME AS OF} to a version on the chain {@code latest} is checked out on,
     * from the commit times the manifests in storage record, over the history
     * {@link LanceSnapshotResolver} describes. A managed table only selects among the versions its
     * namespace records. A version it no longer records between recorded ones cuts the history
     * like a removed one, and so does a recorded version storage lacks, since its commit time is
     * unknown.
     */
    private long resolveVersionAtOrBefore(Dataset latest, LanceTableAccess access, long timestamp,
            String requestedText, ReadState state, LanceMetadataMetrics metrics) {
        state.pinned = null;
        Map<Long, TableVersion> records = null;
        if (access.isManagedVersioning()) {
            records = new HashMap<>();
            for (TableVersion recorded : namespaceVersions(state, access.getBranch(), metrics)) {
                if (recorded.getVersion() != null) {
                    // Selection compares the commit times of the manifests Doris reads, so each
                    // recorded version must be at one of them, not only the selected one.
                    LanceManifestPaths.check(state.access.getDatasetUri(), access.getBranch(), recorded.getVersion(),
                            recorded.getManifestPath(), state.tableName);
                    records.put(recorded.getVersion(), recorded);
                }
            }
        }
        Map<Long, TableVersion> recordedById = records;
        NavigableSet<Long> recorded = records == null ? null : new TreeSet<>(records.keySet());
        long version = metrics.measure(Stage.VERSION_RESOLVE, () -> {
            try {
                return LanceSnapshotResolver.versionAtOrBefore(latest.listVersions(), recorded, timestamp,
                        requestedText);
            } catch (LanceSnapshotResolver.HistoryRemovedException e) {
                // A removed version the namespace still records may only be staged; one it no
                // longer records is gone.
                TableVersion removed = recordedById == null ? null : recordedById.get(e.getVersion());
                boolean staged = removed != null && LanceManifestPaths.check(state.access.getDatasetUri(),
                        access.getBranch(), e.getVersion(), removed.getManifestPath(), state.tableName)
                        == LanceManifestPaths.Recorded.STAGED;
                throw historyRemoved(e.getVersion(), staged, requestedText, state);
            }
        });
        if (records != null) {
            state.pinned = new Recorded(access.getBranch(), version, LanceManifestPaths.check(
                    state.access.getDatasetUri(), access.getBranch(), version, records.get(version).getManifestPath(),
                    state.tableName));
        }
        LOG.debug("Resolved Lance FOR TIME AS OF '{}' to version {} from manifest commit times", requestedText,
                version);
        return version;
    }

    private static LanceUserFacingException historyRemoved(long version, boolean staged, String requestedText,
            ReadState state) {
        // Worded for FOR TIME AS OF and the search functions' timestamp alike.
        return new LanceUserFacingException("Lance cannot select the version at or before '" + requestedText + "' on "
                + state.displayName() + ": version " + version + ", which may hold the state at that time, "
                + (staged ? "cannot be read: " + stagedOnly() : "no longer exists"));
    }

    private static long parseTimeTravelTimestamp(String value) {
        OptionalLong timestamp = LanceSnapshotResolver.parseTimestamp(value);
        if (!timestamp.isPresent()) {
            throw new IllegalArgumentException("Cannot parse Lance FOR TIME AS OF value '" + value
                    + "', expected " + LanceSnapshotResolver.TIMESTAMP_FORMATS);
        }
        return timestamp.getAsLong();
    }

    /**
     * Whether a failed open of an explicitly requested version means that version does not exist.
     * A namespace reports it through {@link TableVersionNotFoundException}. The storage reader
     * reports it as a missing manifest under {@code _versions/} or as Lance's own version-not-found
     * error; a missing dataset or an unreachable store fails differently and keeps its message.
     */
    private static boolean isVersionNotFound(Throwable throwable) {
        if (ExceptionUtils.indexOfType(throwable, TableVersionNotFoundException.class) >= 0) {
            return true;
        }
        String rootMessage = ExceptionUtils.getRootCauseMessage(throwable);
        if (rootMessage == null) {
            return false;
        }
        String lower = rootMessage.toLowerCase(Locale.ROOT);
        return lower.contains("version not found")
                || (lower.contains("not found") && lower.contains("_versions/"));
    }

    private Dataset openDataset(BufferAllocator allocator, LanceTableAccess access, OptionalLong version,
            LanceMetadataMetrics metrics) {
        return metrics.measure(Stage.DATASET_OPEN, () -> Dataset.open().allocator(allocator).uri(access.getDatasetUri())
                .readOptions(LanceReadOptions.forSharedSession(access.getStorageOptions(), version, session)).build());
    }

    @FunctionalInterface
    private interface SnapshotReader<T> {
        T read(Dataset dataset, LanceTableAccess access, LanceMetadataMetrics metrics);
    }

    public List<LanceShowIndexInfo> loadTableIndexesForShow(
            String dbName, String tableName) {
        return inspectTableIndexes(dbName, tableName,
                (dataset, uri) -> LanceIndexInspection.readIndexesForShow(dataset));
    }

    public List<LancePhysicalIndexEntry> loadTableIndexEntries(
            String dbName, String tableName) {
        return inspectTableIndexes(dbName, tableName,
                (dataset, uri) -> LanceIndexInspection.readPhysicalEntries(dataset));
    }

    String resolveCurrentIndexJobLocator(String dbName, String tableName) {
        return LanceIndexDatasetLocator.normalize(
                namespaceClient.resolveTableAccessUncached(dbName, tableName).getDatasetUri());
    }

    public LanceIndexAdmissionSnapshot loadTableIndexAdmissionSnapshot(String dbName, String tableName) {
        return inspectTableIndexes(dbName, tableName, LanceIndexInspection::readAdmissionSnapshot);
    }

    private <T> T inspectTableIndexes(String dbName, String tableName, BiFunction<Dataset, String, T> inspection) {
        LanceTableAccess tableAccess = null;
        try {
            // The worker owns its Dataset and Session even if the caller releases its lease on timeout.
            // Index admission must verify the current target even while query access is cached.
            tableAccess = namespaceClient.resolveTableAccessUncached(dbName, tableName);
            // Index paths open the dataset by URI at storage-latest. They are only reachable for
            // filesystem catalogs, whose Directory namespace never manages versions; a managed
            // table here would bypass the namespace.
            if (tableAccess.isManagedVersioning()) {
                throw new IllegalStateException("Lance index inspection does not support namespace-managed tables");
            }
            LanceTableAccess access = tableAccess;
            return LanceIndexInspectionExecutor.execute(() -> {
                // The caller can time out while JNI is running; the worker must own resource cleanup.
                try (BufferAllocator allocator = new RootAllocator(LanceMetadataLoader.READ_ALLOCATOR_LIMIT);
                        Dataset dataset = Dataset.open().allocator(allocator).uri(access.getDatasetUri())
                                .readOptions(LanceReadOptions.forIndependentRead(
                                        access.getStorageOptions(), OptionalLong.empty()))
                                .build()) {
                    return inspection.apply(dataset, access.getDatasetUri());
                }
            });
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to load Lance index metadata for " + dbName + "." + tableName, e,
                    tableAccess == null ? null : tableAccess.getDatasetUri(),
                    tableAccess == null ? namespaceStorageOptions : tableAccess.getStorageOptions(), catalogSecrets);
        }
    }

}
