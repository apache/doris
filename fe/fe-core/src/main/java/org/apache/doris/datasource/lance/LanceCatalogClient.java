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
import org.apache.doris.common.util.TimeUtils;
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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.Ref;
import org.lance.Session;
import org.lance.Tag;
import org.lance.Version;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.TableBranchNotFoundException;
import org.lance.namespace.errors.TableVersionNotFoundException;
import org.lance.namespace.model.TableVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * One catalog generation: Namespace access, snapshot reads and native resource lifetime.
 * Callers must hold a Lease, acquired while selecting the current generation in the catalog.
 */
final class LanceCatalogClient implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(LanceCatalogClient.class);
    /** Lance's name for the main chain; {@code @branch(main)} and tags on it mean the main chain. */
    static final String MAIN_BRANCH = "main";
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
        LanceTableMetadata metadata = loadQueryMetadata(dbName, tableName, Optional.empty(),
                LanceMetadataLoader.MetadataScope.WITH_INDEXES);
        if (!metadata.getIndexMetadataState().canPlanIndexSegments()) {
            throw new IllegalArgumentException("Lance SDK cannot provide field IDs required for search planning");
        }
        return metadata;
    }

    public LanceTableMetadata loadBasicTableMetadata(String dbName, String tableName) {
        return loadQueryMetadata(dbName, tableName, Optional.empty(), LanceMetadataLoader.MetadataScope.BASIC);
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
            Optional<TableSnapshot> tableSnapshot, LanceMetadataLoader.MetadataScope mode) {
        return loadQueryMetadata(dbName, tableName, LanceRefSelector.snapshot(tableSnapshot), mode);
    }

    private LanceTableMetadata loadQueryMetadata(String dbName, String tableName,
            LanceRefSelector selector, LanceMetadataLoader.MetadataScope mode) {
        return readTableSnapshot(dbName, tableName, selector,
                (dataset, access, metrics) -> LanceMetadataLoader.read(dataset, access, mode, metrics));
    }

    /**
     * Pins one resource generation, resolved table access, and the Dataset version for the whole read.
     *
     * <p>The latest version of the main chain is opened once and every other selector is a
     * checkout from that handle, so the SDK resolves the ref with the same commit handler
     * (the namespace's, for a managed table). A tag is resolved first to the chain and version it
     * points at, so a tag created on a branch selects that branch. The two shortcuts that skip the
     * latest open are an explicit version on the main chain, and {@code FOR TIME AS OF} on a
     * managed table whose namespace reports commit times.
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
                if (direct.isPresent() || isLatestMain(selector)) {
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
            if (state.branch.isPresent() && !state.branchCheckedOut && isBranchNotFound(e, state.branch.get())) {
                throw new RuntimeException("Lance branch '" + state.branch.get() + "' of " + state.tableName
                        + state.selector.getTag().map(tag -> " (tag '" + tag + "')").orElse("")
                        + " was not found" + (isNamespaceMiss(e, "table branch not found") ? " in the namespace" : ""),
                        sanitizedCause(e, uri, options));
            }
            if (state.version.isPresent() && isVersionNotFound(e)) {
                throw new RuntimeException("Lance version " + state.version.getAsLong() + " of " + what
                        + state.selector.getTag().map(tag -> " (tag '" + tag + "')").orElse("")
                        + " was not found" + (isNamespaceMiss(e, "table version not found") ? " in the namespace" : ""),
                        sanitizedCause(e, uri, options));
            }
            String hint = access != null && access.isManagedVersioning() && isAccessDenied(e)
                    ? " (reading a namespace-managed Lance table may need write access to finalize a staged manifest)"
                    : "";
            throw LanceErrorMessages.failure("Failed to load Lance table metadata for " + what + hint, e, uri, options,
                    catalogSecrets);
        } finally {
            metrics.close();
        }
    }

    /** What a read has resolved so far; the catch block reports errors against it. */
    private static final class ReadState {
        private final LanceRefSelector selector;
        private final String tableName;
        private LanceTableAccess access;
        private Optional<String> branch;
        /** Set once the branch's latest version was checked out, i.e. the branch exists. */
        private boolean branchCheckedOut;
        private OptionalLong version = OptionalLong.empty();
        /** The namespace's version list per chain ("" is main), fetched at most once per read. */
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

    private static boolean isLatestMain(LanceRefSelector selector) {
        return !selector.getTag().isPresent() && !selector.getBranch().isPresent()
                && !selector.getSnapshot().isPresent();
    }

    /**
     * The main-chain version a selector names without looking at the latest manifest: an explicit
     * version, or {@code FOR TIME AS OF} on a managed table whose namespace reports commit times.
     */
    private OptionalLong directMainVersion(ReadState state, LanceMetadataMetrics metrics) {
        LanceRefSelector selector = state.selector;
        if (selector.getTag().isPresent() || selector.getBranch().isPresent() || !selector.getSnapshot().isPresent()) {
            return OptionalLong.empty();
        }
        TableSnapshot snapshot = selector.getSnapshot().get();
        if (snapshot.getType() == TableSnapshot.VersionType.VERSION) {
            return OptionalLong.of(LanceSnapshotResolver.parseVersion(snapshot.getValue()));
        }
        if (!state.access.isManagedVersioning()) {
            return OptionalLong.empty();
        }
        long timestamp = parseTimeTravelTimestamp(snapshot.getValue());
        return LanceSnapshotResolver.namespaceVersionAtOrBefore(
                namespaceVersions(state, state.access, metrics), timestamp, snapshot.getValue());
    }

    /** Resolves the selector against the open latest main chain and reads the selected snapshot. */
    private <T> T readFromLatest(Dataset main, ReadState state, SnapshotReader<T> reader, LanceMetadataMetrics metrics)
            throws Exception {
        LanceRefSelector selector = state.selector;
        if (selector.getTag().isPresent()) {
            String tag = selector.getTag().get();
            Tag target = metrics.measure(Stage.VERSION_RESOLVE, () -> main.tags().list().stream()
                    .filter(candidate -> tag.equals(candidate.getName())).findFirst()
                    .orElseThrow(() -> new LanceUserFacingException(
                            "Lance tag '" + tag + "' of " + state.tableName + " was not found")));
            state.branch = target.getBranch().filter(name -> !MAIN_BRANCH.equals(name));
            state.version = OptionalLong.of(target.getVersion());
            if (!state.branch.isPresent()) {
                try (Dataset dataset = checkout(main, Ref.ofMain(target.getVersion()), metrics)) {
                    return reader.read(dataset, state.access, metrics);
                }
            }
        }
        if (state.branch.isPresent()) {
            String branch = state.branch.get();
            // Check out the branch's latest version first even when a version is already known, so
            // a missing branch and a missing version inside an existing branch are told apart.
            try (Dataset latest = checkout(main, Ref.ofBranch(branch), metrics)) {
                state.branchCheckedOut = true;
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
        // FOR TIME AS OF on the main chain, resolved from storage commit times.
        state.version = resolveSnapshotVersion(main, state.access, selector.getSnapshot().get(), state, metrics);
        try (Dataset dataset = checkout(main, Ref.ofMain(state.version.getAsLong()), metrics)) {
            return reader.read(dataset, state.access, metrics);
        }
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

    private RuntimeException sanitizedCause(Throwable error, String uri, Map<String, String> options) {
        return new RuntimeException(LanceErrorMessages.sanitize(error, uri, options, catalogSecrets));
    }

    /**
     * Checks out a ref of an already open dataset. The SDK resolves the ref itself, from the
     * dataset directory or, for a namespace-managed dataset, with its own namespace client.
     */
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
            return OptionalLong.of(LanceSnapshotResolver.parseVersion(snapshot.getValue()));
        }
        long timestamp = parseTimeTravelTimestamp(snapshot.getValue());
        try {
            return OptionalLong.of(resolveVersionAtOrBefore(latest, access, timestamp, snapshot.getValue(), state,
                    metrics));
        } catch (IllegalArgumentException e) {
            if (!access.getBranch().isPresent()) {
                throw e;
            }
            // A branch's chain starts at the version it was created from and carries its own
            // commit times, so an earlier timestamp has nothing to select on the branch.
            throw new LanceUserFacingException("Lance branch '" + access.getBranch().get() + "' of "
                    + state.tableName + " has no version at or before '" + snapshot.getValue()
                    + "'; a branch only holds the versions from its creation on");
        }
    }

    /**
     * Whether a failed branch checkout means the branch does not exist. The SDK reports
     * "branch <name> does not exist", a namespace "Table branch not found", or a missing manifest
     * under the branch directory when nothing was ever committed there.
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
        return lower.contains("table branch not found")
                || lower.contains("branch " + name + " does not exist")
                || (lower.contains("not found") && lower.contains("tree/" + name + "/"));
    }

    /**
     * Whether a not-found came from the namespace rather than storage. The SDK surfaces a
     * namespace error by its display text ("Table version not found: ..."), and the Java client
     * by its exception type.
     */
    private static boolean isNamespaceMiss(Throwable throwable, String namespaceText) {
        if (ExceptionUtils.indexOfType(throwable, TableVersionNotFoundException.class) >= 0
                || ExceptionUtils.indexOfType(throwable, TableBranchNotFoundException.class) >= 0) {
            return true;
        }
        String rootMessage = ExceptionUtils.getRootCauseMessage(throwable);
        return rootMessage != null && rootMessage.toLowerCase(Locale.ROOT).contains(namespaceText);
    }

    /** An HTTP 403 as the object stores report it, or an explicit access-denied error. */
    private static final Pattern ACCESS_DENIED = Pattern.compile(
            "accessdenied|access denied|permission denied|forbidden|(status|http|code)\\W{0,3}403\\b");

    private static boolean isAccessDenied(Throwable throwable) {
        String rootMessage = ExceptionUtils.getRootCauseMessage(throwable);
        return rootMessage != null && ACCESS_DENIED.matcher(rootMessage.toLowerCase(Locale.ROOT)).find();
    }

    /**
     * Every version the namespace records for the chain {@code access} addresses, listed once per
     * read. The whole list is needed: the storage fallback filters by it, and neither the order a
     * namespace returns nor monotonic commit times can be relied on to stop early.
     */
    private List<TableVersion> namespaceVersions(ReadState state, LanceTableAccess access,
            LanceMetadataMetrics metrics) {
        return state.namespaceVersions.computeIfAbsent(access.getBranch().orElse(""), chain -> {
            List<TableVersion> versions = metrics.measure(Stage.VERSION_RESOLVE,
                    () -> namespaceClient.listManagedVersions(access));
            if (versions.isEmpty()) {
                throw new LanceUserFacingException("Lance namespace lists no versions for "
                        + state.tableName + (chain.isEmpty() ? "" : "@" + chain));
            }
            return versions;
        });
    }

    /**
     * Resolves {@code FOR TIME AS OF} to a version on the chain {@code latest} is checked out on.
     * A namespace-managed table is resolved from the commit times the namespace records, so that
     * only versions the namespace knows are selected. If the namespace lists its versions without
     * commit times, the times come from the manifests present in storage, restricted to the
     * versions the namespace lists; a storage-versioned table is resolved from storage alone.
     */
    private long resolveVersionAtOrBefore(Dataset latest, LanceTableAccess access, long timestamp,
            String requestedText, ReadState state, LanceMetadataMetrics metrics) {
        Set<Long> recordedVersions = null;
        if (access.isManagedVersioning()) {
            List<TableVersion> recorded = namespaceVersions(state, access, metrics);
            OptionalLong fromNamespace = LanceSnapshotResolver.namespaceVersionAtOrBefore(
                    recorded, timestamp, requestedText);
            if (fromNamespace.isPresent()) {
                LOG.debug("Resolved Lance FOR TIME AS OF '{}' to version {} from the namespace",
                        requestedText, fromNamespace.getAsLong());
                return fromNamespace.getAsLong();
            }
            recordedVersions = recorded.stream().map(TableVersion::getVersion).collect(Collectors.toSet());
        }
        Set<Long> allowedVersions = recordedVersions;
        long version = metrics.measure(Stage.VERSION_RESOLVE, () -> {
            List<Version> versions = latest.listVersions();
            if (allowedVersions != null) {
                versions = versions.stream().filter(candidate -> allowedVersions.contains(candidate.getId()))
                        .collect(Collectors.toList());
            }
            return LanceSnapshotResolver.versionAtOrBefore(versions, timestamp, requestedText);
        });
        LOG.debug("Resolved Lance FOR TIME AS OF '{}' to version {} from storage manifests{}",
                requestedText, version, allowedVersions == null ? "" : " listed by the namespace");
        return version;
    }

    /**
     * Parses a {@code FOR TIME AS OF} value in the session time zone. Second and millisecond
     * precision are accepted; commit times are compared at millisecond precision, the precision a
     * namespace reports them in, so a timestamp in the millisecond a commit lands in selects it.
     */
    private static long parseTimeTravelTimestamp(String value) {
        long timestamp = TimeUtils.timeStringToLong(value, TimeUtils.getTimeZone());
        if (timestamp < 0) {
            timestamp = TimeUtils.msTimeStringToLong(value, TimeUtils.getTimeZone());
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("Cannot parse Lance FOR TIME AS OF value '" + value
                    + "', expected 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'");
        }
        return timestamp;
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
        ReadOptions readOptions = LanceReadOptions.forSharedSession(access.getStorageOptions(), version, session);
        if (access.isManagedVersioning()) {
            // The SDK re-describes the table and opens the location the namespace returns, so a
            // namespace that returns a relative location cannot be read in this mode.
            return metrics.measure(Stage.DATASET_OPEN,
                    () -> namespaceClient.openManagedDataset(allocator, access, readOptions, session));
        }
        return metrics.measure(Stage.DATASET_OPEN, () -> Dataset.open().allocator(allocator).uri(access.getDatasetUri())
                .readOptions(readOptions).build());
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
