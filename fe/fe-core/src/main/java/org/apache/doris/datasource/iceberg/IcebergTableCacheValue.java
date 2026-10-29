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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsStorageCredentials;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public class IcebergTableCacheValue {
    private volatile Table icebergTable;
    @Nullable
    private final ThreadPoolExecutor planningExecutor;
    private TableCleanupOwner tableCleanupOwner;
    private final Runnable cleanup;
    private final AtomicInteger references;
    private final AtomicBoolean cacheReferenceReleased = new AtomicBoolean();
    private final AtomicBoolean loaderReferenceReleased;
    // The execution authenticator active when this generation was loaded; see the Paimon
    // counterpart for the concurrent catalog-reset rationale.
    @Nullable
    private volatile org.apache.doris.common.security.authentication.ExecutionAuthenticator authenticator;
    @Nullable
    private volatile IcebergRuntimeContext runtimeContext;
    private volatile boolean enableMappingVarbinary;
    private volatile boolean enableMappingTimestampTz;
    private String retainedCurrentSnapshotJson;
    private volatile boolean queryIsolationPrepared;
    private long retainedTablePayloadBytes;
    private MetaCacheSizeEstimate sizeEstimate;

    public IcebergTableCacheValue(Table icebergTable) {
        this(icebergTable, null, () -> null, () -> { }, false);
    }

    IcebergTableCacheValue(Table icebergTable, Supplier<IcebergSnapshotCacheValue> ignoredSnapshotSupplier,
            Runnable cleanup) {
        this(icebergTable, null, ignoredSnapshotSupplier, cleanup, true);
    }

    IcebergTableCacheValue(Table icebergTable, ThreadPoolExecutor planningExecutor,
            Supplier<IcebergSnapshotCacheValue> ignoredSnapshotSupplier, Runnable cleanup) {
        this(icebergTable, planningExecutor, ignoredSnapshotSupplier, cleanup, true);
    }

    private IcebergTableCacheValue(Table icebergTable, @Nullable ThreadPoolExecutor planningExecutor,
            Supplier<IcebergSnapshotCacheValue> ignoredSnapshotSupplier, Runnable cleanup, boolean loading) {
        this(icebergTable, planningExecutor, ignoredSnapshotSupplier, () -> { }, cleanup, loading);
    }

    IcebergTableCacheValue(Table icebergTable, @Nullable ThreadPoolExecutor planningExecutor,
            Supplier<IcebergSnapshotCacheValue> ignoredSnapshotSupplier,
            Runnable tableCleanup, Runnable cleanup) {
        this(icebergTable, planningExecutor, ignoredSnapshotSupplier, tableCleanup, cleanup, true);
    }

    private IcebergTableCacheValue(Table icebergTable, @Nullable ThreadPoolExecutor planningExecutor,
            Supplier<IcebergSnapshotCacheValue> ignoredSnapshotSupplier,
            Runnable tableCleanup, Runnable cleanup, boolean loading) {
        this.icebergTable = IcebergSnapshotCacheValue.retainTableGeneration(icebergTable);
        this.planningExecutor = planningExecutor;
        this.tableCleanupOwner = new TableCleanupOwner(
                Objects.requireNonNull(tableCleanup, "table cleanup"));
        this.cleanup = Objects.requireNonNull(cleanup, "cleanup");
        Objects.requireNonNull(ignoredSnapshotSupplier, "snapshot supplier");
        this.references = new AtomicInteger(loading ? 2 : 1);
        this.loaderReferenceReleased = new AtomicBoolean(!loading);
    }

    Lease tryAcquire() {
        int current = references.get();
        while (current != 0) {
            if (references.compareAndSet(current, current + 1)) {
                return new Lease(this);
            }
            current = references.get();
        }
        return null;
    }

    void releaseCacheReference() {
        if (cacheReferenceReleased.compareAndSet(false, true)) {
            release();
        }
    }

    void releaseLoaderReference() {
        if (loaderReferenceReleased.compareAndSet(false, true)) {
            release();
        }
    }

    void retire() {
        releaseCacheReference();
        releaseLoaderReference();
    }

    private void release() {
        int remaining = references.decrementAndGet();
        if (remaining == 0) {
            try {
                tableCleanupOwner.release();
            } finally {
                cleanup.run();
            }
        } else if (remaining < 0) {
            throw new IllegalStateException("Iceberg table cache value released too many times");
        }
    }

    void bindAuthenticator(
            @Nullable org.apache.doris.common.security.authentication.ExecutionAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    void bindRuntimeContext(IcebergRuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    void bindSchemaMappingOptions(boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        this.enableMappingVarbinary = enableMappingVarbinary;
        this.enableMappingTimestampTz = enableMappingTimestampTz;
    }

    boolean shareTableCleanupWith(IcebergTableCacheValue currentValue) {
        TableCleanupOwner shared = currentValue.tableCleanupOwner;
        if (shared.tryRetain()) {
            tableCleanupOwner.abandon();
            tableCleanupOwner = shared;
            return true;
        }
        return false;
    }

    void abandonTableCleanup() {
        tableCleanupOwner.abandon();
        tableCleanupOwner = new TableCleanupOwner(() -> { });
    }

    @Nullable
    org.apache.doris.common.security.authentication.ExecutionAuthenticator getAuthenticator() {
        return authenticator;
    }

    @Nullable
    IcebergRuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    boolean isEnableMappingVarbinary() {
        return enableMappingVarbinary;
    }

    boolean isEnableMappingTimestampTz() {
        return enableMappingTimestampTz;
    }

    /** One exact closeable shared by every cache generation that publishes the same FileIO. */
    private static final class TableCleanupOwner {
        private final Runnable cleanup;
        private final AtomicInteger owners = new AtomicInteger(1);

        private TableCleanupOwner(Runnable cleanup) {
            this.cleanup = cleanup;
        }

        private boolean tryRetain() {
            int current = owners.get();
            while (current != 0) {
                if (owners.compareAndSet(current, current + 1)) {
                    return true;
                }
                current = owners.get();
            }
            return false;
        }

        private void abandon() {
            int remaining = owners.decrementAndGet();
            if (remaining != 0) {
                throw new IllegalStateException("unpublished Iceberg table cleanup has multiple owners");
            }
        }

        private void release() {
            int remaining = owners.decrementAndGet();
            if (remaining == 0) {
                cleanup.run();
            } else if (remaining < 0) {
                throw new IllegalStateException("Iceberg table cleanup released too many times");
            }
        }
    }

    public Table getIcebergTable() {
        Table retainedTable = icebergTable;
        return queryIsolationPrepared || IcebergSnapshotCacheValue.isNonGrowingGeneration(retainedTable)
                ? IcebergSnapshotCacheValue.createQueryScopedTable(
                        retainedTable, retainedCurrentSnapshotJson)
                : retainedTable;
    }

    public Table getWritableIcebergTable(Table liveTable) {
        Table retainedTable = icebergTable;
        return IcebergSnapshotCacheValue.createWritableTable(retainedTable, liveTable);
    }

    synchronized MetaCacheSizeEstimate prepareForCachePublication(NameMapping key) {
        if (sizeEstimate == null) {
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("iceberg_table_preparation_failed",
                    () -> {
                        // Order matters: serializing a v1 snapshot materializes its transient
                        // manifest list, which the payload accounting rejects, so account first.
                        retainedTablePayloadBytes =
                                IcebergCacheSizeEstimator.retainedTablePayloadBytes(icebergTable);
                        retainedCurrentSnapshotJson =
                                IcebergSnapshotCacheValue.retainCurrentSnapshotJson(icebergTable);
                        return IcebergCacheSizeEstimator.estimateTableEntry(key, this);
                    });
            if (sizeEstimate.isComplete()) {
                icebergTable = IcebergSnapshotCacheValue.retainNonGrowingGeneration(icebergTable);
                queryIsolationPrepared = true;
            }
        }
        return sizeEstimate;
    }

    public MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate == null
                ? MetaCacheSizeEstimate.incomplete("not_prepared") : sizeEstimate;
    }

    Table getRetainedIcebergTable() {
        return icebergTable;
    }

    synchronized Table newQueryScopedTable() {
        if (!queryIsolationPrepared) {
            // A failed optional size preparation must only reject weighted cache admission. Do not
            // repeat the same unsupported metadata access on the query path and turn it into a
            // table-load failure; this value is not retained by the weighted cache in that case.
            if (sizeEstimate != null && !sizeEstimate.isComplete()) {
                return icebergTable;
            }
            retainedCurrentSnapshotJson =
                    IcebergSnapshotCacheValue.retainCurrentSnapshotJson(icebergTable);
            icebergTable = IcebergSnapshotCacheValue.retainNonGrowingGeneration(icebergTable);
            queryIsolationPrepared = true;
        }
        return IcebergSnapshotCacheValue.createQueryScopedTable(
                icebergTable, retainedCurrentSnapshotJson);
    }

    String getRetainedCurrentSnapshotJson() {
        return retainedCurrentSnapshotJson;
    }

    boolean isQueryIsolationPrepared() {
        return queryIsolationPrepared;
    }

    long getRetainedTablePayloadBytes() {
        return retainedTablePayloadBytes;
    }

    long getRetainedCurrentSnapshotPayloadBytes() {
        return IcebergSnapshotCacheValue.retainedSnapshotJsonBytes(
                retainedCurrentSnapshotJson);
    }

    Optional<String> getTableUuid() {
        TableMetadata metadata = retainedMetadata();
        return metadata == null || metadata.uuid() == null || metadata.uuid().isEmpty()
                ? Optional.empty() : Optional.of(metadata.uuid());
    }

    boolean isSamePhysicalGeneration(IcebergTableCacheValue other) {
        if (other == null) {
            return false;
        }
        TableMetadata left = retainedMetadata();
        TableMetadata right = other.retainedMetadata();
        return left != null && right != null
                && Objects.equals(left.uuid(), right.uuid())
                && Objects.equals(left.metadataFileLocation(), right.metadataFileLocation());
    }

    /**
     * Same metadata generation served through the same operational resources. A catalog that
     * reloads the same metadata file may still hand out a new FileIO carrying rotated vended
     * credentials; projections frozen on the previous handle must not outlive that rotation.
     */
    boolean isSameOperationalGeneration(IcebergTableCacheValue other) {
        // The captured execution context is part of the operational generation: an auth-only
        // ALTER hands out the same metadata and equivalent FileIO under a new authenticator,
        // and projections frozen on the old context would fail the planning fence forever
        // instead of being rebuilt.
        return isSamePhysicalGeneration(other) && sharesOperationalResources(other.icebergTable)
                && authenticator == other.authenticator
                && enableMappingVarbinary == other.enableMappingVarbinary
                && enableMappingTimestampTz == other.enableMappingTimestampTz;
    }

    boolean sharesOperationalResources(Table table) {
        return sharesOperationalResources(icebergTable, table);
    }

    boolean sharesFileIoIdentity(Table table) {
        return table != null && icebergTable.io() == table.io();
    }

    /** True when both tables read and write through the same FileIO, encryption and locations. */
    static boolean sharesOperationalResources(Table left, Table right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return sameFileIo(left.io(), right.io())
                && sameEncryption(left.encryption(), right.encryption())
                && sameLocationProvider(left.locationProvider(), right.locationProvider());
    }

    private static boolean sameFileIo(FileIO left, FileIO right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.getClass() != right.getClass()) {
            return false;
        }
        try {
            // Vended credentials live in the FileIO properties / storage credentials; equal
            // configuration means equal credentials even across catalog reload instances.
            return Objects.equals(left.properties(), right.properties())
                    && Objects.equals(storageCredentials(left), storageCredentials(right));
        } catch (RuntimeException e) {
            // A FileIO that cannot expose its configuration cannot prove it is unchanged.
            return false;
        }
    }

    private static Object storageCredentials(FileIO fileIO) {
        return fileIO instanceof SupportsStorageCredentials
                ? ((SupportsStorageCredentials) fileIO).credentials() : null;
    }

    private static boolean sameEncryption(
            org.apache.iceberg.encryption.EncryptionManager left,
            org.apache.iceberg.encryption.EncryptionManager right) {
        if (left == right) {
            return true;
        }
        // Plaintext managers are stateless, so any two instances are equivalent. Every other
        // manager can hold KMS clients, sessions or key state that is not observable from the
        // outside: fail closed so projections rebind to the fresh handle's manager.
        return left instanceof org.apache.iceberg.encryption.PlaintextEncryptionManager
                && right instanceof org.apache.iceberg.encryption.PlaintextEncryptionManager;
    }

    private static boolean sameLocationProvider(Object left, Object right) {
        // This predicate only runs after the metadata file location and the FileIO configuration
        // proved equal; a location provider is constructed deterministically from exactly that
        // state (table location plus properties), so same-class instances are equivalent here.
        return left == right || (left != null && right != null && left.getClass() == right.getClass());
    }

    private TableMetadata retainedMetadata() {
        Table retainedTable = icebergTable;
        return retainedTable instanceof HasTableOperations
                ? ((HasTableOperations) retainedTable).operations().current() : null;
    }

    static final class Lease implements Closeable {
        private final IcebergTableCacheValue value;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(IcebergTableCacheValue value) {
            this.value = value;
        }

        Table getIcebergTable() {
            return value.getIcebergTable();
        }

        @Nullable
        ThreadPoolExecutor getPlanningExecutor() {
            return value.planningExecutor;
        }

        IcebergTableCacheValue getValue() {
            return value;
        }

        Lease retain() {
            Lease retained = value.tryAcquire();
            if (retained == null) {
                throw new IllegalStateException("Iceberg table cache generation was already retired");
            }
            return retained;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                value.release();
            }
        }
    }
}
