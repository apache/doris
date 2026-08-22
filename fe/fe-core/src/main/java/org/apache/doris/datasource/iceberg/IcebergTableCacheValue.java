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

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.base.Suppliers;
import org.apache.iceberg.Table;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class IcebergTableCacheValue {
    private final Table icebergTable;
    private final ThreadPoolExecutor planningExecutor;
    private final ExecutionAuthenticator authenticator;
    private final Map<StorageProperties.Type, StorageProperties> storageProperties;
    private final Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue;
    private final Runnable cleanup;
    // Production-loaded values begin with one cache owner and one temporary loader owner. The temporary
    // owner bridges an invalidation/suppressed-publication race until the first caller acquires a lease.
    private final AtomicInteger references;
    private final AtomicBoolean cacheReferenceReleased;
    private final AtomicBoolean loaderReferenceReleased;

    public IcebergTableCacheValue(Table icebergTable, Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue) {
        this(icebergTable, null, latestSnapshotCacheValue, () -> { }, false);
    }

    IcebergTableCacheValue(Table icebergTable, Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue,
            Runnable cleanup) {
        this(icebergTable, null, latestSnapshotCacheValue, cleanup, true);
    }

    IcebergTableCacheValue(Table icebergTable, ThreadPoolExecutor planningExecutor,
            Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue, Runnable cleanup) {
        this(icebergTable, planningExecutor, null, Collections.emptyMap(), latestSnapshotCacheValue, cleanup, true);
    }

    IcebergTableCacheValue(Table icebergTable, ThreadPoolExecutor planningExecutor,
            ExecutionAuthenticator authenticator,
            Map<StorageProperties.Type, StorageProperties> storageProperties,
            Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue, Runnable cleanup) {
        this(icebergTable, planningExecutor, authenticator, storageProperties,
                latestSnapshotCacheValue, cleanup, true);
    }

    private IcebergTableCacheValue(Table icebergTable, ThreadPoolExecutor planningExecutor,
            Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue, Runnable cleanup, boolean loading) {
        this(icebergTable, planningExecutor, null, Collections.emptyMap(), latestSnapshotCacheValue, cleanup, loading);
    }

    private IcebergTableCacheValue(Table icebergTable, ThreadPoolExecutor planningExecutor,
            ExecutionAuthenticator authenticator,
            Map<StorageProperties.Type, StorageProperties> storageProperties,
            Supplier<IcebergSnapshotCacheValue> latestSnapshotCacheValue, Runnable cleanup, boolean loading) {
        this.icebergTable = icebergTable;
        this.planningExecutor = planningExecutor;
        this.authenticator = authenticator;
        this.storageProperties = Collections.unmodifiableMap(storageProperties);
        this.latestSnapshotCacheValue = Suppliers.memoize(latestSnapshotCacheValue::get);
        this.cleanup = cleanup;
        this.references = new AtomicInteger(loading ? 2 : 1);
        this.cacheReferenceReleased = new AtomicBoolean(false);
        this.loaderReferenceReleased = new AtomicBoolean(!loading);
    }

    public Table getIcebergTable() {
        return icebergTable;
    }

    public IcebergSnapshotCacheValue getLatestSnapshotCacheValue() {
        return latestSnapshotCacheValue.get();
    }

    ThreadPoolExecutor getPlanningExecutor() {
        return planningExecutor;
    }

    ExecutionAuthenticator getAuthenticator() {
        return authenticator;
    }

    Map<StorageProperties.Type, StorageProperties> getStorageProperties() {
        return storageProperties;
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
            cleanup.run();
        } else if (remaining < 0) {
            throw new IllegalStateException("Iceberg table cache value released too many times");
        }
    }

    public static final class Lease implements Closeable {
        private final IcebergTableCacheValue value;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(IcebergTableCacheValue value) {
            this.value = value;
        }

        Table getIcebergTable() {
            return value.getIcebergTable();
        }

        IcebergSnapshotCacheValue getLatestSnapshotCacheValue() {
            return value.getLatestSnapshotCacheValue();
        }

        ThreadPoolExecutor getPlanningExecutor() {
            return value.getPlanningExecutor();
        }

        ExecutionAuthenticator getAuthenticator() {
            return value.getAuthenticator();
        }

        Map<StorageProperties.Type, StorageProperties> getStorageProperties() {
            return value.getStorageProperties();
        }

        Lease retain() {
            Lease retained = value.tryAcquire();
            if (retained == null) {
                throw new IllegalStateException("Iceberg table generation was already retired");
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
