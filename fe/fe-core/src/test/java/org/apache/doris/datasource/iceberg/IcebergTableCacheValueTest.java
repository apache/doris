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

import org.apache.doris.nereids.StatementContext;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergTableCacheValueTest {

    @Test
    void classifiesOnlyPerTableFileIOAsOwned() {
        FileIO tableIo = newProxy(FileIO.class);
        FileIO catalogIo = newProxy(FileIO.class);

        Assertions.assertTrue(IcebergExternalMetaCache.shouldCloseTableFileIO(
                IcebergExternalCatalog.ICEBERG_GLUE, tableIo, null));
        Assertions.assertTrue(IcebergExternalMetaCache.shouldCloseTableFileIO(
                IcebergExternalCatalog.ICEBERG_S3_TABLES, tableIo, null));
        Assertions.assertTrue(IcebergExternalMetaCache.shouldCloseTableFileIO(
                IcebergExternalCatalog.ICEBERG_REST, tableIo, catalogIo));
        Assertions.assertFalse(IcebergExternalMetaCache.shouldCloseTableFileIO(
                IcebergExternalCatalog.ICEBERG_REST, catalogIo, catalogIo));
        Assertions.assertFalse(IcebergExternalMetaCache.shouldCloseTableFileIO(
                IcebergExternalCatalog.ICEBERG_REST, tableIo, null));
        Assertions.assertFalse(IcebergExternalMetaCache.shouldCloseTableFileIO(
                IcebergExternalCatalog.ICEBERG_DLF, tableIo, null));
    }

    @Test
    void evictionWaitsForActiveBorrower() {
        AtomicInteger cleanupCount = new AtomicInteger();
        IcebergTableCacheValue value = newValue(cleanupCount);
        IcebergTableCacheValue.Lease lease = value.tryAcquire();
        Assertions.assertNotNull(lease);
        value.releaseLoaderReference();

        value.releaseCacheReference();
        Assertions.assertEquals(0, cleanupCount.get());

        lease.close();
        Assertions.assertEquals(1, cleanupCount.get());
        lease.close();
        Assertions.assertEquals(1, cleanupCount.get());
    }

    @Test
    void statementCloseReleasesBorrowerAfterPlannerResources() {
        AtomicInteger cleanupCount = new AtomicInteger();
        IcebergTableCacheValue value = newValue(cleanupCount);
        IcebergTableCacheValue.Lease lease = value.tryAcquire();
        Assertions.assertNotNull(lease);
        value.releaseLoaderReference();

        StatementContext statementContext = new StatementContext();
        statementContext.getOrRegisterStatementResource("iceberg-table:1\u0000db\u0000tbl", () -> lease);
        value.releaseCacheReference();
        statementContext.releasePlannerResources();
        Assertions.assertEquals(0, cleanupCount.get());

        statementContext.close();
        Assertions.assertEquals(1, cleanupCount.get());
    }

    @Test
    void loaderReferenceBridgesEvictionBeforeBorrow() {
        AtomicInteger cleanupCount = new AtomicInteger();
        IcebergTableCacheValue value = newValue(cleanupCount);

        value.releaseCacheReference();
        IcebergTableCacheValue.Lease lease = value.tryAcquire();
        Assertions.assertNotNull(lease);
        value.releaseLoaderReference();
        Assertions.assertEquals(0, cleanupCount.get());

        lease.close();
        Assertions.assertEquals(1, cleanupCount.get());
    }

    @Test
    void unborrowedRetiredValueClosesExactlyOnce() {
        AtomicInteger cleanupCount = new AtomicInteger();
        IcebergTableCacheValue value = newValue(cleanupCount);

        value.releaseCacheReference();
        value.releaseLoaderReference();
        value.releaseCacheReference();
        value.releaseLoaderReference();

        Assertions.assertEquals(1, cleanupCount.get());
    }

    @Test
    void catalogRetirementWaitsForTableEvictionAndBorrower() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad();
        IcebergCatalogResourceTracker.ResourceLease catalogLease = guard.promote();
        guard.close();
        AtomicInteger catalogCloseCount = new AtomicInteger();
        IcebergTableCacheValue value = new IcebergTableCacheValue(newProxy(Table.class), () -> null,
                catalogLease::close);
        IcebergTableCacheValue.Lease borrower = value.tryAcquire();
        Assertions.assertNotNull(borrower);
        value.releaseLoaderReference();

        tracker.retireCurrent(catalogCloseCount::incrementAndGet);
        value.releaseCacheReference();
        Assertions.assertEquals(0, catalogCloseCount.get());

        borrower.close();
        Assertions.assertEquals(1, catalogCloseCount.get());
    }

    private IcebergTableCacheValue newValue(AtomicInteger cleanupCount) {
        Table table = newProxy(Table.class);
        return new IcebergTableCacheValue(table, () -> null, cleanupCount::incrementAndGet);
    }

    @SuppressWarnings("unchecked")
    private <T> T newProxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> null);
    }
}
