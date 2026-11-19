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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.deltalake.uc.UnityCatalogProperties;
import org.apache.doris.connector.spi.Connector;

import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

@Timeout(30)
class DeltaLakeVendedFileIOTest {
    @TempDir
    Path directory;

    @Test
    void borrowingTimesOutAtBothLimitsAndRecoversWhenAStreamCloses() throws Exception {
        String path = Files.write(directory.resolve("data"), new byte[] {13}).toUri().toString();
        try (DeltaLakeVendedFileIO.Clients clients = clients()) {
            GenericKeyedObjectPool<DeltaLakeVendedFileIO.Grant, DeltaLakeFileIO> pool = pool(clients);
            Assertions.assertEquals(64, pool.getMaxTotal());
            Assertions.assertEquals(8, pool.getMaxTotalPerKey());
            Assertions.assertEquals(30_000L, pool.getMaxWaitMillis());
            Assertions.assertTrue(pool.getBlockWhenExhausted());
            pool.setMaxTotal(2);
            pool.setMaxTotalPerKey(1);
            pool.setMaxWaitMillis(100);
            try (DeltaLakeVendedFileIO first = io(clients, grant("a"));
                    DeltaLakeVendedFileIO second = io(clients, grant("b"));
                    DeltaLakeVendedFileIO third = io(clients, grant("c"));
                    SeekableInputStream firstStream = first.newInputFile(path, 1).newStream()) {
                assertBorrowTimeout(first, path);
                try (SeekableInputStream secondStream = second.newInputFile(path, 1).newStream()) {
                    Assertions.assertEquals(2, pool.getNumActive());
                    assertBorrowTimeout(third, path);
                    Assertions.assertEquals(13, secondStream.read());
                }
                try (SeekableInputStream thirdStream = third.newInputFile(path, 1).newStream()) {
                    Assertions.assertEquals(13, thirdStream.read());
                }
                Assertions.assertEquals(13, firstStream.read());
            }
        }
    }

    @Test
    void rotationEvictsIdleClientsAndCatalogCloseDestroysOutstandingClientsOnReturn() throws Exception {
        String path = Files.write(directory.resolve("data"), new byte[] {7}).toUri().toString();
        DeltaLakeVendedFileIO.Grant original = grant("old");
        DeltaLakeVendedFileIO.Grant replacement = grant("new");
        AtomicInteger renewals = new AtomicInteger();
        Map<String, String> properties = Map.of("deltalake.catalog.type", "uc", UnityCatalogProperties.NAMESPACE,
                "main", UnityCatalogProperties.URI_KEY, "http://127.0.0.1:1", UnityCatalogProperties.TOKEN, "test");
        try (Connector catalog = new DeltaLakeConnectorProvider().create(properties, new RecordingConnectorContext())) {
            catalog.getMetadata(null).close(); // Initialize the catalog-owned UC pool.
            DeltaLakeRuntime runtime = field(catalog, "runtime");
            DeltaLakeVendedFileIO.Clients clients = field(runtime.resolver, "clients");
            GenericKeyedObjectPool<DeltaLakeVendedFileIO.Grant, DeltaLakeFileIO> pool = pool(clients);
            try (DeltaLakeVendedFileIO old = io(clients, original);
                    DeltaLakeVendedFileIO renewed = new DeltaLakeVendedFileIO(clients, grant("old", 1), () -> {
                        renewals.incrementAndGet();
                        return replacement;
                    });
                    SeekableInputStream oldStream = old.newInputFile(path, 1).newStream();
                    SeekableInputStream newStream = renewed.newInputFile(path, 1).newStream()) {
                old.getFileStatus(path);
                Assertions.assertEquals(1, renewals.get());
                Assertions.assertEquals(1, pool.getNumIdle(original));
                pool.setMinEvictableIdleTimeMillis(1);
                pool.setTimeBetweenEvictionRunsMillis(10);
                await(() -> pool.getDestroyedByEvictorCount() == 1, "idle client for the old grant was not evicted");
                Assertions.assertEquals(0, pool.getNumIdle(original));
                Assertions.assertEquals(1, pool.getNumActive(original));
                Assertions.assertEquals(1, pool.getNumActive(replacement));

                catalog.close();
                await(pool::isClosed, "catalog did not close its UC pool");
                Assertions.assertThrows(IOException.class, () -> renewed.getFileStatus(path));
                Assertions.assertEquals(7, oldStream.read());
                Assertions.assertEquals(7, newStream.read());
            }
            Assertions.assertEquals(0, pool.getNumActive());
            Assertions.assertEquals(0, pool.getNumIdle());
            Assertions.assertEquals(pool.getCreatedCount(), pool.getDestroyedCount());
        }
    }

    private static void await(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(condition.getAsBoolean(), message);
    }

    private static void assertBorrowTimeout(DeltaLakeVendedFileIO io, String path) {
        IOException failure = Assertions.assertThrows(IOException.class, () -> io.getFileStatus(path));
        Assertions.assertTrue(failure.getMessage().contains("borrow timeout 100 ms"));
        Assertions.assertInstanceOf(NoSuchElementException.class, failure.getCause());
    }

    private static DeltaLakeVendedFileIO.Clients clients() {
        return new DeltaLakeVendedFileIO.Clients(new Configuration(false), new RecordingConnectorContext());
    }

    private static DeltaLakeVendedFileIO.Grant grant(String token) {
        return grant(token, System.currentTimeMillis() + 3_600_000);
    }

    private static DeltaLakeVendedFileIO.Grant grant(String token, long expiry) {
        return new DeltaLakeVendedFileIO.Grant("table", Map.of("AWS_ACCESS_KEY", "id", "AWS_SECRET_KEY", "test",
                "AWS_TOKEN", token), expiry);
    }

    private static DeltaLakeVendedFileIO io(DeltaLakeVendedFileIO.Clients clients, DeltaLakeVendedFileIO.Grant grant) {
        return new DeltaLakeVendedFileIO(clients, grant, () -> Assertions.fail("unexpected grant refresh"));
    }

    private static GenericKeyedObjectPool<DeltaLakeVendedFileIO.Grant, DeltaLakeFileIO> pool(
            DeltaLakeVendedFileIO.Clients clients) throws ReflectiveOperationException {
        return field(clients, "pool");
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object owner, String name) throws ReflectiveOperationException {
        Field member = owner.getClass().getDeclaredField(name);
        member.setAccessible(true);
        return (T) member.get(owner);
    }
}
