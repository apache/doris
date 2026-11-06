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

package org.apache.doris.connector.adbc;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class AdbcVendorProbeConcurrencyTest {

    @Test
    void coalescesConcurrentProbesForOneCatalog() throws Exception {
        assertProbesDoNotOverlap(true);
    }

    @Test
    void serializesProbesAcrossCatalogClients() throws Exception {
        assertProbesDoNotOverlap(false);
    }

    @Test
    void cachedIdentityDoesNotWaitForAnotherCatalogsProbe() throws Exception {
        Probe completed = new Probe();
        completed.release.countDown();
        ProbeClient cached = new ProbeClient(completed);
        Assertions.assertEquals(Optional.empty(), cached.vendorName());

        Probe pending = new Probe();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Optional<String>> call = executor.submit(new ProbeClient(pending)::vendorName);
            Assertions.assertTrue(pending.entered.await(30, TimeUnit.SECONDS));
            Assertions.assertEquals(Optional.empty(),
                    CompletableFuture.supplyAsync(cached::vendorName).get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, completed.calls.get());
            pending.release.countDown();
            Assertions.assertEquals(Optional.empty(), call.get(30, TimeUnit.SECONDS));
        } finally {
            pending.release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static void assertProbesDoNotOverlap(boolean sameClient) throws Exception {
        // The native Flight driver shares its mutable info map even between different databases.
        Probe probe = new Probe();
        ProbeClient first = new ProbeClient(probe);
        ProbeClient second = sameClient ? first : new ProbeClient(probe);
        CountDownLatch secondStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<String>> firstCall = executor.submit(first::vendorName);
            Assertions.assertTrue(probe.entered.await(30, TimeUnit.SECONDS));
            Future<Optional<String>> secondCall = executor.submit(() -> {
                secondStarted.countDown();
                return second.vendorName();
            });
            Assertions.assertTrue(secondStarted.await(30, TimeUnit.SECONDS));
            Assertions.assertFalse(probe.overlap.await(1, TimeUnit.SECONDS),
                    "Concurrent getInfo calls would write the native driver's shared map");
            probe.release.countDown();
            Assertions.assertEquals(Optional.empty(), firstCall.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(Optional.empty(), secondCall.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(sameClient ? 1 : 2, probe.calls.get());
        } finally {
            probe.release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static final class Probe {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch overlap = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();

        private ArrowReader read() throws AdbcException {
            calls.incrementAndGet();
            if (active.incrementAndGet() > 1) {
                overlap.countDown();
            }
            entered.countDown();
            try {
                Assertions.assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            } finally {
                active.decrementAndGet();
            }
            // No native library is needed to exercise the real client's cache and probe coordination.
            throw new AdbcException("unsupported vendor info", null, AdbcStatusCode.NOT_IMPLEMENTED, null, 0);
        }
    }

    private static final class ProbeClient extends AdbcClient {
        private final Probe probe;

        private ProbeClient(Probe probe) {
            super(Path.of("driver.so"), "driver.so", null, "grpc://localhost:1", null, null, Map.of());
            this.probe = probe;
        }

        @Override
        public <T> T withConnection(AdbcConnectionCall<T> body) {
            try {
                return body.apply(new AdbcConnection() {
                    @Override
                    public ArrowReader getInfo(int[] infoCodes) throws AdbcException {
                        if (infoCodes[0] == AdbcInfoCode.DRIVER_NAME.getValue()) {
                            throw AdbcException.notImplemented("Driver name unavailable in this fixture");
                        }
                        return probe.read();
                    }

                    @Override
                    public AdbcStatement createStatement() {
                        throw new AssertionError("vendor discovery runs no statements");
                    }

                    @Override
                    public ArrowReader getObjects(GetObjectsDepth depth, String catalog, String schema,
                            String table, String[] tableTypes, String column) {
                        throw new AssertionError("vendor discovery lists no objects");
                    }

                    @Override
                    public void close() {
                    }
                });
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
