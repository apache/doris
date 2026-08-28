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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;
import org.apache.doris.connector.spi.ConnectorOperationAbortedException;
import org.apache.doris.connector.spi.ConnectorOperationControl;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Verifies that query control remains effective while the HMS client pool is exhausted. */
public class ThriftHmsClientPoolControlTest {

    @Test
    public void cancellationBoundsPoolDisabledClientCreationAndDestroysLateClient() throws Exception {
        assertBlockingCreationAborted(0, ConnectorOperationAbortedException.Reason.CANCELLED);
    }

    @Test
    public void deadlineBoundsPooledClientCreationAndDestroysLateClient() throws Exception {
        assertBlockingCreationAborted(1, ConnectorOperationAbortedException.Reason.DEADLINE_EXCEEDED);
    }

    @Test
    public void expiredDeadlineNeverTurnsIntoAnInfinitePoolWait() throws Exception {
        try (PoolHarness harness = new PoolHarness()) {
            harness.occupyOnlyClient();
            AtomicInteger remainingCalls = new AtomicInteger();
            AtomicBoolean expired = new AtomicBoolean();
            ConnectorOperationControl control = new ConnectorOperationControl() {
                @Override
                public void checkActive() {
                    if (expired.get()) {
                        throw new ConnectorOperationAbortedException(
                                ConnectorOperationAbortedException.Reason.DEADLINE_EXCEEDED, "expired");
                    }
                }

                @Override
                public long remainingTimeMillis() {
                    if (remainingCalls.incrementAndGet() >= 2) {
                        expired.set(true);
                        return -1L;
                    }
                    return Long.MAX_VALUE;
                }
            };

            ConnectorOperationAbortedException failure = Assertions.assertThrows(
                    ConnectorOperationAbortedException.class,
                    () -> harness.client.getPartitions(request("p=deadline", control)));
            Assertions.assertEquals(
                    ConnectorOperationAbortedException.Reason.DEADLINE_EXCEEDED, failure.getReason());
            Assertions.assertEquals(0, harness.lastEvent.get().getRpcCount(),
                    "waiting for an HMS client is not a physical HMS RPC");
        }
    }

    @Test
    public void interruptedPoolWaitPreservesInterruptAndReportsCancellation() throws Exception {
        try (PoolHarness harness = new PoolHarness()) {
            harness.occupyOnlyClient();
            CountDownLatch borrowStarted = new CountDownLatch(1);
            AtomicInteger remainingCalls = new AtomicInteger();
            ConnectorOperationControl control = new ConnectorOperationControl() {
                @Override
                public void checkActive() {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new ConnectorOperationAbortedException(
                                ConnectorOperationAbortedException.Reason.CANCELLED, "cancelled");
                    }
                }

                @Override
                public long remainingTimeMillis() {
                    if (remainingCalls.incrementAndGet() == 2) {
                        borrowStarted.countDown();
                    }
                    return TimeUnit.SECONDS.toMillis(30);
                }
            };
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interruptPreserved = new AtomicBoolean();
            Thread waiter = new Thread(() -> {
                try {
                    harness.client.getPartitions(request("p=cancel", control));
                } catch (Throwable t) {
                    failure.set(t);
                    interruptPreserved.set(Thread.currentThread().isInterrupted());
                }
            }, "hms-pool-waiter");

            waiter.start();
            Assertions.assertTrue(borrowStarted.await(5, TimeUnit.SECONDS));
            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(5));

            Assertions.assertFalse(waiter.isAlive(), "the interrupted pool wait must terminate");
            Assertions.assertInstanceOf(ConnectorOperationAbortedException.class, failure.get());
            Assertions.assertEquals(ConnectorOperationAbortedException.Reason.CANCELLED,
                    ((ConnectorOperationAbortedException) failure.get()).getReason());
            Assertions.assertTrue(interruptPreserved.get(), "the interrupted status must be restored");
            Assertions.assertEquals(0, harness.lastEvent.get().getRpcCount(),
                    "an interrupted pool wait is not a physical HMS RPC");
        }
    }

    @Test
    public void cancellationFlagStopsAnExhaustedPoolWaitPromptly() throws Exception {
        try (PoolHarness harness = new PoolHarness()) {
            harness.occupyOnlyClient();
            AtomicBoolean cancelled = new AtomicBoolean();
            CountDownLatch repeatedControlCheck = new CountDownLatch(1);
            AtomicInteger checks = new AtomicInteger();
            ConnectorOperationControl control = new ConnectorOperationControl() {
                @Override
                public void checkActive() {
                    if (checks.incrementAndGet() >= 3) {
                        repeatedControlCheck.countDown();
                    }
                    if (cancelled.get()) {
                        throw new ConnectorOperationAbortedException(
                                ConnectorOperationAbortedException.Reason.CANCELLED, "query killed");
                    }
                }

                @Override
                public long remainingTimeMillis() {
                    return TimeUnit.SECONDS.toMillis(30);
                }
            };
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<List<HmsPartitionInfo>> waiter = executor.submit(
                        () -> harness.client.getPartitions(request("p=cancel-flag", control)));
                Assertions.assertTrue(repeatedControlCheck.await(5, TimeUnit.SECONDS));
                cancelled.set(true);

                ExecutionException failure = Assertions.assertThrows(
                        ExecutionException.class, () -> waiter.get(2, TimeUnit.SECONDS));
                Assertions.assertInstanceOf(ConnectorOperationAbortedException.class, failure.getCause());
                Assertions.assertEquals(ConnectorOperationAbortedException.Reason.CANCELLED,
                        ((ConnectorOperationAbortedException) failure.getCause()).getReason());
                Assertions.assertEquals(0, harness.lastEvent.get().getRpcCount(),
                        "polling an exhausted pool must not be counted as a physical HMS RPC");
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void operationAbortDoesNotTaintHealthyPooledClient() throws Exception {
        AtomicBoolean abort = new AtomicBoolean(true);
        AtomicInteger clientCreations = new AtomicInteger();
        IMetaStoreClient metastore = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        if ("toString".equals(method.getName())) {
                            return "AbortAwareMetastore";
                        }
                        if ("hashCode".equals(method.getName())) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(method.getName())) {
                            return proxy == args[0];
                        }
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if ("getPartitionsByNames".equals(method.getName())) {
                        if (abort.getAndSet(false)) {
                            throw new RuntimeException("retry proxy wrapper",
                                    new ConnectorOperationAbortedException(
                                            ConnectorOperationAbortedException.Reason.CANCELLED,
                                            "cancelled before wire"));
                        }
                        @SuppressWarnings("unchecked")
                        List<String> names = (List<String>) args[2];
                        List<Partition> partitions = new ArrayList<>(names.size());
                        for (String name : names) {
                            Partition partition = new Partition();
                            partition.setValues(HmsPartitionIdentity.fromName(name));
                            partitions.add(partition);
                        }
                        return partitions;
                    }
                    return null;
                });
        HmsClientConfig config = new HmsClientConfig(new HashMap<>(), 1);
        try (ThriftHmsClient client = new ThriftHmsClient(
                config, null, hiveConf -> {
                    clientCreations.incrementAndGet();
                    return metastore;
                }, HmsTypeMapping.Options.DEFAULT)) {
            ConnectorOperationAbortedException failure = Assertions.assertThrows(
                    ConnectorOperationAbortedException.class,
                    () -> client.getPartitions("db", "tbl", Collections.singletonList("p=cancel")));
            Assertions.assertEquals(ConnectorOperationAbortedException.Reason.CANCELLED, failure.getReason());

            Assertions.assertEquals(1,
                    client.getPartitions("db", "tbl", Collections.singletonList("p=success")).size());
            Assertions.assertEquals(1, clientCreations.get(),
                    "cooperative cancellation must not destroy a healthy pooled HMS client");
        }
    }

    @Test
    public void malformedThriftPartitionResponseKeepsTypedIntegrityFailure() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient metastore = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        if ("toString".equals(method.getName())) {
                            return "MalformedResponseMetastore";
                        }
                        if ("hashCode".equals(method.getName())) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(method.getName())) {
                            return proxy == args[0];
                        }
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if ("getPartitionsByNames".equals(method.getName())) {
                        return calls.getAndIncrement() == 0
                                ? null : Collections.singletonList(null);
                    }
                    return null;
                });
        HmsClientConfig config = new HmsClientConfig(new HashMap<>(), 1);
        try (ThriftHmsClient client = new ThriftHmsClient(
                config, null, hiveConf -> metastore, HmsTypeMapping.Options.DEFAULT)) {
            for (int i = 0; i < 2; i++) {
                HmsPartitionResultException failure = Assertions.assertThrows(
                        HmsPartitionResultException.class,
                        () -> client.getPartitions("db", "tbl", Collections.singletonList("p=1")));
                Assertions.assertTrue(failure.getMismatchTypes()
                        .contains(HmsPartitionResultException.MismatchType.INVALID_RESULT));
                Assertions.assertEquals(1, failure.getInvalidCount());
            }
        }
    }

    @Test
    public void cancellationDuringRetryDelayTaintsTheFailedPooledClient() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger clientCreations = new AtomicInteger();
        CountDownLatch retryDelayEntered = new CountDownLatch(1);
        ThriftHmsClient.MetaStoreClientProvider provider = new ThriftHmsClient.MetaStoreClientProvider() {
            @Override
            public IMetaStoreClient create(org.apache.hadoop.hive.conf.HiveConf hiveConf) {
                int clientId = clientCreations.incrementAndGet();
                return (IMetaStoreClient) Proxy.newProxyInstance(
                        IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                        (proxy, method, args) -> {
                            if (method.getDeclaringClass() == Object.class) {
                                if ("toString".equals(method.getName())) {
                                    return "RetryDelayMetastore-" + clientId;
                                }
                                if ("hashCode".equals(method.getName())) {
                                    return System.identityHashCode(proxy);
                                }
                                if ("equals".equals(method.getName())) {
                                    return proxy == args[0];
                                }
                            }
                            if ("close".equals(method.getName())) {
                                return null;
                            }
                            if ("getPartitionsByNames".equals(method.getName())) {
                                @SuppressWarnings("unchecked")
                                List<String> names = (List<String>) args[2];
                                if (clientId == 1) {
                                    try {
                                        HmsRemoteCallTracking.trackWireAttempt(() -> {
                                            throw new shade.doris.hive.org.apache.thrift.TException("retry me");
                                        });
                                    } catch (shade.doris.hive.org.apache.thrift.TException expected) {
                                        retryDelayEntered.countDown();
                                    }
                                    Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                                }
                                return HmsRemoteCallTracking.trackWireAttempt(() -> {
                                    List<Partition> partitions = new ArrayList<>(names.size());
                                    for (String name : names) {
                                        Partition partition = new Partition();
                                        partition.setValues(HmsPartitionIdentity.fromName(name));
                                        partitions.add(partition);
                                    }
                                    return partitions;
                                });
                            }
                            return null;
                        });
            }

            @Override
            public boolean supportsPartitionWireCallTracking() {
                return true;
            }
        };
        ConnectorOperationControl control = new ConnectorOperationControl() {
            @Override
            public void checkActive() {
                if (cancelled.get()) {
                    throw new ConnectorOperationAbortedException(
                            ConnectorOperationAbortedException.Reason.CANCELLED, "query killed");
                }
            }

            @Override
            public long remainingTimeMillis() {
                return TimeUnit.SECONDS.toMillis(30);
            }
        };
        HmsClientConfig config = new HmsClientConfig(new HashMap<>(), 1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ThriftHmsClient client = new ThriftHmsClient(
                config, null, provider, HmsTypeMapping.Options.DEFAULT)) {
            Future<List<HmsPartitionInfo>> retrying = executor.submit(
                    () -> client.getPartitions(request("p=retry", control)));
            Assertions.assertTrue(retryDelayEntered.await(5, TimeUnit.SECONDS));
            cancelled.set(true);

            ExecutionException failure = Assertions.assertThrows(
                    ExecutionException.class, () -> retrying.get(2, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(ConnectorOperationAbortedException.class, failure.getCause());
            Assertions.assertEquals(ConnectorOperationAbortedException.Reason.CANCELLED,
                    ((ConnectorOperationAbortedException) failure.getCause()).getReason());

            Assertions.assertEquals(1,
                    client.getPartitions("db", "tbl", Collections.singletonList("p=success")).size());
            Assertions.assertEquals(2, clientCreations.get(),
                    "a client cancelled after a failed wire attempt must be destroyed, not returned to the pool");
        } finally {
            executor.shutdownNow();
        }
    }

    private static HmsPartitionRequest request(String name, ConnectorOperationControl control) {
        return HmsPartitionRequest.builder()
                .database("db")
                .table("tbl")
                .partitionNames(Collections.singletonList(name))
                .operationControl(control)
                .build();
    }

    private static void assertBlockingCreationAborted(
            int poolSize, ConnectorOperationAbortedException.Reason reason) throws Exception {
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch releaseCreation = new CountDownLatch(1);
        CountDownLatch lateClientClosed = new CountDownLatch(1);
        AtomicBoolean abort = new AtomicBoolean();
        IMetaStoreClient metastore = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        lateClientClosed.countDown();
                    }
                    return null;
                });
        ConnectorOperationControl control = new ConnectorOperationControl() {
            @Override
            public void checkActive() {
                if (abort.get() && reason == ConnectorOperationAbortedException.Reason.CANCELLED) {
                    throw new ConnectorOperationAbortedException(reason, "query killed");
                }
            }

            @Override
            public long remainingTimeMillis() {
                return abort.get() && reason == ConnectorOperationAbortedException.Reason.DEADLINE_EXCEEDED
                        ? 0L : TimeUnit.SECONDS.toMillis(30);
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HmsClientConfig config = new HmsClientConfig(new HashMap<>(), poolSize);
        try (ThriftHmsClient client = new ThriftHmsClient(config, null, hiveConf -> {
            creationEntered.countDown();
            while (true) {
                try {
                    releaseCreation.await();
                    return metastore;
                } catch (InterruptedException ignored) {
                    // Model DNS/Kerberos code that does not cooperate with thread interruption.
                }
            }
        }, HmsTypeMapping.Options.DEFAULT)) {
            Future<List<HmsPartitionInfo>> request = executor.submit(
                    () -> client.getPartitions(request("p=blocked", control)));
            Assertions.assertTrue(creationEntered.await(5, TimeUnit.SECONDS));
            abort.set(true);
            ExecutionException failure = Assertions.assertThrows(
                    ExecutionException.class, () -> request.get(2, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(ConnectorOperationAbortedException.class, failure.getCause());
            Assertions.assertEquals(reason,
                    ((ConnectorOperationAbortedException) failure.getCause()).getReason());
            releaseCreation.countDown();
            Assertions.assertTrue(lateClientClosed.await(5, TimeUnit.SECONDS));
        } finally {
            releaseCreation.countDown();
            executor.shutdownNow();
        }
    }

    private static final class PoolHarness implements AutoCloseable {
        private final CountDownLatch holderEntered = new CountDownLatch(1);
        private final CountDownLatch releaseHolder = new CountDownLatch(1);
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final AtomicReference<ConnectorMetadataAccessEvent> lastEvent = new AtomicReference<>();
        private final ThriftHmsClient client;
        private Future<List<HmsPartitionInfo>> holder;

        private PoolHarness() {
            IMetaStoreClient metastore = (IMetaStoreClient) Proxy.newProxyInstance(
                    IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                    (proxy, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            if ("toString".equals(method.getName())) {
                                return "PoolControlMetastore";
                            }
                            if ("hashCode".equals(method.getName())) {
                                return System.identityHashCode(proxy);
                            }
                            if ("equals".equals(method.getName())) {
                                return proxy == args[0];
                            }
                        }
                        if ("close".equals(method.getName())) {
                            return null;
                        }
                        if ("getPartitionsByNames".equals(method.getName())) {
                            @SuppressWarnings("unchecked")
                            List<String> names = (List<String>) args[2];
                            if (names.contains("p=hold")) {
                                holderEntered.countDown();
                                Assertions.assertTrue(releaseHolder.await(10, TimeUnit.SECONDS));
                            }
                            List<Partition> partitions = new ArrayList<>(names.size());
                            for (String name : names) {
                                Partition partition = new Partition();
                                partition.setValues(HmsPartitionIdentity.fromName(name));
                                partitions.add(partition);
                            }
                            return partitions;
                        }
                        return null;
                    });
            HmsClientConfig config = new HmsClientConfig(new HashMap<>(), 1);
            client = new ThriftHmsClient(
                    config, null, hiveConf -> metastore, HmsTypeMapping.Options.DEFAULT, lastEvent::set);
        }

        private void occupyOnlyClient() throws Exception {
            holder = executor.submit(() -> client.getPartitions(
                    "db", "tbl", Collections.singletonList("p=hold")));
            Assertions.assertTrue(holderEntered.await(5, TimeUnit.SECONDS));
        }

        @Override
        public void close() throws Exception {
            releaseHolder.countDown();
            if (holder != null) {
                holder.get(5, TimeUnit.SECONDS);
            }
            client.close();
            executor.shutdownNow();
        }
    }
}
