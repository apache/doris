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

package org.apache.doris.rpc;

import org.apache.doris.common.Config;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchItem;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchResponse;
import org.apache.doris.proto.InternalService.PTabletKeyLookupRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.codahale.metrics.Histogram;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * One bounded pending batch per BE, shared by all proxy shards. Synchronous callers can own the wait
 * window; asynchronous callers use a bounded flush scheduler and dispatch pool. A full batch is
 * detached under its queue lock, with timer cancellation, RPC submission and completion outside it.
 */
class PointQueryRpcBatcher implements AutoCloseable {
    static final int MAX_ITEMS = 8;
    static final int MAX_BYTES = 1024 * 1024;

    interface ClientProvider {
        BackendServiceClient get(TNetworkAddress address) throws Exception;
    }

    // Entries own no threads or channels. Expiry can safely overlap a submitted batch: its owner
    // and callback keep it alive. DNS and channel replacement remain the proxy's responsibility.
    private final Cache<TNetworkAddress, BackendQueue> queues = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES).build();
    private final ClientProvider clients;
    private final LongSupplier nanoTime;
    private final Executor fallbackExecutor;
    private final Set<Item> asynchronousItems = ConcurrentHashMap.newKeySet();
    private volatile FlushScheduler flushScheduler;
    private volatile boolean closed;
    private final LongAdder rejectedItems = new LongAdder();

    PointQueryRpcBatcher(ClientProvider clients, Executor fallbackExecutor) {
        this(clients, System::nanoTime, fallbackExecutor);
    }

    PointQueryRpcBatcher(ClientProvider clients, LongSupplier nanoTime, Executor fallbackExecutor) {
        this.clients = clients;
        this.nanoTime = nanoTime;
        this.fallbackExecutor = fallbackExecutor;
    }

    ListenableFuture<PTabletKeyLookupResponse> submit(
            TNetworkAddress address, PTabletKeyLookupRequest request, long timeoutMs) {
        boolean asynchronous = Config.enable_point_query_rpc_async_flush;
        timeoutMs = Math.min(Integer.MAX_VALUE, Math.max(0, timeoutMs));
        Item item = new Item(request, nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(timeoutMs));
        if (asynchronous || flushScheduler != null) {
            asynchronousItems.add(item);
            item.future.addListener(() -> asynchronousItems.remove(item), MoreExecutors.directExecutor());
        }
        if (closed) {
            rejectedItems.increment();
            item.future.setException(new RejectedExecutionException("point-query batcher closed"));
            return item.future;
        }
        int maxItems = Math.max(1, Math.min(MAX_ITEMS, Config.point_query_rpc_batch_max_size));
        int maxBytes = Math.max(1, Math.min(MAX_BYTES, Config.point_query_rpc_batch_max_request_bytes));
        int waitUs = Math.max(0, Config.point_query_rpc_batch_max_wait_us);
        // Use the original budget for admission. It can only shrink before dispatch, so this
        // includes the protobuf envelope without serializing/copying the request to count bytes.
        int itemBytes = CodedOutputStream.computeMessageSize(1, request)
                + CodedOutputStream.computeUInt32Size(2, (int) timeoutMs);
        int wireBytes = CodedOutputStream.computeTagSize(1)
                + CodedOutputStream.computeUInt32SizeNoTag(itemBytes) + itemBytes;
        if (maxItems == 1 || waitUs == 0 || wireBytes > maxBytes || timeoutMs <= 0) {
            unary(address, item);
            return item.future;
        }
        BackendQueue queue = queues.getIfPresent(address);
        if (queue == null) {
            queue = queues.asMap().computeIfAbsent(address.deepCopy(), key -> new BackendQueue());
        }
        int inFlight = queue.inFlight.incrementAndGet();
        item.future.addListener(queue.inFlight::decrementAndGet, MoreExecutors.directExecutor());
        // Send the first outstanding lookup immediately. Concurrent lookups can coalesce.
        if (inFlight == 1) {
            unary(address, item);
            return item.future;
        }
        FlushScheduler scheduler = asynchronous ? flushScheduler() : null;
        Batch ready = null;
        Batch batch;
        boolean created = false;
        synchronized (queue) {
            if (queue.pending != null && queue.pending.bytes + wireBytes > maxBytes) {
                ready = queue.detach();
            }
            if (queue.pending == null && !closed && (scheduler == null || scheduler.slots.tryAcquire())) {
                long now = nanoTime.getAsLong();
                queue.pending = new Batch(Math.min(item.deadlineNs,
                        now + TimeUnit.MICROSECONDS.toNanos(waitUs)), scheduler, now,
                        scheduler != null && (++queue.batchSequence & 1023) == 0);
                created = true;
            }
            batch = closed ? null : queue.pending;
            if (batch != null) {
                batch.items.add(item);
                batch.bytes += wireBytes;
                if (batch.items.size() >= maxItems) {
                    ready = queue.detach();
                }
            }
        }
        if (ready != null) {
            dispatch(address, ready);
        }
        if (batch == null) {
            rejectedItems.increment();
            item.future.setException(new RejectedExecutionException("point-query flush capacity exhausted or closed"));
        } else if (batch.scheduler != null) {
            if (created) {
                batch.scheduler.schedule(address, queue, batch);
            }
        } else if (batch.owner == Thread.currentThread()) {
            awaitAndDispatch(address, queue, batch);
        }
        return item.future;
    }

    private void awaitAndDispatch(TNetworkAddress address, BackendQueue queue, Batch batch) {
        while (!batch.detached) {
            long remaining = batch.flushAt - nanoTime.getAsLong();
            if (remaining <= 0 || Thread.currentThread().isInterrupted()) {
                synchronized (queue) {
                    if (batch.detached) {
                        return;
                    }
                    queue.detach();
                }
                dispatch(address, batch);
                return;
            }
            LockSupport.parkNanos(this, remaining);
        }
    }

    private void dispatch(TNetworkAddress address, Batch batch) {
        if (batch.scheduler != null) {
            batch.scheduler.releaseTimer(batch);
        }
        // Full/overflow batches already have a business-thread producer. Only timer flushes
        // need the dispatch pool; another queue hop here adds no execution isolation.
        dispatchRpc(address, batch);
    }

    private void dispatchRpc(TNetworkAddress address, Batch batch) {
        if (batch.sampled) {
            batch.scheduler.waitTime.update(TimeUnit.NANOSECONDS.toMicros(nanoTime.getAsLong() - batch.enqueuedNs));
        }
        // detach publishes the completed batch under the queue monitor. Notify its owner
        // after releasing that monitor, before client resolution can block the dispatching thread.
        if (batch.owner != null && batch.owner != Thread.currentThread()) {
            LockSupport.unpark(batch.owner);
        }
        try {
            // Resolve on the submitting worker, then account for that time in every item budget.
            BackendServiceClient client = clients.get(address);
            List<Item> live = new ArrayList<>(batch.items.size());
            PTabletKeyLookupBatchRequest.Builder request = PTabletKeyLookupBatchRequest.newBuilder();
            long timeoutMs = 0;
            for (Item item : batch.items) {
                long remaining = remainingMillis(item);
                if (!item.future.isDone() && remaining > 0) {
                    live.add(item);
                    request.addItems(PTabletKeyLookupBatchItem.newBuilder().setRequest(item.request)
                            .setRemainingTimeoutMs((int) remaining));
                    timeoutMs = Math.max(timeoutMs, remaining);
                }
            }
            if (live.isEmpty()) {
                return;
            }
            if (live.size() == 1) {
                unary(client, live.get(0));
                return;
            }
            ListenableFuture<PTabletKeyLookupBatchResponse> rpc =
                    client.fetchTabletDataBatchAsync(request.build(), timeoutMs);
            AtomicInteger cancelled = new AtomicInteger();
            for (Item item : live) {
                item.future.addListener(() -> {
                    if (item.future.isCancelled() && cancelled.incrementAndGet() == live.size()) {
                        rpc.cancel(true);
                    }
                }, MoreExecutors.directExecutor());
            }
            Futures.addCallback(rpc, new FutureCallback<PTabletKeyLookupBatchResponse>() {
                @Override
                public void onSuccess(PTabletKeyLookupBatchResponse response) {
                    if (!response.hasStatus()) {
                        fail(live, Status.DATA_LOSS.withDescription("point query batch response has no status")
                                .asRuntimeException());
                    } else if (response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                        PTabletKeyLookupResponse failure = PTabletKeyLookupResponse.newBuilder()
                                .setStatus(response.getStatus()).build();
                        live.forEach(item -> item.future.set(failure));
                    } else if (response.getResultsCount() != live.size()) {
                        fail(live, Status.DATA_LOSS.withDescription("point query batch result count mismatch")
                                .asRuntimeException());
                    } else {
                        for (int i = 0; i < live.size(); i++) {
                            Item item = live.get(i);
                            if (remainingMillis(item) > 0) {
                                item.future.set(response.getResults(i));
                            }
                        }
                    }
                }

                @Override
                public void onFailure(Throwable failure) {
                    // Older BEs may not implement batch RPC. A combined response can also
                    // exceed the channel limit while each lookup fits.
                    // BRPC can report an oversized response as an HTTP/2 flow-control error.
                    Status status = Status.fromThrowable(failure);
                    if (status.getCode() == Status.Code.UNIMPLEMENTED
                            || status.getCode() == Status.Code.RESOURCE_EXHAUSTED
                            || (status.getCode() == Status.Code.INTERNAL && status.getDescription() != null
                            && status.getDescription().contains("FLOW_CONTROL_ERROR"))) {
                        // Spread large unary responses across the existing channel pool; one
                        // connection may not have enough flow-control credit for all of them.
                        // Client selection can resolve DNS, so keep it off the completion thread.
                        try {
                            fallbackExecutor.execute(() -> live.forEach(item -> unary(address, item)));
                        } catch (RejectedExecutionException e) {
                            fail(live, e);
                        }
                    } else {
                        fail(live, failure);
                    }
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            fail(batch.items, e);
        }
    }

    private void unary(TNetworkAddress address, Item item) {
        if (item.future.isDone() || remainingMillis(item) <= 0) {
            return;
        }
        try {
            unary(clients.get(address), item);
        } catch (Exception e) {
            item.future.setException(e);
        }
    }

    private void unary(BackendServiceClient client, Item item) {
        long remaining = remainingMillis(item);
        if (item.future.isDone() || remaining <= 0) {
            return;
        }
        item.future.setFuture(client.fetchTabletDataAsync(item.request));
    }

    private long remainingMillis(Item item) {
        long remaining = item.deadlineNs - nanoTime.getAsLong();
        if (remaining <= 0) {
            item.future.setException(Status.DEADLINE_EXCEEDED
                    .withDescription("point query deadline exceeded").asRuntimeException());
            return 0;
        }
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }

    private static void fail(List<Item> items, Throwable failure) {
        items.forEach(item -> item.future.setException(failure));
    }

    private FlushScheduler flushScheduler() {
        FlushScheduler current = flushScheduler;
        if (current == null) {
            synchronized (this) {
                current = flushScheduler;
                if (current == null) {
                    current = new FlushScheduler();
                    flushScheduler = current;
                    if (closed) {
                        current.close();
                    }
                }
            }
        }
        return current;
    }

    @Override
    public void close() {
        closed = true;
        for (BackendQueue queue : queues.asMap().values()) {
            Batch batch;
            synchronized (queue) {
                batch = queue.pending == null ? null : queue.detach();
            }
            if (batch != null) {
                if (batch.scheduler != null) {
                    batch.scheduler.releaseTimer(batch);
                } else {
                    LockSupport.unpark(batch.owner);
                }
                fail(batch.items, new RejectedExecutionException("point-query batcher closed"));
            }
        }
        asynchronousItems.forEach(item -> item.future.cancel(true));
        FlushScheduler current = flushScheduler;
        if (current != null) {
            current.close();
        }
    }

    private final class FlushScheduler implements AutoCloseable {
        private final Semaphore slots = new Semaphore(Config.point_query_batch_pending_limit);
        private final ScheduledThreadPoolExecutor[] timers =
                new ScheduledThreadPoolExecutor[Config.point_query_async_deadline_shards];
        private final ThreadPoolExecutor dispatchers;
        private final Histogram waitTime = MetricRepo.METRIC_REGISTER.histogram("point_query_batch_async_wait_us");

        private FlushScheduler() {
            if (timers.length == 0 || Config.point_query_batch_dispatch_threads <= 0
                    || Config.point_query_batch_pending_limit <= 0) {
                throw new IllegalArgumentException("point-query flush limits must be positive");
            }
            int threads = Config.point_query_batch_dispatch_threads;
            dispatchers = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(Config.point_query_batch_pending_limit),
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("point-query-dispatch-%d").build(),
                    new ThreadPoolExecutor.AbortPolicy());
            for (int i = 0; i < timers.length; i++) {
                timers[i] = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("point-query-flush-" + i + "-%d").build());
                timers[i].setRemoveOnCancelPolicy(true);
            }
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(new GaugeMetric<Long>(
                    "point_query_batch_async_rejected_items_total", MetricUnit.NOUNIT, "rejected batch items") {
                @Override
                public Long getValue() {
                    return rejectedItems.sum();
                }
            });
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(new GaugeMetric<Integer>(
                    "point_query_batch_async_dispatch_ready", MetricUnit.NOUNIT, "queued RPC batches") {
                @Override
                public Integer getValue() {
                    return dispatchers.getQueue().size();
                }
            });
        }

        private void schedule(TNetworkAddress address, BackendQueue queue, Batch batch) {
            try {
                // This is the batch monitor, not the BE queue monitor. It also serializes
                // timer installation against cancellation/releasing its capacity reservation.
                synchronized (batch) {
                    if (batch.detached) {
                        return;
                    }
                    ScheduledThreadPoolExecutor timer = timers[Math.floorMod(address.hashCode(), timers.length)];
                    batch.flushTask = timer.schedule(() -> {
                        synchronized (queue) {
                            if (queue.pending != batch) {
                                return;
                            }
                            queue.detach();
                        }
                        dispatch(address, batch);
                    }, Math.max(0, batch.flushAt - nanoTime.getAsLong()), TimeUnit.NANOSECONDS);
                }
            } catch (RejectedExecutionException e) {
                boolean detached = false;
                synchronized (queue) {
                    if (queue.pending == batch) {
                        queue.detach();
                        detached = true;
                    }
                }
                if (detached) {
                    releaseTimer(batch);
                    rejectedItems.add(batch.items.size());
                    fail(batch.items, e);
                }
            }
        }

        private void releaseTimer(Batch batch) {
            synchronized (batch) {
                if (!batch.flushReleased) {
                    batch.flushReleased = true;
                    if (batch.flushTask != null) {
                        batch.flushTask.cancel(false);
                    }
                    slots.release();
                }
            }
        }

        private void dispatch(TNetworkAddress address, Batch batch) {
            releaseTimer(batch);
            try {
                dispatchers.execute(new Dispatch(address, batch));
            } catch (RejectedExecutionException e) {
                rejectedItems.add(batch.items.size());
                fail(batch.items, e);
            }
        }

        @Override
        public void close() {
            for (ScheduledThreadPoolExecutor timer : timers) {
                timer.shutdownNow();
            }
            for (Runnable task : dispatchers.shutdownNow()) {
                Dispatch dispatch = (Dispatch) task;
                fail(dispatch.batch.items, new RejectedExecutionException("point-query dispatcher closed"));
            }
        }
    }

    private final class Dispatch implements Runnable {
        private final TNetworkAddress address;
        private final Batch batch;

        private Dispatch(TNetworkAddress address, Batch batch) {
            this.address = address;
            this.batch = batch;
        }

        @Override
        public void run() {
            dispatchRpc(address, batch);
        }
    }

    private static class BackendQueue {
        private final AtomicInteger inFlight = new AtomicInteger();
        // Protected by this queue's monitor; no IO or future completion under it.
        private Batch pending;
        private int batchSequence;

        private Batch detach() {
            Batch batch = pending;
            pending = null;
            batch.detached = true;
            return batch;
        }
    }

    private static class Batch {
        private final List<Item> items = new ArrayList<>(MAX_ITEMS);
        private final Thread owner;
        private final FlushScheduler scheduler;
        private ScheduledFuture<?> flushTask;
        private boolean flushReleased;
        private final long flushAt;
        private final long enqueuedNs;
        private final boolean sampled;
        private int bytes;
        private volatile boolean detached;

        private Batch(long flushAt, FlushScheduler scheduler, long enqueuedNs, boolean sampled) {
            this.flushAt = flushAt;
            this.enqueuedNs = enqueuedNs;
            this.sampled = sampled;
            this.scheduler = scheduler;
            owner = scheduler == null ? Thread.currentThread() : null;
        }
    }

    private static class Item {
        private final PTabletKeyLookupRequest request;
        private final long deadlineNs;
        private final SettableFuture<PTabletKeyLookupResponse> future = SettableFuture.create();

        private Item(PTabletKeyLookupRequest request, long deadlineNs) {
            this.request = request;
            this.deadlineNs = deadlineNs;
        }
    }
}
