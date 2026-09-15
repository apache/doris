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
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchItem;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchResponse;
import org.apache.doris.proto.InternalService.PTabletKeyLookupRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.CodedOutputStream;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * One bounded, pending batch per BE, shared by all proxy shards. The first submitting MySQL worker
 * owns the short wait window. A producer filling the batch sends it immediately and wakes its owner.
 * No timer thread, unbounded work queue, or additional RPC channel is needed. RPC submission and
 * completion always happen outside the queue lock; each query still waits on its own future.
 */
class PointQueryRpcBatcher {
    static final int MAX_ITEMS = 8;
    static final int MAX_BYTES = 1024 * 1024;
    static final long UNARY_BACKOFF_NS = TimeUnit.MINUTES.toNanos(1);

    interface Transport {
        ListenableFuture<PTabletKeyLookupResponse> unary(
                TNetworkAddress address, PTabletKeyLookupRequest request, long timeoutMs) throws Exception;

        ListenableFuture<PTabletKeyLookupBatchResponse> batch(
                TNetworkAddress address, PTabletKeyLookupBatchRequest request, long timeoutMs) throws Exception;
    }

    // Entries own no threads or channels. Expiry can safely overlap a submitted batch: its owner
    // and callback keep it alive. DNS and channel replacement remain the proxy's responsibility.
    private final Cache<TNetworkAddress, BackendQueue> queues = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES).build();
    private final Transport transport;
    private final LongSupplier nanoTime;
    private final Executor fallbackExecutor;

    PointQueryRpcBatcher(Transport transport, Executor fallbackExecutor) {
        this(transport, System::nanoTime, fallbackExecutor);
    }

    PointQueryRpcBatcher(Transport transport, LongSupplier nanoTime, Executor fallbackExecutor) {
        this.transport = transport;
        this.nanoTime = nanoTime;
        this.fallbackExecutor = fallbackExecutor;
    }

    ListenableFuture<PTabletKeyLookupResponse> submit(
            TNetworkAddress address, PTabletKeyLookupRequest request, long timeoutMs) {
        Item item = new Item(request, nanoTime.getAsLong()
                + TimeUnit.MILLISECONDS.toNanos(Math.min(Integer.MAX_VALUE, Math.max(0, timeoutMs))));
        int maxItems = Math.max(1, Math.min(MAX_ITEMS, Config.point_query_rpc_batch_max_size));
        int maxBytes = Math.max(1, Math.min(MAX_BYTES, Config.point_query_rpc_batch_max_request_bytes));
        int waitUs = Math.max(0, Config.point_query_rpc_batch_max_wait_us);
        // Use the original budget for admission. It can only shrink before dispatch, so this
        // includes the protobuf envelope without serializing/copying the request to count bytes.
        int itemBytes = CodedOutputStream.computeMessageSize(1, request)
                + CodedOutputStream.computeUInt32Size(2, (int) Math.min(Integer.MAX_VALUE, Math.max(0, timeoutMs)));
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
        if (shouldUseUnary(queue)) {
            unary(address, item);
            return item.future;
        }
        Batch ready = null;
        Batch batch;
        synchronized (queue) {
            if (queue.pending != null && queue.pending.bytes + wireBytes > maxBytes) {
                ready = queue.detach();
            }
            if (queue.pending == null) {
                queue.pending = new Batch(Math.min(item.deadlineNs,
                        nanoTime.getAsLong() + TimeUnit.MICROSECONDS.toNanos(waitUs)));
            }
            batch = queue.pending;
            batch.items.add(item);
            batch.bytes += wireBytes;
            if (batch.items.size() >= maxItems) {
                ready = queue.detach();
            }
        }
        if (ready != null) {
            dispatch(address, queue, ready);
        }
        if (batch.owner == Thread.currentThread()) {
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
                dispatch(address, queue, batch);
                return;
            }
            LockSupport.parkNanos(this, remaining);
        }
    }

    private void dispatch(TNetworkAddress address, BackendQueue queue, Batch batch) {
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
        // A lone arrival must not pay for a batch envelope or capability probe.
        if (live.size() == 1 || shouldUseUnary(queue)) {
            for (Item item : live) {
                unary(address, item);
            }
            return;
        }
        try {
            ListenableFuture<PTabletKeyLookupBatchResponse> rpc = transport.batch(address, request.build(), timeoutMs);
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
                    if (response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
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
                    batchFailure(address, queue, live, failure);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            batchFailure(address, queue, live, e);
        }
    }

    private boolean shouldUseUnary(BackendQueue queue) {
        long until = queue.unaryUntil;
        return until != 0 && nanoTime.getAsLong() - until < 0;
    }

    private void batchFailure(TNetworkAddress address, BackendQueue queue, List<Item> items, Throwable failure) {
        Status.Code code = Status.fromThrowable(failure).getCode();
        if (code == Status.Code.UNIMPLEMENTED || code == Status.Code.RESOURCE_EXHAUSTED) {
            // Old BEs lack this method; merged responses can also exceed the channel's message
            // limit even when each unary response fits. Back off before replaying these read-only
            // requests, so subsequent queries do not repeatedly incur the same batch failure.
            queue.unaryUntil = nanoTime.getAsLong() + UNARY_BACKOFF_NS;
            // Successful completions only wake their waiting workers. Fallback may resolve a
            // hostname or create a channel, so keep that IO off the gRPC completion thread.
            fallbackExecutor.execute(() -> {
                for (Item item : items) {
                    unary(address, item);
                }
            });
        } else {
            fail(items, failure);
        }
    }

    private void unary(TNetworkAddress address, Item item) {
        long remaining = remainingMillis(item);
        if (item.future.isDone() || remaining <= 0) {
            return;
        }
        try {
            item.future.setFuture(transport.unary(address, item.request, remaining));
        } catch (Exception e) {
            item.future.setException(e);
        }
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

    private static class BackendQueue {
        // Protected by this queue's monitor; no IO or future completion under it.
        private Batch pending;
        private volatile long unaryUntil;

        private Batch detach() {
            Batch batch = pending;
            pending = null;
            batch.detached = true;
            if (batch.owner != Thread.currentThread()) {
                LockSupport.unpark(batch.owner);
            }
            return batch;
        }
    }

    private static class Batch {
        private final List<Item> items = new ArrayList<>(MAX_ITEMS);
        private final Thread owner = Thread.currentThread();
        private final long flushAt;
        private int bytes;
        private volatile boolean detached;

        private Batch(long flushAt) {
            this.flushAt = flushAt;
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
