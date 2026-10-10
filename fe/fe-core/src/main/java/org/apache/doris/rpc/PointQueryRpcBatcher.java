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
import java.util.concurrent.RejectedExecutionException;
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
        timeoutMs = Math.min(Integer.MAX_VALUE, Math.max(0, timeoutMs));
        Item item = new Item(request, nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(timeoutMs));
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
            dispatch(address, ready);
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
                dispatch(address, batch);
                return;
            }
            LockSupport.parkNanos(this, remaining);
        }
    }

    private void dispatch(TNetworkAddress address, Batch batch) {
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
                    // A combined response can exceed the channel limit while each lookup fits.
                    // BRPC can report an oversized response as an HTTP/2 flow-control error.
                    Status status = Status.fromThrowable(failure);
                    if (status.getCode() == Status.Code.RESOURCE_EXHAUSTED
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
        item.future.setFuture(client.fetchTabletDataAsync(item.request, remaining));
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
