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

package org.apache.doris.qe;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.proto.InternalService.KeyTuple;
import org.apache.doris.proto.InternalService.PTabletKeyLookupRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupResponse;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.rpc.TCustomProtocolFactory;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded, single-partition literal IN lookups. Equality/prepared lookups retain their existing executor. */
public class BatchPointQueryExecutor implements CoordInterface {
    public static final int MAX_KEYS = 100;

    private final ShortCircuitQueryContext query;
    private final ConnectContext context;
    private final int maxMessageSize;
    private final LookupTransport transport;
    private final Set<Future<PTabletKeyLookupResponse>> pending = new HashSet<>();
    private volatile boolean cancelled;
    private List<TNetworkAddress> involvedBackends = Collections.emptyList();

    @FunctionalInterface
    interface LookupTransport {
        Future<PTabletKeyLookupResponse> send(Backend backend, PTabletKeyLookupRequest request) throws RpcException;
    }

    static final class TabletRequest {
        final PTabletKeyLookupRequest request;
        final List<Backend> backends;
        int attempts;
        Future<PTabletKeyLookupResponse> future;

        TabletRequest(PTabletKeyLookupRequest request, List<Backend> backends) {
            this.request = request;
            this.backends = backends;
        }
    }

    public BatchPointQueryExecutor(ShortCircuitQueryContext query, int maxMessageSize) {
        this(query, maxMessageSize, ConnectContext.get(), (backend, request) -> BackendServiceProxy.getInstance()
                .fetchTabletDataAsync(backend.getBrpcAddress(), request));
    }

    BatchPointQueryExecutor(ShortCircuitQueryContext query, int maxMessageSize,
            ConnectContext context, LookupTransport transport) {
        this.query = query;
        this.maxMessageSize = maxMessageSize;
        this.context = context;
        this.transport = transport;
    }

    static boolean isBatchQuery(OlapScanNode scan) {
        return scan.getConjuncts().stream().anyMatch(expression -> expression instanceof InPredicate);
    }

    static Map<Long, List<LiteralExpr>> groupKeysByTablet(
            List<LiteralExpr> keys, Column keyColumn, List<Long> tablets) {
        Map<Long, List<LiteralExpr>> grouped = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        for (LiteralExpr key : keys) {
            // SQL IN has set semantics, including when several literals encode the same key.
            if (!seen.add(key.getStringValue())) {
                continue;
            }
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(key, keyColumn.getDataType());
            long tablet = tablets.get((int) ((hashKey.getHashValue() & 0xffffffffL) % tablets.size()));
            grouped.computeIfAbsent(tablet, ignored -> new ArrayList<>()).add(key);
        }
        return grouped;
    }

    private List<TabletRequest> prepareRequests() throws Exception {
        OlapScanNode scan = query.scanNode;
        List<TScanRangeLocations> locations = scan.lazyEvaluateRangeLocations();
        if (scan.getSelectedPartitionIds().isEmpty()) {
            return Collections.emptyList();
        }
        Preconditions.checkState(scan.getSelectedPartitionIds().size() == 1);
        OlapTable table = scan.getOlapTable();
        Partition partition = table.getPartition(scan.getSelectedPartitionIds().iterator().next());
        long version = Config.isCloudMode()
                ? CloudPartition.getSnapshotVisibleVersion(Collections.singletonList((CloudPartition) partition)).get(0)
                : Long.parseLong(locations.get(0).getScanRange().getPaloScanRange().getVersion());
        List<LiteralExpr> keys = new ArrayList<>();
        for (Expr expression : scan.getConjuncts()) {
            if (expression instanceof InPredicate) {
                for (int i = 1; i < expression.getChildren().size(); ++i) {
                    keys.add((LiteralExpr) expression.getChild(i));
                }
            }
        }
        Preconditions.checkState(!keys.isEmpty() && keys.size() <= MAX_KEYS);
        Map<Long, List<LiteralExpr>> grouped = groupKeysByTablet(keys, table.getBaseSchemaKeyColumns().get(0),
                table.getPartitionIndex(partition, table.getBaseIndexId()).getTabletIdsInOrder());
        Map<Long, List<Backend>> replicas = new HashMap<>();
        List<TNetworkAddress> addresses = new ArrayList<>();
        for (TScanRangeLocations location : locations) {
            if (!Config.isCloudMode()
                    && Long.parseLong(location.getScanRange().getPaloScanRange().getVersion()) != version) {
                throw new UserException("Batch point query requires one visible partition version");
            }
            List<Backend> backends = new ArrayList<>();
            for (TScanRangeLocation replica : location.getLocations()) {
                Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (SimpleScheduler.isAvailable(backend)) {
                    backends.add(backend);
                    addresses.add(backend.getBrpcAddress());
                }
            }
            if (PointQueryExecutor.shouldShuffleCandidateBackends(scan)) {
                Collections.shuffle(backends);
            }
            replicas.put(location.getScanRange().getPaloScanRange().getTabletId(), backends);
        }
        involvedBackends = Collections.unmodifiableList(addresses);
        List<TabletRequest> requests = new ArrayList<>();
        TSerializer serializer = new TSerializer();
        String timeZone = context.getSessionVariable().getTimeZone();
        for (Map.Entry<Long, List<LiteralExpr>> entry : grouped.entrySet()) {
            List<Backend> backends = replicas.get(entry.getKey());
            if (backends == null || backends.isEmpty()) {
                throw new UserException("No available backend for batch point query tablet " + entry.getKey());
            }
            PTabletKeyLookupRequest.Builder builder = PTabletKeyLookupRequest.newBuilder()
                    .setTabletId(entry.getKey()).setSnapshotVersion(version).setIsBinaryRow(false)
                    .setDescTbl(query.serializedDescTable).setOutputExpr(query.serializedOutputExpr)
                    .setQueryOptions(query.serializedQueryOptions)
                    .setTimeZone("CST".equals(timeZone) ? "Asia/Shanghai" : timeZone);
            for (LiteralExpr key : entry.getValue()) {
                builder.addKeyTuples(KeyTuple.newBuilder().addKeyColumnLiterals(ByteString.copyFrom(
                        serializer.serialize(ExprToThriftVisitor.treeToThrift(key).getNodes().get(0)))));
            }
            requests.add(new TabletRequest(builder.build(), backends));
        }
        return requests;
    }

    @Override
    public RowBatch getNext() throws Exception {
        long timeoutMs = Math.min(Config.point_query_timeout_ms, context.getExecTimeoutS() * 1000L);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            return executeRequests(prepareRequests(), deadline);
        } finally {
            cancelPending();
        }
    }

    RowBatch executeRequests(List<TabletRequest> requests, long deadline) throws Exception {
        int concurrency = context.getSessionVariable().getBatchPointQueryConcurrency();
        ArrayDeque<TabletRequest> active = new ArrayDeque<>();
        List<ByteBuffer> rows = new ArrayList<>();
        long responseBytes = 0;
        int next = 0;
        try {
            while (next < requests.size() || !active.isEmpty()) {
                while (next < requests.size() && active.size() < concurrency) {
                    TabletRequest task = requests.get(next++);
                    send(task, deadline);
                    active.addLast(task);
                }
                TabletRequest task = active.removeFirst();
                PTabletKeyLookupResponse response = await(task, deadline);
                // Older backends ignore unknown request fields. Never accept their latest-version result.
                if (!response.hasSnapshotVersion()
                        || response.getSnapshotVersion() != task.request.getSnapshotVersion()) {
                    throw new UserException("Backend does not support snapshot batch point queries, tablet "
                            + task.request.getTabletId());
                }
                if (response.hasEmptyBatch() && response.getEmptyBatch()) {
                    continue;
                }
                if (!response.hasRowBatch()) {
                    throw new UserException("Missing batch point query result for tablet "
                            + task.request.getTabletId());
                }
                responseBytes += response.getRowBatch().size();
                if (responseBytes > maxMessageSize) {
                    throw new UserException(
                            "Batch point query result exceeds max_msg_size_of_result_receiver");
                }
                TResultBatch batch = new TResultBatch();
                new TDeserializer(new TCustomProtocolFactory(maxMessageSize))
                        .deserialize(batch, response.getRowBatch().toByteArray());
                rows.addAll(batch.getRows());
            }
            checkDeadline(deadline);
            RowBatch result = new RowBatch();
            result.setBatch(new TResultBatch().setRows(rows).setIsCompressed(false).setPacketSeq(0));
            result.setEos(true);
            return result;
        } finally {
            cancelPending();
        }
    }

    private void checkDeadline(long deadline) throws UserException {
        if (cancelled || context.isKilled()) {
            throw new UserException("Batch point query cancelled");
        }
        if (System.nanoTime() >= deadline) {
            throw new UserException("Batch point query timed out");
        }
    }

    private void send(TabletRequest task, long deadline) throws UserException {
        int attempts = maxAttempts(task);
        while (task.attempts < attempts) {
            checkDeadline(deadline);
            Backend backend = task.backends.get(task.attempts++);
            try {
                Future<PTabletKeyLookupResponse> future = transport.send(backend, task.request);
                synchronized (pending) {
                    if (!cancelled) {
                        pending.add(future);
                    }
                }
                if (cancelled) {
                    future.cancel(true);
                    throw new UserException("Batch point query cancelled");
                }
                task.future = future;
                return;
            } catch (RpcException e) {
                SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
                if (task.attempts == attempts) {
                    throw new UserException("Batch point query RPC failed for tablet "
                            + task.request.getTabletId() + ": " + e.getMessage(), e);
                }
            }
        }
        throw new UserException("No remaining replica for batch point query tablet " + task.request.getTabletId());
    }

    private int maxAttempts(TabletRequest task) {
        return Math.min(Math.max(1, Config.max_point_query_retry_time), task.backends.size());
    }

    private PTabletKeyLookupResponse await(TabletRequest task, long deadline) throws Exception {
        while (true) {
            checkDeadline(deadline);
            try {
                // Polling only makes cancellation observable; all RPCs share the same absolute deadline.
                long waitNanos = Math.min(deadline - System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(100));
                PTabletKeyLookupResponse response = task.future.get(Math.max(1, waitNanos), TimeUnit.NANOSECONDS);
                synchronized (pending) {
                    pending.remove(task.future);
                }
                Status status = new Status(response.getStatus());
                if (!status.ok()) {
                    // As with single-key point queries, a busy/unavailable replica may reject an RPC
                    // with a non-OK response. Try another replica before failing the whole query.
                    if (task.attempts < maxAttempts(task)) {
                        send(task, deadline);
                        continue;
                    }
                    throw new UserException("Batch point query failed for tablet " + task.request.getTabletId()
                            + ": " + status.getErrorMsg());
                }
                return response;
            } catch (TimeoutException e) {
                // The next iteration checks the query deadline and cancellation.
            } catch (ExecutionException e) {
                synchronized (pending) {
                    pending.remove(task.future);
                }
                if (task.attempts == maxAttempts(task)) {
                    throw new UserException("Batch point query RPC failed for tablet "
                            + task.request.getTabletId() + ": " + e.getMessage(), e);
                }
                send(task, deadline);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UserException("Interrupted while waiting for batch point query", e);
            } catch (CancellationException e) {
                throw new UserException("Batch point query cancelled", e);
            }
        }
    }

    private void cancelPending() {
        List<Future<PTabletKeyLookupResponse>> futures;
        synchronized (pending) {
            futures = new ArrayList<>(pending);
            pending.clear();
        }
        for (Future<PTabletKeyLookupResponse> future : futures) {
            future.cancel(true);
        }
    }

    @Override
    public void cancel(Status reason) {
        cancelled = true;
        cancelPending();
    }

    @Override
    public void close() {
        cancelPending();
    }

    @Override
    public void exec() {
        // Lookup RPCs are issued by getNext(), as with single-key point queries.
    }

    @Override
    public List<TNetworkAddress> getInvolvedBackends() {
        return involvedBackends;
    }

    @Override
    public void setIsProfileSafeStmt(boolean isSafe) {
        // The point-query BE log provides lookup timings; no pipeline profile is created.
    }
}
