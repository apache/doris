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

import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.LiteralExprUtils;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.planner.OlapScanNode.PointQueryRoute;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.rpc.TCustomProtocolFactory;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Query-level coordinator for the supported single-column IN point query. Key tuples routed to
 * the same tablet are merged into one ordinary tablet_fetch_data request. RPCs are concurrent across
 * BEs but serial on each BE, because requests with the same UUID share a reusable execution context
 * there. Execution uses lightweight requests when enabled and resends a full request on a cold cache.
 * Cached executions require parameters bound with types compatible with their key columns;
 * parameter type changes requiring different comparison semantics are not supported.
 */
public class PointQueryMultiExecutor extends PointQueryExecutor {
    private final ShortCircuitQueryContext context;
    private final StatementContext statementContext;
    private final int maxMessageSize;
    private final Set<Future<?>> currentRpcFutures = Collections.synchronizedSet(new HashSet<>());
    private final Set<Long> failedBackends = new HashSet<>();
    private long timeoutMs = Config.point_query_timeout_ms;
    private volatile boolean cancelled;

    private static final class TabletTask {
        private final long tabletId;
        private final List<Backend> candidateBackends;
        private final List<InternalService.KeyTuple> keyTuples = new ArrayList<>();
        private Backend backend;
        private InternalService.PTabletKeyLookupRequest request;
        private Future<InternalService.PTabletKeyLookupResponse> future;
        private int attemptCount;
        private String lastFailure;

        private TabletTask(long tabletId, List<Backend> candidateBackends) {
            this.tabletId = tabletId;
            this.candidateBackends = candidateBackends;
        }

        private Backend nextBackend(Set<Long> failedBackends) {
            int maxAttempts = Math.max(1,
                    Math.min(Config.max_point_query_retry_time, candidateBackends.size()));
            while (attemptCount < maxAttempts) {
                Backend backend = candidateBackends.get(attemptCount++);
                if (!failedBackends.contains(backend.getId()) && SimpleScheduler.isAvailable(backend)) {
                    return backend;
                }
            }
            return null;
        }
    }

    private final class ResultAccumulator {
        private final List<ByteBuffer> rows = new ArrayList<>();
        private final TDeserializer deserializer;
        private long resultBytes;

        private ResultAccumulator() throws TException {
            deserializer = new TDeserializer(new TCustomProtocolFactory(maxMessageSize));
        }

        private void add(InternalService.PTabletKeyLookupResponse response) throws TException {
            if (response.hasEmptyBatch() && response.getEmptyBatch()) {
                return;
            }
            if (!response.hasRowBatch() || response.getRowBatch().isEmpty()) {
                throw new TException("No row batch or empty batch found in point-query response");
            }

            TResultBatch batch = new TResultBatch();
            try {
                deserializer.deserialize(batch, response.getRowBatch().toByteArray());
            } catch (TException e) {
                if (ResultReceiver.isMessageSizeExceeded(e)) {
                    throw new TException(
                            "MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                }
                throw e;
            }
            for (ByteBuffer row : batch.getRows()) {
                resultBytes += row.remaining();
                if (resultBytes > maxMessageSize) {
                    throw new TException(
                            "MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                }
                rows.add(row);
            }
        }

        private RowBatch finish() {
            RowBatch rowBatch = new RowBatch();
            if (rows.isEmpty()) {
                return rowBatch;
            }
            TResultBatch resultBatch = new TResultBatch();
            resultBatch.setRows(rows);
            resultBatch.setIsCompressed(false);
            resultBatch.setPacketSeq(0);
            rowBatch.setBatch(resultBatch);
            return rowBatch;
        }
    }

    public PointQueryMultiExecutor(ShortCircuitQueryContext context,
            StatementContext statementContext, int maxMessageSize) {
        super(context, maxMessageSize);
        this.context = context;
        this.statementContext = statementContext;
        this.maxMessageSize = maxMessageSize;
    }

    public static void directExecuteShortCircuitQuery(StmtExecutor executor,
            PreparedStatementContext preparedStmtCtx) throws Exception {
        // Multi-get reads current bindings without mutating the cached IN predicate.
        executor.executeAndSendResult(false, false,
                preparedStmtCtx.shortCircuitQueryContext.get().analzyedQuery,
                executor.getContext().getMysqlChannel(), null, null);
    }

    @Override
    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public RowBatch getNext() throws Exception {
        try {
            return getNextInternal();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            invalidateCache();
            throw e;
        } finally {
            cancelInFlightRpcs();
        }
    }

    private RowBatch getNextInternal() throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        Map<Long, TabletTask> tasksByTablet = buildTabletTasks(deadlineNanos);
        ResultAccumulator accumulator = new ResultAccumulator();
        List<TabletTask> pending = new ArrayList<>(tasksByTablet.values());
        while (!pending.isEmpty()) {
            checkCancelledOrTimedOut(deadlineNanos);
            pending = executeRound(pending, accumulator, deadlineNanos);
        }
        return accumulator.finish();
    }

    private Map<Long, TabletTask> buildTabletTasks(long deadlineNanos) throws TException, UserException {
        List<Column> keyColumns = context.scanNode.getOlapTable().getBaseSchemaKeyColumns();
        Map<String, Integer> keyIndexes = new HashMap<>(keyColumns.size());
        for (int i = 0; i < keyColumns.size(); ++i) {
            keyIndexes.put(normalizeColumnName(keyColumns.get(i).getName()), i);
        }

        List<PlaceholderId> inPlaceholderIds = statementContext.getPointQueryInPlaceholderIds();
        SlotReference inSlot = statementContext.getIdToComparisonSlot().get(inPlaceholderIds.get(0));
        int inKeyIndex = keyIndexes.get(normalizeColumnName(
                inSlot.getOriginalColumn().get().getName()));
        LiteralExpr[] keyValues = new LiteralExpr[keyColumns.size()];
        for (Map.Entry<PlaceholderId, SlotReference> entry
                : statementContext.getIdToComparisonSlot().entrySet()) {
            SlotReference slot = entry.getValue();
            int keyIndex = keyIndexes.get(normalizeColumnName(
                    slot.getOriginalColumn().get().getName()));
            // Eligibility checking guarantees exactly one placeholder for each equality key.
            if (keyIndex != inKeyIndex) {
                keyValues[keyIndex] = ((Literal) statementContext.getIdToPlaceholderRealExpr()
                        .get(entry.getKey())).toLegacyLiteral();
            }
        }

        TSerializer serializer = new TSerializer();
        ByteString[] serializedKeyValues = new ByteString[keyColumns.size()];
        for (int i = 0; i < keyValues.length; ++i) {
            if (i == inKeyIndex) {
                continue;
            }
            if (keyValues[i] instanceof NullLiteral) {
                return Collections.emptyMap();
            }
            keyValues[i] = normalizeKeyLiteral(keyColumns.get(i), keyValues[i]);
            serializedKeyValues[i] = serializeKeyLiteral(keyValues[i], serializer);
        }

        Set<InternalService.KeyTuple> seenTuples = new HashSet<>();
        Map<Long, TabletTask> tasksByTablet = new LinkedHashMap<>();
        for (PlaceholderId placeholderId : inPlaceholderIds) {
            checkCancelledOrTimedOut(deadlineNanos);
            LiteralExpr inValue = ((Literal) statementContext.getIdToPlaceholderRealExpr()
                    .get(placeholderId)).toLegacyLiteral();
            if (inValue instanceof NullLiteral) {
                continue;
            }
            keyValues[inKeyIndex] = normalizeKeyLiteral(keyColumns.get(inKeyIndex), inValue);
            serializedKeyValues[inKeyIndex] = serializeKeyLiteral(keyValues[inKeyIndex], serializer);
            InternalService.KeyTuple.Builder tupleBuilder = InternalService.KeyTuple.newBuilder();
            for (ByteString serializedValue : serializedKeyValues) {
                tupleBuilder.addKeyColumnLiterals(serializedValue);
            }
            InternalService.KeyTuple keyTuple = tupleBuilder.build();
            if (!seenTuples.add(keyTuple)) {
                continue;
            }
            PointQueryRoute route = context.scanNode.routePointQueryKeyTuple(Arrays.asList(keyValues));
            if (route == null) {
                continue;
            }
            TabletTask task = tasksByTablet.get(route.getTabletId());
            if (task == null) {
                List<Backend> candidates = selectPointQueryBackends(route);
                if (candidates.isEmpty()) {
                    throw new UserException("Tablet " + route.getTabletId()
                            + " has no available backend for multi-key point query");
                }
                task = new TabletTask(route.getTabletId(), candidates);
                tasksByTablet.put(route.getTabletId(), task);
            }
            task.keyTuples.add(keyTuple);
        }
        return tasksByTablet;
    }

    private static LiteralExpr normalizeKeyLiteral(Column column, LiteralExpr literalExpr)
            throws TException {
        Type columnType = column.getType();
        if (columnType.equals(literalExpr.getType())
                || columnType.matchesType(literalExpr.getType())) {
            return literalExpr;
        }
        try {
            return LiteralExprUtils.createLiteral(literalExpr.getStringValue(), columnType);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new TException("Failed to re-type literal for key column "
                    + column.getName() + ": " + e.getMessage(), e);
        }
    }

    private static ByteString serializeKeyLiteral(LiteralExpr literalExpr, TSerializer serializer)
            throws TException {
        TExpr thriftExpr = ExprToThriftVisitor.treeToThrift(literalExpr);
        Preconditions.checkState(thriftExpr.getNodesSize() == 1,
                "Expected a single TExprNode for point-query key literal, got %s",
                thriftExpr.getNodesSize());
        TExprNode exprNode = thriftExpr.getNodes().get(0);
        return ByteString.copyFrom(serializer.serialize(exprNode));
    }

    private static String normalizeColumnName(String columnName) {
        return columnName.toLowerCase(Locale.ROOT);
    }

    private static List<Backend> selectPointQueryBackends(PointQueryRoute route) {
        List<Backend> candidates = new ArrayList<>(route.getCandidateBackendIds().size());
        for (Long backendId : route.getCandidateBackendIds()) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
            if (SimpleScheduler.isAvailable(backend)) {
                candidates.add(backend);
            }
        }
        if (!route.isBackendOrderBySelection()) {
            Collections.shuffle(candidates);
        }
        return candidates;
    }

    private InternalService.PTabletKeyLookupRequest buildLookupRequest(
            TabletTask task, boolean includeQueryContext) {
        InternalService.PTabletKeyLookupRequest.Builder builder
                = InternalService.PTabletKeyLookupRequest.newBuilder()
                .setTabletId(task.tabletId)
                .setIsBinaryRow(true)
                .addAllKeyTuples(task.keyTuples);
        if (includeQueryContext) {
            builder.setDescTbl(context.serializedDescTable)
                    .setOutputExpr(context.serializedOutputExpr)
                    .setQueryOptions(context.serializedQueryOptions);
        }
        String timeZone = ConnectContext.get().getSessionVariable().getTimeZone();
        if ("CST".equals(timeZone)) {
            timeZone = "Asia/Shanghai";
        }
        builder.setTimeZone(timeZone);
        InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
        uuidBuilder.setUuidHigh(context.cacheID.getMostSignificantBits());
        uuidBuilder.setUuidLow(context.cacheID.getLeastSignificantBits());
        builder.setUuid(uuidBuilder);
        return builder.build();
    }

    private List<TabletTask> executeRound(List<TabletTask> pending,
            ResultAccumulator accumulator, long deadlineNanos) throws Exception {
        Map<Long, List<TabletTask>> tasksByBackend = new LinkedHashMap<>();
        int waveCount = 0;
        for (TabletTask task : pending) {
            task.backend = task.nextBackend(failedBackends);
            if (task.backend == null) {
                throw new UserException(task.lastFailure == null
                        ? "No available backend for tablet " + task.tabletId : task.lastFailure);
            }
            task.request = buildLookupRequest(task, !Config.enable_lightweight_lookup_request);
            List<TabletTask> backendTasks = tasksByBackend.computeIfAbsent(
                    task.backend.getId(), ignored -> new ArrayList<>());
            backendTasks.add(task);
            waveCount = Math.max(waveCount, backendTasks.size());
        }

        List<TabletTask> retry = new ArrayList<>();
        for (int waveIndex = 0; waveIndex < waveCount; ++waveIndex) {
            List<TabletTask> wave = new ArrayList<>(tasksByBackend.size());
            for (List<TabletTask> backendTasks : tasksByBackend.values()) {
                if (waveIndex < backendTasks.size()) {
                    wave.add(backendTasks.get(waveIndex));
                }
            }
            // Finish cold-cache resends before dispatching the next tablet on the same BE.
            while (!wave.isEmpty()) {
                wave = executeWave(wave, retry, accumulator, deadlineNanos);
            }
        }
        return retry;
    }

    private List<TabletTask> executeWave(List<TabletTask> wave, List<TabletTask> retry,
            ResultAccumulator accumulator, long deadlineNanos) throws Exception {
        List<TabletTask> resend = new ArrayList<>();
        // Dispatch the complete wave before awaiting any response. The coordinator thread remains
        // the only writer of the result accumulator and retry state.
        for (TabletTask task : wave) {
            checkCancelledOrTimedOut(deadlineNanos);
            if (failedBackends.contains(task.backend.getId()) || !SimpleScheduler.isAvailable(task.backend)) {
                recordFailure(task,
                        "Backend became unavailable before point-query RPC dispatch");
                retry.add(task);
                continue;
            }
            try {
                task.future = BackendServiceProxy
                        .getInstance().fetchTabletDataAsync(
                                task.backend.getBrpcAddress(), task.request);
                currentRpcFutures.add(task.future);
            } catch (RpcException e) {
                recordFailure(task, e.getMessage());
                retry.add(task);
                excludeFailedBackend(task.backend, e.getMessage());
            }
        }

        for (TabletTask task : wave) {
            if (task.future == null) {
                continue;
            }
            try {
                checkCancelledOrTimedOut(deadlineNanos);
                InternalService.PTabletKeyLookupResponse response = task.future.get(
                        Math.max(1, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                    recordFailure(task, response.getStatus().getErrorMsgsCount() == 0
                            ? "Multi-key point-query request failed with status "
                                    + response.getStatus().getStatusCode()
                            : response.getStatus().getErrorMsgs(0));
                    retry.add(task);
                } else if (response.getNeedResendQueryContext()) {
                    if (task.request.hasDescTbl()) {
                        recordFailure(task, "Backend requested query context although the request included it");
                        retry.add(task);
                    } else {
                        task.request = task.request.toBuilder().setDescTbl(context.serializedDescTable)
                                .setOutputExpr(context.serializedOutputExpr)
                                .setQueryOptions(context.serializedQueryOptions).build();
                        resend.add(task);
                    }
                } else {
                    accumulator.add(response);
                }
            } catch (ExecutionException e) {
                String message = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
                recordFailure(task, message);
                retry.add(task);
                excludeFailedBackend(task.backend, message);
            } finally {
                // Also covers cancellation/deadline checks that throw before Future.get().
                task.future.cancel(true);
                currentRpcFutures.remove(task.future);
                task.future = null;
            }
        }
        return resend;
    }

    private void checkCancelledOrTimedOut(long deadlineNanos) throws UserException {
        if (cancelled) {
            throw new UserException("Multi-key point query was cancelled");
        }
        if (System.nanoTime() - deadlineNanos >= 0) {
            throw new UserException("Multi-key point query timed out");
        }
    }

    private static void recordFailure(TabletTask task, String failure) {
        task.lastFailure = "Point-query tablet " + task.tabletId + " failed: "
                + failureMessage(failure);
    }

    private static String failureMessage(String failure) {
        return Strings.isNullOrEmpty(failure) ? "Multi-key point-query RPC failed" : failure;
    }

    private void excludeFailedBackend(Backend backend, String message) {
        // Transport failure does not prove BE execution stopped. Do not dispatch another tablet
        // with this UUID to that BE, even before the scheduler's blacklist threshold is reached.
        failedBackends.add(backend.getId());
        invalidateCache();
        SimpleScheduler.addToBlacklist(backend.getId(), failureMessage(message));
    }

    private void invalidateCache() {
        // A cancelled/failed RPC may still be using this UUID on BE. The next execute must replan.
        statementContext.setShortCircuitQuery(false);
        statementContext.setShortCircuitQueryContext(null);
    }

    @Override
    public void cancel(Status cancelReason) {
        cancel();
    }

    @Override
    public void cancel() {
        cancelled = true;
        cancelInFlightRpcs();
    }

    private void cancelInFlightRpcs() {
        List<Future<?>> futures;
        synchronized (currentRpcFutures) {
            futures = new ArrayList<>(currentRpcFutures);
            currentRpcFutures.clear();
        }
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }
}
