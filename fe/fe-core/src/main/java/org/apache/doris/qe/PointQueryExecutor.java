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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.LiteralExprUtils;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.KeyTuple;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.rpc.TCustomProtocolFactory;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PointQueryExecutor implements CoordInterface {
    private static final Logger LOG = LogManager.getLogger(PointQueryExecutor.class);
    private long tabletID = 0;
    private long timeoutMs = Config.point_query_timeout_ms; // default 10s

    private volatile boolean isCancel;
    private volatile ListenableFuture<InternalService.PTabletKeyLookupResponse> activeFuture;
    private List<Backend> candidateBackends;
    private final int maxMsgSizeOfResultReceiver;

    // used for snapshot read in cloud mode
    private List<Long> snapshotVisibleVersions;

    private final ShortCircuitQueryContext shortCircuitQueryContext;

    public PointQueryExecutor(ShortCircuitQueryContext ctx, int maxMessageSize) {
        ctx.sanitize();
        this.shortCircuitQueryContext = ctx;
        this.maxMsgSizeOfResultReceiver = maxMessageSize;
    }

    private void updateCloudPartitionVersions() throws RpcException {
        OlapScanNode scanNode = shortCircuitQueryContext.scanNode;
        List<CloudPartition> partitions = new ArrayList<>();
        Set<Long> partitionSet = new HashSet<>();
        OlapTable table = scanNode.getOlapTable();
        for (Long id : scanNode.getSelectedPartitionIds()) {
            if (!partitionSet.contains(id)) {
                partitionSet.add(id);
                partitions.add((CloudPartition) table.getPartition(id));
            }
        }
        snapshotVisibleVersions = CloudPartition.getSnapshotVisibleVersion(partitions);
        // Only support single partition at present
        Preconditions.checkState(snapshotVisibleVersions.size() == 1);
        LOG.debug("set cloud version {}", snapshotVisibleVersions.get(0));
    }

    void selectPointQueryRoute() throws Exception {
        tabletID = 0;
        candidateBackends = null;
        snapshotVisibleVersions = null;
        OlapScanNode scanNode = shortCircuitQueryContext.scanNode;
        // compute scan range
        List<TScanRangeLocations> locations =
                scanNode.lazyEvaluateRangeLocations(shortCircuitQueryContext.getKeyColumns());
        Preconditions.checkNotNull(locations);
        if (scanNode.getScanTabletIds().isEmpty()) {
            return;
        }
        Preconditions.checkState(scanNode.getScanTabletIds().size() == 1);
        this.tabletID = scanNode.getScanTabletIds().get(0);

        // update partition version if cloud mode
        if (Config.isCloudMode()
                && ConnectContext.get().getSessionVariable().enableSnapshotPointQuery) {
            // TODO: Optimize to reduce the frequency of version checks in the meta service.
            updateCloudPartitionVersions();
        }

        candidateBackends = new ArrayList<>();
        for (Long backendID : scanNode.getScanBackendIds()) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(backendID);
            if (SimpleScheduler.isAvailable(backend)) {
                candidateBackends.add(backend);
            }
        }
        if (shouldShuffleCandidateBackends(scanNode)) {
            Collections.shuffle(candidateBackends);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("set scan locations, backend ids {}, tablet id {}", candidateBackends, tabletID);
        }
    }

    static boolean shouldShuffleCandidateBackends(OlapScanNode scanNode) {
        return !scanNode.isScanBackendOrderBySelection();
    }

    // execute query without analyze & plan
    public static void directExecuteShortCircuitQuery(StmtExecutor executor,
            PreparedStatementContext preparedStmtCtx,
            StatementContext statementContext) throws Exception {
        Preconditions.checkNotNull(preparedStmtCtx.shortCircuitQueryContext);
        ShortCircuitQueryContext shortCircuitQueryContext = preparedStmtCtx.shortCircuitQueryContext.get();
        PointQueryBindings bindings = shortCircuitQueryContext.getPointQueryBindings();
        if (bindings != null && bindings.bindParameters(statementContext, preparedStmtCtx.command)) {
            executor.executeAndSendResult(false, false,
                    shortCircuitQueryContext.analzyedQuery, executor.getContext().getResultSender(), null, null);
            return;
        }
        // update conjuncts
        Map<String, Expr> colNameToConjunct = Maps.newHashMap();
        for (Entry<PlaceholderId, SlotReference> entry : statementContext.getIdToComparisonSlot().entrySet()) {
            String colName = entry.getValue().getOriginalColumn().get().getName();
            Expr conjunctVal = ((Literal)  statementContext.getIdToPlaceholderRealExpr()
                    .get(entry.getKey())).toLegacyLiteral();
            colNameToConjunct.put(colName, conjunctVal);
        }
        if (colNameToConjunct.size() != preparedStmtCtx.command.placeholderCount()) {
            throw new AnalysisException("Mismatched conjuncts values size with prepared"
                    + "statement parameters size, expected "
                    + preparedStmtCtx.command.placeholderCount()
                    + ", but meet " + colNameToConjunct.size());
        }
        updateScanNodeConjuncts(shortCircuitQueryContext.scanNode, colNameToConjunct);
        // short circuit plan and execution
        executor.executeAndSendResult(false, false,
                shortCircuitQueryContext.analzyedQuery, executor.getContext().getResultSender(), null, null);
    }

    @VisibleForTesting
    static void updateScanNodeConjuncts(OlapScanNode scanNode,
                Map<String, Expr> colNameToConjunct) throws TException {
        for (Expr conjunct : scanNode.getConjuncts()) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) conjunct;
            SlotRef slot = null;
            int updateChildIdx = 0;
            if (binaryPredicate.getChild(0) instanceof LiteralExpr) {
                slot = (SlotRef) binaryPredicate.getChildWithoutCast(1);
            } else if (binaryPredicate.getChild(1) instanceof LiteralExpr) {
                slot = (SlotRef) binaryPredicate.getChildWithoutCast(0);
                updateChildIdx = 1;
            } else {
                Preconditions.checkState(false, "Should contains literal in "
                        + binaryPredicate.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }
            // not a placeholder to replace
            if (!colNameToConjunct.containsKey(slot.getColumnName())) {
                continue;
            }
            binaryPredicate.setChild(updateChildIdx, getKeyColumnLiteral(
                    colNameToConjunct.get(slot.getColumnName()), slot.getDesc().getColumn()));
        }
    }

    static Expr getKeyColumnLiteral(Expr literalExpr, Column column) throws TException {
        if (literalExpr instanceof LiteralExpr) {
            Type colType = column.getType();
            if (!colType.equals(literalExpr.getType())
                    && !colType.matchesType(literalExpr.getType())) {
                try {
                    return LiteralExprUtils.createLiteral(
                            ((LiteralExpr) literalExpr).getStringValue(), colType);
                } catch (org.apache.doris.common.AnalysisException e) {
                    throw new TException("Failed to re-type literal for key column "
                            + column.getName() + ": " + e.getMessage(), e);
                }
            }
        }
        return literalExpr;
    }

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    void addKeyTuples(
            InternalService.PTabletKeyLookupRequest.Builder requestBuilder) throws TException {
        PointQueryBindings bindings = shortCircuitQueryContext.getPointQueryBindings();
        Map<String, Expr> columnExpr = null;
        if (bindings == null) {
            columnExpr = Maps.newHashMap();
            for (Expr expr : shortCircuitQueryContext.scanNode.getConjuncts()) {
                BinaryPredicate predicate = (BinaryPredicate) expr;
                SlotRef slot = predicate.getChild(0).unwrapSlotRef();
                columnExpr.put(slot.getColumnName(), predicate.getChild(1));
            }
        }
        KeyTuple.Builder kBuilder = KeyTuple.newBuilder();
        // Serialize each literal expr as TExprNode bytes for typed value transfer.
        // BE deserializes the TExprNode and uses DataType::get_field() to extract
        // typed Field values directly, avoiding string parsing.
        TSerializer serializer = new TSerializer();
        List<Column> keyColumns = shortCircuitQueryContext.getKeyColumns();
        for (int i = 0; i < keyColumns.size(); i++) {
            Column column = keyColumns.get(i);
            Expr literalExpr = bindings != null ? bindings.getKeyLiteral(i) : columnExpr.get(column.getName());
            // Ensure the literal type matches the column type for proper TExprNode
            // deserialization on BE side. Prepared statement parameters may have
            // mismatched types (e.g., setBigDecimal for INT column produces a
            // DecimalLiteral, but BE expects INT_LITERAL for INT columns).
            literalExpr = getKeyColumnLiteral(literalExpr, column);
            TExpr texpr = ExprToThriftVisitor.treeToThrift(literalExpr);
            // For point queries, key column values are always simple literals
            // (CastExpr no-ops are already stripped by treeToThrift).
            Preconditions.checkState(texpr.getNodesSize() == 1,
                    "Expected single TExprNode for key column literal of " + column.getName()
                    + ", got " + texpr.getNodesSize());
            TExprNode exprNode = texpr.getNodes().get(0);
            byte[] serialized = serializer.serialize(exprNode);
            kBuilder.addKeyColumnLiterals(
                    com.google.protobuf.ByteString.copyFrom(serialized));
        }
        requestBuilder.addKeyTuples(kBuilder);
    }

    @Override
    public void cancel(Status cancelReason) {
        cancel();
    }


    @Override
    public RowBatch getNext() throws Exception {
        Lookup lookup = prepareLookup();
        while (!lookup.done()) {
            ListenableFuture<InternalService.PTabletKeyLookupResponse> future = null;
            InternalService.PTabletKeyLookupResponse response = null;
            Throwable failure = null;
            try {
                future = lookup.submit();
                long remaining = lookup.deadlineNs - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("query request timeout");
                }
                response = future.get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException | TimeoutException | ExecutionException | CancellationException
                    | RpcException e) {
                failure = e;
                if (future != null) {
                    future.cancel(true);
                }
            }
            lookup.accept(response, failure);
        }
        return lookup.result();
    }

    private static void checkStatus(Status status) throws UserException, RpcException {
        if (!status.ok()) {
            if (Strings.isNullOrEmpty(status.getErrorMsg())) {
                status.rewriteErrorMsg();
            }
            String errMsg = status.getErrorMsg();
            LOG.warn("query failed: {}", errMsg);
            if (status.isRpcError()) {
                throw new RpcException(null, errMsg);
            }
            // Hide host information in the MySQL error.
            int hostIndex = errMsg.indexOf("host");
            if (hostIndex != -1) {
                errMsg = errMsg.substring(0, hostIndex);
            }
            throw new UserException(errMsg);
        }
    }

    @Override
    public void exec() throws Exception {
        // Point queries don't need to do anthing in execution phase.
        // only handles in getNext()
    }

    Lookup prepareLookup() throws Exception {
        selectPointQueryRoute();
        Lookup lookup = new Lookup();
        if (candidateBackends == null || candidateBackends.isEmpty()) {
            lookup.batch = new RowBatch();
            lookup.batch.setEos(true);
        } else {
            lookup.prepareAttempt();
        }
        return lookup;
    }

    final class Lookup {
        private final Status status = new Status();
        private int attempt;
        private Backend backend;
        private InternalService.PTabletKeyLookupRequest request;
        private boolean fullContext;
        private boolean failed;
        private RowBatch batch;
        private long deadlineNs;

        private void prepareAttempt() throws TException {
            backend = candidateBackends.get(attempt);
            deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            fullContext = !(Config.enable_lightweight_lookup_request
                    && ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE
                    && shortCircuitQueryContext.cacheID != null);
            request = buildLookupRequest(fullContext);
        }

        long deadlineNs() {
            return deadlineNs;
        }

        boolean done() {
            return batch != null || failed;
        }

        RowBatch result() throws UserException, RpcException {
            if (isCancel) {
                status.updateStatus(TStatusCode.CANCELLED, "cancelled");
            }
            checkStatus(status);
            return batch;
        }

        ListenableFuture<InternalService.PTabletKeyLookupResponse> submit() throws RpcException {
            if (isCancel) {
                return com.google.common.util.concurrent.Futures.immediateCancelledFuture();
            }
            long remaining = deadlineNs - System.nanoTime();
            if (remaining <= 0) {
                return com.google.common.util.concurrent.Futures.immediateFailedFuture(
                        new TimeoutException("query request timeout"));
            }
            ListenableFuture<InternalService.PTabletKeyLookupResponse> future =
                    BackendServiceProxy.fetchTabletDataAsync(backend.getBrpcAddress(), request,
                            Math.max(0, TimeUnit.NANOSECONDS.toMillis(remaining)));
            activeFuture = future;
            // Cancellation may precede publication or race with it.
            if (isCancel) {
                future.cancel(true);
            }
            return future;
        }

        void accept(InternalService.PTabletKeyLookupResponse response, Throwable failure)
                throws TException {
            activeFuture = null;
            if (isCancel) {
                status.updateStatus(TStatusCode.CANCELLED, "cancelled");
            } else if (failure != null) {
                recordFailure(status, backend, failure);
            } else {
                Status resultStatus = new Status(response.getStatus());
                status.updateStatus(resultStatus.getErrorCode(), resultStatus.getErrorMsg());
                if (status.ok()) {
                    if (response.hasNeedResendQueryContext() && response.getNeedResendQueryContext()) {
                        if (!fullContext) {
                            fullContext = true;
                            request = buildLookupRequest(true);
                            // A context miss shares this replica's original deadline.
                            return;
                        }
                        status.updateStatus(TStatusCode.INTERNAL_ERROR,
                                "backend requests query context resend but request already contains query context");
                    } else {
                        if (!response.getEmptyBatch()
                                && (!response.hasRowBatch() || response.getRowBatch().isEmpty())) {
                            throw new IllegalStateException("No row batch or empty batch found");
                        }
                        try {
                            batch = buildRowBatch(response.getEmptyBatch(), response.getRowBatch());
                        } catch (TException e) {
                            if (ResultReceiver.isMessageSizeExceeded(e)) {
                                throw new TException(
                                        "MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                            }
                            throw e;
                        }
                        return;
                    }
                }
            }
            if (isCancel || status.isCancelled() || Thread.currentThread().isInterrupted()
                    || ++attempt >= Math.max(1,
                            Math.min(Config.max_point_query_retry_time, candidateBackends.size()))) {
                failed = true;
            } else {
                status.updateStatus(TStatusCode.OK, "");
                prepareAttempt();
            }
        }
    }

    static void recordFailure(Status status, Backend backend, Throwable failure) {
        if (failure instanceof RpcException) {
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, failure.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), failure.getMessage());
        } else if (failure instanceof TimeoutException) {
            status.updateStatus(TStatusCode.INTERNAL_ERROR, "query fetch result timeout");
        } else if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            status.updateStatus(TStatusCode.INTERNAL_ERROR, "interrupted while waiting for point query result");
        } else if (failure instanceof CancellationException) {
            status.updateStatus(TStatusCode.CANCELLED, "cancelled");
        } else {
            Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
            io.grpc.Status.Code code = io.grpc.Status.fromThrowable(cause).getCode();
            String message = cause.getMessage();
            if (code == io.grpc.Status.Code.DEADLINE_EXCEEDED
                    || (message != null && message.contains("time out"))) {
                status.updateStatus(TStatusCode.TIMEOUT, message);
            } else if (code == io.grpc.Status.Code.CANCELLED) {
                status.updateStatus(TStatusCode.CANCELLED, message);
            } else if (cause instanceof RejectedExecutionException) {
                status.updateStatus(TStatusCode.INTERNAL_ERROR, message);
            } else {
                status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, message);
                SimpleScheduler.addToBlacklist(backend.getId(), message);
            }
        }
    }

    private RowBatch buildRowBatch(boolean empty, ByteString serializedBatch) throws TException {
        RowBatch rowBatch = new RowBatch();
        rowBatch.setEos(true);
        if (!empty) {
            rowBatch.setBatch(deserializeRowBatch(serializedBatch, maxMsgSizeOfResultReceiver));
        }
        return rowBatch;
    }

    private InternalService.PTabletKeyLookupRequest buildLookupRequest(boolean includeQueryContext)
            throws TException {
        InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                = InternalService.PTabletKeyLookupRequest.newBuilder()
                .setTabletId(tabletID)
                .setIsBinaryRow(ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE);

        if (includeQueryContext) {
            requestBuilder.setDescTbl(shortCircuitQueryContext.serializedDescTable)
                    .setOutputExpr(shortCircuitQueryContext.serializedOutputExpr)
                    .setQueryOptions(shortCircuitQueryContext.serializedQueryOptions);
        }

        // Set timezone for functions like from_unixtime
        requestBuilder.setTimeZone(TimeUtils.getCanonicalTimeZone(
                ConnectContext.get().getSessionVariable().getTimeZone()));

        if (snapshotVisibleVersions != null && !snapshotVisibleVersions.isEmpty()) {
            requestBuilder.setVersion(snapshotVisibleVersions.get(0));
        }

        // Only set cacheID for prepared statement execute phase,
        // otherwise leading to many redundant cost in BE side
        if (shortCircuitQueryContext.cacheID != null
                && ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE) {
            InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
            uuidBuilder.setUuidHigh(shortCircuitQueryContext.cacheID.getMostSignificantBits());
            uuidBuilder.setUuidLow(shortCircuitQueryContext.cacheID.getLeastSignificantBits());
            requestBuilder.setUuid(uuidBuilder);
        }

        addKeyTuples(requestBuilder);
        return requestBuilder.build();
    }

    static TResultBatch deserializeRowBatch(ByteString bytes, int maxMessageSize) throws TException {
        if (bytes.size() > maxMessageSize) {
            throw new TException("MaxMessageSize reached");
        }
        TResultBatch result = new TResultBatch();
        new TDeserializer(new TCustomProtocolFactory(maxMessageSize)).deserialize(result, bytes.toByteArray());
        return result;
    }

    public void cancel() {
        isCancel = true;
        ListenableFuture<InternalService.PTabletKeyLookupResponse> future = activeFuture;
        if (future != null) {
            future.cancel(true);
        }
    }

    @Override
    public List<TNetworkAddress> getInvolvedBackends() {
        return Lists.newArrayList();
    }

    @Override
    public void setIsProfileSafeStmt(boolean isSafe) {
        // Do nothing
    }
}
