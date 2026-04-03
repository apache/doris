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
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
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
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PointQueryExecutor implements CoordInterface {
    private static final Logger LOG = LogManager.getLogger(PointQueryExecutor.class);
    private long tabletID = 0;
    private long timeoutMs = Config.point_query_timeout_ms; // default 10s

    private boolean isCancel = false;
    private List<Backend> candidateBackends;
    private final int maxMsgSizeOfResultReceiver;

    // used for snapshot read in cloud mode
    private List<Long> snapshotVisibleVersions;

    private final ShortCircuitQueryContext shortCircuitQueryContext;

    // Inverted index point query mode: multiple tablets may need to be queried
    private boolean isInvertedIndexMode = false;
    private List<Long> allTabletIds;
    private Map<Long, List<Backend>> tabletToBackends;

    public PointQueryExecutor(ShortCircuitQueryContext ctx, int maxMessageSize) {
        ctx.sanitize();
        this.shortCircuitQueryContext = ctx;
        this.maxMsgSizeOfResultReceiver = maxMessageSize;
        StatementContext stmtCtx = ConnectContext.get().getStatementContext();
        if (stmtCtx != null && stmtCtx.isInvertedIndexPointQuery()) {
            this.isInvertedIndexMode = true;
        }
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

    void setScanRangeLocations() throws Exception {
        OlapScanNode scanNode = shortCircuitQueryContext.scanNode;
        // compute scan range
        List<TScanRangeLocations> locations = scanNode.lazyEvaluateRangeLocations();
        Preconditions.checkNotNull(locations);
        if (scanNode.getScanTabletIds().isEmpty()) {
            return;
        }

        if (isInvertedIndexMode) {
            // Inverted index mode: may have multiple tablets
            allTabletIds = new ArrayList<>(scanNode.getScanTabletIds());
            tabletToBackends = new HashMap<>();
            for (TScanRangeLocations loc : locations) {
                long tid = loc.getScanRange().getPaloScanRange().getTabletId();
                List<Backend> backends = new ArrayList<>();
                for (var tLoc : loc.getLocations()) {
                    Backend backend = Env.getCurrentSystemInfo().getBackend(tLoc.getBackendId());
                    if (SimpleScheduler.isAvailable(backend)) {
                        backends.add(backend);
                    }
                }
                Collections.shuffle(backends);
                tabletToBackends.put(tid, backends);
            }
            // TODO: BloomFilter-based tablet pruning.
            // When tablet-level BloomFilters are available in tablet metadata
            // (populated by BE during segment write/compaction via InvertedIndexBloomFilterPB),
            // probe the BloomFilter here to filter allTabletIds down to only those tablets
            // that may contain the queried value.
            // This would reduce RPC fanout from O(num_tablets) to O(num_tablets * false_positive_rate).
            //
            // Pseudocode:
            //   String queryValue = statementContext.getInvertedIndexPointQueryLiteralValue();
            //   int columnUid = statementContext.getInvertedIndexPointQueryColumnUniqueId();
            //   allTabletIds.removeIf(tid -> {
            //       BloomFilter bf = tabletBloomFilterCache.get(tid, columnUid);
            //       return bf != null && !bf.mightContain(queryValue);
            //   });
            if (LOG.isDebugEnabled()) {
                LOG.debug("Inverted index point query: {} tablets before BloomFilter pruning",
                        allTabletIds.size());
            }
        } else {
            Preconditions.checkState(scanNode.getScanTabletIds().size() == 1);
            this.tabletID = scanNode.getScanTabletIds().get(0);
        }

        // update partition version if cloud mode
        if (Config.isCloudMode()
                && ConnectContext.get().getSessionVariable().enableSnapshotPointQuery) {
            // TODO: Optimize to reduce the frequency of version checks in the meta service.
            updateCloudPartitionVersions();
        }

        if (!isInvertedIndexMode) {
            candidateBackends = new ArrayList<>();
            for (Long backendID : scanNode.getScanBackendIds()) {
                Backend backend = Env.getCurrentSystemInfo().getBackend(backendID);
                if (SimpleScheduler.isAvailable(backend)) {
                    candidateBackends.add(backend);
                }
            }
            // Random read replicas
            Collections.shuffle(this.candidateBackends);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("set scan locations, inverted_index_mode={}, tablet_count={}",
                    isInvertedIndexMode, isInvertedIndexMode ? allTabletIds.size() : 1);
        }
    }

    // execute query without analyze & plan
    public static void directExecuteShortCircuitQuery(StmtExecutor executor,
            PreparedStatementContext preparedStmtCtx,
            StatementContext statementContext) throws Exception {
        Preconditions.checkNotNull(preparedStmtCtx.shortCircuitQueryContext);
        ShortCircuitQueryContext shortCircuitQueryContext = preparedStmtCtx.shortCircuitQueryContext.get();
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
                shortCircuitQueryContext.analzyedQuery, executor.getContext()
                        .getMysqlChannel(), null, null);
    }

    private static void updateScanNodeConjuncts(OlapScanNode scanNode,
                Map<String, Expr> colNameToConjunct) {
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
                Preconditions.checkState(false, "Should contains literal in " + binaryPredicate.toSqlImpl());
            }
            // not a placeholder to replace
            if (!colNameToConjunct.containsKey(slot.getColumnName())) {
                continue;
            }
            binaryPredicate.setChild(updateChildIdx, colNameToConjunct.get(slot.getColumnName()));
        }
    }

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    void addKeyTuples(
            InternalService.PTabletKeyLookupRequest.Builder requestBuilder) {
        // TODO handle IN predicates
        Map<String, Expr> columnExpr = Maps.newHashMap();
        KeyTuple.Builder kBuilder = KeyTuple.newBuilder();
        for (Expr expr : shortCircuitQueryContext.scanNode.getConjuncts()) {
            BinaryPredicate predicate = (BinaryPredicate) expr;
            Expr left = predicate.getChild(0);
            Expr right = predicate.getChild(1);
            SlotRef columnSlot = left.unwrapSlotRef();
            columnExpr.put(columnSlot.getColumnName(), right);
        }
        // add key tuple in keys order
        for (Column column : shortCircuitQueryContext.scanNode.getOlapTable().getBaseSchemaKeyColumns()) {
            kBuilder.addKeyColumnRep(columnExpr.get(column.getName()).getStringValue());
        }
        requestBuilder.addKeyTuples(kBuilder);
    }

    void addInvertedIndexQuery(
            InternalService.PTabletKeyLookupRequest.Builder requestBuilder) {
        StatementContext stmtCtx = ConnectContext.get().getStatementContext();
        requestBuilder.setInvertedIndexColumnUniqueId(
                stmtCtx.getInvertedIndexPointQueryColumnUniqueId());
        String literalValue = stmtCtx.getInvertedIndexPointQueryLiteralValue();
        // Strip surrounding quotes from the SQL literal representation if present
        if (literalValue != null && literalValue.length() >= 2
                && literalValue.startsWith("'") && literalValue.endsWith("'")) {
            literalValue = literalValue.substring(1, literalValue.length() - 1);
        }
        requestBuilder.setInvertedIndexQueryValue(
                com.google.protobuf.ByteString.copyFromUtf8(literalValue));
    }

    @Override
    public void cancel(Status cancelReason) {
        // Do nothing
    }


    @Override
    public RowBatch getNext() throws Exception {
        setScanRangeLocations();

        if (isInvertedIndexMode) {
            return getNextForInvertedIndex();
        }

        // No partition/tablet found return emtpy row batch
        if (candidateBackends == null || candidateBackends.isEmpty()) {
            return new RowBatch();
        }
        Iterator<Backend> backendIter = candidateBackends.iterator();
        RowBatch rowBatch = null;
        int tryCount = 0;
        int maxTry = Math.min(Config.max_point_query_retry_time, candidateBackends.size());
        Status status = new Status();
        do {
            Backend backend = backendIter.next();
            rowBatch = getNextInternal(status, backend);
            if (rowBatch != null) {
                break;
            }
            if (++tryCount >= maxTry) {
                break;
            }
        } while (true);
        // handle status code
        if (!status.ok()) {
            if (Strings.isNullOrEmpty(status.getErrorMsg())) {
                status.rewriteErrorMsg();
            }
            String errMsg = status.getErrorMsg();
            LOG.warn("query failed: {}", errMsg);
            if (status.isRpcError()) {
                throw new RpcException(null, errMsg);
            } else {
                // hide host info
                int hostIndex = errMsg.indexOf("host");
                if (hostIndex != -1) {
                    errMsg = errMsg.substring(0, hostIndex);
                }
                throw new UserException(errMsg);
            }
        }
        return rowBatch;
    }

    private RowBatch getNextForInvertedIndex() throws Exception {
        if (allTabletIds == null || allTabletIds.isEmpty()) {
            return new RowBatch();
        }
        // Parallel RPC fanout: send requests to all tablets concurrently
        Map<Long, Future<InternalService.PTabletKeyLookupResponse>> futures = new HashMap<>();
        for (Long tid : allTabletIds) {
            List<Backend> backends = tabletToBackends.get(tid);
            if (backends == null || backends.isEmpty()) {
                continue;
            }
            Backend backend = backends.get(0);
            try {
                InternalService.PTabletKeyLookupRequest request = buildInvertedIndexRequest(tid);
                Future<InternalService.PTabletKeyLookupResponse> future =
                        BackendServiceProxy.getInstance().fetchTabletDataAsync(
                                backend.getBrpcAddress(), request);
                futures.put(tid, future);
            } catch (RpcException e) {
                LOG.warn("inverted index point query RPC failed on tablet {}: {}", tid, e);
            }
        }
        // Collect results from all parallel RPCs
        List<TResultBatch> allBatches = new ArrayList<>();
        for (Map.Entry<Long, Future<InternalService.PTabletKeyLookupResponse>> entry
                : futures.entrySet()) {
            Long tid = entry.getKey();
            try {
                InternalService.PTabletKeyLookupResponse pResult =
                        entry.getValue().get(timeoutMs, TimeUnit.MILLISECONDS);
                Status resultStatus = new Status(pResult.getStatus());
                if (resultStatus.getErrorCode() != TStatusCode.OK) {
                    LOG.warn("inverted index point query failed on tablet {}: {}",
                            tid, resultStatus.getErrorMsg());
                    continue;
                }
                if (pResult.hasEmptyBatch() && pResult.getEmptyBatch()) {
                    continue;
                }
                if (pResult.hasRowBatch() && pResult.getRowBatch().size() > 0) {
                    byte[] serialResult = pResult.getRowBatch().toByteArray();
                    TResultBatch resultBatch = new TResultBatch();
                    TDeserializer deserializer = new TDeserializer(
                            new TCustomProtocolFactory(this.maxMsgSizeOfResultReceiver));
                    deserializer.deserialize(resultBatch, serialResult);
                    allBatches.add(resultBatch);
                }
            } catch (TimeoutException e) {
                LOG.warn("inverted index point query timeout on tablet {}", tid);
            } catch (Exception e) {
                LOG.warn("inverted index point query error on tablet {}: {}", tid, e);
            }
        }
        if (allBatches.isEmpty()) {
            RowBatch empty = new RowBatch();
            empty.setEos(true);
            return empty;
        }
        TResultBatch merged = allBatches.get(0);
        for (int i = 1; i < allBatches.size(); i++) {
            merged.getRows().addAll(allBatches.get(i).getRows());
        }
        RowBatch result = new RowBatch();
        result.setBatch(merged);
        result.setEos(true);
        return result;
    }

    private InternalService.PTabletKeyLookupRequest buildInvertedIndexRequest(long tid) {
        InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                = InternalService.PTabletKeyLookupRequest.newBuilder()
                .setTabletId(tid)
                .setDescTbl(shortCircuitQueryContext.serializedDescTable)
                .setOutputExpr(shortCircuitQueryContext.serializedOutputExpr)
                .setQueryOptions(shortCircuitQueryContext.serializedQueryOptions)
                .setIsBinaryRow(ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE);
        String timeZone = ConnectContext.get().getSessionVariable().getTimeZone();
        if ("CST".equals(timeZone)) {
            timeZone = "Asia/Shanghai";
        }
        requestBuilder.setTimeZone(timeZone);
        addInvertedIndexQuery(requestBuilder);
        return requestBuilder.build();
    }

    @Override
    public void exec() throws Exception {
        // Point queries don't need to do anthing in execution phase.
        // only handles in getNext()
    }

    private RowBatch getNextInternal(Status status, Backend backend) throws TException {
        long timeoutTs = System.currentTimeMillis() + timeoutMs;
        RowBatch rowBatch = new RowBatch();
        InternalService.PTabletKeyLookupResponse pResult = null;
        try {
            Preconditions.checkNotNull(shortCircuitQueryContext.serializedDescTable);

            InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                    = InternalService.PTabletKeyLookupRequest.newBuilder()
                    .setTabletId(tabletID)
                    .setDescTbl(shortCircuitQueryContext.serializedDescTable)
                    .setOutputExpr(shortCircuitQueryContext.serializedOutputExpr)
                    .setQueryOptions(shortCircuitQueryContext.serializedQueryOptions)
                    .setIsBinaryRow(ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE);
            // Set timezone for functions like from_unixtime
            String timeZone = ConnectContext.get().getSessionVariable().getTimeZone();
            if ("CST".equals(timeZone)) {
                timeZone = "Asia/Shanghai";
            }
            requestBuilder.setTimeZone(timeZone);
            if (snapshotVisibleVersions != null && !snapshotVisibleVersions.isEmpty()) {
                requestBuilder.setVersion(snapshotVisibleVersions.get(0));
            }
            // Only set cacheID for prepared statement excute phase,
            // otherwise leading to many redundant cost in BE side
            if (shortCircuitQueryContext.cacheID != null
                        && ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE) {
                InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
                uuidBuilder.setUuidHigh(shortCircuitQueryContext.cacheID.getMostSignificantBits());
                uuidBuilder.setUuidLow(shortCircuitQueryContext.cacheID.getLeastSignificantBits());
                requestBuilder.setUuid(uuidBuilder);
            }
            if (isInvertedIndexMode) {
                addInvertedIndexQuery(requestBuilder);
            } else {
                addKeyTuples(requestBuilder);
            }

            InternalService.PTabletKeyLookupRequest request = requestBuilder.build();
            Future<InternalService.PTabletKeyLookupResponse> futureResponse =
                    BackendServiceProxy.getInstance().fetchTabletDataAsync(backend.getBrpcAddress(), request);
            long currentTs = System.currentTimeMillis();
            if (currentTs >= timeoutTs) {
                LOG.warn("fetch result timeout {}", backend.getBrpcAddress());
                status.updateStatus(TStatusCode.INTERNAL_ERROR, "query request timeout");
                return null;
            }
            try {
                pResult = futureResponse.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // continue to get result
                LOG.warn("future get interrupted Exception");
                if (isCancel) {
                    status.updateStatus(TStatusCode.CANCELLED, "cancelled");
                    return null;
                }
            } catch (TimeoutException e) {
                futureResponse.cancel(true);
                LOG.warn("fetch result timeout {}, addr {}", timeoutTs - currentTs, backend.getBrpcAddress());
                status.updateStatus(TStatusCode.INTERNAL_ERROR, "query fetch result timeout");
                return null;
            }
        } catch (RpcException e) {
            LOG.warn("query fetch rpc exception {}, e {}", backend.getBrpcAddress(), e);
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            return null;
        } catch (ExecutionException e) {
            LOG.warn("query fetch execution exception {}, addr {}", e, backend.getBrpcAddress());
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.updateStatus(TStatusCode.TIMEOUT, e.getMessage());
            } else {
                status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
                SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            }
            return null;
        }
        Status resultStatus = new Status(pResult.getStatus());
        if (resultStatus.getErrorCode() != TStatusCode.OK) {
            status.updateStatus(resultStatus.getErrorCode(), resultStatus.getErrorMsg());
            return null;
        }

        if (pResult.hasEmptyBatch() && pResult.getEmptyBatch()) {
            LOG.debug("get empty rowbatch");
            rowBatch.setEos(true);
            status.updateStatus(TStatusCode.OK, "");
            return rowBatch;
        } else if (pResult.hasRowBatch() && pResult.getRowBatch().size() > 0) {
            byte[] serialResult = pResult.getRowBatch().toByteArray();
            TResultBatch resultBatch = new TResultBatch();
            TDeserializer deserializer = new TDeserializer(
                    new TCustomProtocolFactory(this.maxMsgSizeOfResultReceiver));
            try {
                deserializer.deserialize(resultBatch, serialResult);
            } catch (TException e) {
                if (e.getMessage().contains("MaxMessageSize reached")) {
                    throw new TException("MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                } else {
                    throw e;
                }
            }
            rowBatch.setBatch(resultBatch);
            rowBatch.setEos(true);
            status.updateStatus(TStatusCode.OK, "");
            return rowBatch;
        } else {
            Preconditions.checkState(false, "No row batch or empty batch found");
        }

        if (isCancel) {
            status.updateStatus(TStatusCode.CANCELLED, "cancelled");
        }
        return rowBatch;
    }

    public void cancel() {
        isCancel = true;
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
