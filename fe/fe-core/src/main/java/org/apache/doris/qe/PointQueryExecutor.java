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
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.planner.ColumnBound;
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
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

// import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PointQueryExecutor implements CoordInterface {
    private static final Logger LOG = LogManager.getLogger(PointQueryExecutor.class);
    private List<Long> tabletIDs = new ArrayList<>();
    private long timeoutMs = Config.point_query_timeout_ms; // default 10s

    private boolean isCancel = false;
    private List<Backend> candidateBackends;
    private final int maxMsgSizeOfResultReceiver;

    // used for snapshot read in cloud mode
    private List<Long> snapshotVisibleVersions;

    private final ShortCircuitQueryContext shortCircuitQueryContext;

    HashMap<Backend, InternalService.PTabletBatchKeyLookupRequest> beToReq;

    Map<Long, Set<Long>> backendId2TabletIds;

    Map<PartitionKey, Set<Long>> distributionKeys2TabletID = null;

    Map<String, RangeMap<ColumnBound, List<Long>>> partitionCol2PartitionID = null;

    List<Set<Long>> keyTupleID2TabletID = null;  // key tuple id is the index in array

    List<List<String>> allKeyTuples = null;

    // distribution and also key column, maybe partition column
    private List<Integer> distributionKeyColumns = Lists.newArrayList();

    // partition column and also key column, not distribution column
    private List<Integer> partitionKeyColumns = Lists.newArrayList();

    // neither a partition column nor a distribution column, but a key column
    private List<Integer> plainColumns = Lists.newArrayList();

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

    void setScanRangeLocations() throws Exception {
        OlapScanNode scanNode = shortCircuitQueryContext.scanNode;
        // compute scan range
        List<TScanRangeLocations> locations = scanNode.lazyEvaluateRangeLocations();
        Preconditions.checkNotNull(locations);
        if (scanNode.getScanTabletIds().isEmpty()) {
            return;
        }
        // Preconditions.checkState(scanNode.getScanTabletIds().size() == 1);
        // this.tabletID = scanNode.getScanTabletIds().get(0);
        this.distributionKeys2TabletID = scanNode.getDistributionKeys2TabletID();
        this.partitionCol2PartitionID = scanNode.getPartitionCol2PartitionID();
        this.tabletIDs = scanNode.getScanTabletIds();

        // update partition version if cloud mode
        if (Config.isCloudMode()
                && ConnectContext.get().getSessionVariable().enableSnapshotPointQuery) {
            // TODO: Optimize to reduce the frequency of version checks in the meta service.
            updateCloudPartitionVersions();
        }

        candidateBackends = new ArrayList<>();
        this.backendId2TabletIds = scanNode.getBackendId2TabletIds();
        for (Long backendID : scanNode.getScanBackendIds()) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(backendID);
            if (SimpleScheduler.isAvailable(backend)) {
                candidateBackends.add(backend);
                if (backendId2TabletIds.containsKey(backendID)) {
                    backendId2TabletIds.get(backendID).retainAll(tabletIDs);
                } else {
                    throw new AnalysisException("Backend " + backendID + " not found in backendId2TabletIds");
                }
            }
        }
        // Random read replicas
        Collections.shuffle(this.candidateBackends);
        if (LOG.isDebugEnabled()) {
            LOG.debug("set scan locations, backend ids {}, tablet ids {}", candidateBackends, tabletIDs);
        }
    }

    // execute query without analyze & plan
    public static void directExecuteShortCircuitQuery(StmtExecutor executor,
            PreparedStatementContext preparedStmtCtx,
            StatementContext statementContext) throws Exception {
        Preconditions.checkNotNull(preparedStmtCtx.shortCircuitQueryContext);
        ShortCircuitQueryContext shortCircuitQueryContext = preparedStmtCtx.shortCircuitQueryContext.get();
        // update conjuncts
        List<Expr> conjunctVals = statementContext.getIdToPlaceholderRealExpr().values().stream().map(
                        expression -> (
                                (Literal) expression).toLegacyLiteral())
                .collect(Collectors.toList());
        if (conjunctVals.size() != preparedStmtCtx.command.placeholderCount()) {
            throw new AnalysisException("Mismatched conjuncts values size with prepared"
                    + "statement parameters size, expected "
                    + preparedStmtCtx.command.placeholderCount()
                    + ", but meet " + conjunctVals.size());
        }
        updateScanNodeConjuncts(shortCircuitQueryContext.scanNode, conjunctVals);
        // short circuit plan and execution
        executor.executeAndSendResult(false, false,
                shortCircuitQueryContext.analzyedQuery, executor.getContext()
                        .getMysqlChannel(), null, null);
    }

    private static void updateScanNodeConjuncts(OlapScanNode scanNode, List<Expr> conjunctVals) {
        for (int i = 0; i < conjunctVals.size(); ++i) {
            Expr expr = scanNode.getConjuncts().get(i);
            if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (inPredicate.isNotIn()) {
                    throw new AnalysisException("Not support NOT IN predicate in point query");
                }
                // if (inPredicate.getChild(0) instanceof LiteralExpr) {   // seems not necessary
                //     inPredicate.setChild(0, conjunctVals.get(i));
                // }
                for (int j = 1; j < inPredicate.getChildren().size(); ++j) {
                    if (inPredicate.getChild(j) instanceof LiteralExpr) {
                        inPredicate.setChild(j, conjunctVals.get(i + j - 1));
                    } else {
                        Preconditions.checkState(false, "Should conatains literal in " + inPredicate.toSqlImpl());
                    }
                }
            } else if (expr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                if (binaryPredicate.getChild(0) instanceof LiteralExpr) {
                    binaryPredicate.setChild(0, conjunctVals.get(i));
                } else if (binaryPredicate.getChild(1) instanceof LiteralExpr) {
                    binaryPredicate.setChild(1, conjunctVals.get(i));
                } else {
                    Preconditions.checkState(false, "Should conatains literal in " + binaryPredicate.toSqlImpl());
                }
            } else {
                throw new AnalysisException("Not support conjunct type " + expr.getClass().getName());
            }
        }
    }

    public static byte[] appendBytes(byte[] original, byte[] toAppend) {
        byte[] result = new byte[original.length + toAppend.length];
        System.arraycopy(original, 0, result, 0, original.length);
        System.arraycopy(toAppend, 0, result, original.length, toAppend.length);
        return result;
    }

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    void getAllKeyTupleCombination(List<Expr> conjuncts, int index,
                        List<String> currentKeyTuple,   // current key tuple
                        List<List<String>> result,
                        List<String> columnExpr) {
        if (index == conjuncts.size()) {
            List<String> orderedKeyTuple = new ArrayList<>(currentKeyTuple.size());
            OlapTable olapTable = shortCircuitQueryContext.scanNode.getOlapTable();
            List<Column> keyColumns = olapTable.getBaseSchemaKeyColumns();

            // add key tuple in keys order
            for (Column column : keyColumns) {
                int colIdx = columnExpr.indexOf(column.getName());
                String currentKey = currentKeyTuple.get(colIdx);
                orderedKeyTuple.add(currentKey);
            }
            List<String> keyTupleForDistributionPrune =
                    Lists.newArrayList(distributionKeyColumns.stream().map((idx) -> {
                        return orderedKeyTuple.get(idx);
                    }).collect(Collectors.toList()));

            result.add(Lists.newArrayList(orderedKeyTuple));
            Set<Long> tabletIDs = Sets.newHashSet();
            for (PartitionKey key : distributionKeys2TabletID.keySet()) {
                List<String> distributionKeys =
                        key.getKeys().stream().map(LiteralExpr::getStringValue).collect(Collectors.toList());
                if (distributionKeys.equals(keyTupleForDistributionPrune)) {
                    tabletIDs.addAll(distributionKeys2TabletID.get(key));
                    break;
                }
            }
            // if all key columns are distribution columns, then we don't need to prune
            if (distributionKeyColumns.size() == keyColumns.size()) {
                keyTupleID2TabletID.add(tabletIDs);
                return;
            }
            for (Integer colIdx : partitionKeyColumns) {
                String partitionColName = keyColumns.get(colIdx).getName();
                String keyColValue = orderedKeyTuple.get(colIdx);
                try {
                    ColumnBound bound =
                            ColumnBound.of(LiteralExpr.create(keyColValue, keyColumns.get(colIdx).getType()));
                    List<Long> partitionID = partitionCol2PartitionID.get(partitionColName)
                            .get(bound);
                    Preconditions.checkState(partitionID.size() == 1);
                    Partition partition = olapTable.getPartition(partitionID.get(0));
                    MaterializedIndex selectedTable =
                            partition.getIndex(shortCircuitQueryContext.scanNode.getSelectedIndexId());
                    tabletIDs.retainAll(selectedTable.getTablets()
                                        .stream().map(Tablet::getId)
                                        .collect(Collectors.toList()));
                } catch (Exception e) {
                    throw new AnalysisException(e.getMessage());
                }
            }
            keyTupleID2TabletID.add(tabletIDs);
            return;
        }

        Expr expr = conjuncts.get(index);
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate predicate = (BinaryPredicate) expr;
            Expr left = predicate.getChild(0);
            Expr right = predicate.getChild(1);
            SlotRef columnSlot = left.unwrapSlotRef();
            if (left instanceof SlotRef && ((SlotRef) left).getColumnName().equalsIgnoreCase(Column.DELETE_SIGN)) {
                getAllKeyTupleCombination(conjuncts, index + 1, currentKeyTuple, result, columnExpr);
                return;
            }
            columnExpr.add(columnSlot.getColumnName());
            currentKeyTuple.add(right.getStringValue());
            getAllKeyTupleCombination(conjuncts, index + 1, currentKeyTuple, result, columnExpr);
            currentKeyTuple.remove(currentKeyTuple.size() - 1);
            columnExpr.remove(columnExpr.size() - 1);
        } else if (expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            if (inPredicate.isNotIn()) {
                throw new AnalysisException("Not support NOT IN predicate in point query");
            }
            SlotRef columnSlot = inPredicate.getChild(0).unwrapSlotRef();
            columnExpr.add(columnSlot.getColumnName());
            for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
                currentKeyTuple.add(inPredicate.getChild(i).getStringValue());
                getAllKeyTupleCombination(conjuncts, index + 1, currentKeyTuple, result, columnExpr);
                currentKeyTuple.remove(currentKeyTuple.size() - 1);
            }
            columnExpr.remove(columnExpr.size() - 1);
        } else {
            throw new AnalysisException("Not support conjunct type " + expr.getClass().getName());
        }
    }

    void addKeyTuples(
            InternalService.PTabletKeyLookupRequest.Builder requestBuilder) {
        // TODO handle IN predicates
        KeyTuple.Builder kBuilder = KeyTuple.newBuilder();
        for (Expr expr : shortCircuitQueryContext.scanNode.getConjuncts()) {
            BinaryPredicate predicate = (BinaryPredicate) expr;
            Expr left = predicate.getChild(0);
            Expr right = predicate.getChild(1);
            // ignore delete sign conjuncts only collect key conjuncts
            if (left instanceof SlotRef && ((SlotRef) left).getColumnName().equalsIgnoreCase(Column.DELETE_SIGN)) {
                continue;
            }
            kBuilder.addKeyColumnRep(right.getStringValue());
        }
        requestBuilder.addKeyTuples(kBuilder);
    }

    List<List<String>> addAllKeyTuples() {
        List<Expr> conjuncts = shortCircuitQueryContext.scanNode.getConjuncts();
        List<List<String>> keyTuples = Lists.newArrayList();
        if (keyTupleID2TabletID == null) {
            keyTupleID2TabletID = Lists.newArrayList();
        }
        List<String> columnExpr = Lists.newArrayList();
        getAllKeyTupleCombination(conjuncts, 0, new ArrayList<>(), keyTuples, columnExpr);
        Preconditions.checkState(keyTuples.size() == keyTupleID2TabletID.size());
        return keyTuples;
    }

    @Override
    public void cancel(Status cancelReason) {
        // Do nothing
    }


    @Override
    public RowBatch getNext() throws Exception {
        setScanRangeLocations();
        // No partition/tablet found return emtpy row batch
        if (candidateBackends == null || candidateBackends.isEmpty()) {
            return new RowBatch();
        }
        OlapTable olapTable = shortCircuitQueryContext.scanNode.getOlapTable();
        for (int i = 0; i < olapTable.getBaseSchemaKeyColumns().size(); ++i) {
            Column column = olapTable.getBaseSchemaKeyColumns().get(i);
            if (olapTable.isDistributionColumn(column.getName())) {
                distributionKeyColumns.add(i);
            } else if (olapTable.isPartitionColumn(column.getName())) {
                partitionKeyColumns.add(i);
            } else {
                plainColumns.add(i);
            }
        }
        Iterator<Backend> backendIter = candidateBackends.iterator();
        RowBatch rowBatch = new RowBatch();
        int tryCount = 0;
        int maxTry = Math.min(Config.max_point_query_retry_time, candidateBackends.size());
        Status status = new Status();
        this.allKeyTuples = addAllKeyTuples();
        TResultBatch resultBatch = new TResultBatch();
        resultBatch.setRows(Lists.newArrayList());
        List<byte[]> batchSerialResult = Lists.newArrayList();
        do {
            Backend backend = backendIter.next();
            // rowBatch = getNextInternal(status, backend);;
            List<byte[]> subSerialResult = batchGetNext(status, backend);
            batchSerialResult.addAll(subSerialResult);
            if (!subSerialResult.isEmpty()) {
                continue;
            }
            if (++tryCount >= maxTry) {
                break;
            }
        } while (backendIter.hasNext());

        // todo: maybe there is a better way
        if (!batchSerialResult.isEmpty()) {
            TDeserializer deserializer = new TDeserializer(
                        new TCustomProtocolFactory(this.maxMsgSizeOfResultReceiver));
            for (byte[] serialResult : batchSerialResult) {
                TResultBatch tmpResultBatch = new TResultBatch();
                try {
                    deserializer.deserialize(tmpResultBatch, serialResult);
                    tmpResultBatch.getRows().forEach(row -> {
                        resultBatch.addToRows(row);
                    });
                } catch (TException e) {
                    if (e.getMessage().contains("MaxMessageSize reached")) {
                        throw new TException("MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                    } else {
                        throw e;
                    }
                }
            }
            rowBatch.setBatch(resultBatch);
        }
        rowBatch.setEos(true);
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

    @Override
    public void exec() throws Exception {
        // Point queries don't need to do anthing in execution phase.
        // only handles in getNext()
    }

    // private RowBatch getNextInternal(Status status, Backend backend) throws TException {
    //     long timeoutTs = System.currentTimeMillis() + timeoutMs;
    //     RowBatch rowBatch = new RowBatch();
    //     InternalService.PTabletKeyLookupResponse pResult = null;
    //     try {
    //         Preconditions.checkNotNull(shortCircuitQueryContext.serializedDescTable);

    //         InternalService.PTabletKeyLookupRequest.Builder requestBuilder
    //                 = InternalService.PTabletKeyLookupRequest.newBuilder()
    //                 .setTabletId(tabletIDs.get(0))
    //                 .setDescTbl(shortCircuitQueryContext.serializedDescTable)
    //                 .setOutputExpr(shortCircuitQueryContext.serializedOutputExpr)
    //                 .setQueryOptions(shortCircuitQueryContext.serializedQueryOptions)
    //                 .setIsBinaryRow(ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE);
    //         if (snapshotVisibleVersions != null && !snapshotVisibleVersions.isEmpty()) {
    //             requestBuilder.setVersion(snapshotVisibleVersions.get(0));
    //         }
    //         if (shortCircuitQueryContext.cacheID != null) {
    //             InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
    //             uuidBuilder.setUuidHigh(shortCircuitQueryContext.cacheID.getMostSignificantBits());
    //             uuidBuilder.setUuidLow(shortCircuitQueryContext.cacheID.getLeastSignificantBits());
    //             requestBuilder.setUuid(uuidBuilder);
    //         }
    //         addKeyTuples(requestBuilder);

    //         InternalService.PTabletKeyLookupRequest request = requestBuilder.build();
    //         Future<InternalService.PTabletKeyLookupResponse> futureResponse =
    //                 BackendServiceProxy.getInstance().fetchTabletDataAsync(backend.getBrpcAddress(), request);
    //         long currentTs = System.currentTimeMillis();
    //         if (currentTs >= timeoutTs) {
    //             LOG.warn("fetch result timeout {}", backend.getBrpcAddress());
    //             status.updateStatus(TStatusCode.INTERNAL_ERROR, "query request timeout");
    //             return null;
    //         }
    //         try {
    //             pResult = futureResponse.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
    //         } catch (InterruptedException e) {
    //             // continue to get result
    //             LOG.warn("future get interrupted Exception");
    //             if (isCancel) {
    //                 status.updateStatus(TStatusCode.CANCELLED, "cancelled");
    //                 return null;
    //             }
    //         } catch (TimeoutException e) {
    //             futureResponse.cancel(true);
    //             LOG.warn("fetch result timeout {}, addr {}", timeoutTs - currentTs, backend.getBrpcAddress());
    //             status.updateStatus(TStatusCode.INTERNAL_ERROR, "query fetch result timeout");
    //             return null;
    //         }
    //     } catch (RpcException e) {
    //         LOG.warn("query fetch rpc exception {}, e {}", backend.getBrpcAddress(), e);
    //         status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
    //         SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
    //         return null;
    //     } catch (ExecutionException e) {
    //         LOG.warn("query fetch execution exception {}, addr {}", e, backend.getBrpcAddress());
    //         if (e.getMessage().contains("time out")) {
    //             // if timeout, we set error code to TIMEOUT, and it will not retry querying.
    //             status.updateStatus(TStatusCode.TIMEOUT, e.getMessage());
    //         } else {
    //             status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
    //             SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
    //         }
    //         return null;
    //     }
    //     Status resultStatus = new Status(pResult.getStatus());
    //     if (resultStatus.getErrorCode() != TStatusCode.OK) {
    //         status.updateStatus(resultStatus.getErrorCode(), resultStatus.getErrorMsg());
    //         return null;
    //     }

    //     if (pResult.hasEmptyBatch() && pResult.getEmptyBatch()) {
    //         LOG.debug("get empty rowbatch");
    //         rowBatch.setEos(true);
    //         status.updateStatus(TStatusCode.OK, "");
    //         return rowBatch;
    //     } else if (pResult.hasRowBatch() && pResult.getRowBatch().size() > 0) {
    //         byte[] serialResult = pResult.getRowBatch().toByteArray();
    //         TResultBatch resultBatch = new TResultBatch();
    //         TDeserializer deserializer = new TDeserializer(
    //                 new TCustomProtocolFactory(this.maxMsgSizeOfResultReceiver));
    //         try {
    //             deserializer.deserialize(resultBatch, serialResult);
    //         } catch (TException e) {
    //             if (e.getMessage().contains("MaxMessageSize reached")) {
    //                 throw new TException("MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
    //             } else {
    //                 throw e;
    //             }
    //         }
    //         rowBatch.setBatch(resultBatch);
    //         rowBatch.setEos(true);
    //         status.updateStatus(TStatusCode.OK, "");
    //         return rowBatch;
    //     } else {
    //         Preconditions.checkState(false, "No row batch or empty batch found");
    //     }

    //     if (isCancel) {
    //         status.updateStatus(TStatusCode.CANCELLED, "cancelled");
    //     }
    //     return rowBatch;
    // }

    private List<byte[]> batchGetNext(Status status, Backend backend) throws TException {
        // RowBatch rowBatch = new RowBatch();
        TResultBatch resultBatch = new TResultBatch();
        List<byte[]> result = Lists.newArrayList();
        resultBatch.setRows(Lists.newArrayList());
        InternalService.PTabletBatchKeyLookupRequest.Builder pBatchRequestBuilder
                = InternalService.PTabletBatchKeyLookupRequest.newBuilder();
        Set<Long> tabletIdsOfBe = backendId2TabletIds.get(backend.getId());
        Preconditions.checkNotNull(shortCircuitQueryContext.serializedDescTable);
        // assemble sub request
        for (int i = 0; i < keyTupleID2TabletID.size(); ++i) {
            KeyTuple.Builder kBuilder = KeyTuple.newBuilder();
            for (String key : this.allKeyTuples.get(i)) {
                kBuilder.addKeyColumnRep(key);
            }
            Set<Long> prunedTabletIdsOfBe = Sets.newHashSet(tabletIdsOfBe);
            prunedTabletIdsOfBe.retainAll(keyTupleID2TabletID.get(i));
            if (!prunedTabletIdsOfBe.isEmpty()) {
                for (Long tabletId : prunedTabletIdsOfBe) {
                    InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                            = InternalService.PTabletKeyLookupRequest.newBuilder()
                            .setDescTbl(shortCircuitQueryContext.serializedDescTable)
                            .setOutputExpr(shortCircuitQueryContext.serializedOutputExpr)
                            .setQueryOptions(shortCircuitQueryContext.serializedQueryOptions)
                            .setIsBinaryRow(ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE);

                    if (snapshotVisibleVersions != null && !snapshotVisibleVersions.isEmpty()) {
                        requestBuilder.setVersion(snapshotVisibleVersions.get(0));
                    }
                    if (shortCircuitQueryContext.cacheID != null) {
                        InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
                        uuidBuilder.setUuidHigh(shortCircuitQueryContext.cacheID.getMostSignificantBits());
                        uuidBuilder.setUuidLow(shortCircuitQueryContext.cacheID.getLeastSignificantBits());
                        requestBuilder.setUuid(uuidBuilder);
                    }
                    requestBuilder.addKeyTuples(kBuilder);
                    requestBuilder.setTabletId(tabletId);
                    pBatchRequestBuilder.addSubKeyLookupReq(requestBuilder);
                }
            }
        }
        if (pBatchRequestBuilder.getSubKeyLookupReqCount() == 0) {
            status.updateStatus(TStatusCode.OK, "");
            // rowBatch.setEos(true);  // todo
            return result;
        }
        // batch fetch data
        InternalService.PTabletBatchKeyLookupRequest pBatchRequest = pBatchRequestBuilder.build();
        long timeoutTs = System.currentTimeMillis() + timeoutMs;
        InternalService.PTabletBatchKeyLookupResponse pBatchResult = null;
        try {
            Future<InternalService.PTabletBatchKeyLookupResponse> futureBatchResponse =
                    BackendServiceProxy.getInstance()
                    .batchFetchTabletDataAsync(backend.getBrpcAddress(), pBatchRequest);
            long currentTs = System.currentTimeMillis();
            if (currentTs >= timeoutTs) {
                LOG.warn("batch fetch result timeout {}", backend.getBrpcAddress());
                status.updateStatus(TStatusCode.INTERNAL_ERROR, "query request timeout");
                return null;
            }
            try {
                // todo: 统一get
                pBatchResult = futureBatchResponse.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // continue to get result
                LOG.warn("future get interrupted Exception");
                if (isCancel) {
                    status.updateStatus(TStatusCode.CANCELLED, "cancelled");
                    return null;
                }
            } catch (TimeoutException e) {
                futureBatchResponse.cancel(true);
                LOG.warn("fetch result timeout {}, addr {}", timeoutTs - currentTs, backend.getBrpcAddress());
                status.updateStatus(TStatusCode.INTERNAL_ERROR, "query fetch result timeout");
                return null;
            }
        } catch (RpcException e) {
            LOG.warn("query batch fetch rpc exception {}, e {}", backend.getBrpcAddress(), e);
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            return null;
        } catch (ExecutionException e) {
            LOG.warn("query batch fetch execution exception {}, addr {}", e, backend.getBrpcAddress());
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.updateStatus(TStatusCode.TIMEOUT, e.getMessage());
            } else {
                status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
                SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            }
            return null;
        }

        // handle the response
        // int resultCount = 0;
        boolean isOK = true;
        // List<TResultBatch> resultBatch = new ArrayList<>();
        // TResultBatch resultBatch = new TResultBatch();
        // resultBatch.setRows(Lists.newArrayList());
        for (InternalService.PTabletKeyLookupResponse subResponse : pBatchResult.getSubKeyLookupResList()) {
            Status resultStatus = new Status(subResponse.getStatus());
            if (resultStatus.getErrorCode() != TStatusCode.OK) {
                status.updateStatus(resultStatus.getErrorCode(), resultStatus.getErrorMsg());
                return null;
            }
            // byte[] serialResult = new byte[0];
            if (subResponse.hasEmptyBatch() && subResponse.getEmptyBatch()) {
                LOG.debug("get empty rowbatch");
                // status.updateStatus(TStatusCode.OK, "");
                // ++resultCount;
                continue;
            } else if (subResponse.hasRowBatch() && subResponse.getRowBatch().size() > 0) {
                byte[] serialResult = subResponse.getRowBatch().toByteArray();
                result.add(serialResult);
                // resultBatch.addToRows(ByteBuffer.wrap(serialResult));
                // resultBatch.addToRows(ByteBuffer.wrap(serialResult));
                // TDeserializer deserializer = new TDeserializer(
                //         new TCustomProtocolFactory(this.maxMsgSizeOfResultReceiver));
                // try {
                //     deserializer.deserialize(tmpResultBatch, serialResult);
                //     resultBatch.addToRows(ByteBuffer.wrap(serialResult));
                // } catch (TException e) {
                //     if (e.getMessage().contains("MaxMessageSize reached")) {
                //         throw new TException("MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                //     } else {
                //         throw e;
                //     }
                // }
                // ++resultCount;
                // resultBatch.add(subResultBatch);
                // status.updateStatus(TStatusCode.OK, "");
                continue;
            } else {
                Preconditions.checkState(false, "No row batch or empty batch found");
            }

            if (isCancel) {
                status.updateStatus(TStatusCode.CANCELLED, "cancelled");
                isOK = false;
                break;
            }
        }
        if (isOK) {
            // ++resultCount;
            status.updateStatus(TStatusCode.OK, "");
        }

        // if (resultCount != tabletIdsOfBe.size()) {
        //     LOG.info("result count {} not equal to tablet count {}", resultCount, tabletIdsOfBe.size());
        //     throw new TException("result count not equal to tablet count");
        // }
        // merge the resultBatch
        // rowBatch.setBatch(resultBatch);
        // rowBatch.setEos(true);
        return result;
    }

    public void cancel() {
        isCancel = true;
    }

    @Override
    public List<TNetworkAddress> getInvolvedBackends() {
        return Lists.newArrayList();
    }
}
