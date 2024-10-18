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
import org.apache.doris.catalog.Replica;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
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
import com.google.common.collect.Maps;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PointQueryExecutor implements CoordInterface {
    private static final Logger LOG = LogManager.getLogger(PointQueryExecutor.class);
    private static final ConcurrentHashMap<Class<? extends Expr>, ConjunctHandler> handlers = new ConcurrentHashMap<>();

    private class RoundRobinScheduler<T> {
        private final List<T> list;
        private AtomicInteger currentIndex;

        public RoundRobinScheduler(List<T> list) {
            this.list = list;
            this.currentIndex = new AtomicInteger(0);
        }

        public T next() {
            if (list.isEmpty()) {
                return null;
            }
            int index = currentIndex.getAndUpdate(i -> (i + 1) % list.size());
            return list.get(index);
        }
    }

    private List<Long> scanTabletIDs = new ArrayList<>();
    private long timeoutMs = Config.point_query_timeout_ms; // default 10s

    private boolean isCancel = false;
    private List<Backend> candidateBackends;

    // key: tablet id, value: backend and replica id
    private HashMap<Long, Pair<Backend, Long>> pickedCandidateReplicas = Maps.newHashMap();

    RoundRobinScheduler<Backend> roundRobinscheduler = null;

    // tablet id -> backend id -> replica
    Table<Long, Long, Replica> replicaMetaTable = null;

    private final int maxMsgSizeOfResultReceiver;

    // used for snapshot read in cloud mode
    // Key: cloud partition id, Value: snapshot visible version
    private HashMap<CloudPartition, Long> snapshotVisibleVersions;

    private final ShortCircuitQueryContext shortCircuitQueryContext;

    Map<PartitionKey, Set<Long>> distributionKeys2TabletID = null;

    // Maps partition column names to a RangeMap that associates ColumnBound ranges with lists of partition IDs,
    // similar to the implementation in PartitionPrunerV2Base.
    Map<String, RangeMap<ColumnBound, List<Long>>> partitionCol2PartitionID = null;

    List<Set<Long>> keyTupleIndex2TabletID = null;

    List<List<String>> allKeyTuples = null;

    private List<Integer> distributionKeyColumns = Lists.newArrayList();

    private List<Integer> partitionKeyColumns = Lists.newArrayList();

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
        snapshotVisibleVersions = (snapshotVisibleVersions == null) ? Maps.newHashMap() : snapshotVisibleVersions;
        List<Long> versionList = CloudPartition.getSnapshotVisibleVersion(partitions);
        for (int i = 0; i < versionList.size(); ++i) {
            snapshotVisibleVersions.put(partitions.get(i), versionList.get(i));
        }

        LOG.debug("set cloud version {}", snapshotVisibleVersions);
    }

    void setScanRangeLocations() throws Exception {
        OlapScanNode scanNode = shortCircuitQueryContext.scanNode;
        // compute scan range
        List<TScanRangeLocations> locations = scanNode.lazyEvaluateRangeLocations();
        Preconditions.checkNotNull(locations);
        if (scanNode.getScanTabletIds().isEmpty()) {
            return;
        }
        this.distributionKeys2TabletID = scanNode.getDistributionKeys2TabletID();
        this.partitionCol2PartitionID = scanNode.getPartitionCol2PartitionID();
        this.scanTabletIDs = scanNode.getScanTabletIds();

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

        if (LOG.isDebugEnabled()) {
            LOG.debug("set scan locations, backend ids {}, tablet ids {}", candidateBackends, scanTabletIDs);
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

    /*
     * Interface for handling different conjunct types
     */
    private interface ConjunctHandler {
        int handle(Expr expr, List<Expr> conjunctVals, int handledConjunctVals) throws AnalysisException;
    }

    private static class InPredicateHandler implements ConjunctHandler {
        public static final InPredicateHandler INSTANCE = new InPredicateHandler();

        private InPredicateHandler() {
        }

        @Override
        public int handle(Expr expr, List<Expr> conjunctVals, int handledConjunctVals) throws AnalysisException {
            InPredicate inPredicate = (InPredicate) expr;
            if (inPredicate.isNotIn()) {
                throw new AnalysisException("Not support NOT IN predicate in point query");
            }
            for (int j = 1; j < inPredicate.getChildren().size(); ++j) {
                if (inPredicate.getChild(j) instanceof LiteralExpr) {
                    inPredicate.setChild(j, conjunctVals.get(handledConjunctVals++));
                } else {
                    Preconditions.checkState(false, "Should contains literal in " + inPredicate.toSqlImpl());
                }
            }
            return handledConjunctVals;
        }
    }

    private static class BinaryPredicateHandler implements ConjunctHandler {
        public static final BinaryPredicateHandler INSTANCE = new BinaryPredicateHandler();

        private BinaryPredicateHandler() {
        }

        @Override
        public int handle(Expr expr, List<Expr> conjunctVals, int handledConjunctVals) throws AnalysisException {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            Expr left = binaryPredicate.getChild(0);
            Expr right = binaryPredicate.getChild(1);

            if (isDeleteSign(left) || isDeleteSign(right)) {
                return handledConjunctVals;
            }

            if (isLiteralExpr(left)) {
                binaryPredicate.setChild(0, conjunctVals.get(handledConjunctVals++));
            } else if (isLiteralExpr(right)) {
                binaryPredicate.setChild(1, conjunctVals.get(handledConjunctVals++));
            } else {
                Preconditions.checkState(false, "Should contains literal in " + binaryPredicate.toSqlImpl());
            }
            return handledConjunctVals;
        }

        private boolean isLiteralExpr(Expr expr) {
            return expr instanceof LiteralExpr;
        }

        private boolean isDeleteSign(Expr expr) {
            return expr instanceof SlotRef && ((SlotRef) expr).getColumnName().equalsIgnoreCase(Column.DELETE_SIGN);
        }
    }

    private static void initHandler() {
        handlers.put(InPredicate.class, InPredicateHandler.INSTANCE);
        handlers.put(BinaryPredicate.class, BinaryPredicateHandler.INSTANCE);
    }

    private static void updateScanNodeConjuncts(OlapScanNode scanNode, List<Expr> conjunctVals) {
        List<Expr> conjuncts = scanNode.getConjuncts();
        if (handlers.isEmpty()) {
            initHandler();
        }
        int handledConjunctVals = 0;
        for (Expr expr : conjuncts) {
            ConjunctHandler handler = handlers.get(expr.getClass());
            if (handler == null) {
                throw new AnalysisException("Not support conjunct type " + expr.getClass().getName());
            }
            handledConjunctVals = handler.handle(expr, conjunctVals, handledConjunctVals);
        }

        Preconditions.checkState(handledConjunctVals == conjunctVals.size());
    }

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    /*
     * According to the current ordered key tuple, based on the leftmost partition principle,
     * get the partition ID to which it belongs
     */
    private Set<Long> getLeftMostPartitionIDs(List<String> orderedKeyTuple,
                                            List<Column> keyColumns) {
        Set<Long> leftMostPartitionIDs = Sets.newHashSet();
        for (int i = 0; i < partitionKeyColumns.size(); ++i) {
            int colIdx = partitionKeyColumns.get(i);
            String partitionColName = keyColumns.get(colIdx).getName();
            try {
                ColumnBound partitionKey = ColumnBound.of(LiteralExpr.create(orderedKeyTuple.get(colIdx),
                        keyColumns.get(colIdx).getType()));
                List<Long> partitionIDs = Lists.newArrayList(
                        Optional.ofNullable(
                                partitionCol2PartitionID.get(partitionColName).get(partitionKey))
                                .orElse(Collections.emptyList()));
                // Add the first partition column directly
                if (i == 0) {
                    leftMostPartitionIDs.addAll(partitionIDs);
                    continue;
                }
                if (leftMostPartitionIDs.isEmpty() || partitionIDs == null) {
                    break;
                }
                partitionIDs.retainAll(leftMostPartitionIDs);
                if (partitionIDs.isEmpty()) {
                    break;
                }
            } catch (Exception e) {
                throw new AnalysisException("Failed to create partition key for key tuple: " + orderedKeyTuple);
            }
        }
        return leftMostPartitionIDs;
    }

    private void pickCandidateBackends() {
        for (Long tabletID : scanTabletIDs) {
            roundRobinPickReplica(tabletID);
        }
    }

    private void roundRobinPickReplica(Long tabletID) {
        while (true) {
            Backend backend = roundRobinscheduler.next();
            if (backend == null) {
                break;
            }
            Map<Long, Replica> beWithReplica = replicaMetaTable.row(tabletID);
            if (!beWithReplica.containsKey(backend.getId())) {
                continue;
            }
            pickedCandidateReplicas
                    .putIfAbsent(tabletID,
                                 Pair.of(backend, beWithReplica.get(backend.getId()).getId()));
            break;
        }
    }

    // Use the leftmost matching partitions to filter out tablets that do not belong to these partitions
    private void addTabletIDsForKeyTuple(List<String> orderedKeyTuple, List<Column> keyColumns,
                                         OlapTable olapTable, Set<Long> leftMostPartitionIDs) {
        // get part of the key tuple using distribution columns
        List<String> keyTupleForDistributionPrune = Lists.newArrayList();
        for (Integer idx : distributionKeyColumns) {
            keyTupleForDistributionPrune.add(orderedKeyTuple.get(idx));
        }
        Set<Long> tabletIDs = Sets.newHashSet();
        for (PartitionKey key : distributionKeys2TabletID.keySet()) {
            List<String> distributionKeys = Lists.newArrayList();
            for (LiteralExpr expr : key.getKeys()) {
                distributionKeys.add(expr.getStringValue());
            }
            if (distributionKeys.equals(keyTupleForDistributionPrune)) {
                Set<Long> originTabletIDs = Sets.newHashSet(distributionKeys2TabletID.get(key));
                // If partitions are not explicitly created, this condition holds true
                if (leftMostPartitionIDs.isEmpty()) {
                    tabletIDs.addAll(originTabletIDs);
                } else {
                    Set<Long> prunedTabletIDs = Sets.newHashSet();
                    for (Long partitionID : leftMostPartitionIDs) {
                        Partition partition = olapTable.getPartition(partitionID);
                        MaterializedIndex selectedTable =
                                partition.getIndex(shortCircuitQueryContext.scanNode.getSelectedIndexId());
                        // filter out tablets that do not belong to this partition
                        selectedTable.getTablets().forEach(tablet -> {
                            if (originTabletIDs.contains(tablet.getId())) {
                                prunedTabletIDs.add(tablet.getId());
                            }
                        });
                        tabletIDs.addAll(prunedTabletIDs);
                    }
                }
                break;
            }
        }
        keyTupleIndex2TabletID.add(tabletIDs.isEmpty() ? null : tabletIDs);
    }

    // Get all possible key tuple combinations
    void getAllKeyTupleCombination(List<Expr> conjuncts, int index,
                        List<String> currentKeyTuple,
                        List<List<String>> result,
                        List<String> columnExpr,
                        List<Column> keyColumns) {
        if (index == conjuncts.size()) {
            List<String> orderedKeyTuple = new ArrayList<>(currentKeyTuple.size());
            OlapTable olapTable = shortCircuitQueryContext.scanNode.getOlapTable();

            // add key tuple in keys order
            for (Column column : keyColumns) {
                int colIdx = columnExpr.indexOf(column.getName());
                String currentKey = currentKeyTuple.get(colIdx);
                orderedKeyTuple.add(currentKey);
            }
            result.add(Lists.newArrayList(orderedKeyTuple));
            Set<Long> leftMostPartitionIDs = getLeftMostPartitionIDs(orderedKeyTuple, keyColumns);
            addTabletIDsForKeyTuple(orderedKeyTuple, keyColumns, olapTable, leftMostPartitionIDs);
            return;
        }

        Expr expr = conjuncts.get(index);
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate predicate = (BinaryPredicate) expr;
            Expr left = predicate.getChild(0);
            Expr right = predicate.getChild(1);
            SlotRef columnSlot = left.unwrapSlotRef();
            if (left instanceof SlotRef && ((SlotRef) left).getColumnName().equalsIgnoreCase(Column.DELETE_SIGN)) {
                getAllKeyTupleCombination(conjuncts, index + 1, currentKeyTuple, result, columnExpr, keyColumns);
                return;
            }
            columnExpr.add(columnSlot.getColumnName());
            currentKeyTuple.add(right.getStringValue());
            getAllKeyTupleCombination(conjuncts, index + 1, currentKeyTuple, result, columnExpr, keyColumns);
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
                getAllKeyTupleCombination(conjuncts, index + 1, currentKeyTuple, result, columnExpr, keyColumns);
                currentKeyTuple.remove(currentKeyTuple.size() - 1);
            }
            columnExpr.remove(columnExpr.size() - 1);
        } else {
            throw new AnalysisException("Not support conjunct type " + expr.getClass().getName());
        }
    }

    List<List<String>> addAllKeyTuples(List<Column> keyColumns) {
        List<Expr> conjuncts = shortCircuitQueryContext.scanNode.getConjuncts();
        List<List<String>> keyTuples = Lists.newArrayList();
        if (keyTupleIndex2TabletID == null) {
            keyTupleIndex2TabletID = Lists.newArrayList();
        }
        getAllKeyTupleCombination(conjuncts, 0, new ArrayList<>(), keyTuples, Lists.newArrayList(), keyColumns);
        Preconditions.checkState(keyTuples.size() == keyTupleIndex2TabletID.size());
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

        // pick candidate backends
        OlapScanNode scanNode = shortCircuitQueryContext.scanNode;
        replicaMetaTable = scanNode.getScanBackendReplicaTable();
        roundRobinscheduler = new RoundRobinScheduler<>(candidateBackends);
        pickCandidateBackends();

        OlapTable olapTable = scanNode.getOlapTable();
        List<Column> keyColumns = olapTable.getBaseSchemaKeyColumns();
        for (int i = 0; i < keyColumns.size(); ++i) {
            Column column = keyColumns.get(i);
            if (olapTable.isPartitionColumn(column.getName())) {
                partitionKeyColumns.add(i);
            }
            if (olapTable.isDistributionColumn(column.getName())) {
                distributionKeyColumns.add(i);
            }
        }
        RowBatch rowBatch = new RowBatch();
        Status status = new Status();
        this.allKeyTuples = addAllKeyTuples(keyColumns);
        TResultBatch resultBatch = new TResultBatch();
        resultBatch.setRows(Lists.newArrayList());
        List<byte[]> batchSerialResult = Lists.newArrayList();
        Map<Backend, InternalService.PTabletBatchKeyLookupRequest.Builder> batchRequestBuilders =
                buildBatchRequest(status);
        // send batch request
        if (batchRequestBuilders.isEmpty()) {
            status.updateStatus(TStatusCode.OK, "");
            rowBatch.setEos(true);
            return rowBatch;
        }
        for (Map.Entry<Backend, InternalService.PTabletBatchKeyLookupRequest.Builder> entry :
                    batchRequestBuilders.entrySet()) {
            List<byte[]> subSerialResult = batchGetNext(status, entry.getKey(), entry.getValue());

            if (!status.ok()) {
                break;
            }
            batchSerialResult.addAll(subSerialResult);
        }

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

    private void collectBatchRequests(
            Map<Backend, InternalService.PTabletBatchKeyLookupRequest.Builder> batchRequests,
            KeyTuple.Builder kBuilder, Set<Long> tabletIDsOfKeyTuple) {
        // check containsKey
        for (Long tabletID : tabletIDsOfKeyTuple) {
            Preconditions.checkState(pickedCandidateReplicas.containsKey(tabletID));
            Pair<Backend, Long> beWithReplicaID = pickedCandidateReplicas.get(tabletID);
            Backend candidate = beWithReplicaID.first;
            batchRequests.putIfAbsent(
                    candidate,
                    InternalService.PTabletBatchKeyLookupRequest.newBuilder());
            buildSubRequest(tabletID, kBuilder, beWithReplicaID.second,
                            batchRequests.get(candidate));
        }
    }

    // Find the tabletID, backend, and replica corresponding to each keyTuple,
    // and then add them to batchRequests. Each backend corresponds to a batchRequest.
    private Map<Backend, InternalService.PTabletBatchKeyLookupRequest.Builder>
            buildBatchRequest(Status status) throws TException {
        Map<Backend, InternalService.PTabletBatchKeyLookupRequest.Builder> batchRequestBuilders =
                Maps.newHashMap();
        for (int i = 0; i < keyTupleIndex2TabletID.size(); ++i) {
            if (keyTupleIndex2TabletID.get(i) == null) {
                continue;
            }
            KeyTuple.Builder kBuilder = KeyTuple.newBuilder();
            for (String key : this.allKeyTuples.get(i)) {
                kBuilder.addKeyColumnRep(key);
            }
            collectBatchRequests(batchRequestBuilders, kBuilder, keyTupleIndex2TabletID.get(i));
        }
        return batchRequestBuilders;
    }

    // Build a request about a keyTuple, that is, SubRequest
    private void buildSubRequest(
            Long prunedTabletIdsOfBe, KeyTuple.Builder kBuilder, Long replicaID,
            InternalService.PTabletBatchKeyLookupRequest.Builder pBatchRequestBuilder) {
        InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                = InternalService.PTabletKeyLookupRequest.newBuilder()
                .setDescTbl(shortCircuitQueryContext.serializedDescTable)
                .setOutputExpr(shortCircuitQueryContext.serializedOutputExpr)
                .setQueryOptions(shortCircuitQueryContext.serializedQueryOptions)
                .setIsBinaryRow(ConnectContext.get().command == MysqlCommand.COM_STMT_EXECUTE);

        // TODO: optimize me
        if (snapshotVisibleVersions != null && !snapshotVisibleVersions.isEmpty()) {
            Long versionToSet = -1L;
            for (Map.Entry<CloudPartition, Long> entry : snapshotVisibleVersions.entrySet()) {
                MaterializedIndex selectedTable =
                        entry.getKey().getIndex(shortCircuitQueryContext.scanNode.getSelectedIndexId());
                if (selectedTable.getTabletIdsInOrder().contains(prunedTabletIdsOfBe)) {
                    versionToSet = entry.getValue();
                    break;
                }
            }
            requestBuilder.setVersion(versionToSet);
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
        requestBuilder.addKeyTuples(kBuilder);
        requestBuilder.setTabletId(prunedTabletIdsOfBe);
        requestBuilder.setReplicaId(replicaID);
        pBatchRequestBuilder.addSubKeyLookupReq(requestBuilder);
    }

    private List<byte[]> batchGetNext(
            Status status, Backend backend,
            InternalService.PTabletBatchKeyLookupRequest.Builder pBatchRequestBuilder) throws TException {
        TResultBatch resultBatch = new TResultBatch();
        List<byte[]> result = Lists.newArrayList();
        resultBatch.setRows(Lists.newArrayList());

        Preconditions.checkState(pBatchRequestBuilder.getSubKeyLookupReqCount() > 0);

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
                // todo: get the result asynchrously
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
        boolean isOK = true;
        for (InternalService.PTabletKeyLookupResponse subResponse : pBatchResult.getSubKeyLookupResList()) {
            Status resultStatus = new Status(subResponse.getStatus());
            if (resultStatus.getErrorCode() != TStatusCode.OK) {
                status.updateStatus(resultStatus.getErrorCode(), resultStatus.getErrorMsg());
                return null;
            }
            if (subResponse.hasEmptyBatch() && subResponse.getEmptyBatch()) {
                LOG.debug("get empty rowbatch");
                continue;
            } else if (subResponse.hasRowBatch() && subResponse.getRowBatch().size() > 0) {
                byte[] serialResult = subResponse.getRowBatch().toByteArray();
                result.add(serialResult);
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
            status.updateStatus(TStatusCode.OK, "");
        }

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
