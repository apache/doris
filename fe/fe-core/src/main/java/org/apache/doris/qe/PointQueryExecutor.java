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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
        // Random read replicas
        Collections.shuffle(this.candidateBackends);
        if (LOG.isDebugEnabled()) {
            LOG.debug("set scan locations, backend ids {}, tablet id {}", candidateBackends, tabletID);
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
            BinaryPredicate binaryPredicate = (BinaryPredicate) scanNode.getConjuncts().get(i);
            if (binaryPredicate.getChild(0) instanceof LiteralExpr) {
                binaryPredicate.setChild(0, conjunctVals.get(i));
            } else if (binaryPredicate.getChild(1) instanceof LiteralExpr) {
                binaryPredicate.setChild(1, conjunctVals.get(i));
            } else {
                Preconditions.checkState(false, "Should conatains literal in " + binaryPredicate.toSqlImpl());
            }
        }
    }

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
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
            if (snapshotVisibleVersions != null && !snapshotVisibleVersions.isEmpty()) {
                requestBuilder.setVersion(snapshotVisibleVersions.get(0));
            }
            if (shortCircuitQueryContext.cacheID != null) {
                InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
                uuidBuilder.setUuidHigh(shortCircuitQueryContext.cacheID.getMostSignificantBits());
                uuidBuilder.setUuidLow(shortCircuitQueryContext.cacheID.getLeastSignificantBits());
                requestBuilder.setUuid(uuidBuilder);
            }
            addKeyTuples(requestBuilder);

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
}
