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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PrepareStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.KeyTuple;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.rpc.TCustomProtocolFactory;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PointQueryExec implements CoordInterface {
    private static final Logger LOG = LogManager.getLogger(PointQueryExec.class);
    // SlotRef sorted by column id
    private Map<SlotRef, Expr> equalPredicats;
    // ByteString serialized for prepared statement
    private ByteString serializedDescTable;
    private ByteString serializedOutputExpr;
    private ByteString serializedQueryOptions;
    private ArrayList<Expr> outputExprs;
    private DescriptorTable descriptorTable;
    private TQueryOptions queryOptions;
    private long tabletID = 0;
    private long timeoutMs = Config.point_query_timeout_ms; // default 10s

    private boolean isCancel = false;
    private boolean isBinaryProtocol = false;

    private List<Backend> candidateBackends;
    Planner planner;

    // For parepared statement cached structure,
    // there are some pre caculated structure in Backend TabletFetch service
    // using this ID to find for this prepared statement
    private UUID cacheID;

    private final int maxMsgSizeOfResultReceiver;

    // used for snapshot read in cloud mode
    private List<Long> versions;

    private OlapScanNode getPlanRoot() {
        List<PlanFragment> fragments = planner.getFragments();
        PlanFragment fragment = fragments.get(0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("execPointGet fragment {}", fragment);
        }
        OlapScanNode planRoot = (OlapScanNode) fragment.getPlanRoot();
        Preconditions.checkNotNull(planRoot);
        return planRoot;
    }

    public PointQueryExec(Planner planner, Analyzer analyzer, int maxMessageSize) {
        // init from planner
        this.planner = planner;
        List<PlanFragment> fragments = planner.getFragments();
        PlanFragment fragment = fragments.get(0);
        OlapScanNode planRoot = getPlanRoot();
        this.equalPredicats = planRoot.getPointQueryEqualPredicates();
        this.descriptorTable = planRoot.getDescTable();
        this.outputExprs = fragment.getOutputExprs();
        this.queryOptions = planner.getQueryOptions();

        PrepareStmt prepareStmt = analyzer == null ? null : analyzer.getPrepareStmt();
        if (prepareStmt != null && prepareStmt.getPreparedType() == PrepareStmt.PreparedType.FULL_PREPARED) {
            // Used cached or better performance
            this.cacheID = prepareStmt.getID();
            this.serializedDescTable = prepareStmt.getSerializedDescTable();
            this.serializedOutputExpr = prepareStmt.getSerializedOutputExprs();
            this.isBinaryProtocol = true;
            this.serializedQueryOptions = prepareStmt.getSerializedQueryOptions();
        } else {
            // TODO
            // planner.getDescTable().toThrift();
        }
        this.maxMsgSizeOfResultReceiver = maxMessageSize;
    }

    private void updateCloudPartitionVersions() throws RpcException {
        OlapScanNode planRoot = getPlanRoot();
        List<CloudPartition> partitions = new ArrayList<>();
        Set<Long> partitionSet = new HashSet<>();
        OlapTable table = planRoot.getOlapTable();
        for (Long id : planRoot.getSelectedPartitionIds()) {
            if (!partitionSet.contains(id)) {
                partitionSet.add(id);
                partitions.add((CloudPartition) table.getPartition(id));
            }
        }
        versions = CloudPartition.getSnapshotVisibleVersion(partitions);
        // Only support single partition at present
        Preconditions.checkState(versions.size() == 1);
        LOG.debug("set cloud version {}", versions.get(0));
    }

    void setScanRangeLocations() throws Exception {
        OlapScanNode planRoot = getPlanRoot();
        // compute scan range
        List<TScanRangeLocations> locations = planRoot.lazyEvaluateRangeLocations();
        if (planRoot.getScanTabletIds().isEmpty()) {
            return;
        }
        Preconditions.checkState(planRoot.getScanTabletIds().size() == 1);
        this.tabletID = planRoot.getScanTabletIds().get(0);

        // update partition version if cloud mode
        if (Config.isCloudMode()
                && ConnectContext.get().getSessionVariable().enableSnapshotPointQuery) {
            // TODO: Optimize to reduce the frequency of version checks in the meta service.
            updateCloudPartitionVersions();
        }

        Preconditions.checkNotNull(locations);
        candidateBackends = new ArrayList<>();
        for (Long backendID : planRoot.getScanBackendIds()) {
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

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    void addKeyTuples(
            InternalService.PTabletKeyLookupRequest.Builder requestBuilder) {
        // TODO handle IN predicates
        KeyTuple.Builder kBuilder = KeyTuple.newBuilder();
        for (Expr expr : equalPredicats.values()) {
            LiteralExpr lexpr = (LiteralExpr) expr;
            kBuilder.addKeyColumnRep(lexpr.getStringValue());
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
            ++tryCount;
            if (rowBatch != null) {
                break;
            }
            if (tryCount >= maxTry) {
                break;
            }
            status.updateStatus(TStatusCode.OK, "");
        } while (true);
        // handle status code
        if (!status.ok()) {
            if (Strings.isNullOrEmpty(status.getErrorMsg())) {
                status.rewriteErrorMsg();
            }
            if (status.isRpcError()) {
                throw new RpcException(null, status.getErrorMsg());
            } else {
                String errMsg = status.getErrorMsg();
                LOG.warn("query failed: {}", errMsg);

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
        // Do nothing
    }

    private RowBatch getNextInternal(Status status, Backend backend) throws TException {
        long timeoutTs = System.currentTimeMillis() + timeoutMs;
        RowBatch rowBatch = new RowBatch();
        InternalService.PTabletKeyLookupResponse pResult = null;
        try {
            if (serializedDescTable == null) {
                serializedDescTable = ByteString.copyFrom(
                        new TSerializer().serialize(descriptorTable.toThrift()));
            }
            if (serializedOutputExpr == null) {
                List<TExpr> exprs = new ArrayList<>();
                for (Expr expr : outputExprs) {
                    exprs.add(expr.treeToThrift());
                }
                TExprList exprList = new TExprList(exprs);
                serializedOutputExpr = ByteString.copyFrom(
                        new TSerializer().serialize(exprList));
            }
            if (serializedQueryOptions == null) {
                serializedQueryOptions = ByteString.copyFrom(
                        new TSerializer().serialize(queryOptions));
            }

            InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                        = InternalService.PTabletKeyLookupRequest.newBuilder()
                            .setTabletId(tabletID)
                            .setDescTbl(serializedDescTable)
                            .setOutputExpr(serializedOutputExpr)
                            .setQueryOptions(serializedQueryOptions)
                            .setIsBinaryRow(isBinaryProtocol);
            if (versions != null && !versions.isEmpty()) {
                requestBuilder.setVersion(versions.get(0));
            }
            if (cacheID != null) {
                InternalService.UUID.Builder uuidBuilder = InternalService.UUID.newBuilder();
                uuidBuilder.setUuidHigh(cacheID.getMostSignificantBits());
                uuidBuilder.setUuidLow(cacheID.getLeastSignificantBits());
                requestBuilder.setUuid(uuidBuilder);
            }
            addKeyTuples(requestBuilder);

            while (pResult == null) {
                InternalService.PTabletKeyLookupRequest request = requestBuilder.build();
                Future<InternalService.PTabletKeyLookupResponse> futureResponse =
                         BackendServiceProxy.getInstance().fetchTabletDataAsync(backend.getBrpcAddress(), request);
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    LOG.warn("fetch result timeout {}", backend.getBrpcAddress());
                    status.updateStatus(TStatusCode.INTERNAL_ERROR, "query timeout");
                    return null;
                }
                try {
                    pResult = futureResponse.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // continue to get result
                    LOG.info("future get interrupted Exception");
                    if (isCancel) {
                        status.updateStatus(TStatusCode.CANCELLED, "cancelled");
                        return null;
                    }
                } catch (TimeoutException e) {
                    futureResponse.cancel(true);
                    LOG.warn("fetch result timeout {}, addr {}", timeoutTs - currentTs, backend.getBrpcAddress());
                    status.updateStatus(TStatusCode.INTERNAL_ERROR, "query timeout");
                    return null;
                }
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception {}, e {}", backend.getBrpcAddress(), e);
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            return null;
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception {}, addr {}", e, backend.getBrpcAddress());
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
            LOG.info("get empty rowbatch");
            rowBatch.setEos(true);
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
            return rowBatch;
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
