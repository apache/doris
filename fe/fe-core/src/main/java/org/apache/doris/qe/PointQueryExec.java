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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.KeyTuple;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;

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
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PointQueryExec {
    private static final Logger LOG = LogManager.getLogger(PointQueryExec.class);
    // SlotRef sorted by column id
    private Map<SlotRef, Expr> equalPredicats;
    // ByteString serialized for prepared statement
    private ByteString serializedDescTable;
    private ByteString serializedOutputExpr;
    private ArrayList<Expr> outputExprs;
    private DescriptorTable descriptorTable;
    private long tabletID = 0;
    private long timeoutMs = Config.point_query_timeout_ms; // default 10s

    private boolean isCancel = false;
    private boolean isBinaryProtocol = false;

    private List<Backend> candidateBackends;

    // For parepared statement cached structure,
    // there are some pre caculated structure in Backend TabletFetch service
    // using this ID to find for this prepared statement
    private UUID cacheID;

    public PointQueryExec(Map<SlotRef, Expr> equalPredicats, DescriptorTable descTable,
            ArrayList<Expr> outputExprs) {
        this.equalPredicats = equalPredicats;
        this.descriptorTable = descTable;
        this.outputExprs = outputExprs;
    }

    void setCandidateBackends(HashSet<Long> backendsIds) {
        candidateBackends = new ArrayList<>();
        for (Long backendID : backendsIds) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(backendID);
            if (SimpleScheduler.isAvailable(backend)) {
                candidateBackends.add(backend);
            }
        }
        // Random read replicas
        Collections.shuffle(this.candidateBackends);
    }

    public void setSerializedDescTable(ByteString serializedDescTable) {
        this.serializedDescTable = serializedDescTable;
    }

    public void setSerializedOutputExpr(ByteString serializedOutputExpr) {
        this.serializedOutputExpr = serializedOutputExpr;
    }

    public void setCacheID(UUID cacheID) {
        this.cacheID = cacheID;
    }

    public void setTabletId(long tabletID) {
        this.tabletID = tabletID;
    }

    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void setBinaryProtocol(boolean isBinaryProtocol) {
        this.isBinaryProtocol = isBinaryProtocol;
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

    public RowBatch getNext(Status status) throws TException {
        Iterator<Backend> backendIter = candidateBackends.iterator();
        RowBatch rowBatch = null;
        int tryCount = 0;
        int maxTry = Math.min(Config.max_point_query_retry_time, candidateBackends.size());
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
            status.setStatus(Status.OK);
        } while (true);
        return rowBatch;
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

            InternalService.PTabletKeyLookupRequest.Builder requestBuilder
                        = InternalService.PTabletKeyLookupRequest.newBuilder()
                            .setTabletId(tabletID)
                            .setDescTbl(serializedDescTable)
                            .setOutputExpr(serializedOutputExpr)
                            .setIsBinaryRow(isBinaryProtocol);
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
                         BackendServiceProxy.getInstance().fetchTabletDataAsync(backend.getBrpcAdress(), request);
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    LOG.warn("fetch result timeout {}", backend.getBrpcAdress());
                    status.setStatus("query timeout");
                    return null;
                }
                try {
                    pResult = futureResponse.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // continue to get result
                    LOG.info("future get interrupted Exception");
                    if (isCancel) {
                        status.setStatus(Status.CANCELLED);
                        return null;
                    }
                } catch (TimeoutException e) {
                    futureResponse.cancel(true);
                    LOG.warn("fetch result timeout {}, addr {}", timeoutTs - currentTs, backend.getBrpcAdress());
                    status.setStatus("query timeout");
                    return null;
                }
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception {}, e {}", backend.getBrpcAdress(), e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            return null;
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception {}, addr {}", e, backend.getBrpcAdress());
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, e.getMessage()));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            }
            return null;
        }
        TStatusCode code = TStatusCode.findByValue(pResult.getStatus().getStatusCode());
        if (code != TStatusCode.OK) {
            status.setPstatus(pResult.getStatus());
            return null;
        }

        if (pResult.hasEmptyBatch() && pResult.getEmptyBatch()) {
            LOG.info("get empty rowbatch");
            rowBatch.setEos(true);
            return rowBatch;
        } else if (pResult.hasRowBatch() && pResult.getRowBatch().size() > 0) {
            byte[] serialResult = pResult.getRowBatch().toByteArray();
            TResultBatch resultBatch = new TResultBatch();
            TDeserializer deserializer = new TDeserializer();
            deserializer.deserialize(resultBatch, serialResult);
            rowBatch.setBatch(resultBatch);
            rowBatch.setEos(true);
            return rowBatch;
        }

        if (isCancel) {
            status.setStatus(Status.CANCELLED);
        }
        return rowBatch;
    }

    public void cancel() {
        isCancel = true;
    }
}
