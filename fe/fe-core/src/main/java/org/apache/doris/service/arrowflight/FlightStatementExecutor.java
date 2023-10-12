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
// This file is copied from
// https://github.com/apache/arrow/blob/main/java/flight/flight-sql/src/test/java/org/apache/arrow/flight/sql/example/StatementContext.java
// and modified by Doris

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class FlightStatementExecutor implements AutoCloseable {
    private ConnectContext connectContext;
    private final String query;
    private TUniqueId queryId;
    private TUniqueId finstId;
    private TNetworkAddress resultFlightServerAddr;
    private TNetworkAddress resultInternalServiceAddr;
    private ArrayList<Expr> resultOutputExprs;

    public FlightStatementExecutor(final String query, ConnectContext connectContext) {
        this.query = query;
        this.connectContext = connectContext;
        connectContext.setThreadLocalInfo();
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public void setFinstId(TUniqueId finstId) {
        this.finstId = finstId;
    }

    public void setResultFlightServerAddr(TNetworkAddress resultFlightServerAddr) {
        this.resultFlightServerAddr = resultFlightServerAddr;
    }

    public void setResultInternalServiceAddr(TNetworkAddress resultInternalServiceAddr) {
        this.resultInternalServiceAddr = resultInternalServiceAddr;
    }

    public void setResultOutputExprs(ArrayList<Expr> resultOutputExprs) {
        this.resultOutputExprs = resultOutputExprs;
    }

    public String getQuery() {
        return query;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public TUniqueId getFinstId() {
        return finstId;
    }

    public TNetworkAddress getResultFlightServerAddr() {
        return resultFlightServerAddr;
    }

    public TNetworkAddress getResultInternalServiceAddr() {
        return resultInternalServiceAddr;
    }

    public ArrayList<Expr> getResultOutputExprs() {
        return resultOutputExprs;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof FlightStatementExecutor)) {
            return false;
        }
        return this == other;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this);
    }

    public void executeQuery() {
        try {
            UUID uuid = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            setQueryId(queryId);
            connectContext.setQueryId(queryId);
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, getQuery());
            connectContext.setExecutor(stmtExecutor);
            stmtExecutor.executeArrowFlightQuery(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to coord exec", e);
        }
    }

    public Schema fetchArrowFlightSchema(int timeoutMs) {
        TNetworkAddress address = getResultInternalServiceAddr();
        TUniqueId tid = getFinstId();
        ArrayList<Expr> resultOutputExprs = getResultOutputExprs();
        Types.PUniqueId finstId = Types.PUniqueId.newBuilder().setHi(tid.hi).setLo(tid.lo).build();
        try {
            InternalService.PFetchArrowFlightSchemaRequest request =
                    InternalService.PFetchArrowFlightSchemaRequest.newBuilder()
                            .setFinstId(finstId)
                            .build();

            Future<InternalService.PFetchArrowFlightSchemaResult> future
                    = BackendServiceProxy.getInstance().fetchArrowFlightSchema(address, request);
            InternalService.PFetchArrowFlightSchemaResult pResult;
            pResult = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (pResult == null) {
                throw new RuntimeException(String.format("fetch arrow flight schema timeout, finstId: %s",
                        DebugUtil.printId(tid)));
            }
            TStatusCode code = TStatusCode.findByValue(pResult.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                Status status = null;
                status.setPstatus(pResult.getStatus());
                throw new RuntimeException(String.format("fetch arrow flight schema failed, finstId: %s, errmsg: %s",
                        DebugUtil.printId(tid), status));
            }
            if (pResult.hasSchema() && pResult.getSchema().size() > 0) {
                RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowStreamReader arrowStreamReader = new ArrowStreamReader(
                        new ByteArrayInputStream(pResult.getSchema().toByteArray()),
                        rootAllocator
                );
                try {
                    VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
                    List<FieldVector> fieldVectors = root.getFieldVectors();
                    if (fieldVectors.size() != resultOutputExprs.size()) {
                        throw new RuntimeException(String.format(
                                "Schema size %s' is not equal to arrow field size %s, finstId: %s.",
                                fieldVectors.size(), resultOutputExprs.size(), DebugUtil.printId(tid)));
                    }
                    return root.getSchema();
                } catch (Exception e) {
                    throw new RuntimeException("Read Arrow Flight Schema failed.", e);
                }
            } else {
                throw new RuntimeException(String.format("get empty arrow flight schema, finstId: %s",
                        DebugUtil.printId(tid)));
            }
        } catch (RpcException e) {
            throw new RuntimeException(String.format(
                    "arrow flight schema fetch catch rpc exception, finstId: %s，backend: %s",
                    DebugUtil.printId(tid), address), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format(
                    "arrow flight schema future get interrupted exception, finstId: %s，backend: %s",
                    DebugUtil.printId(tid), address), e);
        } catch (ExecutionException e) {
            throw new RuntimeException(String.format(
                    "arrow flight schema future get execution exception, finstId: %s，backend: %s",
                    DebugUtil.printId(tid), address), e);
        } catch (TimeoutException e) {
            throw new RuntimeException(String.format(
                    "arrow flight schema fetch timeout, finstId: %s，backend: %s",
                    DebugUtil.printId(tid), address), e);
        }
    }

    @Override
    public void close() throws Exception {
        ConnectContext.remove();
    }
}
