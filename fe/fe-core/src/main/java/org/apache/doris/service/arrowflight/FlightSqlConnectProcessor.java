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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Process one flgiht sql connection.
 */
public class FlightSqlConnectProcessor extends ConnectProcessor implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(FlightSqlConnectProcessor.class);

    public FlightSqlConnectProcessor(ConnectContext context) {
        super(context);
        connectType = ConnectType.ARROW_FLIGHT_SQL;
        context.setThreadLocalInfo();
        context.setReturnResultFromLocal(true);
    }

    public void prepare(MysqlCommand command) {
        // set status of query to OK.
        ctx.getState().reset();
        executor = null;

        if (command == null) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_COM_ERROR);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR, "Unknown command(" + command.toString() + ")");
            LOG.warn("Unknown command(" + command + ")");
            return;
        }
        LOG.debug("arrow flight sql handle command {}", command);
        ctx.setCommand(command);
        ctx.setStartTime();
    }

    public void handleQuery(String query) {
        MysqlCommand command = MysqlCommand.COM_QUERY;
        prepare(command);

        ctx.setRunningQuery(query);
        ctx.initTracer("trace");
        Span rootSpan = ctx.getTracer().spanBuilder("handleQuery").setNoParent().startSpan();
        try (Scope scope = rootSpan.makeCurrent()) {
            handleQuery(command, query);
        } catch (Exception e) {
            rootSpan.recordException(e);
            throw e;
        } finally {
            rootSpan.end();
        }
    }

    // TODO
    // private void handleInitDb() {
    //     handleInitDb(fullDbName);
    // }

    // TODO
    // private void handleFieldList() {
    //     handleFieldList(tableName);
    // }

    public Schema fetchArrowFlightSchema(int timeoutMs) {
        TNetworkAddress address = ctx.getResultInternalServiceAddr();
        TUniqueId tid = ctx.getFinstId();
        ArrayList<Expr> resultOutputExprs = ctx.getResultOutputExprs();
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
                Status status = new Status();
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
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        // TODO support query profile
        for (StmtExecutor asynExecutor : returnResultFromRemoteExecutor) {
            asynExecutor.finalizeQuery();
        }
        returnResultFromRemoteExecutor.clear();
        ConnectContext.remove();
    }
}


