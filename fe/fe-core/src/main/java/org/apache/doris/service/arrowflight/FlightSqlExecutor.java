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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FlightSqlExecutor {
    private static final Logger LOG = LogManager.getLogger(FlightSqlExecutor.class);

    public static void executeQuery(FlightStatementContext<Statement> flightStatementContext) {
        try (AutoCloseConnectContext r = FlightSqlExecutor.buildConnectContext()) {
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, flightStatementContext.getQuery());
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.executeArrowFlightQuery(flightStatementContext);
        }
    }

    public static AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        sessionVariable.setEnableNereidsPlanner(false);
        connectContext.setEnv(Env.getCurrentEnv());
        // connectContext.setDatabase(FeConstants.INTERNAL_DB_NAME);
        connectContext.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        connectContext.setResultSinkType(TResultSinkType.ARROW_FLIGHT_PROTOCAL);
        return new AutoCloseConnectContext(connectContext);
    }

    public static Schema fetchArrowFlightSchema(FlightStatementContext<Statement> flightStatementContext,
            int timeoutMs, Status status) {
        TNetworkAddress address = flightStatementContext.getResultInternalServiceAddr();
        TUniqueId tid = flightStatementContext.getFinstId();
        ArrayList<Expr> resultOutputExprs = flightStatementContext.getResultOutputExprs();
        Types.PUniqueId finstId = Types.PUniqueId.newBuilder().setHi(tid.hi).setLo(tid.lo).build();
        try {
            InternalService.PFetchArrowFlightSchemaRequest request =
                    InternalService.PFetchArrowFlightSchemaRequest.newBuilder()
                    .setFinstId(finstId)
                    .build();

            Future<InternalService.PFetchArrowFlightSchemaResult> future
                    = BackendServiceProxy.getInstance().fetchArrowFlightSchema(address, request);
            InternalService.PFetchArrowFlightSchemaResult pResult = null;
            pResult = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (pResult == null) {
                LOG.warn("fetch arrow flight schema timeout, finstId={}", DebugUtil.printId(tid));
                status.setStatus("fetch arrow flight schema timeout");
                return null;
            }
            TStatusCode code = TStatusCode.findByValue(pResult.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                status.setPstatus(pResult.getStatus());
                return null;
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
                        LOG.error("Schema size '{}' is not equal to arrow field size '{}'.",
                                fieldVectors.size(), resultOutputExprs.size());
                        status.setStatus(new Status(TStatusCode.INTERNAL_ERROR,
                                "Load Doris data failed, schema size of fetch data is wrong."));
                        return null;
                    }
                    return root.getSchema();
                } catch (Exception e) {
                    LOG.error("Read Arrow Flight Schema failed because: ", e);
                    status.setStatus(new Status(TStatusCode.INTERNAL_ERROR,
                            "Read Arrow Flight Schema failed because."));
                    return null;
                }
            } else {
                LOG.info("finistId={}, get empty arrow flight schema", DebugUtil.printId(tid));
                return null;
            }
        } catch (RpcException e) {
            LOG.warn("arrow flight schema fetch catch rpc exception, finstId {} backend {}",
                    DebugUtil.printId(tid), address.toString(), e);
            status.setRpcStatus(e.getMessage());
        } catch (InterruptedException e) {
            LOG.warn("arrow flight schema future get interrupted exception, finstId {} backend {}",
                    DebugUtil.printId(tid), address.toString(), e);
            status.setStatus("interrupted exception");
        } catch (ExecutionException e) {
            LOG.warn("arrow flight schema future get execution exception, finstId {} backend {}",
                    DebugUtil.printId(tid), address.toString(), e);
            status.setStatus("execution exception");
        } catch (TimeoutException e) {
            LOG.warn("arrow flight schema fetch timeout, finstId {} backend {}",
                    DebugUtil.printId(tid), address.toString(), e);
            status.setStatus("fetch timeout");
        }
        return null;
    }
}
