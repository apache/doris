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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TShowProcessListRequest;
import org.apache.doris.thrift.TShowProcessListResult;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Represents the command for SHOW PROCESSLIST
 */
public class ShowProcessListCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowProcessListCommand.class);
    private static final ShowResultSetMetaData PROCESSLIST_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("CurrentConnected", ScalarType.createVarchar(16)))
            .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("User", ScalarType.createVarchar(16)))
            .addColumn(new Column("Host", ScalarType.createVarchar(16)))
            .addColumn(new Column("LoginTime", ScalarType.createVarchar(16)))
            .addColumn(new Column("Catalog", ScalarType.createVarchar(16)))
            .addColumn(new Column("Db", ScalarType.createVarchar(16)))
            .addColumn(new Column("Command", ScalarType.createVarchar(16)))
            .addColumn(new Column("Time", ScalarType.createType(PrimitiveType.INT)))
            .addColumn(new Column("State", ScalarType.createVarchar(64)))
            .addColumn(new Column("QueryId", ScalarType.createVarchar(64)))
            .addColumn(new Column("Info", ScalarType.STRING))
            .addColumn(new Column("FE", ScalarType.createVarchar(16)))
            .addColumn(new Column("CloudCluster", ScalarType.createVarchar(16))).build();

    private final boolean isFull;

    public ShowProcessListCommand(boolean isFull) {
        super(PlanType.SHOW_PROCESSLIST_COMMAND);
        this.isFull = isFull;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        boolean isShowFullSql = isFull;
        boolean isShowAllFe = ConnectContext.get().getSessionVariable().getShowAllFeConnection();

        List<List<String>> rowSet = Lists.newArrayList();
        List<ConnectContext.ThreadInfo> threadInfos = ctx.getConnectScheduler()
                .listConnection(ctx.getQualifiedUser(), isShowFullSql);
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(ctx.getConnectionId(), nowMs));
        }

        if (isShowAllFe) {
            try {
                TShowProcessListRequest request = new TShowProcessListRequest();
                request.setShowFullSql(isShowFullSql);
                request.setCurrentUserIdent(ConnectContext.get().getCurrentUserIdentity().toThrift());
                List<Pair<String, Integer>> frontends = FrontendsProcNode.getFrontendWithRpcPort(Env.getCurrentEnv(),
                        false);
                FrontendService.Client client = null;
                for (Pair<String, Integer> fe : frontends) {
                    TNetworkAddress thriftAddress = new TNetworkAddress(fe.key(), fe.value());
                    try {
                        client = ClientPool.frontendPool.borrowObject(thriftAddress, 3000);
                    } catch (Exception e) {
                        LOG.warn("Failed to get frontend {} client. exception: {}", fe.key(), e);
                        continue;
                    }

                    boolean isReturnToPool = false;
                    try {
                        TShowProcessListResult result = client.showProcessList(request);
                        if (result.process_list != null && result.process_list.size() > 0) {
                            rowSet.addAll(result.process_list);
                        }
                        isReturnToPool = true;
                    } catch (Exception e) {
                        LOG.warn("Failed to request processlist to fe: {} . exception: {}", fe.key(), e);
                    } finally {
                        if (isReturnToPool) {
                            ClientPool.frontendPool.returnObject(thriftAddress, client);
                        } else {
                            ClientPool.frontendPool.invalidateObject(thriftAddress, client);
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.warn(" fetch process list from other fe failed, ", t);
            }
        }

        return new ShowResultSet(PROCESSLIST_META_DATA, rowSet);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowProcessListCommand(this, context);
    }
}
