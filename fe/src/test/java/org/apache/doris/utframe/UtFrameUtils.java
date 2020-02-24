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

package org.apache.doris.utframe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.MockedBackendFactory.DefaultBeThriftServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultHeartbeatServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.StringReader;
import java.net.ServerSocket;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class UtFrameUtils {
    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx() throws IOException {
        SocketChannel channel = SocketChannel.open();
        ConnectContext ctx = new ConnectContext(channel);
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(PaloAuth.ROOT_USER);
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    public static StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx)
            throws Exception {
        System.out.println("begin to parse stmt: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);

        StatementBase statementBase = (StatementBase) parser.parse().value;
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        statementBase.analyze(analyzer);
        return statementBase;
    }

    public static int startFEServer(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // get DORIS_HOME
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }

        int fe_http_port = findValidPort();
        int fe_rpc_port = findValidPort();
        int fe_query_port = findValidPort();
        int fe_edit_log_port = findValidPort();

        // start fe in "DORIS_HOME/fe/mocked/"
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("http_port", String.valueOf(fe_http_port));
        feConfMap.put("rpc_port", String.valueOf(fe_rpc_port));
        feConfMap.put("query_port", String.valueOf(fe_query_port));
        feConfMap.put("edit_log_port", String.valueOf(fe_edit_log_port));
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(dorisHome + "/" + runningDir, feConfMap);
        frontend.start(new String[0]);
        return fe_rpc_port;
    }

    public static void createMinDorisCluster(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        int fe_rpc_port = startFEServer(runningDir);

        int be_heartbeat_port = findValidPort();
        int be_thrift_port = findValidPort();
        int be_brpc_port = findValidPort();
        int be_http_port = findValidPort();

        // start be
        MockedBackend backend = MockedBackendFactory.createBackend("127.0.0.1",
                be_heartbeat_port, be_thrift_port, be_brpc_port, be_http_port,
                new DefaultHeartbeatServiceImpl(be_thrift_port, be_http_port, be_brpc_port),
                new DefaultBeThriftServiceImpl(), new DefaultPBackendServiceImpl());
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", fe_rpc_port));
        backend.start();

        // add be
        List<Pair<String, Integer>> bes = Lists.newArrayList();
        bes.add(Pair.create(backend.getHost(), backend.getHeartbeatPort()));
        Catalog.getCurrentSystemInfo().addBackends(bes, false, "default_cluster");

        // sleep to wait first heartbeat
        Thread.sleep(6000);
    }

    public static int findValidPort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public static String getSQLPlanOrErrorMsg(ConnectContext ctx, String queryStr) throws Exception {
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        if (ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            Planner planner = stmtExecutor.planner();
            return planner.getExplainString(planner.getFragments(), TExplainLevel.VERBOSE);
        } else {
            return ctx.getState().getErrorMessage();
        }
    }
}
