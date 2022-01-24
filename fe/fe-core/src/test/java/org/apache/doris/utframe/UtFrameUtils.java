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
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.MockedBackendFactory.DefaultBeThriftServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultHeartbeatServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
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
        ctx.setRemoteIP("127.0.0.1");
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
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        statementBase.analyze(analyzer);
        return statementBase;
    }

    // for analyzing multi statements
    public static List<StatementBase> parseAndAnalyzeStmts(String originStmt, ConnectContext ctx) throws Exception {
        System.out.println("begin to parse stmts: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        List<StatementBase> statementBases = null;
        try {
            statementBases = SqlParserUtils.getMultiStmts(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        for (StatementBase stmt : statementBases) {
            stmt.analyze(analyzer);
        }
        return statementBases;
    }

    public static int startFEServer(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // get DORIS_HOME
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Config.plugin_dir = dorisHome + "/plugins";
        Config.custom_config_dir = dorisHome + "/conf";
        File file = new File(Config.custom_config_dir);
        if (!file.exists()) {
            file.mkdir();
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

    public static void createDorisCluster(String runningDir) throws InterruptedException, NotInitException,
            IOException, DdlException, EnvVarNotSetException, FeStartException {
        createDorisCluster(runningDir, 1);
    }

    public static void createDorisCluster(String runningDir, int backendNum) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        int fe_rpc_port = startFEServer(runningDir);
        for (int i = 0; i < backendNum; i++) {
            createBackend("127.0.0.1", fe_rpc_port);
            // sleep to wait first heartbeat
            Thread.sleep(6000);
        }
    }

    // Create multi backends with different host for unit test.
    // the host of BE will be "127.0.0.1", "127.0.0.2"
    public static void createDorisClusterWithMultiTag(String runningDir, int backendNum) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // set runningUnitTest to true, so that for ut, the agent task will be send to "127.0.0.1" to make cluster running well.
        FeConstants.runningUnitTest = true;
        int fe_rpc_port = startFEServer(runningDir);
        for (int i = 0; i < backendNum; i++) {
            String host = "127.0.0." + (i + 1);
            createBackend(host, fe_rpc_port);
        }
        // sleep to wait first heartbeat
        Thread.sleep(6000);
    }

    public static void createBackend(String beHost, int fe_rpc_port) throws IOException, InterruptedException {
        int be_heartbeat_port = findValidPort();
        int be_thrift_port = findValidPort();
        int be_brpc_port = findValidPort();
        int be_http_port = findValidPort();

        // start be
        MockedBackend backend = MockedBackendFactory.createBackend(beHost,
                be_heartbeat_port, be_thrift_port, be_brpc_port, be_http_port,
                new DefaultHeartbeatServiceImpl(be_thrift_port, be_http_port, be_brpc_port),
                new DefaultBeThriftServiceImpl(), new DefaultPBackendServiceImpl());
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", fe_rpc_port));
        backend.start();

        // add be
        Backend be = new Backend(Catalog.getCurrentCatalog().getNextId(), backend.getHost(), backend.getHeartbeatPort());
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path" + be.getId());
        diskInfo1.setTotalCapacityB(1000000);
        diskInfo1.setAvailableCapacityB(500000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(true);
        be.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        be.setBePort(be_thrift_port);
        be.setHttpPort(be_http_port);
        be.setBrpcPort(be_brpc_port);
        Catalog.getCurrentSystemInfo().addBackend(be);
    }

    public static void cleanDorisFeDir(String baseDir) {
        try {
            FileUtils.deleteDirectory(new File(baseDir));
        } catch (IOException e) {
        }
    }

    public static int findValidPort() {
        int port = 0;
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    System.out.println("The port " + port  + " is invalid and try another port.");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
            }
        }
        return port;
    }

    public static String getSQLPlanOrErrorMsg(ConnectContext ctx, String queryStr) throws Exception {
        return getSQLPlanOrErrorMsg(ctx, queryStr, false);
    }

    public static String getSQLPlanOrErrorMsg(ConnectContext ctx, String queryStr, boolean isVerbose) throws Exception {
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        ctx.setExecutor(stmtExecutor);
        ConnectContext.get().setExecutor(stmtExecutor);
        stmtExecutor.execute();
        if (ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            Planner planner = stmtExecutor.planner();
            return planner.getExplainString(planner.getFragments(), new ExplainOptions(isVerbose, false));
        } else {
            return ctx.getState().getErrorMessage();
        }
    }

    public static Planner getSQLPlanner(ConnectContext ctx, String queryStr) throws Exception {
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        if (ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            return stmtExecutor.planner();
        } else {
            return null;
        }
    }

    public static StmtExecutor getSqlStmtExecutor(ConnectContext ctx, String queryStr) throws Exception {
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        if (ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            return stmtExecutor;
        } else {
            return null;
        }
    }
}

