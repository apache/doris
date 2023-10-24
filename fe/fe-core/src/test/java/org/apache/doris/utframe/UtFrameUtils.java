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
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowPartitionsStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @deprecated If you want to start a FE server in unit test, please let your test
 * class extend {@link TestWithFeService}.
 */
@Deprecated
public class UtFrameUtils {

    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx(UserIdentity userIdentity, String remoteIp) throws IOException {
        ConnectContext ctx = new ConnectContext();
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setQualifiedUser(userIdentity.getQualifiedUser());
        ctx.setRemoteIP(remoteIp);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Help to create a mocked ConnectContext for root.
    public static ConnectContext createDefaultCtx() throws IOException {
        return createDefaultCtx(UserIdentity.ROOT, "127.0.0.1");
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    public static StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx)
            throws Exception {
        System.out.println("begin to parse stmt: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
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
        statementBase.setOrigStmt(new OriginStatement(originStmt, 0));
        return statementBase;
    }

    // for analyzing multi statements
    public static List<StatementBase> parseAndAnalyzeStmts(String originStmt, ConnectContext ctx) throws Exception {
        System.out.println("begin to parse stmts: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
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

    public static String generateRandomFeRunningDir(Class testSuiteClass) {
        return generateRandomFeRunningDir(testSuiteClass.getSimpleName());
    }

    public static String generateRandomFeRunningDir(String testSuiteName) {
        return "fe" + "/mocked/" + testSuiteName + "/" + UUID.randomUUID().toString() + "/";
    }

    public static int startFEServer(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        IOException exception = null;
        for (int i = 0; i <= 3; i++) {
            try {
                return startFEServerWithoutRetry(runningDir);
            } catch (IOException ignore) {
                exception = ignore;
            }
        }
        throw exception;
    }

    private static int startFEServerWithoutRetry(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException {
        // get DORIS_HOME
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Config.plugin_dir = dorisHome + "/plugins";
        Config.custom_config_dir = dorisHome + "/conf";
        Config.edit_log_type = "local";
        Config.disable_decimalv2 = false;
        Config.disable_datev1 = false;
        File file = new File(Config.custom_config_dir);
        if (!file.exists()) {
            file.mkdir();
        }
        if (null != System.getenv("DORIS_HOME")) {
            File metaDir = new File(Config.meta_dir);
            if (!metaDir.exists()) {
                metaDir.mkdir();
            }
        }

        int feHttpPort = findValidPort();
        int feRpcPort = findValidPort();
        int feQueryPort = findValidPort();
        int feEditLogPort = findValidPort();

        // start fe in "DORIS_HOME/fe/mocked/"
        MockedFrontend frontend = new MockedFrontend();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("http_port", String.valueOf(feHttpPort));
        feConfMap.put("rpc_port", String.valueOf(feRpcPort));
        feConfMap.put("query_port", String.valueOf(feQueryPort));
        feConfMap.put("edit_log_port", String.valueOf(feEditLogPort));
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(dorisHome + "/" + runningDir, feConfMap);
        frontend.start(new String[0]);
        return feRpcPort;
    }

    public static void createDorisCluster(String runningDir) throws InterruptedException, NotInitException,
            IOException, DdlException, EnvVarNotSetException, FeStartException {
        createDorisCluster(runningDir, 1);
    }

    public static void createDorisCluster(String runningDir, int backendNum) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        FeConstants.enableInternalSchemaDb = false;
        int feRpcPort = startFEServer(runningDir);
        List<Backend> bes = Lists.newArrayList();
        for (int i = 0; i < backendNum; i++) {
            bes.add(createBackend("127.0.0.1", feRpcPort));
        }
        System.out.println("after create backend");
        checkBEHeartbeat(bes);
        // Thread.sleep(2000);
        System.out.println("after create backend2");
    }

    private static void checkBEHeartbeat(List<Backend> bes) throws InterruptedException {
        int maxTry = 10;
        boolean allAlive = false;
        while (maxTry-- > 0 && !allAlive) {
            Thread.sleep(1000);
            boolean hasDead = false;
            for (Backend be : bes) {
                if (!be.isAlive()) {
                    hasDead = true;
                }
            }
            allAlive = !hasDead;
        }
    }

    // Create multi backends with different host for unit test.
    // the host of BE will be "127.0.0.1", "127.0.0.2"
    public static void createDorisClusterWithMultiTag(String runningDir, int backendNum)
            throws EnvVarNotSetException, IOException, FeStartException, NotInitException, DdlException,
            InterruptedException {
        // set runningUnitTest to true, so that for ut,
        // the agent task will be sent to "127.0.0.1" to make cluster running well.
        FeConstants.runningUnitTest = true;
        FeConstants.enableInternalSchemaDb = false;
        int feRpcPort = startFEServer(runningDir);
        for (int i = 0; i < backendNum; i++) {
            String host = "127.0.0." + (i + 1);
            createBackend(host, feRpcPort);
        }
        // sleep to wait first heartbeat
        Thread.sleep(6000);
    }

    public static Backend createBackend(String beHost, int feRpcPort) throws IOException, InterruptedException {
        IOException exception = null;
        for (int i = 0; i <= 3; i++) {
            try {
                return createBackendWithoutRetry(beHost, feRpcPort);
            } catch (IOException ignore) {
                exception = ignore;
            }
        }
        throw exception;
    }

    private static Backend createBackendWithoutRetry(String beHost, int feRpcPort) throws IOException {
        int beHeartbeatPort = findValidPort();
        int beThriftPort = findValidPort();
        int beBrpcPort = findValidPort();
        int beHttpPort = findValidPort();

        // start be
        MockedBackend backend = MockedBackendFactory.createBackend(beHost, beHeartbeatPort, beThriftPort, beBrpcPort,
                beHttpPort, new DefaultHeartbeatServiceImpl(beThriftPort, beHttpPort, beBrpcPort),
                new DefaultBeThriftServiceImpl(), new DefaultPBackendServiceImpl());
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", feRpcPort));
        backend.start();

        // add be
        Backend be = new Backend(Env.getCurrentEnv().getNextId(), backend.getHost(), backend.getHeartbeatPort());
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path" + be.getId());
        diskInfo1.setTotalCapacityB(10L << 30);
        diskInfo1.setAvailableCapacityB(5L << 30);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(true);
        be.setBePort(beThriftPort);
        be.setHttpPort(beHttpPort);
        be.setBrpcPort(beBrpcPort);
        Env.getCurrentSystemInfo().addBackend(be);
        return be;
    }

    public static void cleanDorisFeDir(String baseDir) {
        try {
            FileUtils.deleteDirectory(new File(baseDir));
        } catch (IOException e) {
            // ignore
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
                    System.out.println("The port " + port + " is invalid and try another port.");
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
            return planner.getExplainString(new ExplainOptions(isVerbose, false));
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

    public static String getStmtDigest(ConnectContext connectContext, String originStmt) throws Exception {
        SqlScanner input = new SqlScanner(new StringReader(originStmt),
                connectContext.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);
        Preconditions.checkState(statementBase instanceof QueryStmt);
        QueryStmt queryStmt = (QueryStmt) statementBase;
        String digest = queryStmt.toDigest();
        return DigestUtils.md5Hex(digest);
    }

    public static boolean checkPlanResultContainsNode(String planResult, int idx, String nodeName) {
        String realNodeName = idx + ":" + nodeName;
        String realVNodeName = idx + ":V" + nodeName;
        return planResult.contains(realNodeName) || planResult.contains(realVNodeName);
    }

    public static void createDatabase(ConnectContext ctx, String db) throws Exception {
        String createDbStmtStr = "CREATE DATABASE " + db;
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    public static void createTable(ConnectContext ctx, String sql) throws Exception {
        try {
            createTables(ctx, sql);
        } catch (ConcurrentModificationException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void createTables(ConnectContext ctx, String... sqls) throws Exception {
        for (String sql : sqls) {
            CreateTableStmt stmt = (CreateTableStmt) parseAndAnalyzeStmt(sql, ctx);
            Env.getCurrentEnv().createTable(stmt);
        }
        updateReplicaPathHash();
    }

    public static ShowResultSet showCreateTable(ConnectContext ctx, String sql) throws Exception {
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) parseAndAnalyzeStmt(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        return executor.execute();
    }

    public static ShowResultSet showCreateTableByName(ConnectContext ctx, String table) throws Exception {
        String sql = "show create table " + table;
        return showCreateTable(ctx, sql);
    }

    public static ShowResultSet showPartitions(ConnectContext ctx, String sql) throws Exception {
        ShowPartitionsStmt stmt = (ShowPartitionsStmt) parseAndAnalyzeStmt(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        return executor.execute();
    }

    public static ShowResultSet showPartitionsByName(ConnectContext ctx, String table) throws Exception {
        String sql = "show partitions from " + table;
        return showPartitions(ctx, sql);
    }

    private static void updateReplicaPathHash() {
        com.google.common.collect.Table<Long, Long, Replica> replicaMetaTable = Env.getCurrentInvertedIndex()
                .getReplicaMetaTable();
        for (com.google.common.collect.Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
            long beId = cell.getColumnKey();
            Backend be = Env.getCurrentSystemInfo().getBackend(beId);
            if (be == null) {
                continue;
            }
            Replica replica = cell.getValue();
            TabletMeta tabletMeta = Env.getCurrentInvertedIndex().getTabletMeta(cell.getRowKey());
            ImmutableMap<String, DiskInfo> diskMap = be.getDisks();
            for (DiskInfo diskInfo : diskMap.values()) {
                if (diskInfo.getStorageMedium() == tabletMeta.getStorageMedium()) {
                    replica.setPathHash(diskInfo.getPathHash());
                    break;
                }
            }
        }
    }
}
