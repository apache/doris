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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.DropSqlBlockRuleStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.ShowCreateFunctionStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowFunctionsStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This is the base class for unit class that wants to start a FE service.
 * Concrete test class must be derived class of {@link TestWithFeService}, {@link DemoTest} is
 * an example.
 * This class use {@link TestInstance} in JUnit5 to do initialization and cleanup stuff. Unlike
 * deprecated legacy combination-based implementation {@link UtFrameUtils}, we use an inherit-manner,
 * thus we could wrap common logic in this base class. It's more easy to use.
 * Note:
 * Unit-test method in derived classes must use the JUnit5 {@link org.junit.jupiter.api.Test}
 * annotation, rather than the old JUnit4 {@link org.junit.Test} or others.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestWithFeService {
    protected String dorisHome;
    protected String runningDir = "fe/mocked/" + getClass().getSimpleName() + "/" + UUID.randomUUID() + "/";
    protected ConnectContext connectContext;
    protected boolean needCleanDir = true;

    protected static final String DEFAULT_CLUSTER_PREFIX = "default_cluster:";

    @BeforeAll
    public final void beforeAll() throws Exception {
        FeConstants.enableInternalSchemaDb = false;
        beforeCreatingConnectContext();
        connectContext = createDefaultCtx();
        beforeCluster();
        createDorisCluster();
        new InternalSchemaInitializer().start();
        runBeforeAll();
    }

    protected void beforeCluster() {
    }

    @AfterAll
    public final void afterAll() throws Exception {
        runAfterAll();
        Env.getCurrentEnv().clear();
        StatementScopeIdGenerator.clear();
        if (needCleanDir) {
            cleanDorisFeDir();
        }
    }

    @BeforeEach
    public final void beforeEach() throws Exception {
        runBeforeEach();
    }

    protected void beforeCreatingConnectContext() throws Exception {

    }

    protected void runBeforeAll() throws Exception {
    }

    protected void runAfterAll() throws Exception {
    }

    protected void runBeforeEach() throws Exception {
    }

    // Override this method if you want to start multi BE
    protected int backendNum() {
        return 1;
    }

    // Help to create a mocked ConnectContext.
    protected ConnectContext createDefaultCtx() throws IOException {
        return createCtx(UserIdentity.ROOT, "127.0.0.1");
    }

    protected StatementContext createStatementCtx(String sql) {
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        return statementContext;
    }

    protected StatementContext createStatementCtx(String sql, ConnectContext ctx) {
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(sql, 0));
        ctx.setStatementContext(statementContext);
        return statementContext;
    }

    protected  <T extends StatementBase> T createStmt(String showSql)
            throws Exception {
        return (T) parseAndAnalyzeStmt(showSql, connectContext);
    }

    protected CascadesContext createCascadesContext(String sql) {
        StatementContext statementCtx = createStatementCtx(sql);
        return MemoTestUtils.createCascadesContext(statementCtx, sql);
    }

    protected CascadesContext createCascadesContext(String sql, ConnectContext ctx) {
        StatementContext statementCtx = createStatementCtx(sql, ctx);
        return MemoTestUtils.createCascadesContext(statementCtx, sql);
    }

    public LogicalPlan analyzeAndGetLogicalPlanByNereids(String sql) {
        Set<String> originDisableRules = connectContext.getSessionVariable().getDisableNereidsRules();
        Set<String> disableRuleWithAuth = Sets.newHashSet(originDisableRules);
        disableRuleWithAuth.add(RuleType.RELATION_AUTHENTICATION.name());
        connectContext.getSessionVariable().setDisableNereidsRules(String.join(",", disableRuleWithAuth));
        CascadesContext cascadesContext = createCascadesContext(sql);
        cascadesContext.newAnalyzer().analyze();
        connectContext.getSessionVariable().setDisableNereidsRules(String.join(",", originDisableRules));
        cascadesContext.toMemo();
        return (LogicalPlan) cascadesContext.getRewritePlan();
    }

    public LogicalPlan analyzeAndGetLogicalPlanByNereids(String sql, ConnectContext ctx) {
        Set<String> originDisableRules = ctx.getSessionVariable().getDisableNereidsRules();
        Set<String> disableRuleWithAuth = Sets.newHashSet(originDisableRules);
        disableRuleWithAuth.add(RuleType.RELATION_AUTHENTICATION.name());
        ctx.getSessionVariable().setDisableNereidsRules(String.join(",", disableRuleWithAuth));
        CascadesContext cascadesContext = createCascadesContext(sql, ctx);
        cascadesContext.newAnalyzer().analyze();
        ctx.getSessionVariable().setDisableNereidsRules(String.join(",", originDisableRules));
        cascadesContext.toMemo();
        return (LogicalPlan) cascadesContext.getRewritePlan();
    }

    // Parse an origin stmt and analyze it by nereids. Return a StatementBase instance.
    public StatementBase analyzeAndGetStmtByNereids(String sql) {
        return analyzeAndGetStmtByNereids(sql, connectContext);
    }

    // Parse an origin stmt and analyze it by nereids. Return a StatementBase instance.
    public StatementBase analyzeAndGetStmtByNereids(String sql, ConnectContext ctx) {
        Set<String> originDisableRules = ctx.getSessionVariable().getDisableNereidsRules();
        Set<String> disableRuleWithAuth = Sets.newHashSet(originDisableRules);
        disableRuleWithAuth.add(RuleType.RELATION_AUTHENTICATION.name());
        ctx.getSessionVariable().setDisableNereidsRules(String.join(",", disableRuleWithAuth));
        CascadesContext cascadesContext = createCascadesContext(sql, ctx);
        cascadesContext.newAnalyzer().analyze();
        ctx.getSessionVariable().setDisableNereidsRules(String.join(",", originDisableRules));
        cascadesContext.toMemo();
        LogicalPlan plan = (LogicalPlan) cascadesContext.getRewritePlan();
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, cascadesContext.getStatementContext());
        adapter.setViewDdlSqls(cascadesContext.getStatementContext().getViewDdlSqls());
        cascadesContext.getStatementContext().setParsedStatement(adapter);
        return adapter;
    }

    protected ConnectContext createCtx(UserIdentity user, String host) throws IOException {
        ConnectContext ctx = new ConnectContext();
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(user);
        ctx.setQualifiedUser(user.getQualifiedUser());
        ctx.setRemoteIP(host);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    protected StatementBase parseAndAnalyzeStmt(String originStmt) throws Exception {
        return parseAndAnalyzeStmt(originStmt, connectContext);
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    protected StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx) throws Exception {
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
        statementBase.setOrigStmt(new OriginStatement(originStmt, 0));
        statementBase.analyze(analyzer);
        return statementBase;
    }

    // for analyzing multi statements
    protected List<StatementBase> parseAndAnalyzeStmts(String originStmt) throws Exception {
        System.out.println("begin to parse stmts: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt),
                connectContext.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(connectContext.getEnv(), connectContext);
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

    protected String generateRandomFeRunningDir(Class testSuiteClass) {
        return generateRandomFeRunningDir(testSuiteClass.getSimpleName());
    }

    protected String generateRandomFeRunningDir(String testSuiteName) {
        return "fe" + "/mocked/" + testSuiteName + "/" + UUID.randomUUID().toString() + "/";
    }

    protected int startFEServer(String runningDir)
            throws EnvVarNotSetException, IOException, FeStartException, NotInitException, DdlException,
            InterruptedException {
        IOException exception = null;
        try {
            return startFEServerWithoutRetry(runningDir);
        } catch (IOException ignore) {
            exception = ignore;
        }
        throw exception;
    }

    protected int startFEServerWithoutRetry(String runningDir)
            throws EnvVarNotSetException, IOException, FeStartException, NotInitException, DdlException,
            InterruptedException {
        // get DORIS_HOME
        dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        System.out.println("CREATE FE SERVER DIR: " + dorisHome);
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
        System.out.println("CREATE FE SERVER DIR: " + Config.custom_config_dir);

        int feHttpPort = findValidPort();
        int feRpcPort = findValidPort();
        int feQueryPort = findValidPort();
        int feEditLogPort = findValidPort();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("http_port", String.valueOf(feHttpPort));
        feConfMap.put("rpc_port", String.valueOf(feRpcPort));
        feConfMap.put("query_port", String.valueOf(feQueryPort));
        feConfMap.put("edit_log_port", String.valueOf(feEditLogPort));
        feConfMap.put("tablet_create_timeout_second", "10");
        // start fe in "DORIS_HOME/fe/mocked/"
        MockedFrontend frontend = new MockedFrontend();
        frontend.init(dorisHome + "/" + runningDir, feConfMap);
        frontend.start(new String[0]);
        return feRpcPort;
    }

    protected void createDorisCluster()
            throws InterruptedException, NotInitException, IOException, DdlException, EnvVarNotSetException,
            FeStartException {
        createDorisCluster(runningDir, backendNum());
    }

    protected void createDorisCluster(String runningDir, int backendNum)
            throws EnvVarNotSetException, IOException, FeStartException, NotInitException, DdlException,
            InterruptedException {
        int feRpcPort = startFEServer(runningDir);
        List<Backend> bes = Lists.newArrayList();
        System.out.println("start create backend, backend num " + backendNum);
        for (int i = 0; i < backendNum; i++) {
            bes.add(createBackend("127.0.0.1", feRpcPort));
        }
        System.out.println("after create backend");
        checkBEHeartbeat(bes);
        // Thread.sleep(2000);
        System.out.println("after create backend2");
    }

    private void checkBEHeartbeat(List<Backend> bes) throws InterruptedException {
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
    protected void createDorisClusterWithMultiTag(String runningDir, int backendNum)
            throws EnvVarNotSetException, IOException, FeStartException, NotInitException, DdlException,
            InterruptedException {
        // set runningUnitTest to true, so that for ut, the agent task will be send to "127.0.0.1"
        // to make cluster running well.
        FeConstants.runningUnitTest = true;
        int feRpcPort = startFEServer(runningDir);
        List<Backend> bes = Lists.newArrayList();
        for (int i = 0; i < backendNum; i++) {
            String host = "127.0.0." + (i + 1);
            bes.add(createBackend(host, feRpcPort));
        }
        checkBEHeartbeat(bes);
    }

    protected Backend createBackend(String beHost, int feRpcPort) throws IOException, InterruptedException {
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

    private Backend createBackendWithoutRetry(String beHost, int feRpcPort) throws IOException, InterruptedException {
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
        DiskInfo diskInfo1 = new DiskInfo("/path" + be.getId());
        diskInfo1.setPathHash(be.getId());
        diskInfo1.setTotalCapacityB(1000000);
        diskInfo1.setAvailableCapacityB(500000);
        diskInfo1.setDataUsedCapacityB(480000);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(false);
        be.setBePort(beThriftPort);
        be.setHttpPort(beHttpPort);
        be.setBrpcPort(beBrpcPort);
        Env.getCurrentSystemInfo().addBackend(be);
        return be;
    }

    protected void cleanDorisFeDir() {
        try {
            cleanDir(dorisHome + "/" + runningDir);
            cleanDir(Config.plugin_dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected int findValidPort() {
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

    public String getSQLPlanOrErrorMsg(String sql) throws Exception {
        return getSQLPlanOrErrorMsg(sql, false);
    }

    public String getSQLPlanOrErrorMsg(String sql, boolean isVerbose) throws Exception {
        return getSQLPlanOrErrorMsg(connectContext, sql, isVerbose);
    }

    public String getSQLPlanOrErrorMsg(ConnectContext ctx, String sql, boolean isVerbose) throws Exception {
        ctx.setThreadLocalInfo();
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
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

    public Planner getSQLPlanner(String queryStr) throws Exception {
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, queryStr);
        stmtExecutor.execute();
        if (connectContext.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            return stmtExecutor.planner();
        } else {
            throw new Exception(
                    connectContext.getState().toString() + ", " + connectContext.getState().getErrorMessage());
        }
    }

    public StmtExecutor getSqlStmtExecutor(String queryStr) throws Exception {
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, queryStr);
        stmtExecutor.execute();
        if (connectContext.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            return stmtExecutor;
        } else {
            return null;
        }
    }

    public void createDatabase(String db) throws Exception {
        String createDbStmtStr = "CREATE DATABASE " + db;
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt(createDbStmtStr);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    public void dropDatabase(String db) throws Exception {
        String createDbStmtStr = "DROP DATABASE " + db;
        DropDbStmt createDbStmt = (DropDbStmt) parseAndAnalyzeStmt(createDbStmtStr);
        Env.getCurrentEnv().dropDb(createDbStmt);
    }

    public void useDatabase(String dbName) {
        connectContext.setDatabase(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
    }

    protected ShowResultSet showCreateTable(String sql) throws Exception {
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) parseAndAnalyzeStmt(sql);
        ShowExecutor executor = new ShowExecutor(connectContext, stmt);
        return executor.execute();
    }

    protected ShowResultSet showCreateFunction(String sql) throws Exception {
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) parseAndAnalyzeStmt(sql);
        ShowExecutor executor = new ShowExecutor(connectContext, stmt);
        return executor.execute();
    }

    protected ShowResultSet showFunctions(String sql) throws Exception {
        ShowFunctionsStmt stmt = (ShowFunctionsStmt) parseAndAnalyzeStmt(sql);
        ShowExecutor executor = new ShowExecutor(connectContext, stmt);
        return executor.execute();
    }

    protected ShowResultSet showCreateTableByName(String table) throws Exception {
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) parseAndAnalyzeStmt("show create table " + table);
        ShowExecutor executor = new ShowExecutor(connectContext, stmt);
        return executor.execute();
    }

    public void createTable(String sql) throws Exception {
        try {
            Config.enable_odbc_table = true;
            createTables(sql);
        } catch (ConcurrentModificationException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void dropTable(String table, boolean force) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) parseAndAnalyzeStmt(
                "drop table " + table + (force ? " force" : "") + ";", connectContext);
        Env.getCurrentEnv().dropTable(dropTableStmt);
    }

    public void recoverTable(String table) throws Exception {
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) parseAndAnalyzeStmt(
                "recover table " + table + ";", connectContext);
        Env.getCurrentEnv().recoverTable(recoverTableStmt);
    }

    public void createTableAsSelect(String sql) throws Exception {
        CreateTableAsSelectStmt createTableAsSelectStmt = (CreateTableAsSelectStmt) parseAndAnalyzeStmt(sql);
        Env.getCurrentEnv().createTableAsSelect(createTableAsSelectStmt);
    }

    public void createTables(String... sqls) throws Exception {
        for (String sql : sqls) {
            CreateTableStmt stmt = (CreateTableStmt) parseAndAnalyzeStmt(sql);
            Env.getCurrentEnv().createTable(stmt);
        }
        updateReplicaPathHash();
    }

    public void createView(String sql) throws Exception {
        CreateViewStmt createViewStmt = (CreateViewStmt) parseAndAnalyzeStmt(sql);
        Env.getCurrentEnv().createView(createViewStmt);
    }

    protected void createPolicy(String sql) throws Exception {
        CreatePolicyStmt createPolicyStmt = (CreatePolicyStmt) parseAndAnalyzeStmt(sql);
        Env.getCurrentEnv().getPolicyMgr().createPolicy(createPolicyStmt);
    }

    public void createFunction(String sql) throws Exception {
        CreateFunctionStmt createFunctionStmt = (CreateFunctionStmt) parseAndAnalyzeStmt(sql);
        Env.getCurrentEnv().createFunction(createFunctionStmt);
    }

    protected void dropPolicy(String sql) throws Exception {
        DropPolicyStmt stmt = (DropPolicyStmt) parseAndAnalyzeStmt(sql);
        Env.getCurrentEnv().getPolicyMgr().dropPolicy(stmt);
    }

    protected void createSqlBlockRule(String sql) throws Exception {
        Env.getCurrentEnv().getSqlBlockRuleMgr()
                .createSqlBlockRule((CreateSqlBlockRuleStmt) parseAndAnalyzeStmt(sql));
    }

    protected void alterSqlBlockRule(String sql) throws Exception {
        Env.getCurrentEnv().getSqlBlockRuleMgr()
                .alterSqlBlockRule((AlterSqlBlockRuleStmt) parseAndAnalyzeStmt(sql));
    }

    protected void dropSqlBlockRule(String sql) throws Exception {
        Env.getCurrentEnv().getSqlBlockRuleMgr()
                .dropSqlBlockRule((DropSqlBlockRuleStmt) parseAndAnalyzeStmt(sql));
    }

    protected void assertSQLPlanOrErrorMsgContains(String sql, String expect) throws Exception {
        // Note: adding `EXPLAIN` is necessary for non-query SQL, e.g., DDL, DML, etc.
        // TODO: Use a graceful way to get explain plan string, rather than modifying the SQL string.
        Assertions.assertTrue(getSQLPlanOrErrorMsg("EXPLAIN " + sql).contains(expect),
                getSQLPlanOrErrorMsg("EXPLAIN " + sql));
    }

    protected void assertSQLPlanOrErrorMsgContains(String sql, String... expects) throws Exception {
        String str = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        for (String expect : expects) {
            Assertions.assertTrue(str.contains(expect));
        }
    }

    protected void useUser(String userName) throws AnalysisException {
        UserIdentity user = new UserIdentity(userName, "%");
        user.analyze(SystemInfoService.DEFAULT_CLUSTER);
        connectContext.setCurrentUserIdentity(user);
        connectContext.setQualifiedUser(SystemInfoService.DEFAULT_CLUSTER + ":" + userName);
    }

    protected void addRollup(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().alterTable(alterTableStmt);
        // waiting alter job state: finished AND table state: normal
        checkAlterJob();
        Thread.sleep(100);
    }

    protected void alterTableSync(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().alterTable(alterTableStmt);
        Thread.sleep(100);
    }

    protected void createMv(String sql) throws Exception {
        CreateMaterializedViewStmt createMaterializedViewStmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createMaterializedView(createMaterializedViewStmt);
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(100);
    }

    private void updateReplicaPathHash() {
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

    private void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getDbId()
                        + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(100);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            try {
                // Add table state check in case of below Exception:
                // there is still a short gap between "job finish" and "table become normal",
                // so if user send next alter job right after the "job finish",
                // it may encounter "table's state not NORMAL" error.
                Database db =
                        Env.getCurrentInternalCatalog().getDbOrMetaException(alterJobV2.getDbId());
                OlapTable tbl = (OlapTable) db.getTableOrMetaException(alterJobV2.getTableId(), Table.TableType.OLAP);
                while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                    Thread.sleep(1000);
                }
            } catch (MetaNotFoundException e) {
                // Sometimes table could be dropped by tests, but the corresponding alter job is not deleted yet.
                // Ignore this error.
                System.out.println(e.getMessage());
            }
        }
    }

    // clear the specified dir
    private void cleanDir(String dir) throws IOException {
        File localDir = new File(dir);
        if (localDir.exists()) {
            Files.walk(Paths.get(dir))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(file -> {
                        System.out.println("DELETE FE SERVER DIR: " + file.getAbsolutePath());
                        file.delete();
                    });
        } else {
            System.out.println("No need clean DIR: " + dir);
        }
    }
}
