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
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRowPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.DropSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropTableCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.planner.Planner;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
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
import mockit.Mock;
import mockit.MockUp;
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
import java.util.ArrayList;
import java.util.Comparator;
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
 * thus we could wrap common logic in this base class. It's easier to use.
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
    protected int lastFeRpcPort = 0;
    // make it default to enable_advance_next_id
    protected boolean enableAdvanceNextId = Config.enable_advance_next_id;

    protected static final String DEFAULT_CLUSTER_PREFIX = "";

    @BeforeAll
    public final void beforeAll() throws Exception {
        // this.enableAdvanceNextId may be reset by children classes
        Config.enable_advance_next_id = this.enableAdvanceNextId;
        FeConstants.enableInternalSchemaDb = false;
        FeConstants.disableWGCheckerForUT = true;
        beforeCreatingConnectContext();
        connectContext = createDefaultCtx();
        connectContext.getSessionVariable().feDebug = true;
        beforeCluster();
        createDorisCluster();
        Env.getCurrentEnv().getWorkloadGroupMgr().createNormalWorkloadGroupForUT();
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

    protected boolean needDiffHost() {
        return false;
    }

    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx() throws IOException {
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
        Set<String> originDisableRules = connectContext.getSessionVariable().getDisableNereidsRuleNames();
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
        Set<String> originDisableRules = ctx.getSessionVariable().getDisableNereidsRuleNames();
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
        Set<String> originDisableRules = ctx.getSessionVariable().getDisableNereidsRuleNames();
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

    public static ConnectContext createCtx(UserIdentity user, String host) throws IOException {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(user);
        ctx.setRemoteIP(host);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext());
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
        StatementBase statementBase = null;
        try {
            List<StatementBase> stmts = (List<StatementBase>) parser.parse().value;
            statementBase = stmts.get(0);
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
        statementBase.analyze();
        return statementBase;
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
        int arrowFlightSqlPort = findValidPort();
        int feEditLogPort = findValidPort();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("http_port", String.valueOf(feHttpPort));
        feConfMap.put("rpc_port", String.valueOf(feRpcPort));
        feConfMap.put("query_port", String.valueOf(feQueryPort));
        feConfMap.put("arrow_flight_sql_port", String.valueOf(arrowFlightSqlPort));
        feConfMap.put("edit_log_port", String.valueOf(feEditLogPort));
        feConfMap.put("tablet_create_timeout_second", "10");
        // start fe in "DORIS_HOME/fe/mocked/"
        MockedFrontend frontend = new MockedFrontend();
        frontend.init(dorisHome + "/" + runningDir, feConfMap);
        frontend.start(new String[0]);
        lastFeRpcPort = feRpcPort;
        return feRpcPort;
    }

    protected void createDorisCluster()
            throws InterruptedException, NotInitException, IOException, DdlException, EnvVarNotSetException,
            FeStartException {
        createDorisClusterWithMultiTag(runningDir, backendNum());
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
        if (!checkBEHeartbeat(bes)) {
            System.out.println("Some backends dead, all backends: " + bes);
        }
        // Thread.sleep(2000);
        System.out.println("after create backend2");
    }

    // Create multi backends with different host for unit test.
    // the host of BE will be "127.0.0.1", "127.0.0.2"
    protected void createDorisClusterWithMultiTag(String runningDir, int backendNum)
            throws EnvVarNotSetException, IOException, FeStartException, NotInitException, DdlException,
            InterruptedException {
        // set runningUnitTest to true, so that for ut, the agent task will be send to "127.0.0.1"
        // to make cluster running well.
        if (backendNum > 1) {
            FeConstants.runningUnitTest = true;
        }
        int feRpcPort = startFEServer(runningDir);
        List<Backend> bes = Lists.newArrayList();
        System.out.println("start create backend, backend num " + backendNum);
        for (int i = 0; i < backendNum; i++) {
            String host = "127.0.0." + (i + 1);
            bes.add(createBackend(host, feRpcPort));
        }
        System.out.println("after create backend");
        if (!checkBEHeartbeat(bes)) {
            System.out.println("Some backends dead, all backends: " + bes);
        }
        System.out.println("after create backend2");
    }

    protected boolean checkBEHeartbeat(List<Backend> bes) {
        return checkBEHeartbeatStatus(bes, true);
    }

    protected boolean checkBELostHeartbeat(List<Backend> bes) {
        return checkBEHeartbeatStatus(bes, false);
    }

    private boolean checkBEHeartbeatStatus(List<Backend> bes, boolean isAlive) {
        int maxTry = Config.heartbeat_interval_second + 2;
        while (maxTry-- > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // no exception
            }
            if (bes.stream().allMatch(be -> be.isAlive() == isAlive)) {
                return true;
            }
        }

        return false;
    }

    protected Backend addNewBackend() throws IOException, InterruptedException {
        Backend be = createBackend("127.0.0.1", lastFeRpcPort);
        checkBEHeartbeat(Lists.newArrayList(be));
        return be;
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
        int beArrowFlightSqlPort = findValidPort();

        // start be
        MockedBackendFactory.BeThriftService beThriftService = new DefaultBeThriftServiceImpl();
        MockedBackend backend = MockedBackendFactory.createBackend(beHost, beHeartbeatPort, beThriftPort, beBrpcPort,
                beHttpPort, beArrowFlightSqlPort, new DefaultHeartbeatServiceImpl(beThriftPort, beHttpPort, beBrpcPort, beArrowFlightSqlPort),
                beThriftService, new DefaultPBackendServiceImpl());
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", feRpcPort));
        backend.start();

        // add be
        Backend be = new Backend(Env.getCurrentEnv().getNextId(), backend.getHost(), backend.getHeartbeatPort());
        DiskInfo diskInfo1 = new DiskInfo("/path" + be.getId());
        diskInfo1.setPathHash(be.getId());
        diskInfo1.setTotalCapacityB(10L << 40);
        diskInfo1.setAvailableCapacityB(5L << 40);
        diskInfo1.setDataUsedCapacityB(480000);
        diskInfo1.setPathHash(be.getId());
        Map<String, DiskInfo> disks = Maps.newHashMap();
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(false);
        be.setBePort(beThriftPort);
        be.setHttpPort(beHttpPort);
        be.setBrpcPort(beBrpcPort);
        be.setArrowFlightSqlPort(beArrowFlightSqlPort);
        beThriftService.setBackendInFe(be);
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
            return planner.getExplainString(new ExplainOptions(isVerbose, false, false));
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
        if (connectContext.getState().getStateType() != QueryState.MysqlStateType.ERR
                && connectContext.getState().getErrorCode() == null) {
            return stmtExecutor;
        } else {
            // throw new IllegalStateException(connectContext.getState().getErrorMessage());
            return null;
        }
    }

    public void executeSql(String queryStr) throws Exception {
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, queryStr);
        stmtExecutor.execute();
        if (connectContext.getState().getStateType() == QueryState.MysqlStateType.ERR
                || connectContext.getState().getErrorCode() != null) {
            throw new IllegalStateException(connectContext.getState().getErrorMessage());
        }
    }

    public void createDatabase(String db) throws Exception {
        String createDbStmtStr = "CREATE DATABASE " + db;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
    }

    public void createDatabaseAndUse(String db) throws Exception {
        createDatabase(db);
        useDatabase(db);
    }

    public void createDatabaseWithSql(String createDbSql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbSql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbSql);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
    }

    public void dropDatabase(String db) throws Exception {
        String dropDbStmtStr = "DROP DATABASE " + db;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(dropDbStmtStr);
        if (logicalPlan instanceof DropDatabaseCommand) {
            ((DropDatabaseCommand) logicalPlan).run(connectContext, null);
        }
    }

    public void dropDatabaseWithSql(String dropDbSql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(dropDbSql);
        if (logicalPlan instanceof DropDatabaseCommand) {
            ((DropDatabaseCommand) logicalPlan).run(connectContext, null);
        }
    }

    public void useDatabase(String dbName) {
        connectContext.setDatabase(dbName);
    }

    public void createTable(String sql) throws Exception {
        createTable(sql, true);
    }

    public void createTable(String sql, boolean enableNerieds) throws Exception {
        try {
            createTables(enableNerieds, sql);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void dropTable(String table, boolean force) throws Exception {
        String sql;
        if (force) {
            sql = "drop table " + table;
        } else {
            sql = "drop table if exists " + table;
        }
        dropTableWithSql(sql);
    }

    public void dropTableWithSql(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropTableCommand) {
            ((DropTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    public void recoverTable(String table) throws Exception {
        RecoverTableStmt recoverTableStmt = (RecoverTableStmt) parseAndAnalyzeStmt(
                "recover table " + table + ";", connectContext);
        Env.getCurrentEnv().recoverTable(recoverTableStmt);
    }

    public void createCatalog(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(connectContext, null);
        }
    }

    public CatalogIf getCatalog(String name) throws Exception {
        return Env.getCurrentEnv().getCatalogMgr().getCatalog(name);
    }

    public void createTables(String... sqls) throws Exception {
        createTables(true, sqls);
    }

    public void createTables(boolean enableNereids, String... sqls) throws Exception {
        createTablesAndReturnPlans(enableNereids, sqls);
    }

    public List<LogicalPlan> createTablesAndReturnPlans(boolean enableNereids, String... sqls) throws Exception {
        List<LogicalPlan> logicalPlans = new ArrayList<>();
        if (enableNereids) {
            for (String sql : sqls) {
                NereidsParser nereidsParser = new NereidsParser();
                LogicalPlan parsed = nereidsParser.parseSingle(sql);
                logicalPlans.add(parsed);
                StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
                if (parsed instanceof CreateTableCommand) {
                    ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
                }
            }
        } else {
            for (String sql : sqls) {
                CreateTableStmt stmt = (CreateTableStmt) parseAndAnalyzeStmt(sql);
                Env.getCurrentEnv().createTable(stmt);
            }
        }
        updateReplicaPathHash();
        return logicalPlans;
    }

    public void createView(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        CreateViewCommand command = (CreateViewCommand) nereidsParser.parseSingle(sql);
        command.run(connectContext, new StmtExecutor(connectContext, sql));
    }

    protected void createPolicy(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        CreatePolicyCommand command = (CreatePolicyCommand) nereidsParser.parseSingle(sql);
        command.run(connectContext, new StmtExecutor(connectContext, sql));
    }

    public void createFunction(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        CreateFunctionCommand command = (CreateFunctionCommand) nereidsParser.parseSingle(sql);
        command.run(connectContext, new StmtExecutor(connectContext, sql));
    }

    public void addConstraint(String sql) throws Exception {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AddConstraintCommand) {
            ((AddConstraintCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    public void dropConstraint(String sql) throws Exception {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropConstraintCommand) {
            ((DropConstraintCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    protected void dropPolicy(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        DropRowPolicyCommand command = (DropRowPolicyCommand) nereidsParser.parseSingle(sql);
        TableNameInfo tableNameInfo = command.getTableNameInfo();
        DropPolicyLog dropPolicyLog = new DropPolicyLog(tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), PolicyTypeEnum.ROW, command.getPolicyName(), command.getUser(), command.getRoleName());
        Env.getCurrentEnv().getPolicyMgr().dropPolicy(dropPolicyLog, command.isIfExists());
    }

    protected void createSqlBlockRule(String sql) throws Exception {
        NereidsParser parser = new NereidsParser();
        CreateSqlBlockRuleCommand command = (CreateSqlBlockRuleCommand) parser.parseSingle(sql);
        Env.getCurrentEnv().getSqlBlockRuleMgr()
                .createSqlBlockRule(new SqlBlockRule(command.getRuleName(), command.getSql(), command.getSqlHash(),
                command.getPartitionNum(), command.getTabletNum(), command.getCardinality(),
                command.getGlobal(), command.getEnable()), command.isIfNotExists());
    }

    protected void alterSqlBlockRule(String sql) throws Exception {
        NereidsParser parser = new NereidsParser();
        AlterSqlBlockRuleCommand command = (AlterSqlBlockRuleCommand) parser.parseSingle(sql);
        Env.getCurrentEnv().getSqlBlockRuleMgr()
                .alterSqlBlockRule(new SqlBlockRule(command.getRuleName(), command.getSql(), command.getSqlHash(),
                command.getPartitionNum(), command.getTabletNum(), command.getCardinality(),
                command.getGlobal(), command.getEnable()));
    }

    protected void dropSqlBlockRule(String sql) throws Exception {
        NereidsParser parser = new NereidsParser();
        DropSqlBlockRuleCommand command = (DropSqlBlockRuleCommand) parser.parseSingle(sql);
        Env.getCurrentEnv().getSqlBlockRuleMgr().dropSqlBlockRule(command.getRuleNames(), command.isIfExists());
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
        useUser(userName, "%");
    }

    protected void useUser(String userName, String host) throws AnalysisException {
        UserIdentity user = new UserIdentity(userName, host);
        user.analyze();
        connectContext.setCurrentUserIdentity(user);
    }

    protected void addUser(String userName, boolean ifNotExists) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "create user " + (ifNotExists ? "if not exists " : "") + userName + "@'%'";
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateUserCommand) {
            ((CreateUserCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    protected void createRole(String roleName) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "CREATE ROLE " + roleName;
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateRoleCommand) {
            ((CreateRoleCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    protected void grantRole(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof GrantRoleCommand) {
            ((GrantRoleCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    protected void grantPriv(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof GrantTablePrivilegeCommand) {
            ((GrantTablePrivilegeCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    protected void addRollup(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().alterTable(alterTableStmt);
        // waiting alter job state: finished AND table state: normal
        checkAlterJob();
        Thread.sleep(100);
    }

    protected void alterTableSync(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
        Thread.sleep(100);
    }

    protected void createMv(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateMaterializedViewCommand) {
            ((CreateMaterializedViewCommand) parsed).run(connectContext, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(100);
    }

    protected void alterMv(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterMTMVCommand) {
            ((AlterMTMVCommand) parsed).run(connectContext, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(1000);
    }


    protected void createMvByNereids(String sql) throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logCreateTable(CreateTableInfo info) {
                System.out.println("skip log create table...");
            }

            @Mock
            public void logCreateJob(AbstractJob job) {
                System.out.println("skip log create job...");
            }
        };
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateMTMVCommand) {
            ((CreateMTMVCommand) parsed).run(connectContext, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(1000);

    }

    protected void dropMvByNereids(String sql) throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logCreateTable(CreateTableInfo info) {
                System.out.println("skip log create table...");
            }

            @Mock
            public void logCreateJob(AbstractJob job) {
                System.out.println("skip log create job...");
            }
        };
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropMTMVCommand) {
            ((DropMTMVCommand) parsed).run(connectContext, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(1000);

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
            if (tabletMeta == null) {
                continue;
            }
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
