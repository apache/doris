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

package org.apache.doris.service;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tablefunction.BackendsTableValuedFunction;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TCommitTxnRequest;
import org.apache.doris.thrift.TCreatePartitionRequest;
import org.apache.doris.thrift.TCreatePartitionResult;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TLoadTxnRollbackRequest;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TNullableStringLiteral;
import org.apache.doris.thrift.TRollbackTxnRequest;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TShowUserRequest;
import org.apache.doris.thrift.TShowUserResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class FrontendServiceImplTest {
    private static String runningDir = "fe/mocked/FrontendServiceImplTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private ExecuteEnv exeEnv = Mockito.mock(ExecuteEnv.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void createTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void executeCommand(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof Command) {
            ((Command) parsed).run(connectContext, stmtExecutor);
            return;
        }
        throw new IllegalArgumentException("Unsupported command in test: " + sql);
    }


    @Test
    public void testCreatePartitionRange() throws Exception {
        String createOlapTblStmt = new String("CREATE TABLE test.partition_range(\n"
                + "    event_day DATETIME NOT NULL,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100)\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY range (date_trunc( event_day,'day')) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_range");

        List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        List<TNullableStringLiteral> values = new ArrayList<>();

        TNullableStringLiteral start = new TNullableStringLiteral();
        start.setValue("2023-08-07 00:00:00");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        Partition p20230807 = table.getPartition("p20230807000000");
        Assert.assertNotNull(p20230807);
    }

    @Test
    public void testCreatePartitionList() throws Exception {
        String createOlapTblStmt = new String("CREATE TABLE test.partition_list(\n"
                + "    event_day DATETIME,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY list (city_code) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_list");

        List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        List<TNullableStringLiteral> values = new ArrayList<>();

        TNullableStringLiteral start = new TNullableStringLiteral();
        start.setValue("BEIJING");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        List<Partition> pbs = (List<Partition>) table.getAllPartitions();
        Assert.assertEquals(pbs.size(), 1);
    }

    @Test
    public void testGetDBNames() throws Exception {
        // create database
        String createDbStmtStr = "create database `test_`;";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetDbsParams params = new TGetDbsParams();
        params.setPattern("tes%");
        params.setCurrentUserIdent(connectContext.getCurrentUserIdentity().toThrift());
        TGetDbsResult dbNames = impl.getDbNames(params);

        Assert.assertEquals(dbNames.getDbs().size(), 2);
        List<String> expected = Arrays.asList("test", "test_");
        dbNames.getDbs().sort(String::compareTo);
        expected.sort(String::compareTo);
        Assert.assertEquals(dbNames.getDbs(), expected);
    }

    @Test
    public void fetchSchemaTableData() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.setSchemaTableName(TSchemaTableName.METADATA_TABLE);

        TFetchSchemaTableDataResult result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(result.getStatus().getErrorMsgs().get(0), "Metadata table params is not set. ");

        TMetadataTableRequestParams params = new TMetadataTableRequestParams();
        request.setMetadaTableParams(params);
        result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(result.getStatus().getErrorMsgs().get(0), "Metadata table params is not set. ");

        params.setMetadataType(TMetadataType.BACKENDS);
        request.setMetadaTableParams(params);
        result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(result.getStatus().getErrorMsgs().get(0), "backends metadata param is not set.");

        params.setMetadataType(TMetadataType.BACKENDS);
        TBackendsMetadataParams backendsMetadataParams = new TBackendsMetadataParams();
        backendsMetadataParams.setClusterName("");
        params.setBackendsMetadataParams(backendsMetadataParams);
        params.setColumnsName((new BackendsTableValuedFunction(new HashMap<String, String>())).getTableColumns()
                .stream().map(c -> c.getName()).collect(Collectors.toList()));
        request.setMetadaTableParams(params);
        result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.OK);
        Assert.assertEquals(result.getDataBatchSize(), 1);
    }

    @Test
    public void testShowUser() {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TShowUserRequest request = new TShowUserRequest();
        TShowUserResult result = impl.showUser(request);
        System.out.println(result);
    }

    @Test
    public void testFetchAuthenticationIntegrationsSchemaTableData() throws Exception {
        String integrationName = "test_authentication_integration";
        Env.getCurrentEnv().getAuthenticationIntegrationMgr().dropAuthenticationIntegration(integrationName, true);

        LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "ldap");
        properties.put("server", "ldap://127.0.0.1:389");
        properties.put("bind_password", "plain_secret");
        properties.put("secret.endpoint", "masked_by_prefix");
        Env.getCurrentEnv().getAuthenticationIntegrationMgr()
                .createAuthenticationIntegration(
                        integrationName, false, properties, "ldap comment", connectContext.getQualifiedUser());

        try {
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
            request.setSchemaTableName(TSchemaTableName.AUTHENTICATION_INTEGRATIONS);
            TSchemaTableRequestParams params = new TSchemaTableRequestParams();
            params.setCurrentUserIdent(connectContext.getCurrentUserIdentity().toThrift());
            request.setSchemaTableParams(params);

            TFetchSchemaTableDataResult result = impl.fetchSchemaTableData(request);
            Assert.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());

            List<String> rowValues = result.getDataBatch().stream()
                    .filter(row -> integrationName.equals(row.getColumnValue().get(0).getStringVal()))
                    .map(row -> row.getColumnValue().stream()
                            .map(cell -> cell.isSetStringVal() ? cell.getStringVal() : null)
                            .collect(Collectors.toList()))
                    .findFirst()
                    .orElseThrow(() -> new java.lang.AssertionError("authentication integration row not found"));

            Assert.assertEquals(integrationName, rowValues.get(0));
            Assert.assertEquals("ldap", rowValues.get(1));
            Assert.assertTrue(rowValues.get(2).contains("\"server\" = \"ldap://127.0.0.1:389\""));
            Assert.assertTrue(rowValues.get(2).contains(
                    "\"bind_password\" = \"" + DatasourcePrintableMap.PASSWORD_MASK + "\""));
            Assert.assertTrue(rowValues.get(2).contains(
                    "\"secret.endpoint\" = \"" + DatasourcePrintableMap.PASSWORD_MASK + "\""));
            Assert.assertFalse(rowValues.get(2).contains("plain_secret"));
            Assert.assertFalse(rowValues.get(2).contains("masked_by_prefix"));
            Assert.assertEquals("ldap comment", rowValues.get(3));
            Assert.assertEquals(connectContext.getQualifiedUser(), rowValues.get(4));
            Assert.assertNotNull(rowValues.get(5));
            Assert.assertFalse(rowValues.get(5).isEmpty());
            Assert.assertEquals(connectContext.getQualifiedUser(), rowValues.get(6));
            Assert.assertEquals(rowValues.get(5), rowValues.get(7));
        } finally {
            Env.getCurrentEnv().getAuthenticationIntegrationMgr().dropAuthenticationIntegration(integrationName, true);
        }
    }

    @Test
    public void testFetchRoleMappingsSchemaTableData() throws Exception {
        String mappingName = "test_role_mapping_system_table";
        String integrationName = "test_role_mapping_system_table_ldap";
        String readerRole = "test_role_mapping_reader";
        String financeReaderRole = "test_role_mapping_fin_reader";
        String financeWriterRole = "test_role_mapping_fin_writer";
        String expectedRules = "RULE (USING CEL 'has_group(\"analyst\")' GRANT ROLE " + readerRole + "); "
                + "RULE (USING CEL 'attr(\"department\") == \"finance\"' GRANT ROLE "
                + financeReaderRole + ", " + financeWriterRole + ")";

        executeCommand("DROP ROLE MAPPING IF EXISTS " + mappingName);
        Env.getCurrentEnv().getAuthenticationIntegrationMgr().dropAuthenticationIntegration(integrationName, true);
        executeCommand("DROP ROLE IF EXISTS " + financeWriterRole);
        executeCommand("DROP ROLE IF EXISTS " + financeReaderRole);
        executeCommand("DROP ROLE IF EXISTS " + readerRole);

        try {
            executeCommand("CREATE ROLE " + readerRole);
            executeCommand("CREATE ROLE " + financeReaderRole);
            executeCommand("CREATE ROLE " + financeWriterRole);
            executeCommand("CREATE AUTHENTICATION INTEGRATION " + integrationName
                    + " PROPERTIES ('type'='ldap', 'ldap.server'='ldap://127.0.0.1:389') "
                    + "COMMENT 'role mapping auth'");
            executeCommand("CREATE ROLE MAPPING " + mappingName
                    + " ON AUTHENTICATION INTEGRATION " + integrationName
                    + " RULE (USING CEL 'has_group(\"analyst\")' GRANT ROLE " + readerRole + ")"
                    + ", RULE (USING CEL 'attr(\"department\") == \"finance\"' GRANT ROLE "
                    + financeReaderRole + ", " + financeWriterRole + ")"
                    + " COMMENT 'role mapping comment'");

            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
            request.setSchemaTableName(TSchemaTableName.ROLE_MAPPINGS);
            TSchemaTableRequestParams params = new TSchemaTableRequestParams();
            params.setCurrentUserIdent(connectContext.getCurrentUserIdentity().toThrift());
            request.setSchemaTableParams(params);

            TFetchSchemaTableDataResult result = impl.fetchSchemaTableData(request);
            Assert.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());

            List<String> rowValues = result.getDataBatch().stream()
                    .filter(row -> mappingName.equals(row.getColumnValue().get(0).getStringVal()))
                    .map(row -> row.getColumnValue().stream()
                            .map(cell -> cell.isSetStringVal() ? cell.getStringVal() : null)
                            .collect(Collectors.toList()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("role mapping row not found"));

            Assert.assertEquals(mappingName, rowValues.get(0));
            Assert.assertEquals(integrationName, rowValues.get(1));
            Assert.assertEquals(expectedRules, rowValues.get(2));
            Assert.assertEquals("role mapping comment", rowValues.get(3));
            Assert.assertEquals(connectContext.getQualifiedUser(), rowValues.get(4));
            Assert.assertNotNull(rowValues.get(5));
            Assert.assertFalse(rowValues.get(5).isEmpty());
            Assert.assertEquals(connectContext.getQualifiedUser(), rowValues.get(6));
            Assert.assertEquals(rowValues.get(5), rowValues.get(7));
        } finally {
            executeCommand("DROP ROLE MAPPING IF EXISTS " + mappingName);
            Env.getCurrentEnv().getAuthenticationIntegrationMgr().dropAuthenticationIntegration(integrationName, true);
            executeCommand("DROP ROLE IF EXISTS " + financeWriterRole);
            executeCommand("DROP ROLE IF EXISTS " + financeReaderRole);
            executeCommand("DROP ROLE IF EXISTS " + readerRole);
        }
    }

    @Test
    public void testFetchRoleMappingsSchemaTableDataWithoutAdmin() throws Exception {
        String mappingName = "test_role_mapping_non_admin_system_table";
        String integrationName = "test_role_mapping_non_admin_system_table_ldap";
        String readerRole = "test_role_mapping_non_admin_reader";
        String normalUser = "test_role_mapping_non_admin_user";

        executeCommand("DROP ROLE MAPPING IF EXISTS " + mappingName);
        Env.getCurrentEnv().getAuthenticationIntegrationMgr().dropAuthenticationIntegration(integrationName, true);
        executeCommand("DROP USER IF EXISTS " + normalUser);
        executeCommand("DROP ROLE IF EXISTS " + readerRole);

        try {
            executeCommand("CREATE ROLE " + readerRole);
            executeCommand("CREATE USER " + normalUser);
            executeCommand("CREATE AUTHENTICATION INTEGRATION " + integrationName
                    + " PROPERTIES ('type'='ldap', 'ldap.server'='ldap://127.0.0.1:389') "
                    + "COMMENT 'role mapping auth'");
            executeCommand("CREATE ROLE MAPPING " + mappingName
                    + " ON AUTHENTICATION INTEGRATION " + integrationName
                    + " RULE (USING CEL 'has_group(\"analyst\")' GRANT ROLE " + readerRole + ")"
                    + " COMMENT 'role mapping comment'");

            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
            request.setSchemaTableName(TSchemaTableName.ROLE_MAPPINGS);
            TSchemaTableRequestParams params = new TSchemaTableRequestParams();
            params.setCurrentUserIdent(UserIdentity.createAnalyzedUserIdentWithIp(normalUser, "%").toThrift());
            request.setSchemaTableParams(params);

            TFetchSchemaTableDataResult result = impl.fetchSchemaTableData(request);
            Assert.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
            Assert.assertEquals(1, result.getStatus().getErrorMsgsSize());
            Assert.assertEquals(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR.formatErrorMsg("ADMIN"),
                    result.getStatus().getErrorMsgs().get(0));
        } finally {
            executeCommand("DROP ROLE MAPPING IF EXISTS " + mappingName);
            Env.getCurrentEnv().getAuthenticationIntegrationMgr().dropAuthenticationIntegration(integrationName, true);
            executeCommand("DROP USER IF EXISTS " + normalUser);
            executeCommand("DROP ROLE IF EXISTS " + readerRole);
        }
    }

    @Test
    public void testLoadTxnCommitRejectsInvalidToken() {
        FrontendServiceImpl impl = Mockito.spy(new FrontendServiceImpl(exeEnv));
        TLoadTxnCommitRequest request = new TLoadTxnCommitRequest();
        request.setToken("bad-token");
        Mockito.doReturn(false).when(impl).checkToken("bad-token");

        assertInvalidToken(impl, "loadTxnCommitImpl", request);
    }

    @Test
    public void testLoadTxnRollbackRejectsInvalidToken() {
        FrontendServiceImpl impl = Mockito.spy(new FrontendServiceImpl(exeEnv));
        TLoadTxnRollbackRequest request = new TLoadTxnRollbackRequest();
        request.setToken("bad-token");
        Mockito.doReturn(false).when(impl).checkToken("bad-token");

        assertInvalidToken(impl, "loadTxnRollbackImpl", request);
    }

    @Test
    public void testCommitTxnRejectsInvalidToken() {
        FrontendServiceImpl impl = Mockito.spy(new FrontendServiceImpl(exeEnv));
        TCommitTxnRequest request = new TCommitTxnRequest();
        request.setUser("root");
        request.setPasswd("");
        request.setDb("test");
        request.setTxnId(100L);
        request.setCommitInfos(Collections.emptyList());
        request.setToken("bad-token");
        Mockito.doReturn(false).when(impl).checkToken("bad-token");

        mockTransactionForTokenValidation(100L);
        try {
            assertInvalidToken(impl, "commitTxnImpl", request);
        } finally {
            closeTransactionValidationMock();
        }
    }

    @Test
    public void testRollbackTxnRejectsInvalidToken() {
        FrontendServiceImpl impl = Mockito.spy(new FrontendServiceImpl(exeEnv));
        TRollbackTxnRequest request = new TRollbackTxnRequest();
        request.setUser("root");
        request.setPasswd("");
        request.setDb("test");
        request.setTxnId(100L);
        request.setToken("bad-token");
        Mockito.doReturn(false).when(impl).checkToken("bad-token");

        mockTransactionForTokenValidation(100L);
        try {
            assertInvalidToken(impl, "rollbackTxnImpl", request);
        } finally {
            closeTransactionValidationMock();
        }
    }

    private MockedStatic<Env> transactionValidationEnvMock;

    private void mockTransactionForTokenValidation(long txnId) {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        TransactionState transactionState = Mockito.mock(TransactionState.class);
        GlobalTransactionMgrIface globalTransactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        List<Long> tableIds = Collections.singletonList(10L);

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("test")).thenReturn(db);
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(globalTransactionMgr.getTransactionState(1L, txnId)).thenReturn(transactionState);
        Mockito.when(transactionState.getTableIdList()).thenReturn(tableIds);
        try {
            Mockito.doReturn(Collections.emptyList()).when(db).getTablesOnIdOrderOrThrowException(tableIds);
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        transactionValidationEnvMock = Mockito.mockStatic(Env.class);
        transactionValidationEnvMock.when(Env::getCurrentEnv).thenReturn(env);
        transactionValidationEnvMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTransactionMgr);
    }

    private void closeTransactionValidationMock() {
        if (transactionValidationEnvMock != null) {
            transactionValidationEnvMock.close();
            transactionValidationEnvMock = null;
        }
    }

    private void assertInvalidToken(FrontendServiceImpl impl, String methodName, Object request) {
        try {
            Method method = FrontendServiceImpl.class.getDeclaredMethod(methodName, request.getClass());
            method.setAccessible(true);
            method.invoke(impl, request);
            Assert.fail("expected invalid token");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof AuthenticationException);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid token"));
            Assert.assertFalse(e.getCause().getMessage().contains("bad-token"));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
