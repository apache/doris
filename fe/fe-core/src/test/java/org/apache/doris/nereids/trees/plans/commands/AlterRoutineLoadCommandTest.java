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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class AlterRoutineLoadCommandTest {
    private static final NereidsParser PARSER = new NereidsParser();

    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessManager;
    private InternalCatalog catalog;
    private Database db;
    private OlapTable currentTable;
    private RoutineLoadManager routineLoadManager;
    private RoutineLoadJob routineLoadJob;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        catalog = Mockito.mock(InternalCatalog.class);
        db = Mockito.mock(Database.class);
        currentTable = Mockito.mock(OlapTable.class);
        routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        routineLoadJob = Mockito.mock(RoutineLoadJob.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
        Mockito.when(connectContext.getQualifiedUser()).thenReturn("testUser");
        Mockito.when(connectContext.getRemoteIP()).thenReturn("127.0.0.1");
        envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException(Mockito.anyString());
        Mockito.doReturn(currentTable).when(db).getTableOrAnalysisException(Mockito.anyString());
        Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(true);
        Mockito.when(routineLoadManager.getJob(Mockito.anyString(), Mockito.anyString())).thenReturn(routineLoadJob);
        Mockito.when(routineLoadJob.getDbFullName()).thenReturn("testDb");
        Mockito.when(routineLoadJob.getTableName()).thenReturn("testTable");
        Mockito.when(routineLoadJob.getDbId()).thenReturn(1000L);
        Mockito.when(routineLoadJob.getTableId()).thenReturn(2000L);
        Mockito.when(routineLoadJob.isMultiTable()).thenReturn(false);
        Mockito.when(routineLoadJob.getMergeType()).thenReturn(LoadTask.MergeType.APPEND);
        Mockito.when(routineLoadJob.isLoadToSingleTablet()).thenReturn(false);
        Mockito.when(routineLoadJob.getUniqueKeyUpdateMode()).thenReturn(TUniqueKeyUpdateMode.UPSERT);
        Mockito.when(currentTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(currentTable.isTemporary()).thenReturn(false);
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    private void runBefore() {
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
    }

    private void mockTargetTable(Table table) {
        try {
            Mockito.doReturn(table).when(db).getTableOrAnalysisException("testTable2");
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testValidate() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        jobProperties.put(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "100");
        jobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.01");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY, "10");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY, "300000");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY, "1048576000");
        jobProperties.put(CreateRoutineLoadInfo.STRICT_MODE, "false");
        jobProperties.put(CreateRoutineLoadInfo.TIMEZONE, "Asia/Shanghai");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(labelNameInfo, jobProperties, dataSourceProperties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        Assertions.assertEquals(8, command.getAnalyzedJobProperties().size());
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.STRICT_MODE));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.TIMEZONE));
    }

    @Test
    public void testParseAlterRoutineLoadOnTargetTable() {
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");
        Assertions.assertEquals("testDb", command.getDbName());
        Assertions.assertEquals("label1", command.getJobName());
        Assertions.assertTrue(command.hasTargetTable());
        Assertions.assertEquals("testTable2", command.getTargetTableName());
    }

    @Test
    public void testParseAlterRoutineLoadOnTargetTableRejectMixedProperties() {
        Assertions.assertThrows(ParseException.class, () -> PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2 PROPERTIES(\"max_error_number\"=\"1\")"));
        Assertions.assertThrows(ParseException.class, () -> PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2 FROM KAFKA(\"kafka_offsets\"=\"100\")"));
        Assertions.assertThrows(ParseException.class, () -> PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2 COLUMNS(k1)"));
    }

    @Test
    public void testValidateTargetTableOnlyDoesNotRequireOtherProperties() throws Exception {
        runBefore();
        mockTargetTable(currentTable);

        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals("testTable2", command.getTargetTableName());
    }

    @Test
    public void testValidateTargetTableRejectsMultiTableJob() throws Exception {
        runBefore();
        Mockito.when(routineLoadJob.isMultiTable()).thenReturn(true);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertTrue(Assertions.assertThrows(Exception.class, () -> command.validate(connectContext))
                .getMessage().contains("single-table"));
    }

    @Test
    public void testValidateTargetTableRejectsWithoutLoadPrivilege() throws Exception {
        runBefore();
        mockTargetTable(currentTable);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.eq("testDb"), Mockito.eq("testTable2"), Mockito.any())).thenReturn(false);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertTrue(Assertions.assertThrows(Exception.class, () -> command.validate(connectContext))
                .getMessage().contains("LOAD"));
    }

    @Test
    public void testValidateTargetTableRejectsTemporaryTable() throws Exception {
        runBefore();
        OlapTable tempTable = Mockito.mock(OlapTable.class);
        Mockito.when(tempTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(tempTable.isTemporary()).thenReturn(true);
        mockTargetTable(tempTable);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertTrue(Assertions.assertThrows(Exception.class, () -> command.validate(connectContext))
                .getMessage().contains("temporary table"));
    }

    @Test
    public void testValidateTargetTableRejectsLoadToSingleTabletWithoutRandomDistribution() throws Exception {
        runBefore();
        OlapTable newTable = Mockito.mock(OlapTable.class);
        Mockito.when(newTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(newTable.isTemporary()).thenReturn(false);
        Mockito.when(newTable.getDefaultDistributionInfo()).thenReturn(null);
        mockTargetTable(newTable);
        Mockito.when(routineLoadJob.isLoadToSingleTablet()).thenReturn(true);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertTrue(Assertions.assertThrows(Exception.class, () -> command.validate(connectContext))
                .getMessage().contains("load_to_single_tablet"));
    }

    @Test
    public void testValidateTargetTableAllowsLoadToSingleTabletWithRandomDistribution() throws Exception {
        runBefore();
        OlapTable newTable = Mockito.mock(OlapTable.class);
        RandomDistributionInfo distributionInfo = Mockito.mock(RandomDistributionInfo.class);
        Mockito.when(newTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(newTable.isTemporary()).thenReturn(false);
        Mockito.when(newTable.getDefaultDistributionInfo()).thenReturn(distributionInfo);
        mockTargetTable(newTable);
        Mockito.when(routineLoadJob.isLoadToSingleTablet()).thenReturn(true);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testValidateTargetTablePassesTargetTableToJobValidation() throws Exception {
        runBefore();
        OlapTable targetTable = Mockito.mock(OlapTable.class);
        Mockito.when(targetTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(targetTable.isTemporary()).thenReturn(false);
        Mockito.doReturn(targetTable).when(db).getTableOrAnalysisException("testTable2");
        Mockito.when(routineLoadJob.isLoadToSingleTablet()).thenReturn(false);
        Mockito.doAnswer(invocation -> {
            Assertions.assertSame(db, invocation.getArgument(0));
            Assertions.assertSame(targetTable, invocation.getArgument(1));
            return null;
        }).when(routineLoadJob).validateTargetTable(Mockito.any(Database.class), Mockito.any(OlapTable.class));

        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testValidateTargetTableRejectsExistingFlexiblePartialUpdateOnNonMowTable() throws Exception {
        runBefore();
        OlapTable targetTable = Mockito.mock(OlapTable.class);
        Mockito.when(targetTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(targetTable.isTemporary()).thenReturn(false);
        Mockito.when(targetTable.getEnableUniqueKeyMergeOnWrite()).thenReturn(false);
        Mockito.doReturn(targetTable).when(db).getTableOrAnalysisException("testTable2");
        Mockito.when(routineLoadJob.getUniqueKeyUpdateMode()).thenReturn(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS);
        Mockito.doThrow(new AnalysisException("Only unique key merge on write support partial update"))
                .when(routineLoadJob).validateTargetTable(Mockito.any(Database.class), Mockito.any(OlapTable.class));

        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2");

        Assertions.assertTrue(Assertions.assertThrows(Exception.class, () -> command.validate(connectContext))
                .getMessage().contains("partial update"));
    }

    @Test
    public void testValidateRejectsUniqueKeyUpdateModeOnNonMowTable() {
        runBefore();
        Mockito.when(routineLoadJob.getUniqueKeyUpdateMode()).thenReturn(TUniqueKeyUpdateMode.UPSERT);
        Mockito.when(currentTable.getEnableUniqueKeyMergeOnWrite()).thenReturn(false);
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FIXED_COLUMNS");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                new LabelNameInfo("testDb", "label1"), jobProperties, Maps.newHashMap());

        Assertions.assertTrue(Assertions.assertThrows(Exception.class, () -> command.validate(connectContext))
                .getMessage().contains("PARTIAL_COLUMNS"));
    }
}
