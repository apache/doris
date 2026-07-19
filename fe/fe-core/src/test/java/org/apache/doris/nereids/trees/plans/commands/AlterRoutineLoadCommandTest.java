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
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kinesis.KinesisDataSourceProperties;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;

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
        Mockito.when(routineLoadManager.checkPrivAndGetJob(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(routineLoadJob);
        Mockito.when(routineLoadJob.getDbFullName()).thenReturn("testDb");
        Mockito.when(routineLoadJob.getTableName()).thenReturn("testTable");
        Mockito.when(routineLoadJob.isMultiTable()).thenReturn(false);
        Mockito.when(routineLoadJob.getMergeType()).thenReturn(LoadTask.MergeType.APPEND);
        Mockito.when(routineLoadJob.getDataSourceType()).thenReturn(LoadDataSourceType.KAFKA);
        Mockito.when(routineLoadJob.getTimezone()).thenReturn("UTC");
        Mockito.when(routineLoadJob.isLoadToSingleTablet()).thenReturn(false);
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

    private void mockTargetTable(Table table) throws AnalysisException {
        Mockito.doReturn(table).when(db).getTableOrAnalysisException("testTable2");
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
    public void testParseAlterRoutineLoadSetTargetTable() {
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\"");
        Assertions.assertEquals("testDb", command.getDbName());
        Assertions.assertEquals("label1", command.getJobName());
        Assertions.assertTrue(command.hasTargetTable());
        Assertions.assertEquals("testTable2", command.getTargetTableName());

        AlterRoutineLoadCommand escapedNameCommand = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR label1 SET TARGET TABLE = \"test\"\"Table2\"");
        Assertions.assertEquals("test\"Table2", escapedNameCommand.getTargetTableName());

        AlterRoutineLoadCommand emptyNameCommand = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR label1 SET TARGET TABLE = \"\"");
        Assertions.assertTrue(emptyNameCommand.hasTargetTable());
        Assertions.assertEquals("", emptyNameCommand.getTargetTableName());
    }

    @Test
    public void testParseAlterRoutineLoadSetTargetTableRejectsUnsupportedSyntax() {
        Assertions.assertThrows(ParseException.class, () -> PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 ON testTable2"));
        Assertions.assertThrows(ParseException.class, () -> PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = testTable2"));
        Assertions.assertThrows(ParseException.class, () -> PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\" COLUMNS(k1)"));
    }

    @Test
    public void testValidateTargetTableWithJobAndDataSourceProperties() throws Exception {
        runBefore();
        Mockito.when(currentTable.getId()).thenReturn(3000L);
        mockTargetTable(currentTable);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\" "
                        + "PROPERTIES(\"max_error_number\"=\"1\") "
                        + "FROM KAFKA(\"property.client.id\"=\"target-switch\")");

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals("1", command.getAnalyzedJobProperties()
                .get(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertInstanceOf(KafkaDataSourceProperties.class, command.getDataSourceProperties());
        Assertions.assertEquals("target-switch", command.getDataSourceProperties()
                .getOriginalDataSourceProperties().get("property.client.id"));
        Assertions.assertEquals(3000L, command.getTargetTableId());
        Mockito.verify(routineLoadJob).validateTargetTable(db, currentTable);
    }

    @Test
    public void testValidateTargetTableOnlyDoesNotRequireOtherProperties() throws Exception {
        runBefore();
        Mockito.when(currentTable.getId()).thenReturn(3000L);
        mockTargetTable(currentTable);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\"");

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertTrue(command.getAnalyzedJobProperties().isEmpty());
        Assertions.assertNull(command.getDataSourceProperties());
        Assertions.assertEquals(3000L, command.getTargetTableId());
    }

    @Test
    public void testValidateTargetTableWithKinesisDataSourceProperties() throws Exception {
        runBefore();
        Mockito.when(routineLoadJob.getDataSourceType()).thenReturn(LoadDataSourceType.KINESIS);
        Mockito.when(currentTable.getId()).thenReturn(3000L);
        mockTargetTable(currentTable);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\" "
                        + "PROPERTIES(\"max_error_number\"=\"1\") "
                        + "FROM KINESIS(\"property.client.timeout\"=\"1000\")");

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertInstanceOf(KinesisDataSourceProperties.class, command.getDataSourceProperties());
        Assertions.assertEquals("1000", command.getDataSourceProperties()
                .getOriginalDataSourceProperties().get("property.client.timeout"));
        Assertions.assertEquals(3000L, command.getTargetTableId());
        Mockito.verify(routineLoadJob).validateTargetTable(db, currentTable);
    }

    @Test
    public void testValidateTargetTableRejectsMultiTableJob() throws Exception {
        runBefore();
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\"");

        Mockito.when(routineLoadJob.isMultiTable()).thenReturn(true);
        Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(connectContext)).getMessage().contains("single-table"));
    }

    @Test
    public void testValidateUnauthorizedTargetDoesNotResolveMetadata() throws Exception {
        runBefore();
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.eq("testDb"), Mockito.eq("testTable2"), Mockito.eq(PrivPredicate.LOAD)))
                .thenReturn(false);
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\"");

        Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(connectContext)).getMessage().contains("LOAD"));
        Mockito.verify(routineLoadManager).checkPrivAndGetJob("testDb", "label1");
        Mockito.verify(catalog, Mockito.never()).getDbOrAnalysisException(Mockito.anyString());
    }

    @Test
    public void testValidateUnauthorizedJobDoesNotResolveTarget() throws Exception {
        runBefore();
        Mockito.when(routineLoadManager.checkPrivAndGetJob("testDb", "label1"))
                .thenThrow(new AnalysisException("access denied"));
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\"");

        Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(connectContext)).getMessage().contains("access denied"));
        Mockito.verify(accessManager, Mockito.never()).checkTblPriv(
                Mockito.any(ConnectContext.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any());
        Mockito.verify(catalog, Mockito.never()).getDbOrAnalysisException(Mockito.anyString());
    }

    @Test
    public void testValidateTargetTableConstraints() throws Exception {
        runBefore();
        AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) PARSER.parseSingle(
                "ALTER ROUTINE LOAD FOR testDb.label1 SET TARGET TABLE = \"testTable2\"");

        mockTargetTable(Mockito.mock(Table.class));
        Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(connectContext)).getMessage().contains("only supports OLAP table"));

        OlapTable temporaryTable = Mockito.mock(OlapTable.class);
        Mockito.when(temporaryTable.isTemporary()).thenReturn(true);
        mockTargetTable(temporaryTable);
        Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(connectContext)).getMessage().contains("temporary table"));

        OlapTable hashDistributedTable = Mockito.mock(OlapTable.class);
        Mockito.when(hashDistributedTable.isTemporary()).thenReturn(false);
        mockTargetTable(hashDistributedTable);
        Mockito.when(routineLoadJob.isLoadToSingleTablet()).thenReturn(true);
        Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(connectContext)).getMessage().contains("load_to_single_tablet"));

        RandomDistributionInfo randomDistributionInfo = Mockito.mock(RandomDistributionInfo.class);
        Mockito.when(hashDistributedTable.getDefaultDistributionInfo()).thenReturn(randomDistributionInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}
