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
import org.apache.doris.catalog.Table;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
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
    private Env env;
    private ConnectContext connectContext;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;
    private RoutineLoadJob rlJob;

    @BeforeEach
    public void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Table tbl = Mockito.mock(Table.class);
        envMockedStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException(Mockito.anyString());
        Mockito.doReturn(tbl).when(db).getTableOrAnalysisException(Mockito.anyString());
        Mockito.when(env.getRoutineLoadManager()).thenReturn(Mockito.mock(RoutineLoadManager.class));
        RoutineLoadManager rlm = env.getRoutineLoadManager();
        rlJob = Mockito.mock(RoutineLoadJob.class);
        Mockito.when(rlm.getJob(Mockito.anyString(), Mockito.anyString())).thenReturn(rlJob);
        Mockito.when(rlJob.getDbFullName()).thenReturn("testDb");
        Mockito.when(rlJob.getTableName()).thenReturn("testTable");
        Mockito.when(rlJob.isMultiTable()).thenReturn(false);
        Mockito.when(rlJob.getMergeType()).thenReturn(LoadTask.MergeType.APPEND);
        Mockito.when(rlJob.getFormat()).thenReturn("json");
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
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "true");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                labelNameInfo, jobProperties, dataSourceProperties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        Map<String, String> analyzed = command.getAnalyzedJobProperties();
        Assertions.assertEquals(9, analyzed.size());
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.STRICT_MODE));
        Assertions.assertTrue(analyzed.containsKey(CreateRoutineLoadInfo.TIMEZONE));
        Assertions.assertTrue(analyzed.containsKey(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
        Assertions.assertEquals("true",
                analyzed.get(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testValidateFillMissingColumnsFalse() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "false");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                labelNameInfo, jobProperties, dataSourceProperties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals("false",
                command.getAnalyzedJobProperties().get(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testValidateFillMissingColumnsInvalidValue() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "invalid");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                labelNameInfo, jobProperties, dataSourceProperties);
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> command.validate(connectContext));
    }

    @Test
    public void testValidateFillMissingColumnsRejectedForCsvJob() {
        runBefore();
        // The target job uses CSV format; fill_missing_columns is JSON-only and must be rejected.
        Mockito.when(rlJob.getFormat()).thenReturn("csv");
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "true");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                labelNameInfo, jobProperties, dataSourceProperties);
        org.apache.doris.common.AnalysisException e = Assertions.assertThrows(
                org.apache.doris.common.AnalysisException.class,
                () -> command.validate(connectContext));
        Assertions.assertTrue(e.getMessage().contains(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testValidateFillMissingColumnsUppercaseTrue() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "TRUE");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                new LabelNameInfo("testDb", "label1"), jobProperties, Maps.newHashMap());
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        // AlterRoutineLoad relies on JsonFileFormatProperties parsing, which accepts case-insensitive
        // "true"/"false" but stores the raw user input in the analyzed map.
        Assertions.assertEquals("TRUE",
                command.getAnalyzedJobProperties().get(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testValidateFillMissingColumnsMixedCaseFalse() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "False");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                new LabelNameInfo("testDb", "label1"), jobProperties, Maps.newHashMap());
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals("False",
                command.getAnalyzedJobProperties().get(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testValidateFillMissingColumnsEmptyStringRejected() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(
                new LabelNameInfo("testDb", "label1"), jobProperties, Maps.newHashMap());
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> command.validate(connectContext));
    }
}
