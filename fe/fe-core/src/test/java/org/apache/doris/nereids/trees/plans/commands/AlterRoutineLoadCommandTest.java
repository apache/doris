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
        RoutineLoadJob rlJob = Mockito.mock(RoutineLoadJob.class);
        Mockito.when(rlm.getJob(Mockito.anyString(), Mockito.anyString())).thenReturn(rlJob);
        Mockito.when(rlJob.getDbFullName()).thenReturn("testDb");
        Mockito.when(rlJob.getTableName()).thenReturn("testTable");
        Mockito.when(rlJob.isMultiTable()).thenReturn(false);
        Mockito.when(rlJob.getMergeType()).thenReturn(LoadTask.MergeType.APPEND);
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
}
