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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class CreateRoutineLoadInfoTest {
    private Env env;
    private ConnectContext connectContext;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
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

    private CreateRoutineLoadInfo buildInfo(Map<String, String> jobProperties) {
        Map<String, LoadProperty> loadPropertyMap = Maps.newHashMap();
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("kafka_broker_list", "127.0.0.1:9092");
        dataSourceProperties.put("kafka_topic", "test_topic");
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");
        return new CreateRoutineLoadInfo(labelNameInfo, "testTable", loadPropertyMap,
                jobProperties, LoadDataSourceType.KAFKA.name(), dataSourceProperties,
                LoadTask.MergeType.APPEND, "");
    }

    @Test
    public void testCheckJobPropertiesFillMissingColumnsRejectedForCsvDefault() {
        // format is omitted, defaults to CSV; fill_missing_columns is JSON-only and must be rejected
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "treu");

        CreateRoutineLoadInfo info = buildInfo(jobProperties);
        org.apache.doris.common.AnalysisException e = Assertions.assertThrows(
                org.apache.doris.common.AnalysisException.class, info::checkJobProperties);
        Assertions.assertTrue(e.getMessage().contains(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testCheckJobPropertiesFillMissingColumnsRejectedForExplicitCsv() {
        // format is explicitly CSV; even a syntactically valid value must be rejected
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(FileFormatProperties.PROP_FORMAT, "csv");
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "true");

        CreateRoutineLoadInfo info = buildInfo(jobProperties);
        org.apache.doris.common.AnalysisException e = Assertions.assertThrows(
                org.apache.doris.common.AnalysisException.class, info::checkJobProperties);
        Assertions.assertTrue(e.getMessage().contains(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testCheckJobPropertiesFillMissingColumnsAcceptedForJson() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "true");

        CreateRoutineLoadInfo info = buildInfo(jobProperties);
        Assertions.assertDoesNotThrow(info::checkJobProperties);
    }

    @Test
    public void testCheckJobPropertiesFillMissingColumnsInvalidValueForJson() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "treu");

        CreateRoutineLoadInfo info = buildInfo(jobProperties);
        // the JSON format properties validator rejects invalid boolean values with its own
        // AnalysisException type (nereids.exceptions), distinct from common.AnalysisException
        Assertions.assertThrows(AnalysisException.class, info::checkJobProperties);
    }
}
