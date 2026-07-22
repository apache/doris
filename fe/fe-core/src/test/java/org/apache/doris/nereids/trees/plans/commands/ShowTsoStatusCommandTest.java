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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ShowTsoStatusCommandTest {
    private boolean originalEnableFeatureBinlog;
    private Env env;
    private TSOService tsoService;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    public void setUp() {
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        Config.enable_feature_binlog = true;

        env = Mockito.mock(Env.class);
        tsoService = Mockito.mock(TSOService.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
    }

    @Test
    public void testParse() {
        NereidsParser parser = new NereidsParser();
        LogicalPlan upperCasePlan = parser.parseSingle("SHOW TSO STATUS");
        LogicalPlan lowerCasePlan = parser.parseSingle("show tso status");

        Assertions.assertTrue(upperCasePlan instanceof ShowTsoStatusCommand);
        Assertions.assertTrue(lowerCasePlan instanceof ShowTsoStatusCommand);
    }

    @Test
    public void testMetaData() {
        List<Column> columns = new ShowTsoStatusCommand().getMetaData().getColumns();
        List<String> expectedNames = ImmutableList.of(
                "window_end_physical_time",
                "current_tso",
                "current_tso_physical_time",
                "current_tso_logical_counter");

        Assertions.assertEquals(expectedNames.size(), columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Assertions.assertEquals(expectedNames.get(i), columns.get(i).getName());
            Assertions.assertEquals(PrimitiveType.BIGINT, columns.get(i).getType().getPrimitiveType());
        }
    }

    @Test
    public void testResultDoesNotAllocateTso() throws Exception {
        long physicalTime = 1_725_000_000_000L;
        long logicalCounter = 17L;
        long currentTso = TSOTimestamp.composeTimestamp(physicalTime, logicalCounter);
        long windowEndPhysicalTime = physicalTime + 5_000L;
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, currentTso, windowEndPhysicalTime));

        ShowResultSet resultSet = new ShowTsoStatusCommand().doRun(null, null);

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(
                String.valueOf(windowEndPhysicalTime),
                String.valueOf(currentTso),
                String.valueOf(physicalTime),
                String.valueOf(logicalCounter))), resultSet.getResultRows());
        Mockito.verify(tsoService, Mockito.times(1)).getStatusSnapshot();
        Mockito.verify(tsoService, Mockito.never()).getTSO();
        Mockito.verify(tsoService, Mockito.never()).getCurrentTSO();
        Mockito.verify(tsoService, Mockito.never()).getWindowEndTSO();
    }

    @Test
    public void testDisabled() {
        Config.enable_feature_binlog = false;

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowTsoStatusCommand().doRun(null, null));

        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Mockito.verifyNoInteractions(tsoService);
    }

    @Test
    public void testNotCalibrated() {
        long currentTso = TSOTimestamp.composeTimestamp(1_725_000_000_000L, 0L);
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(false, currentTso, 0L));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowTsoStatusCommand().doRun(null, null));

        Assertions.assertTrue(exception.getMessage().contains("not calibrated"));
        Mockito.verify(tsoService, Mockito.times(1)).getStatusSnapshot();
        Mockito.verify(tsoService, Mockito.never()).getCurrentTSO();
        Mockito.verify(tsoService, Mockito.never()).getWindowEndTSO();
        Mockito.verify(tsoService, Mockito.never()).getTSO();
    }

    @Test
    public void testRedirectStatus() {
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowTsoStatusCommand().toRedirectStatus());
    }
}
