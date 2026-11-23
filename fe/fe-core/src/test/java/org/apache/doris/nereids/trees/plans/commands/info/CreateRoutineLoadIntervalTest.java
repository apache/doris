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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

class CreateRoutineLoadIntervalTest {
    private MockedStatic<ConnectContext> contextMock;

    @BeforeEach
    void setUp() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        contextMock = Mockito.mockStatic(ConnectContext.class);
        contextMock.when(ConnectContext::get).thenReturn(context);
    }

    @AfterEach
    void tearDown() {
        contextMock.close();
    }

    private CreateRoutineLoadInfo createInfo(Map<String, String> properties) {
        // Property analysis needs no table, rows, Kafka broker, or persisted job.
        return new CreateRoutineLoadInfo(new LabelNameInfo("testDb", "interval_job"), "testTable",
                Map.of(), properties, "KAFKA", Map.of(), LoadTask.MergeType.APPEND, "");
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 9223372036854776L, Long.MAX_VALUE})
    void rejectInvalidInterval(long seconds) {
        // 9223372036854776 is the first positive second count that overflows int64 milliseconds.
        CreateRoutineLoadInfo info = createInfo(Map.of("max_batch_interval", Long.toString(seconds)));
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, info::checkJobProperties);
        Assertions.assertTrue(error.getMessage().contains(
                "max_batch_interval must be between 1 and 9223372036854775"), error.getMessage());
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 9223372036854775L})
    void acceptValidIntervalBoundaries(long seconds) throws Exception {
        // The largest valid value must remain exact; admission must not narrow or clamp it.
        CreateRoutineLoadInfo info = createInfo(Map.of("max_batch_interval", Long.toString(seconds)));
        info.checkJobProperties();
        Assertions.assertEquals(seconds, info.getMaxBatchIntervalS());
    }

    @Test
    void preserveDefaultInterval() throws Exception {
        CreateRoutineLoadInfo info = createInfo(Map.of());
        info.checkJobProperties();
        Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_INTERVAL_SECOND, info.getMaxBatchIntervalS());
    }

    @Test
    void replayHistoricalCreateInterval() throws Exception {
        // Replay parses the original CREATE to rebuild load descriptors. A later ALTER can have
        // corrected this interval. Rejecting the old statement would cancel the restored job.
        CreateRoutineLoadInfo info = createInfo(Map.of("max_batch_interval", Long.toString(Long.MAX_VALUE)));
        info.setReplay(true);
        info.checkJobProperties();
        Assertions.assertEquals(Long.MAX_VALUE, info.getMaxBatchIntervalS());
    }
}
