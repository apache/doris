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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.spi.Split;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class LocalParallelPlanningSplitProducerTest {
    private static final ExecutionAuthenticator NOOP_AUTHENTICATOR = new ExecutionAuthenticator() {
    };

    @Test
    public void testStartSuccess() throws Exception {
        LocalParallelPlanningSplitProducer.PlanningContext context = Mockito.mock(
                LocalParallelPlanningSplitProducer.PlanningContext.class);
        SplitSink sink = Mockito.mock(SplitSink.class);
        TableScan scan = Mockito.mock(TableScan.class);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Split split = Mockito.mock(Split.class);

        Mockito.when(context.isBatchMode()).thenReturn(true);
        Mockito.when(context.numApproximateSplits()).thenReturn(1);
        Mockito.when(context.getExecutionAuthenticator()).thenReturn(NOOP_AUTHENTICATOR);
        Mockito.when(context.getScheduleExecutor()).thenReturn(Runnable::run);
        Mockito.when(context.createTableScan()).thenReturn(scan);
        Mockito.when(context.planFileScanTask(scan))
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(task)));
        Mockito.when(context.createSplit(task)).thenReturn(split);
        Mockito.when(context.checkNotSupportedException(Mockito.any(Exception.class))).thenReturn(Optional.empty());
        Mockito.when(sink.needMore()).thenReturn(true);

        LocalParallelPlanningSplitProducer producer = new LocalParallelPlanningSplitProducer(context);
        producer.start(1, sink);

        ArgumentCaptor<java.util.List<Split>> splitCaptor = ArgumentCaptor.forClass(java.util.List.class);
        Mockito.verify(sink).addBatch(splitCaptor.capture());
        Assertions.assertEquals(1, splitCaptor.getValue().size());
        Assertions.assertEquals(split, splitCaptor.getValue().get(0));
        Mockito.verify(sink).finish();
        Mockito.verify(sink, Mockito.never()).fail(Mockito.any());
        Mockito.verify(context).recordManifestCacheProfile();
    }

    @Test
    public void testStartFail() throws Exception {
        LocalParallelPlanningSplitProducer.PlanningContext context = Mockito.mock(
                LocalParallelPlanningSplitProducer.PlanningContext.class);
        SplitSink sink = Mockito.mock(SplitSink.class);
        RuntimeException expected = new RuntimeException("boom");

        Mockito.when(context.getExecutionAuthenticator()).thenReturn(NOOP_AUTHENTICATOR);
        Mockito.when(context.getScheduleExecutor()).thenReturn(Runnable::run);
        Mockito.when(context.createTableScan()).thenThrow(expected);

        LocalParallelPlanningSplitProducer producer = new LocalParallelPlanningSplitProducer(context);
        UserException userException = Assertions.assertThrows(UserException.class, () -> producer.start(1, sink));
        Assertions.assertTrue(userException.getMessage().contains("boom"));
        Mockito.verify(sink, Mockito.never()).fail(Mockito.any());
        Mockito.verify(sink, Mockito.never()).finish();
    }

    @Test
    public void testStopOnSinkNeedMoreFalse() throws Exception {
        LocalParallelPlanningSplitProducer.PlanningContext context = Mockito.mock(
                LocalParallelPlanningSplitProducer.PlanningContext.class);
        SplitSink sink = Mockito.mock(SplitSink.class);
        TableScan scan = Mockito.mock(TableScan.class);
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);

        Mockito.when(context.getExecutionAuthenticator()).thenReturn(NOOP_AUTHENTICATOR);
        Mockito.when(context.getScheduleExecutor()).thenReturn(Runnable::run);
        Mockito.when(context.createTableScan()).thenReturn(scan);
        Mockito.when(context.planFileScanTask(scan))
                .thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));
        Mockito.when(context.checkNotSupportedException(Mockito.any(Exception.class))).thenReturn(Optional.empty());
        Mockito.when(sink.needMore()).thenReturn(false);

        LocalParallelPlanningSplitProducer producer = new LocalParallelPlanningSplitProducer(context);
        producer.start(1, sink);

        Mockito.verify(sink, Mockito.never()).addBatch(Mockito.anyList());
        Mockito.verify(sink).finish();
        Mockito.verify(context).recordManifestCacheProfile();
    }

    @Test
    public void testStopIdempotent() throws Exception {
        LocalParallelPlanningSplitProducer.PlanningContext context = Mockito.mock(
                LocalParallelPlanningSplitProducer.PlanningContext.class);
        SplitSink sink = Mockito.mock(SplitSink.class);
        TableScan scan = Mockito.mock(TableScan.class);

        Mockito.when(context.getExecutionAuthenticator()).thenReturn(NOOP_AUTHENTICATOR);
        Mockito.when(context.getScheduleExecutor()).thenReturn(Runnable::run);
        Mockito.when(context.createTableScan()).thenReturn(scan);
        Mockito.when(context.planFileScanTask(scan))
                .thenReturn(CloseableIterable.withNoopClose(Collections.emptyList()));
        Mockito.when(context.checkNotSupportedException(Mockito.any(Exception.class))).thenReturn(Optional.empty());
        Mockito.when(sink.needMore()).thenReturn(false);

        LocalParallelPlanningSplitProducer producer = new LocalParallelPlanningSplitProducer(context);
        producer.start(1, sink);
        Assertions.assertDoesNotThrow(() -> {
            producer.stop();
            producer.stop();
        });
    }
}
