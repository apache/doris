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
import org.apache.doris.datasource.SplitSink;
import org.apache.doris.spi.Split;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LocalParallelPlanningSplitProducerTest {
    @Test
    public void testStartUsesRewriteTasksFromContext() throws Exception {
        IcebergSplitPlanningSupport support = Mockito.mock(IcebergSplitPlanningSupport.class);
        FileScanTask firstTask = Mockito.mock(FileScanTask.class);
        FileScanTask secondTask = Mockito.mock(FileScanTask.class);
        Split firstSplit = Mockito.mock(Split.class);
        Split secondSplit = Mockito.mock(Split.class);
        RecordingSplitSink splitSink = new RecordingSplitSink();

        Mockito.when(support.getFileScanTasksFromContext())
                .thenReturn(Arrays.asList(firstTask, secondTask));
        Mockito.when(support.createSplit(firstTask)).thenReturn(firstSplit);
        Mockito.when(support.createSplit(secondTask)).thenReturn(secondSplit);

        LocalParallelPlanningSplitProducer producer =
                new LocalParallelPlanningSplitProducer(support, Runnable::run);
        producer.start(2, splitSink);

        Assert.assertTrue(splitSink.finished);
        Assert.assertNull(splitSink.failure);
        Assert.assertEquals(2, splitSink.batches.size());
        Assert.assertEquals(Collections.singletonList(firstSplit), splitSink.batches.get(0));
        Assert.assertEquals(Collections.singletonList(secondSplit), splitSink.batches.get(1));
        Mockito.verify(support, Mockito.never()).createTableScan();
        Mockito.verify(support, Mockito.never()).planFileScanTask(Mockito.any());
        Mockito.verify(support).recordManifestCacheProfile();
    }

    @Test
    public void testStartUsesCountPushDownBranch() throws Exception {
        IcebergSplitPlanningSupport support = Mockito.mock(IcebergSplitPlanningSupport.class);
        TableScan scan = Mockito.mock(TableScan.class);
        FileScanTask firstTask = Mockito.mock(FileScanTask.class);
        FileScanTask secondTask = Mockito.mock(FileScanTask.class);
        FileScanTask thirdTask = Mockito.mock(FileScanTask.class);
        Split firstSplit = Mockito.mock(Split.class);
        Split secondSplit = Mockito.mock(Split.class);
        RecordingSplitSink splitSink = new RecordingSplitSink();

        Mockito.when(support.getFileScanTasksFromContext()).thenReturn(null);
        Mockito.when(support.createTableScan()).thenReturn(scan);
        Mockito.when(support.planFileScanTask(scan)).thenReturn(
                CloseableIterable.withNoopClose(Arrays.asList(firstTask, secondTask, thirdTask)));
        Mockito.when(support.hasTableLevelPushDownCount()).thenReturn(true);
        Mockito.when(support.getCountPushDownSplitCount(2)).thenReturn(2);
        Mockito.when(support.createSplit(firstTask)).thenReturn(firstSplit);
        Mockito.when(support.createSplit(secondTask)).thenReturn(secondSplit);

        LocalParallelPlanningSplitProducer producer =
                new LocalParallelPlanningSplitProducer(support, Runnable::run);
        producer.start(2, splitSink);

        Assert.assertTrue(splitSink.finished);
        Assert.assertNull(splitSink.failure);
        Assert.assertEquals(1, splitSink.batches.size());
        Assert.assertEquals(Arrays.asList(firstSplit, secondSplit), splitSink.batches.get(0));
        Mockito.verify(support).assignCountToSplits(Mockito.argThat(splits ->
                splits.size() == 2 && splits.get(0) == firstSplit && splits.get(1) == secondSplit));
        Mockito.verify(support, Mockito.never()).createSplit(thirdTask);
        Mockito.verify(support).recordManifestCacheProfile();
    }

    @Test
    public void testStartReportsTranslatedFailureToSink() throws Exception {
        IcebergSplitPlanningSupport support = Mockito.mock(IcebergSplitPlanningSupport.class);
        TableScan scan = Mockito.mock(TableScan.class);
        IOException planningFailure = new IOException("plan failed");
        UserException translatedFailure =
                new UserException("wrapped: plan failed", planningFailure);
        RecordingSplitSink splitSink = new RecordingSplitSink();

        Mockito.when(support.getFileScanTasksFromContext()).thenReturn(null);
        Mockito.when(support.createTableScan()).thenReturn(scan);
        Mockito.when(support.planFileScanTask(scan)).thenThrow(planningFailure);
        Mockito.when(support.translatePlanningException(planningFailure))
                .thenReturn(translatedFailure);

        LocalParallelPlanningSplitProducer producer =
                new LocalParallelPlanningSplitProducer(support, Runnable::run);
        producer.start(1, splitSink);

        Assert.assertFalse(splitSink.finished);
        Assert.assertSame(translatedFailure, splitSink.failure);
        Mockito.verify(support, Mockito.never()).recordManifestCacheProfile();
    }

    private static class RecordingSplitSink implements SplitSink {
        private final List<List<Split>> batches = new ArrayList<>();
        private UserException failure;
        private boolean finished;

        @Override
        public void addBatch(List<Split> splits) {
            batches.add(new ArrayList<>(splits));
        }

        @Override
        public boolean needMore() {
            return true;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public void fail(UserException e) {
            failure = e;
        }
    }
}
