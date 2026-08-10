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

package org.apache.doris.clone;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TableDispatchSchedulerTest {

    @Test
    public void testEnqueueTabletsByBatch() throws Exception {
        CountDownLatch batchLatch = new CountDownLatch(2);
        List<List<TabletSchedCtx>> processedBatches = new CopyOnWriteArrayList<>();
        TableDispatchScheduler scheduler = new TableDispatchScheduler(batch -> {
            processedBatches.add(Lists.newArrayList(batch));
            batchLatch.countDown();
        }, 2);

        TabletSchedCtx tablet1 = mockTabletCtx(100L);
        TabletSchedCtx tablet2 = mockTabletCtx(100L);
        TabletSchedCtx tablet3 = mockTabletCtx(100L);

        scheduler.enqueueTablets(Lists.newArrayList(tablet1, tablet2, tablet3));

        Assert.assertTrue(batchLatch.await(3, TimeUnit.SECONDS));
        Assert.assertEquals(2, processedBatches.size());
        Assert.assertEquals(2, processedBatches.get(0).size());
        Assert.assertEquals(1, processedBatches.get(1).size());
        Assert.assertTrue(scheduler.getWorkerQueuedTablets().isEmpty());
    }

    @Test
    public void testWorkerQueueEmpty() {
        TableDispatchScheduler scheduler = new TableDispatchScheduler(batch -> {
        }, 2);

        scheduler.clear();
        Assert.assertTrue(scheduler.getWorkerQueuedTablets().isEmpty());

        List<TabletSchedCtx> appended = Lists.newArrayList();
        scheduler.appendWorkerQueuedTablets(appended, 10);
        Assert.assertTrue(appended.isEmpty());
    }

    private TabletSchedCtx mockTabletCtx(long tableId) {
        TabletSchedCtx tabletCtx = Mockito.mock(TabletSchedCtx.class);
        Mockito.when(tabletCtx.getTblId()).thenReturn(tableId);
        return tabletCtx;
    }
}
