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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.hudi.HudiFsViewCacheValue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class HudiBatchFsViewOwnerTest {

    @Test
    void statementCloseReturnsWhileRunningTaskKeepsLeasePinned() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> close = executor.submit(owner::close);
            Mockito.verify(assignment, Mockito.timeout(3000)).stop();
            close.get(3, TimeUnit.SECONDS);
            Mockito.verify(lease, Mockito.never()).close();

            owner.finish();

            Mockito.verify(lease).close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void normalCompletionDoesNotStopFinishedAssignment() {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);

        owner.finish();
        owner.close();

        Mockito.verify(assignment, Mockito.never()).stop();
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseCancelsAcceptedTaskBeforeItStarts() {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(
                () -> Assertions.fail("cancelled task must not run"), owner::finish);
        owner.track(task);

        owner.close();

        Assertions.assertTrue(task.isCancelled());
        Mockito.verify(assignment).stop();
        Mockito.verify(lease).close();
    }

    @Test
    void statementCloseDoesNotWaitForAlreadyStartedBlockedTask() throws Exception {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        HudiFsViewCacheValue.Lease lease = Mockito.mock(HudiFsViewCacheValue.Lease.class);
        HudiScanNode.BatchFsViewOwner owner = new HudiScanNode.BatchFsViewOwner(assignment, lease);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HudiScanNode.TerminalTask task = new HudiScanNode.TerminalTask(() -> {
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    interrupted.countDown();
                }
            }
        }, owner::finish);
        owner.track(task);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.execute(task);
            Assertions.assertTrue(started.await(3, TimeUnit.SECONDS));

            owner.close();

            Mockito.verify(assignment).stop();
            Assertions.assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            Mockito.verify(lease, Mockito.never()).close();
            release.countDown();
            Mockito.verify(lease, Mockito.timeout(3000)).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
