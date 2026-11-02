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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.datasource.SplitAssignment;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

public class MaxComputePartitionBatchTaskTest {
    @Test
    public void testBoundedContinuationUsesDedicatedBatchSize() {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        Mockito.when(assignment.needMoreSplit()).thenReturn(true);
        Queue<Runnable> queue = new ArrayDeque<>();
        List<Integer> sizes = new ArrayList<>();
        Executor executor = task -> {
            Assert.assertTrue("one queued continuation per scan", queue.isEmpty());
            queue.add(task);
        };
        new MaxComputeScanNode.PartitionBatchTask(33, 16, executor, assignment,
                (begin, end) -> sizes.add(end - begin)).submit();
        while (!queue.isEmpty()) {
            queue.remove().run();
        }
        Assert.assertEquals(Arrays.asList(16, 16, 1), sizes);
        Mockito.verify(assignment).finishSchedule();
    }

    @Test
    public void testRejectedInitialAndContinuationSubmissionsAreTerminal() {
        for (boolean rejectInitial : Arrays.asList(true, false)) {
            SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
            Mockito.when(assignment.needMoreSplit()).thenReturn(true);
            Queue<Runnable> queue = new ArrayDeque<>();
            Executor executor = task -> {
                if (rejectInitial || queue.isEmpty()) {
                    throw new RejectedExecutionException("full");
                }
                queue.add(task);
            };
            if (!rejectInitial) {
                queue.add(() -> {});
            }
            new MaxComputeScanNode.PartitionBatchTask(2, 1, executor, assignment, (begin, end) -> {}).submit();
            if (!rejectInitial) {
                queue.remove();
                queue.remove().run();
            }
            Mockito.verify(assignment).setException(Mockito.any());
            Mockito.verify(assignment).finishSchedule();
        }
    }

    @Test
    public void testFailureAndCancellationDoNotScheduleMoreWork() {
        SplitAssignment failed = Mockito.mock(SplitAssignment.class);
        Mockito.when(failed.needMoreSplit()).thenReturn(true);
        new MaxComputeScanNode.PartitionBatchTask(2, 1, Runnable::run, failed, (begin, end) -> {
            throw new java.io.IOException("session failed");
        }).submit();
        Mockito.verify(failed).setException(Mockito.any());
        Mockito.verify(failed).finishSchedule();
        SplitAssignment stopped = Mockito.mock(SplitAssignment.class);
        new MaxComputeScanNode.PartitionBatchTask(2, 1, Runnable::run, stopped, (begin, end) -> {
            Assert.fail("a cancelled scan must not open another session");
        }).submit();
        Mockito.verify(stopped).finishSchedule();
    }
}
