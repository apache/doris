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

package org.apache.doris.master;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PublishVersionReportTest {
    private MasterImpl master;
    private PublishVersionTask task;

    @BeforeEach
    public void setUp() {
        master = new MasterImpl();
        task = new PublishVersionTask(100L, 200L, 300L, Lists.newArrayList(), 0);
        AgentTaskQueue.addTask(task);
    }

    @AfterEach
    public void tearDown() {
        ((ReportHandler) Deencapsulation.getField(master, "reportHandler")).exit();
        AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
    }

    private void report(TStatusCode status, Map<Long, Long> successes) {
        TFinishTaskRequest request = new TFinishTaskRequest();
        request.setTaskStatus(new TStatus(status));
        if (successes != null) {
            request.setSuccTablets(successes);
        }
        request.setErrorTabletIds(Lists.newArrayList(4L));
        request.setTableIdToTabletIdToDeltaNumRows(ImmutableMap.of(10L, ImmutableMap.of(1L, 20L)));
        Deencapsulation.invoke(master, "finishPublishVersion", task, request);
    }

    @Test
    public void testMergeProgressAndFinish() {
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(1L, 0L));
        Map<Long, Long> firstSnapshot = task.getSuccTablets();
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(2L, 0L));
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(1L, 0L));
        Assertions.assertEquals(ImmutableMap.of(1L, 0L), firstSnapshot);
        Assertions.assertEquals(ImmutableMap.of(1L, 0L, 2L, 0L), task.getSuccTablets());
        Assertions.assertFalse(task.isFinished());
        Assertions.assertTrue(task.getErrorTablets().isEmpty());
        Assertions.assertTrue(task.getTableIdToTabletDeltaRows().isEmpty());
        Assertions.assertSame(task, AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));

        report(TStatusCode.OK, ImmutableMap.of(3L, 0L));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertEquals(ImmutableMap.of(1L, 0L, 2L, 0L, 3L, 0L), task.getSuccTablets());
        Assertions.assertEquals(Lists.newArrayList(4L), task.getErrorTablets());
        Assertions.assertEquals(ImmutableMap.of(10L, ImmutableMap.of(1L, 20L)), task.getTableIdToTabletDeltaRows());
        Assertions.assertNull(AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));

        // An in-flight progress report can merge successes without reopening the task or adding rows.
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(4L, 0L));
        report(TStatusCode.OK, ImmutableMap.of(3L, 0L));
        Assertions.assertEquals(ImmutableMap.of(1L, 0L, 2L, 0L, 3L, 0L, 4L, 0L), task.getSuccTablets());
        Assertions.assertTrue(task.isFinished());
        Assertions.assertNull(AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));
        Assertions.assertEquals(ImmutableMap.of(10L, ImmutableMap.of(1L, 20L)), task.getTableIdToTabletDeltaRows());
    }

    @Test
    public void testLegacyFinalReport() {
        report(TStatusCode.OK, null);
        Assertions.assertTrue(task.isFinished());
        Assertions.assertTrue(task.getSuccTablets().isEmpty());
        Assertions.assertEquals(Lists.newArrayList(4L), task.getErrorTablets());
        Assertions.assertNull(AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));
    }

    @Test
    public void testProgressAfterFailedFinalReport() {
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(1L, 0L));
        report(TStatusCode.INTERNAL_ERROR, ImmutableMap.of(2L, 0L));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertSame(task, AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));

        // A failed task is resent without resetting isFinished; accept progress from that retry.
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(3L, 0L));
        report(TStatusCode.INCOMPLETE, ImmutableMap.of(3L, 0L));
        Assertions.assertTrue(task.isFinished());
        Assertions.assertEquals(ImmutableMap.of(1L, 0L, 2L, 0L, 3L, 0L), task.getSuccTablets());
        Assertions.assertEquals(Lists.newArrayList(4L), task.getErrorTablets());
        Assertions.assertTrue(task.getTableIdToTabletDeltaRows().isEmpty());
        Assertions.assertSame(task, AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));

        report(TStatusCode.OK, ImmutableMap.of(4L, 0L));
        Assertions.assertEquals(ImmutableMap.of(1L, 0L, 2L, 0L, 3L, 0L, 4L, 0L), task.getSuccTablets());
        Assertions.assertTrue(task.isFinished());
        Assertions.assertNull(AgentTaskQueue.getTask(100L, TTaskType.PUBLISH_VERSION, 200L));
        Assertions.assertEquals(ImmutableMap.of(10L, ImmutableMap.of(1L, 20L)), task.getTableIdToTabletDeltaRows());
    }
}
