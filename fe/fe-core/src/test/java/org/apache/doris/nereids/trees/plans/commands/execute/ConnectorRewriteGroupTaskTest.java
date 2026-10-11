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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards the cancellation/publication handoff of {@link ConnectorRewriteGroupTask}. The distributed write
 * path needs a live cluster, but the handoff itself is unit-testable: a queued task must report its
 * cancellation through the collector, and a cancel that lands after the executor is published must reach it.
 */
public class ConnectorRewriteGroupTaskTest {

    private ConnectorRewriteGroupTask newTask(
            AtomicBoolean completed,
            AtomicReference<Long> failedId,
            AtomicReference<Exception> failure) {
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0);
        return new ConnectorRewriteGroupTask(
                group,
                7L,
                Mockito.mock(ConnectorTransaction.class),
                Mockito.mock(ExternalTable.class),
                Mockito.mock(ConnectContext.class),
                new ConnectorRewriteGroupTask.RewriteResultCallback() {
                    @Override
                    public void onTaskCompleted(Long taskId) {
                        completed.set(true);
                    }

                    @Override
                    public void onTaskFailed(Long taskId, Exception error) {
                        failedId.set(taskId);
                        failure.set(error);
                    }
                });
    }

    @Test
    public void queuedCancellationIsReportedToTheCollector() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);

        // Cancelled while still queued: the scheduler may still invoke execute().
        task.cancel();
        Assertions.assertThrows(JobException.class, task::execute);

        Assertions.assertFalse(completed.get(), "a cancelled task must never report completion");
        Assertions.assertEquals(task.getId(), failedId.get(), "the collector must be told this task failed");
        Assertions.assertNotNull(failure.get());
    }

    @Test
    public void cancelAfterExecutorPublicationCancelsTheExecutor() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);

        // Simulate a running task whose executor has been published.
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Deencapsulation.setField(task, "stmtExecutor", stmtExecutor);

        task.cancel();

        Mockito.verify(stmtExecutor).cancel(Mockito.any(Status.class));
        Assertions.assertFalse(completed.get());
    }

    @Test
    public void cancelAfterFinishIsIgnored() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);
        ((AtomicBoolean) Deencapsulation.getField(task, "isFinished")).set(true);
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Deencapsulation.setField(task, "stmtExecutor", stmtExecutor);

        task.cancel();

        Mockito.verify(stmtExecutor, Mockito.never()).cancel(Mockito.any(Status.class));
    }
}
