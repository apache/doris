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

package org.apache.doris.persist;

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.Journal;
import org.apache.doris.persist.EditLog.EditLogItem;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class EditLogSubmitTest {
    @Test
    public void testQueueOrderAndAwaitCompletion() throws Exception {
        boolean batch = Config.enable_batch_editlog;
        Config.enable_batch_editlog = true;
        try {
            EditLog log = Mockito.mock(EditLog.class, Mockito.CALLS_REAL_METHODS);
            BlockingQueue<EditLogItem> queue = new java.util.concurrent.LinkedBlockingQueue<>();
            Deencapsulation.setField(log, "logEditQueue", queue);
            Mockito.when(log.getNumEditStreams()).thenReturn(1);
            EditLogItem first = log.submitEdit(OperationType.OP_ALTER_MTMV, new Text("first"));
            EditLogItem second = log.submitEdit(OperationType.OP_ALTER_MTMV, new Text("second"));
            Assert.assertSame(first, queue.remove());
            Assert.assertSame(second, queue.remove());
            Assert.assertTrue(queue.isEmpty());
            CompletableFuture<Long> flushed = CompletableFuture.supplyAsync(first::await);
            Assert.assertFalse(flushed.isDone());
            synchronized (first.lock) {
                first.logId = 42L;
                first.finished = true;
                first.lock.notifyAll();
            }
            Assert.assertEquals(Long.valueOf(42), flushed.get(10, TimeUnit.SECONDS));
            Assert.assertEquals(42L, first.await());
        } finally {
            Config.enable_batch_editlog = batch;
        }
    }

    @Test
    public void testDirectSubmissionIsAlreadyDurable() throws Exception {
        boolean batch = Config.enable_batch_editlog;
        int rollNum = Config.edit_log_roll_num;
        Config.enable_batch_editlog = false;
        Config.edit_log_roll_num = Integer.MAX_VALUE;
        try {
            EditLog log = Mockito.mock(EditLog.class, Mockito.CALLS_REAL_METHODS);
            Journal journal = Mockito.mock(Journal.class);
            Deencapsulation.setField(log, "journal", journal);
            Deencapsulation.setField(log, "lastEditLogRollTimeMs", System.currentTimeMillis());
            Text record = new Text("delta");
            Mockito.when(journal.write(OperationType.OP_ALTER_MTMV, record)).thenReturn(7L);
            EditLogItem item = log.submitEdit(OperationType.OP_ALTER_MTMV, record);
            Assert.assertTrue(item.finished);
            Assert.assertEquals(7L, item.await());
            Mockito.verify(journal).write(OperationType.OP_ALTER_MTMV, record);
        } finally {
            Config.enable_batch_editlog = batch;
            Config.edit_log_roll_num = rollNum;
        }
    }

}
