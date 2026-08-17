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
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalBatch;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EditLogTest {
    @Test
    public void testTimestampUsesQueueWhenBatchEditLogDisabled() {
        boolean original = Config.enable_batch_editlog;
        try {
            Config.enable_batch_editlog = false;
            Assert.assertTrue(EditLog.shouldUseQueue(OperationType.OP_TIMESTAMP));
            Assert.assertTrue(EditLog.requiresDirectJournalWrite(OperationType.OP_TIMESTAMP));
            Assert.assertTrue(EditLog.shouldUseQueue(OperationType.OP_ADD_CONSTRAINT));
            Assert.assertFalse(EditLog.requiresDirectJournalWrite(OperationType.OP_ADD_CONSTRAINT));

            Config.enable_batch_editlog = true;
            Assert.assertFalse(EditLog.shouldUseQueue(OperationType.OP_TIMESTAMP));
            Assert.assertTrue(EditLog.shouldUseQueue(OperationType.OP_ADD_CONSTRAINT));
        } finally {
            Config.enable_batch_editlog = original;
        }
    }

    @Test
    public void testQueuedTimestampSplitsJournalBatchInFifoOrder() throws Exception {
        Journal journal = Mockito.mock(Journal.class);
        AtomicLong nextLogId = new AtomicLong(10);
        List<String> writes = new ArrayList<>();
        Mockito.when(journal.write(Mockito.any(JournalBatch.class))).thenAnswer(invocation -> {
            JournalBatch batch = invocation.getArgument(0);
            List<JournalBatch.Entity> entities = batch.getJournalEntities();
            writes.add("batch:" + entities.get(0).getOpCode());
            return nextLogId.getAndAdd(entities.size());
        });
        Mockito.when(journal.write(
                        Mockito.eq(OperationType.OP_TIMESTAMP), Mockito.any()))
                .thenAnswer(invocation -> {
                    writes.add("timestamp");
                    return nextLogId.getAndIncrement();
                });

        List<EditLog.EditLogItem> requests = List.of(
                new EditLog.EditLogItem(OperationType.OP_ADD_CONSTRAINT, new Text("add")),
                new EditLog.EditLogItem(OperationType.OP_TIMESTAMP, new Text("timestamp")),
                new EditLog.EditLogItem(OperationType.OP_DROP_CONSTRAINT, new Text("drop")));
        List<long[]> logIdNumPairs = EditLog.writeJournalBatch(journal, requests);

        Assert.assertEquals(List.of(
                "batch:" + OperationType.OP_ADD_CONSTRAINT,
                "timestamp",
                "batch:" + OperationType.OP_DROP_CONSTRAINT), writes);
        Assert.assertArrayEquals(new long[]{10, 1}, logIdNumPairs.get(0));
        Assert.assertArrayEquals(new long[]{11, 1}, logIdNumPairs.get(1));
        Assert.assertArrayEquals(new long[]{12, 1}, logIdNumPairs.get(2));
    }
}
