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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.OperationType;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MTMVAlterJournalTest {
    private MTMV createMv() {
        MTMV mv = Mockito.spy(new MTMV());
        mv.setStatus(new MTMVStatus());
        mv.setRefreshInfo(new MTMVRefreshInfo());
        mv.setMvProperties(new HashMap<>());
        mv.setJobInfo(new MTMVJobInfo("journal_test"));
        mv.setMvPartitionInfo(new MTMVPartitionInfo());
        mv.setRefreshSnapshot(new MTMVRefreshSnapshot());
        Mockito.doReturn(Collections.emptySet()).when(mv).getPartitionNames();
        return mv;
    }

    private AlterMTMV operation(MTMV mv, MTMVAlterOpType type) {
        AlterMTMV alter = new AlterMTMV(new TableNameInfo("db", "mv"), type);
        alter.setRefreshInfo(new MTMVRefreshInfo());
        alter.setStatus(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "changed"));
        alter.setMvProperties(Collections.singletonMap("grace_period", "12"));
        MTMVTask task = new MTMVTask(mv, null, null);
        task.setTaskId(1L);
        task.setStatus(TaskStatus.FAILED);
        alter.setTask(task);
        alter.setPartitionSnapshots(Collections.emptyMap());
        return alter;
    }

    private void verifyJournalBoundary(MTMVAlterOpType type) {
        MTMV mv = createMv();
        AlterMTMV alter = operation(mv, type);
        ReentrantReadWriteLock lock = Deencapsulation.getField(mv, "mvRwLock");
        Env env = Mockito.mock(Env.class);
        EditLog log = Mockito.mock(EditLog.class);
        EditLogItem item = Mockito.mock(EditLogItem.class);
        Mockito.when(env.getEditLog()).thenReturn(log);
        Mockito.when(env.getMtmvService()).thenReturn(Mockito.mock(MTMVService.class));
        Mockito.when(log.submitEdit(OperationType.OP_ALTER_MTMV, alter)).thenAnswer(invocation -> {
            Assert.assertTrue("journal enqueue must hold the mutation lock", lock.isWriteLockedByCurrentThread());
            if (type == MTMVAlterOpType.ALTER_STATUS) {
                Assert.assertEquals(MTMVState.SCHEMA_CHANGE, mv.getStatus().getState());
            }
            return item;
        });
        Mockito.when(item.await()).thenAnswer(invocation -> {
            Assert.assertFalse("journal flush must not hold the MV lock", lock.isWriteLockedByCurrentThread());
            return 1L;
        });
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLogItem pending = null;
            switch (type) {
                case ALTER_REFRESH_INFO:
                    pending = mv.alterRefreshInfo(alter, false);
                    break;
                case ALTER_STATUS:
                    pending = mv.alterStatus(alter, false);
                    break;
                case ALTER_PROPERTY:
                    pending = mv.alterMvProperties(alter, false);
                    break;
                case ADD_TASK:
                    Assert.assertTrue(mv.addTaskResult(alter, false));
                    break;
                default:
                    throw new AssertionError(type);
            }
            if (pending != null) {
                pending.await();
            }
            Mockito.verify(log).submitEdit(OperationType.OP_ALTER_MTMV, alter);
            Mockito.verify(item).await();
        }
    }

    @Test
    public void testRefreshJournalBoundary() {
        verifyJournalBoundary(MTMVAlterOpType.ALTER_REFRESH_INFO);
    }

    @Test
    public void testStatusJournalBoundary() {
        verifyJournalBoundary(MTMVAlterOpType.ALTER_STATUS);
    }

    @Test
    public void testPropertiesJournalBoundary() {
        verifyJournalBoundary(MTMVAlterOpType.ALTER_PROPERTY);
    }

    @Test
    public void testTaskJournalBoundary() {
        verifyJournalBoundary(MTMVAlterOpType.ADD_TASK);
    }

    @Test
    public void testReplayAndStaleTaskDoNotJournal() {
        MTMV mv = createMv();
        AlterMTMV task = operation(mv, MTMVAlterOpType.ADD_TASK);
        Env env = Mockito.mock(Env.class);
        EditLog log = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(log);
        Mockito.when(env.getMtmvService()).thenReturn(Mockito.mock(MTMVService.class));
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Assert.assertNull(mv.alterRefreshInfo(operation(mv, MTMVAlterOpType.ALTER_REFRESH_INFO), true));
            Assert.assertNull(mv.alterStatus(operation(mv, MTMVAlterOpType.ALTER_STATUS), true));
            Assert.assertNull(mv.alterMvProperties(operation(mv, MTMVAlterOpType.ALTER_PROPERTY), true));
            Assert.assertFalse(mv.addTaskResult(task, false));
            Assert.assertTrue(mv.addTaskResult(task, true));
            Mockito.verifyNoInteractions(log);
        }
    }
}
