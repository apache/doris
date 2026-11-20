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

package org.apache.doris.scheduler.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.scheduler.disruptor.TaskDisruptor;
import org.apache.doris.scheduler.disruptor.TaskEvent;
import org.apache.doris.scheduler.disruptor.TaskHandler;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Guards the register-then-publish contract of {@link TransientTaskManager}: {@code addMemoryTask}
 * inserts the task before it publishes the event, but {@code TaskHandler} only removes tasks it actually
 * runs. A publication failure must therefore unregister the task instead of leaking it for the FE lifetime.
 */
public class TransientTaskManagerTest {
    @Test
    public void publicationFailureUnregistersTheRegisteredTask() throws Exception {
        JobException cause = new JobException("There is not enough available capacity in the RingBuffer.");
        assertPublicationFailure(cause);
    }

    @Test
    public void runtimePublicationFailureUnregistersTheRegisteredTask() throws Exception {
        assertPublicationFailure(new IllegalStateException("publisher unavailable"));
    }

    private void assertPublicationFailure(Exception cause) throws Exception {
        TransientTaskManager manager = new TransientTaskManager();
        TaskDisruptor disruptor = Mockito.mock(TaskDisruptor.class);
        Deencapsulation.setField(manager, "disruptor", disruptor);
        TransientTaskExecutor other = executor(41L);
        manager.addMemoryTask(other);
        TransientTaskExecutor task = executor(42L);
        Mockito.doAnswer(invocation -> {
            Assertions.assertSame(task, manager.getMemoryTaskExecutor(42L), "registration precedes publication");
            throw cause;
        }).when(disruptor).tryPublishTask(42L);

        JobException error = Assertions.assertThrows(JobException.class, () -> manager.addMemoryTask(task));
        Assertions.assertSame(cause, error.getCause());
        Assertions.assertTrue(error.getMessage().contains(cause.getMessage()));
        Assertions.assertNull(manager.getMemoryTaskExecutor(42L));
        Assertions.assertSame(other, manager.getMemoryTaskExecutor(41L));
        Mockito.verify(task, Mockito.never()).cancel();
        Assertions.assertEquals(Long.valueOf(43L), manager.addMemoryTask(executor(43L)));
    }

    @Test
    public void closedDisruptorFailsInsteadOfSilentlyDroppingTheTask() {
        TransientTaskManager manager = new TransientTaskManager();
        TaskDisruptor disruptor = new TaskDisruptor();
        disruptor.close();
        Deencapsulation.setField(manager, "disruptor", disruptor);
        JobException error = Assertions.assertThrows(JobException.class, () -> manager.addMemoryTask(executor(7L)));
        Assertions.assertTrue(error.getMessage().contains("closed"));
        Assertions.assertNull(manager.getMemoryTaskExecutor(7L));
    }

    @Test
    public void duplicateIdDoesNotReplaceOrRepublishActiveExecutor() throws Exception {
        TransientTaskManager manager = new TransientTaskManager();
        TaskDisruptor disruptor = Mockito.mock(TaskDisruptor.class);
        Deencapsulation.setField(manager, "disruptor", disruptor);
        TransientTaskExecutor first = executor(1L);
        Assertions.assertEquals(Long.valueOf(1L), manager.addMemoryTask(first));
        Assertions.assertThrows(JobException.class, () -> manager.addMemoryTask(executor(1L)));
        Assertions.assertSame(first, manager.getMemoryTaskExecutor(1L));
        Mockito.verify(disruptor, Mockito.times(1)).tryPublishTask(1L);
    }

    @Test
    public void rollbackRemovesOnlyItsOwnRegistration() throws Exception {
        TransientTaskManager manager = new TransientTaskManager();
        TaskDisruptor disruptor = Mockito.mock(TaskDisruptor.class);
        Deencapsulation.setField(manager, "disruptor", disruptor);
        ConcurrentHashMap<Long, TransientTaskExecutor> tasks = Deencapsulation.getField(manager, "taskExecutorMap");
        TransientTaskExecutor other = executor(1L);
        Mockito.doAnswer(invocation -> {
            // Exercise comparison deletion directly; production task IDs are not reused.
            tasks.put(1L, other);
            throw new JobException("rejected");
        }).when(disruptor).tryPublishTask(1L);
        Assertions.assertThrows(JobException.class, () -> manager.addMemoryTask(executor(1L)));
        Assertions.assertSame(other, manager.getMemoryTaskExecutor(1L));
    }

    @Test
    public void completedTaskNeedNotRemainRegisteredWhenAddReturns() throws Exception {
        TransientTaskManager manager = new TransientTaskManager();
        TaskDisruptor disruptor = Mockito.mock(TaskDisruptor.class);
        Deencapsulation.setField(manager, "disruptor", disruptor);
        Mockito.doAnswer(invocation -> {
            manager.removeMemoryTask(1L);
            return null;
        }).when(disruptor).tryPublishTask(1L);
        Assertions.assertEquals(Long.valueOf(1L), manager.addMemoryTask(executor(1L)));
        Assertions.assertNull(manager.getMemoryTaskExecutor(1L));
    }

    @Test
    public void originalHandlerReclaimsNormalFailedAndCancelledTasks() throws Exception {
        TransientTaskManager manager = new TransientTaskManager();
        Deencapsulation.setField(manager, "disruptor", Mockito.mock(TaskDisruptor.class));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getTransientTaskManager()).thenReturn(manager);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            TaskHandler handler = new TaskHandler();
            for (long id = 1; id <= 3; id++) {
                TransientTaskExecutor task = executor(id);
                if (id == 2) {
                    Mockito.doThrow(new JobException("execution failed")).when(task).execute();
                }
                manager.addMemoryTask(task);
                if (id == 3) {
                    manager.cancelMemoryTask(id);
                }
                TaskEvent event = new TaskEvent();
                event.setId(id);
                handler.onTransientTaskHandle(event);
                Assertions.assertNull(manager.getMemoryTaskExecutor(id));
                Mockito.verify(task, Mockito.times(id == 3 ? 0 : 1)).execute();
                Mockito.verify(task, Mockito.times(id == 3 ? 1 : 0)).cancel();
            }
        }
    }

    private static TransientTaskExecutor executor(long id) {
        TransientTaskExecutor executor = Mockito.mock(TransientTaskExecutor.class);
        Mockito.when(executor.getId()).thenReturn(id);
        return executor;
    }
}
