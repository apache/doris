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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.scheduler.disruptor.TaskDisruptor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Guards the register-then-publish contract of {@link TransientTaskManager}: {@code addMemoryTask}
 * inserts the task before it publishes the event, but {@code TaskHandler} only removes tasks it actually
 * runs. A publication failure must therefore unregister the task instead of leaking it for the FE lifetime.
 */
public class TransientTaskManagerTest {

    @Test
    public void publicationFailureUnregistersTheRegisteredTask() throws Exception {
        TransientTaskManager manager = new TransientTaskManager();
        TaskDisruptor disruptor = Mockito.mock(TaskDisruptor.class);
        Mockito.doThrow(new JobException("There is not enough available capacity in the RingBuffer."))
                .when(disruptor).tryPublishTask(Mockito.anyLong());
        Deencapsulation.setField(manager, "disruptor", disruptor);
        TransientTaskExecutor executor = Mockito.mock(TransientTaskExecutor.class);
        Mockito.when(executor.getId()).thenReturn(42L);

        // addMemoryTask performs the real map insertion first; only the event publication is stubbed to fail.
        Assertions.assertThrows(JobException.class, () -> manager.addMemoryTask(executor));
        Assertions.assertNull(manager.getMemoryTaskExecutor(42L),
                "a task whose scheduler publication failed must not stay registered");
    }

    @Test
    public void closedDisruptorFailsInsteadOfSilentlyDroppingTheTask() {
        TaskDisruptor disruptor = Mockito.mock(TaskDisruptor.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(disruptor, "isClosed", true);

        // A closed disruptor must fail the publish rather than silently leave the caller's task registered.
        Assertions.assertThrows(JobException.class, () -> disruptor.tryPublishTask(7L));
    }
}
