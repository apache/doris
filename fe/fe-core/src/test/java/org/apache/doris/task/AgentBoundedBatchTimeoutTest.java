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

package org.apache.doris.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

class AgentBoundedBatchTimeoutTest {
    @Test
    void largeTimeoutDoesNotClassifyRecentHeartbeatAsExpired() {
        int original = Config.agent_task_be_unavailable_heartbeat_timeout_second;
        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            Config.agent_task_be_unavailable_heartbeat_timeout_second = 2147484;
            SystemInfoService system = Mockito.mock(SystemInfoService.class);
            current.when(Env::getCurrentSystemInfo).thenReturn(system);
            Backend backend = Mockito.mock(Backend.class);
            Mockito.when(system.getBackend(1L)).thenReturn(backend);
            Mockito.when(backend.isAlive()).thenReturn(false);
            Mockito.when(backend.getLastUpdateMs()).thenReturn(System.currentTimeMillis());
            AgentTask task = Mockito.mock(AgentTask.class);
            Mockito.when(task.getBackendId()).thenReturn(1L);
            AtomicReference<String> firstMessage = new AtomicReference<>();
            Mockito.doAnswer(call -> {
                firstMessage.compareAndSet(null, call.getArgument(0));
                // The decision is complete. End the loop's three-second sleep immediately.
                Thread.currentThread().interrupt();
                return null;
            }).when(task).failedWithMsg(Mockito.anyString());
            // One task and one offline backend suffice. No RPC or tablet is needed.
            AgentBoundedBatchTask batch = new AgentBoundedBatchTask(1, 1);
            batch.addTask(task);
            batch.run();
            Assertions.assertEquals("backend 1 is not alive", firstMessage.get());
        } finally {
            Thread.interrupted();
            Config.agent_task_be_unavailable_heartbeat_timeout_second = original;
        }
    }
}
