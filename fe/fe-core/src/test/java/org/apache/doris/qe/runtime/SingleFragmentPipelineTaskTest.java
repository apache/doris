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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.Status;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class SingleFragmentPipelineTaskTest {
    @Test
    void backendWithUnchangedProcessEpochIsHealthy() {
        Backend backend = createBackend(100L);
        SingleFragmentPipelineTask task = createTask(backend);

        Assertions.assertTrue(task.getBackendHealthStatus(-1L).ok());
    }

    @Test
    void backendRestartIsUnhealthyEvenWhenBackendIsAlive() {
        Backend backend = createBackend(100L);
        SingleFragmentPipelineTask task = createTask(backend);

        backend.setLastStartTime(200L);

        Status status = task.getBackendHealthStatus(-1L);
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, status.getErrorCode());
        Assertions.assertEquals(
                "backend 1 process epoch changed from 100 to 200, indicating that the backend restarted",
                status.getErrorMsg());
    }

    @Test
    void zeroCurrentProcessEpochIsIgnoredForCompatibility() {
        Backend backend = createBackend(100L);
        SingleFragmentPipelineTask task = createTask(backend);

        backend.setLastStartTime(0L);

        Assertions.assertTrue(task.getBackendHealthStatus(-1L).ok());
    }

    @Test
    void backendDownAfterMissingHeartbeatIsUnhealthy() {
        Backend backend = createBackend(100L);
        SingleFragmentPipelineTask task = createTask(backend);

        backend.setLastMissingHeartbeatTime(1L);
        backend.setAlive(false);

        Status status = task.getBackendHealthStatus(-1L);
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, status.getErrorCode());
        Assertions.assertEquals("backend 1 is down", status.getErrorMsg());
    }

    private static Backend createBackend(long processEpoch) {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setAlive(true);
        backend.setLastStartTime(processEpoch);
        return backend;
    }

    private static SingleFragmentPipelineTask createTask(Backend backend) {
        return new SingleFragmentPipelineTask(backend, 0, Collections.<TUniqueId>emptySet());
    }
}
