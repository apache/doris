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
import org.apache.doris.thrift.TTaskType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class AgentTaskCleanupDaemonTest {
    private static final long BACKEND_ID = 980001;

    @BeforeEach
    void setUp() {
        AgentTaskQueue.clearAllTasks();
        Assertions.assertNull(Env.getCurrentSystemInfo().getBackend(BACKEND_ID));
    }

    @AfterEach
    void tearDown() {
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    void expiresOldUdfCleanupWithoutAnExistingBackend() {
        long beforeCreation = System.currentTimeMillis();
        CleanUDFCacheTask recent = new CleanUDFCacheTask(BACKEND_ID, "", 1, true);
        Assertions.assertTrue(recent.createTime >= beforeCreation);
        Assertions.assertTrue(recent.createTime <= System.currentTimeMillis());
        CleanUDFCacheTask expired = new CleanUDFCacheTask(BACKEND_ID, "", 2, true);
        expired.createTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6);
        AgentTask otherType = new DropReplicaTask(BACKEND_ID, 3, 4, 0, false);
        otherType.createTime = expired.createTime;
        AgentTaskQueue.addTask(recent);
        AgentTaskQueue.addTask(expired);
        AgentTaskQueue.addTask(otherType);

        new AgentTaskCleanupDaemon().runAfterCatalogReady();

        Assertions.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, 2));
        Assertions.assertSame(recent, AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, 1));
        Assertions.assertSame(otherType,
                AgentTaskQueue.getTask(BACKEND_ID, otherType.getTaskType(), otherType.getSignature()));
        Assertions.assertEquals(2, AgentTaskQueue.getTaskNum());
    }

    @Test
    void repeatedChecksPreserveReplacementWithSameFunctionId() {
        AgentTaskCleanupDaemon daemon = new AgentTaskCleanupDaemon();
        CleanUDFCacheTask expired = new CleanUDFCacheTask(BACKEND_ID, "", 1, true);
        expired.createTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6);
        AgentTaskQueue.addTask(expired);
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());

        CleanUDFCacheTask replacement = new CleanUDFCacheTask(BACKEND_ID, "", 1, true);
        Assertions.assertTrue(AgentTaskQueue.addTask(replacement));
        daemon.runAfterCatalogReady();
        daemon.runAfterCatalogReady();
        Assertions.assertSame(replacement, AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, 1));
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
    }
}
