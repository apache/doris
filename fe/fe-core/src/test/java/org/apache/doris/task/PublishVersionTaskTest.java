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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Regression tests for the invariant: PublishVersionTask.getSuccTablets() must never return null,
 * so that DatabaseTransactionMgr.checkReplicaContinuousVersionSucc cannot NPE when a task is
 * force-finished without a real BE response (AgentTaskCleanupDaemon path) or finished from a
 * non-OK BE callback (MasterImpl.finishPublishVersion path).
 */
public class PublishVersionTaskTest {

    private PublishVersionTask newTask() {
        return new PublishVersionTask(
                /* backendId   */ 10001L,
                /* transactionId*/ 99L,
                /* dbId        */ 1L,
                /* partitionVersionInfos */ null,
                /* createTime  */ System.currentTimeMillis());
    }

    /** Default constructor must yield a non-null succTablets. */
    @Test
    public void testDefaultSuccTabletsIsNotNull() {
        PublishVersionTask task = newTask();
        Assert.assertNotNull("succTablets must be non-null right after construction",
                task.getSuccTablets());
        Assert.assertTrue("succTablets must start empty", task.getSuccTablets().isEmpty());
        // Should not NPE.
        Assert.assertFalse(task.getSuccTablets().containsKey(1L));
    }

    /** setSuccTablets(null) must coerce to an empty map, not store null. */
    @Test
    public void testSetSuccTabletsNullCoercesToEmptyMap() {
        PublishVersionTask task = newTask();
        task.setSuccTablets(null);
        Assert.assertNotNull(task.getSuccTablets());
        Assert.assertTrue(task.getSuccTablets().isEmpty());
        Assert.assertFalse(task.getSuccTablets().containsKey(123L));
    }

    /** A populated map must be returned as-is by the getter. */
    @Test
    public void testSetSuccTabletsKeepsValues() {
        PublishVersionTask task = newTask();
        Map<Long, Long> populated = ImmutableMap.of(1L, 100L, 2L, 200L);
        task.setSuccTablets(populated);
        Assert.assertEquals(populated, task.getSuccTablets());
        Assert.assertTrue(task.getSuccTablets().containsKey(1L));
    }

    /**
     * Simulate AgentTaskCleanupDaemon.removeInactiveBeAgentTasks: the daemon flips isFinished to
     * true on every queued PublishVersionTask without ever calling setSuccTablets. Pre-fix this
     * left succTablets at the constructor's null and any caller of getSuccTablets() NPE'd.
     * After the fix, succTablets is a non-null empty map and downstream checks see
     * "no tablet succeeded" instead of crashing.
     */
    @Test
    public void testForceFinishWithoutSetSuccTabletsDoesNotNpe() {
        PublishVersionTask task = newTask();
        task.setFinished(true);
        // No setSuccTablets call — this is the AgentTaskCleanupDaemon code path.
        Map<Long, Long> succ = task.getSuccTablets();
        Assert.assertNotNull("getSuccTablets() must not return null even when force-finished", succ);
        Assert.assertTrue(task.isFinished());
        Assert.assertFalse(succ.containsKey(42L));
    }

    /**
     * Simulate MasterImpl.finishPublishVersion on a non-OK BE response that does not set the
     * succTablets field on the Thrift request. Pre-fix this stored null on the task; after the
     * fix it stores an empty map.
     */
    @Test
    public void testFinishPublishVersionPathWithNullSuccTablets() {
        PublishVersionTask task = newTask();
        task.setSuccTablets(null);     // emulates request.isSetSuccTablets() == false
        task.setFinished(true);        // matches MasterImpl ordering
        Map<Long, Long> succ = task.getSuccTablets();
        Assert.assertNotNull(succ);
        Assert.assertEquals(Collections.emptyMap(), succ);
        // The exact line that crashed pre-fix at DatabaseTransactionMgr.java:1478.
        Assert.assertFalse(succ.containsKey(7L));
    }
}
