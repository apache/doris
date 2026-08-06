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

package org.apache.doris.transaction;

import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.Replica;
import org.apache.doris.task.PublishVersionTask;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * Regression test: even if a PublishVersionTask somehow has a null succTablets field (e.g. via a
 * future regression that re-introduces the AgentTaskCleanupDaemon force-finish path),
 * DatabaseTransactionMgr.checkReplicaContinuousVersionSucc must not throw NPE. It must treat the
 * replica as "publish not yet succeeded for this tablet" and route it through the normal
 * error/version-failed branches.
 */
public class CheckReplicaContinuousVersionSuccTest {

    private static final long BACKEND_ID = 10001L;
    private static final long TXN_ID = 99L;
    private static final long DB_ID = 1L;
    private static final long TABLET_ID = 3001L;
    private static final long REPLICA_ID = 2001L;

    private PublishVersionTask newFinishedTaskWithNullSuccTablets() throws Exception {
        PublishVersionTask task = new PublishVersionTask(BACKEND_ID, TXN_ID, DB_ID,
                /* partitionVersionInfos */ null, System.currentTimeMillis());
        task.setFinished(true);
        // Force succTablets to null via reflection. After the constructor-level fix the field
        // is initialized to an empty map, so we simulate the pre-fix bad state to independently
        // exercise the call-site null guard in checkReplicaContinuousVersionSucc.
        Field f = PublishVersionTask.class.getDeclaredField("succTablets");
        f.setAccessible(true);
        f.set(task, null);
        Assert.assertNull("precondition: succTablets must be null for this test",
                task.getSuccTablets());
        return task;
    }

    private void invokeCheck(Set<Long> errorReplicaIds,
            List<Replica> tabletSuccReplicas,
            List<Replica> tabletWriteFailedReplicas,
            List<Replica> tabletVersionFailedReplicas,
            PublishVersionTask task,
            Replica replica,
            long minReplicaVersion, long maxReplicaVersion) throws Exception {
        DatabaseTransactionMgr mgr =
                Mockito.mock(DatabaseTransactionMgr.class, Mockito.CALLS_REAL_METHODS);
        Method m = DatabaseTransactionMgr.class.getDeclaredMethod(
                "checkReplicaContinuousVersionSucc",
                List.class, long.class, long.class,
                Replica.class, long.class, long.class,
                List.class, Set.class, List.class, List.class, List.class);
        m.setAccessible(true);
        try {
            m.invoke(mgr,
                    Lists.newArrayList(TXN_ID), -1L, TABLET_ID,
                    replica, minReplicaVersion, maxReplicaVersion,
                    Lists.newArrayList(task), errorReplicaIds,
                    tabletSuccReplicas, tabletWriteFailedReplicas, tabletVersionFailedReplicas);
        } catch (InvocationTargetException ite) {
            if (ite.getCause() instanceof NullPointerException) {
                Assert.fail("checkReplicaContinuousVersionSucc threw NPE on null succTablets: "
                        + ite.getCause());
            }
            throw ite;
        }
    }

    /**
     * Negative case: task.isFinished() is true but task.getSuccTablets() is null. Pre-fix this
     * NPE'd at line 1478. Post-fix it must treat the replica as a write-failure (or similar
     * non-success branch) without throwing.
     */
    @Test
    public void testNoNpeWhenSuccTabletsIsNull() throws Exception {
        PublishVersionTask task = newFinishedTaskWithNullSuccTablets();
        Replica replica = new LocalReplica(REPLICA_ID, BACKEND_ID, /*version*/100L, /*schemaHash*/0,
                /*dataSize*/0L, /*remoteDataSize*/0L, /*rowCount*/0L,
                Replica.ReplicaState.NORMAL, /*lastFailedVersion*/-1L, /*lastSuccessVersion*/100L);

        Set<Long> errorReplicaIds = Sets.newHashSet();
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();

        // replica.version (100) < maxReplicaVersion (101) → after the failure branch,
        // replica should land in tabletWriteFailedReplicas.
        invokeCheck(errorReplicaIds, tabletSuccReplicas, tabletWriteFailedReplicas,
                tabletVersionFailedReplicas, task, replica, /*minReplicaVersion*/100L,
                /*maxReplicaVersion*/101L);

        Assert.assertTrue("replica should be classified as write-failed when succTablets is null",
                tabletWriteFailedReplicas.contains(replica));
        Assert.assertTrue(tabletSuccReplicas.isEmpty());
        Assert.assertTrue(tabletVersionFailedReplicas.isEmpty());
    }

    /**
     * Positive case: task.isFinished() is true, succTablets contains the tablet — replica must
     * be treated as success and removed from errorReplicaIds.
     */
    @Test
    public void testHappyPathWhenSuccTabletsContainsTabletId() throws Exception {
        PublishVersionTask task = new PublishVersionTask(BACKEND_ID, TXN_ID, DB_ID, null,
                System.currentTimeMillis());
        task.setFinished(true);
        java.util.Map<Long, Long> populated = new java.util.HashMap<>();
        populated.put(TABLET_ID, 100L);
        task.setSuccTablets(populated);

        Replica replica = new LocalReplica(REPLICA_ID, BACKEND_ID, /*version*/100L, /*schemaHash*/0,
                /*dataSize*/0L, /*remoteDataSize*/0L, /*rowCount*/0L,
                Replica.ReplicaState.NORMAL, /*lastFailedVersion*/-1L, /*lastSuccessVersion*/100L);

        Set<Long> errorReplicaIds = Sets.newHashSet(REPLICA_ID); // pretend it was tagged earlier
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();

        invokeCheck(errorReplicaIds, tabletSuccReplicas, tabletWriteFailedReplicas,
                tabletVersionFailedReplicas, task, replica, /*minReplicaVersion*/100L,
                /*maxReplicaVersion*/100L);

        Assert.assertFalse("happy path must clear the replica from errorReplicaIds",
                errorReplicaIds.contains(REPLICA_ID));
        Assert.assertTrue("happy path must add the replica to tabletSuccReplicas",
                tabletSuccReplicas.contains(replica));
    }

    /**
     * Task is null in the list (older code path observed in production logs). Should be treated
     * as a failure without NPE.
     */
    @Test
    public void testNoNpeWhenTaskIsNull() throws Exception {
        Replica replica = new LocalReplica(REPLICA_ID, BACKEND_ID, /*version*/100L, /*schemaHash*/0,
                /*dataSize*/0L, /*remoteDataSize*/0L, /*rowCount*/0L,
                Replica.ReplicaState.NORMAL, /*lastFailedVersion*/-1L, /*lastSuccessVersion*/100L);

        Set<Long> errorReplicaIds = Sets.newHashSet();
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();

        invokeCheck(errorReplicaIds, tabletSuccReplicas, tabletWriteFailedReplicas,
                tabletVersionFailedReplicas, /*task*/null, replica, 100L, 101L);

        Assert.assertTrue(tabletWriteFailedReplicas.contains(replica));
    }
}
