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

package org.apache.doris.cooldown;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TCooldownType;
import org.apache.doris.thrift.TStorageMedium;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class CooldownJobTest {

    private static long jobId = 100L;
    private static long dbId = 101L;
    private static long tableId = 102L;
    private static long partitionId = 103L;
    private static long indexId = 104L;
    private static long tabletId = 105L;
    private static long replicaId = 106L;
    private static long backendId = 107L;
    private static TCooldownType cooldownType = TCooldownType.UPLOAD_DATA;
    private static long timeoutMs = 10000L;
    private static Replica replica = new Replica(replicaId, backendId, 1, Replica.ReplicaState.NORMAL);

    @Mocked
    private EditLog editLog;

    public static CooldownJob createCooldownJob() {
        return new CooldownJob(jobId, dbId, tableId, partitionId, indexId, tabletId, replicaId, backendId, cooldownType,
                timeoutMs);
    }

    @Before
    public void setUp() {
        Cluster testCluster = new Cluster("test_cluster", 0);
        Database db = new Database(dbId, "db1");
        db.setClusterName("test_cluster");
        Env.getCurrentEnv().addCluster(testCluster);
        Env.getCurrentEnv().unprotectCreateDb(db);
        OlapTable table = new OlapTable(tableId, "testTable", new ArrayList<>(), KeysType.DUP_KEYS,
                new PartitionInfo(), null);
        table.setId(tableId);
        db.createTable(table);
        MaterializedIndex baseIndex = new MaterializedIndex();
        baseIndex.setIdForRestore(indexId);
        Partition partition = new Partition(partitionId, "part1", baseIndex, null);
        table.addPartition(partition);
        Tablet tablet = new Tablet(tabletId);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        tablet.addReplica(replica);
        Env.getCurrentEnv().setEditLog(editLog);
    }

    @Test
    public void testPending() throws Exception {
        CooldownJob cooldownJob = createCooldownJob();
        cooldownJob.runPendingJob();
        Assert.assertEquals(CooldownJob.JobState.SEND_CONF, cooldownJob.jobState);
        Assert.assertEquals(TCooldownType.UPLOAD_DATA, replica.getCooldownType());
        CooldownJob job1 = createCooldownJob();
        job1.replay(cooldownJob);
        Assert.assertEquals(CooldownJob.JobState.SEND_CONF, job1.jobState);
        // run send job
        cooldownJob.runSendJob();
        Assert.assertEquals(CooldownJob.JobState.RUNNING, cooldownJob.jobState);
        // run replay finish job
        cooldownJob.jobState = CooldownJob.JobState.FINISHED;
        job1.replay(cooldownJob);
        Assert.assertEquals(CooldownJob.JobState.FINISHED, job1.jobState);
    }

    @Test
    public void testCancelJob() throws Exception {
        CooldownJob cooldownJob = createCooldownJob();
        cooldownJob.runPendingJob();
        Assert.assertEquals(CooldownJob.JobState.SEND_CONF, cooldownJob.jobState);
        Assert.assertEquals(TCooldownType.UPLOAD_DATA, replica.getCooldownType());
        // run send job
        cooldownJob.runSendJob();
        Assert.assertEquals(CooldownJob.JobState.RUNNING, cooldownJob.jobState);
        // run cancel job
        cooldownJob.cancelImpl("test cancel");
        Assert.assertEquals(CooldownJob.JobState.CANCELLED, cooldownJob.jobState);
    }

}
