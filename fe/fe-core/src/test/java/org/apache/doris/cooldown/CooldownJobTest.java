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
import org.apache.doris.thrift.TStorageMedium;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CooldownJobTest {

    private static long jobId = 100L;
    private static long dbId = 101L;
    private static long tableId = 102L;
    private static long partitionId = 103L;
    private static long indexId = 104L;
    private static long tabletId = 105L;
    private static long replicaId = 106L;
    private static long backendId = 107L;
    private static long cooldownReplicaId = 106L;
    private static long cooldownTerm = 109L;
    private static long timeoutMs = 10000L;
    private static Tablet tablet = new Tablet(tabletId);
    private static Replica replica = new Replica(replicaId, backendId, 1, Replica.ReplicaState.NORMAL);

    private static CooldownConf cooldownConf = new CooldownConf(dbId, tableId, partitionId, indexId, tabletId,
            cooldownReplicaId, cooldownTerm);

    private static List<CooldownConf> cooldownConfList = new LinkedList<>();

    @Mocked
    private EditLog editLog;

    public static CooldownJob createCooldownJob() {
        tablet.setCooldownReplicaId(cooldownReplicaId);
        tablet.setCooldownTerm(cooldownTerm);
        cooldownConfList.add(cooldownConf);
        return new CooldownJob(jobId, cooldownConfList, timeoutMs);
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
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1, TStorageMedium.HDD,
                cooldownReplicaId, cooldownTerm);
        baseIndex.addTablet(tablet, tabletMeta);
        tablet.addReplica(replica);
        Env.getCurrentEnv().setEditLog(editLog);
    }

    @Test
    public void testPending() throws Exception {
        CooldownJob cooldownJob = createCooldownJob();
        cooldownJob.runPendingJob();
        Assert.assertEquals(CooldownJob.JobState.SEND_CONF, cooldownJob.jobState);
        for (CooldownConf conf : cooldownJob.getCooldownConfList()) {
            Assert.assertEquals(conf.getCooldownReplicaId(), replica.getId());
        }
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
        Assert.assertEquals(cooldownReplicaId, replica.getId());
        // run send job
        cooldownJob.runSendJob();
        Assert.assertEquals(CooldownJob.JobState.RUNNING, cooldownJob.jobState);
        // run cancel job
        cooldownJob.cancelImpl("test cancel");
        Assert.assertEquals(CooldownJob.JobState.CANCELLED, cooldownJob.jobState);
    }

}
