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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.thrift.TCooldownType;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CooldownJobTest {

    private static Replica replica = new Replica();
    private static CooldownJob cooldownJob;
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

    public static CooldownJob createCooldownJob() {
        return new CooldownJob(jobId, dbId, tableId, partitionId, indexId, tabletId, replicaId, backendId, cooldownType,
                timeoutMs);
    }
    @BeforeClass
    public static void beforeClass() throws Exception {
        cooldownJob = createCooldownJob();

        Database db = new Database(dbId, "db1");
        Env.getCurrentInternalCatalog().replayCreateDb(db, "db1");
        OlapTable table = new OlapTable();
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
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

    @Test
    public void testPending() throws Exception {
        cooldownJob.runPendingJob();
        Assert.assertEquals(CooldownJob.JobState.SEND_CONF, cooldownJob.jobState);
        CooldownJob job1 = createCooldownJob();
        cooldownJob.replay(cooldownJob);
        Assert.assertEquals(CooldownJob.JobState.SEND_CONF, job1.jobState);
    }

    @Test
    public void testSendConf() throws Exception {
        cooldownJob.runPendingJob();
        cooldownJob.runSendJob();
        Assert.assertEquals(CooldownJob.JobState.RUNNING, cooldownJob.jobState);
        CooldownJob job1 = createCooldownJob();
        cooldownJob.replay(cooldownJob);
        Assert.assertEquals(CooldownJob.JobState.RUNNING, job1.jobState);
        Assert.assertEquals(TCooldownType.UPLOAD_DATA, replica.getCooldownType());
    }

}
