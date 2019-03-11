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

package org.apache.doris.clone;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.CloneJob.JobPriority;
import org.apache.doris.clone.CloneJob.JobState;
import org.apache.doris.clone.CloneJob.JobType;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletInfo;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ Catalog.class })
public class CloneTest {
    private Clone clone;
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;

    @Before
    public void setUp() {
        clone = new Clone();
        dbId = 0L;
        tableId = 0L;
        partitionId = 0L;
        indexId = 0L;

        Catalog.getCurrentInvertedIndex().clear();
    }
    
    @Test
    public void testAddCloneJob() {
        // check job num
        Assert.assertEquals(0, clone.getJobNum());
        
        // add tablet0 clone job
        long tabletId = 0L;
        long backendId = 0L;
        JobType type = JobType.SUPPLEMENT;
        JobPriority priority = JobPriority.LOW;
        long timeoutSecond = 3600L;
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority, timeoutSecond));
        // add same tablet clone job 
        Assert.assertFalse(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                             type, priority,
                timeoutSecond));
        // check job num
        Assert.assertEquals(1, clone.getJobNum());

        // check job state and priority
        List<CloneJob> pendingJobs = clone.getCloneJobs(JobState.PENDING);
        Assert.assertEquals(1, pendingJobs.size());
        CloneJob job = pendingJobs.get(0);
        Assert.assertEquals(JobState.PENDING, job.getState());
        Assert.assertEquals(JobPriority.LOW, job.getPriority());
        // change job priority
        priority = JobPriority.NORMAL;
        Assert.assertFalse(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                             type, priority,
                timeoutSecond));
        pendingJobs = clone.getCloneJobs(JobState.PENDING);
        Assert.assertEquals(1, pendingJobs.size());
        job = pendingJobs.get(0);
        Assert.assertEquals(JobState.PENDING, job.getState());
        Assert.assertEquals(JobPriority.NORMAL, job.getPriority());
 
        // test job num threshold
        Config.clone_max_job_num = 2;
        // add tablet1 clone job
        tabletId = 1L;
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority,
                timeoutSecond));
        // add tablet2 low priority clone job error
        tabletId = 2L;
        priority = JobPriority.LOW;
        Assert.assertFalse(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                             type, priority,
                timeoutSecond));
        // add tablet2 high priority clone job success
        priority = JobPriority.NORMAL;
        Assert.assertFalse(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority, timeoutSecond));
    }
    
    @Test
    public void testCheckTimeout() {
        // add two clone job
        long tabletId = 0L;
        long backendId = 0L;
        JobType type = JobType.SUPPLEMENT;
        JobPriority priority = JobPriority.LOW;
        long timeoutSecond = 3600L;
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority, timeoutSecond));
        Assert.assertTrue(clone.getCloneTabletIds().contains(tabletId));

        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1, TStorageMedium.HDD);
        Catalog.getCurrentInvertedIndex().addTablet(tabletId, tabletMeta);
        Replica replica = new Replica();
        Catalog.getCurrentInvertedIndex().addReplica(tabletId, replica);

        tabletId = 1L;
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority, timeoutSecond));
        Assert.assertTrue(clone.getCloneTabletIds().contains(tabletId));
        Assert.assertEquals(2, clone.getJobNum());
        
        Replica replica2 = new Replica();
        Catalog.getCurrentInvertedIndex().addTablet(tabletId, tabletMeta);
        Catalog.getCurrentInvertedIndex().addReplica(tabletId, replica2);

        // change tablet0 creationTime
        List<CloneJob> pendingJobs = clone.getCloneJobs(JobState.PENDING);
        for (CloneJob job : pendingJobs) {
            if (job.getTabletId() == 0L) {
                job.setCreateTimeMs(job.getCreateTimeMs() - 3600 * 1000L - 1L);
            }
        }
        // check timeout
        clone.checkTimeout();
        Assert.assertEquals(2, clone.getJobNum());
        
        // remove cancelled clone job
        clone.removeCloneJobs();
        pendingJobs = clone.getCloneJobs(JobState.PENDING);
        Assert.assertEquals(1, pendingJobs.size());
        CloneJob job = pendingJobs.get(0);
        Assert.assertEquals(1L, job.getTabletId());
    }
    
    @Test
    public void testCancelCloneJob() {
        // add tablet0 clone job
        long tabletId = 0L;
        long backendId = 0L;
        JobType type = JobType.SUPPLEMENT;
        JobPriority priority = JobPriority.LOW;
        long timeoutSecond = 3600L;
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority, timeoutSecond));
        
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1, TStorageMedium.HDD);
        Catalog.getCurrentInvertedIndex().addTablet(tabletId, tabletMeta);
        Replica replica = new Replica();
        Catalog.getCurrentInvertedIndex().addReplica(tabletId, replica);


        // cancel clone job
        List<CloneJob> pendingJobs = clone.getCloneJobs(JobState.PENDING);
        CloneJob job = pendingJobs.get(0);
        job.setState(JobState.RUNNING);
        clone.cancelCloneJob(job, "timeout");
        Assert.assertEquals(1, clone.getJobNum());
        List<CloneJob> cancelledJobs = clone.getCloneJobs(JobState.CANCELLED);
        Assert.assertEquals(1, cancelledJobs.size());
        Assert.assertEquals("timeout", cancelledJobs.get(0).getFailMsg());
        
        // remove cancelled clone jobs
        clone.removeCloneJobs();
        Assert.assertEquals(0, clone.getJobNum());
    }
    
    @Test
    public void testFinishCloneJob() {
        // add tablet0 clone job
        long tabletId = 0L;
        long backendId = 0L;
        long version = 1L;
        long versionHash = 0L;
        int schemaHash = UnitTestUtil.SCHEMA_HASH;
        JobType type = JobType.SUPPLEMENT;
        JobPriority priority = JobPriority.LOW;
        long timeoutSecond = 3600L;
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority, timeoutSecond));
        Assert.assertEquals(1, clone.getJobNum());

        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1, TStorageMedium.HDD);
        Catalog.getCurrentInvertedIndex().addTablet(tabletId, tabletMeta);
        Replica replica = new Replica();
        Catalog.getCurrentInvertedIndex().addReplica(tabletId, replica);

        // finish clone job: db does not exist
        List<CloneJob> pendingJobs = clone.getCloneJobs(JobState.PENDING);
        CloneJob job = pendingJobs.get(0);
        job.setState(JobState.RUNNING);
        CloneTask task = new CloneTask(backendId, dbId, tableId, partitionId, indexId, tabletId,
                schemaHash, new ArrayList<TBackend>(), TStorageMedium.HDD,
                version, versionHash);
        TTabletInfo tabletInfo = new TTabletInfo(tabletId, schemaHash, version, versionHash, 0L, 0L);
        clone.finishCloneJob(task, tabletInfo);
        Assert.assertEquals(1, clone.getJobNum());
        List<CloneJob> cancelledJobs = clone.getCloneJobs(JobState.CANCELLED);
        Assert.assertEquals(1, cancelledJobs.size());
        
        // remove cancelled clone job
        clone.removeCloneJobs();
        Assert.assertEquals(0, clone.getJobNum());
        
        // add tablet0 clone job again
        Assert.assertTrue(clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            type, priority,
                timeoutSecond));
        Assert.assertEquals(1, clone.getJobNum());
        pendingJobs = clone.getCloneJobs(JobState.PENDING);
        job = pendingJobs.get(0);
        job.setState(JobState.RUNNING);

        // finish clone job success
        Database db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId,
                                            version, versionHash);
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        MaterializedIndex index = partition.getBaseIndex();
        Tablet tablet = index.getTablet(tabletId);
        Replica replica2 = tablet.getReplicaByBackendId(backendId);
        replica2.setState(ReplicaState.CLONE);

        Catalog catalog = EasyMock.createMock(Catalog.class);
        EasyMock.expect(catalog.getDb(EasyMock.anyLong())).andReturn(db).anyTimes();
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        EasyMock.replay(catalog);

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        clone.finishCloneJob(task, tabletInfo);
        Assert.assertEquals(1, clone.getJobNum());
        List<CloneJob> finishedJobs = clone.getCloneJobs(JobState.FINISHED);
        Assert.assertEquals(1, finishedJobs.size());
        Assert.assertEquals(ReplicaState.NORMAL, replica2.getState());
    }
    
    @Test
    public void testCalculatePriority() {
        short onlineReplicaNum = 2;
        short replicationNum = 3;
        Assert.assertEquals(JobPriority.LOW, Clone.calculatePriority(onlineReplicaNum, replicationNum));
        
        onlineReplicaNum = 2;
        replicationNum = 4;
        Assert.assertEquals(JobPriority.NORMAL, Clone.calculatePriority(onlineReplicaNum, replicationNum));
        
        onlineReplicaNum = 1;
        replicationNum = 2;
        Assert.assertEquals(JobPriority.NORMAL, Clone.calculatePriority(onlineReplicaNum, replicationNum));
    }
    
}
