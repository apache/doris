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

package org.apache.doris.load;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.ReplicaPersistInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Before
    public void setUp() {
        UnitTestUtil.initDppConfig();
    }
    
    public Source makeSource(int startId, int size) {
        List<String> files = new ArrayList<String>(size);
        List<String> columns = new ArrayList<String>(size);
        for (int count = startId; count < startId + size; ++count) {
            String filename = "hdfs://host:port/dir/load-" + count;
            String column = "column-" + count;
            files.add(filename);
            columns.add(column);
        }
        Source source = new Source(files, columns, "\t", "\n", false);
        return source;
    }
    
    public List<Predicate> makeDeleteConditions(int startId, int size) {
        List<Predicate> deletions = new ArrayList<Predicate>();
        for (int count = startId; count < startId + size; ++count) {
            Operator op = Operator.GT;
            SlotRef slotRef = new SlotRef(null, "clicks");
            StringLiteral value = new StringLiteral(Integer.toString(count));
            Predicate deleteCondition = new BinaryPredicate(op, slotRef, value);
            deletions.add(deleteCondition);
        }
        return deletions;
    }
    
    public LoadJob getLoadJob() {
        Source source1 = makeSource(0, 10);
        Source source2 = makeSource(10, 30);
        Source source3 = makeSource(20, 40);
        Source source4 = makeSource(40, 60);
        Source source5 = makeSource(70, 80);
        Source source6 = makeSource(80, 100);
        List<Source> sources1 = new ArrayList<Source>();
        List<Source> sources2 = new ArrayList<Source>();
        List<Source> sources3 = new ArrayList<Source>();
        sources1.add(source1);
        sources1.add(source2);
        sources2.add(source3);
        sources2.add(source4);
        sources3.add(source5);
        sources3.add(source6);
        PartitionLoadInfo partitionLoadInfo1 = new PartitionLoadInfo(sources1);
        PartitionLoadInfo partitionLoadInfo2 = new PartitionLoadInfo(sources2);
        PartitionLoadInfo partitionLoadInfo3 = new PartitionLoadInfo(sources3);
        Map<Long, TableLoadInfo> idToTableLoadInfo = new HashMap<Long, TableLoadInfo>();
        Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = new HashMap<Long, PartitionLoadInfo>();
        idToPartitionLoadInfo.put(1L, partitionLoadInfo1);
        idToPartitionLoadInfo.put(2L, partitionLoadInfo2);
        idToPartitionLoadInfo.put(3L, partitionLoadInfo3);
        TableLoadInfo tableLoadInfo = new TableLoadInfo(idToPartitionLoadInfo);
        idToTableLoadInfo.put(0L, tableLoadInfo);

        TabletLoadInfo tabletLoadInfo1 = new TabletLoadInfo("tabletLoadInfo1", 1L);
        TabletLoadInfo tabletLoadInfo2 = new TabletLoadInfo("tabletLoadInfo2", 1L);
        TabletLoadInfo tabletLoadInfo3 = new TabletLoadInfo("tabletLoadInfo3", 1L);
        Map<Long, TabletLoadInfo> tabletLoadInfos = new HashMap<Long, TabletLoadInfo>();
        tabletLoadInfos.put(1L, tabletLoadInfo1);
        tabletLoadInfos.put(2L, tabletLoadInfo2);
        tabletLoadInfos.put(3L, tabletLoadInfo3);
        
        LoadJob loadJob3 = new LoadJob("datalabel-2014-12-5", 1000, 0.1);
        loadJob3.setIdToTableLoadInfo(idToTableLoadInfo);
        loadJob3.setIdToTabletLoadInfo(tabletLoadInfos);
        
        loadJob3.addFullTablet(1);
        loadJob3.addFullTablet(2);
        loadJob3.addFullTablet(3);
        
        loadJob3.addReplicaPersistInfos(ReplicaPersistInfo.createForLoad(1, 1, 1, 1, 1, 1, 0, 1, 1));
        loadJob3.addReplicaPersistInfos(ReplicaPersistInfo.createForLoad(2, 2, 2, 2, 2, 2, 0, 2, 2));
        loadJob3.addReplicaPersistInfos(ReplicaPersistInfo.createForLoad(3, 3, 3, 3, 3, 3, 0, 3, 3));

        return loadJob3;
    }

    @Test
    public void testSerialization() throws Exception {
        // mock meta version
        FakeCatalog fakeCatalog = new FakeCatalog();
        FakeCatalog.setMetaVersion(FeConstants.meta_version);

        File file = new File("./loadJobTest" + System.currentTimeMillis());
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        LoadJob loadJob0 = new LoadJob();
        loadJob0.write(dos);

        LoadJob loadJob1 = new LoadJob("datalabel-2014-12-5", 1000, 0.1);
        loadJob1.setId(2000);
        loadJob1.setDbId(3000);
        String cluster = Config.dpp_default_cluster;
        loadJob1.setClusterInfo(cluster, Load.clusterToDppConfig.get(cluster));
        loadJob1.setState(JobState.FINISHED);
        loadJob1.setProgress(100);
        loadJob1.setHadoopEtlJobId("etl-job-id");
        loadJob1.write(dos);
        
        LoadJob loadJob3 = getLoadJob();
        loadJob3.write(dos);

        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        LoadJob rLoadJob0 = new LoadJob();
        rLoadJob0.readFields(dis);

        LoadJob rLoadJob1 = new LoadJob();
        rLoadJob1.readFields(dis);

        LoadJob rLoadJob3 = new LoadJob();
        rLoadJob3.readFields(dis);

        Assert.assertTrue(loadJob0.equals(rLoadJob0));
        Assert.assertTrue(loadJob1.equals(rLoadJob1));
        Assert.assertTrue(loadJob3.equals(rLoadJob3));
        
        Assert.assertFalse(loadJob0.equals(rLoadJob1));
        
        dis.close();
        file.delete();
    }
    
    @Test
    public void testClear() throws Exception {
        LoadJob job = getLoadJob();
        
        Assert.assertFalse(job.getIdToTableLoadInfo() == null);
        Assert.assertFalse(job.getIdToTabletLoadInfo() == null);
        Assert.assertFalse(job.getQuorumTablets() == null);
        Assert.assertFalse(job.getFullTablets() == null);
        Assert.assertFalse(job.getReplicaPersistInfos() == null);
        
        job.clearRedundantInfoForHistoryJob();
        
        Assert.assertTrue(job.getIdToTableLoadInfo() == null);
        Assert.assertTrue(job.getIdToTabletLoadInfo() == null);
        Assert.assertTrue(job.getQuorumTablets() == null);
        Assert.assertTrue(job.getFullTablets() == null);
        Assert.assertTrue(job.getReplicaPersistInfos() == null);
    }
    
    @Test
    public void testEqual() throws Exception {
        LoadJob job1 = getLoadJob();
        LoadJob job2 = new LoadJob();
        Thread.sleep(10);
        LoadJob job3 = getLoadJob();
    }
    
    @Test
    public void testGetAndSet() throws Exception {
        LoadJob job = new LoadJob();
        job.setId(1);
        Assert.assertEquals(1, job.getId());
        
        job.setDbId(2);
        Assert.assertEquals(2, job.getDbId());
        
        Assert.assertEquals("", job.getLabel());
        
        job.setTimeoutSecond(3);
        Assert.assertEquals(3, job.getTimeoutSecond());

        String cluster = Config.dpp_default_cluster;
        job.setClusterInfo(cluster, Load.clusterToDppConfig.get(cluster));
        Assert.assertEquals(cluster, job.getHadoopCluster());
        
        job.setState(JobState.CANCELLED);
        Assert.assertEquals(JobState.CANCELLED, job.getState());

        job.setProgress(4);
        Assert.assertEquals(4, job.getProgress());

        job.setEtlStartTimeMs(6);
        Assert.assertEquals(6, job.getEtlStartTimeMs());
        
        job.setEtlFinishTimeMs(7);
        Assert.assertEquals(7, job.getEtlFinishTimeMs());

        job.setLoadStartTimeMs(8);
        Assert.assertEquals(8, job.getLoadStartTimeMs());

        job.setLoadFinishTimeMs(9);
        Assert.assertEquals(9, job.getLoadFinishTimeMs());
    }
}
