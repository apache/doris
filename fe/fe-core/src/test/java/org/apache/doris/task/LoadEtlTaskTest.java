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

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.load.DppScheduler;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.load.PartitionLoadInfo;
import org.apache.doris.load.Source;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.load.TabletLoadInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TEtlState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoadEtlTaskTest {
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long backendId;

    private String label;
    @Mocked
    private Catalog catalog;
    @Mocked
    private EditLog editLog;
    @Mocked
    private Load load;
    private Database db;

    @Before
    public void setUp() {
        dbId = 0L;
        tableId = 0L;
        partitionId = 0L;
        indexId = 0L;
        tabletId = 0L;
        backendId = 0L;
        
        label = "test_label";
        
        UnitTestUtil.initDppConfig();
    }
    
    @Test
    public void testRunEtlTask(@Mocked DppScheduler dppScheduler) throws Exception {
        // mock catalog
        db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, 1L);
        new Expectations(catalog) {
            {
                catalog.getDbNullable(dbId);
                minTimes = 0;
                result = db;

                catalog.getDbNullable(db.getFullName());
                minTimes = 0;
                result = db;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
        // create job
        LoadJob job = new LoadJob(label);
        job.setState(JobState.ETL);
        job.setDbId(dbId);
        String cluster = Config.dpp_default_cluster;
        job.setClusterInfo(cluster, Load.clusterToDppConfig.get(cluster));
        // set partition load infos
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
        Partition partition = table.getPartition(partitionId);
        Source source = new Source(new ArrayList<String>());
        List<Source> sources = Lists.newArrayList();
        sources.add(source);
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(sources);
        Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = new HashMap<Long, PartitionLoadInfo>();
        idToPartitionLoadInfo.put(partitionId, partitionLoadInfo);
        TableLoadInfo tableLoadInfo = new TableLoadInfo(idToPartitionLoadInfo);
        tableLoadInfo.addIndexSchemaHash(partition.getBaseIndex().getId(), 0);
        Map<Long, TableLoadInfo> idToTableLoadInfo = new HashMap<Long, TableLoadInfo>();
        idToTableLoadInfo.put(tableId, tableLoadInfo);
        job.setIdToTableLoadInfo(idToTableLoadInfo);

        // mock load
        new Expectations() {
            {
                load.addLoadingPartitions((Set) any);
                minTimes = 0;
                result = true;

                load.updateLoadJobState(job, JobState.LOADING);
                times = 1;
                result = true;

                catalog.getLoadInstance();
                times = 1;
                result = load;
            }
        };
        
        // mock dppscheduler
        EtlStatus runningStatus = new EtlStatus();
        runningStatus.setState(TEtlState.RUNNING);
        Map<String, String> stats = Maps.newHashMap();
        stats.put("map() completion", "1");
        stats.put("reduce() completion", "0.2");
        runningStatus.setStats(stats);

        EtlStatus finishedStatus = new EtlStatus();
        finishedStatus.setState(TEtlState.FINISHED);
        Map<String, String> counters = Maps.newHashMap();
        counters.put("dpp.norm.ALL", "100");
        counters.put("dpp.abnorm.ALL", "0");
        finishedStatus.setCounters(counters);

        Map<String, Long> etlFiles = Maps.newHashMap();
        etlFiles.put("label_0.0.0.0", 1L);

        new Expectations() {
            {
                dppScheduler.getEtlJobStatus(anyString);
                times = 2;
                returns(runningStatus, finishedStatus);

                dppScheduler.getEtlFiles(anyString);
                times = 1;
                result = etlFiles;
            }
        };

        // test exec: running
        HadoopLoadEtlTask loadEtlTask = new HadoopLoadEtlTask(job);
        loadEtlTask.exec();
        
        // verify running
        Assert.assertEquals(job.getId(), loadEtlTask.getSignature());
        Assert.assertEquals(60, job.getProgress());
        Assert.assertEquals(JobState.ETL, job.getState());
        
        // test exec: finished
        loadEtlTask.exec();
        
        // verify finished
        Assert.assertEquals(100, job.getProgress());
        long expectVersion = partition.getVisibleVersion() + 1;
        Assert.assertEquals(-1,
                            job.getIdToTableLoadInfo().get(tableId)
                .getIdToPartitionLoadInfo().get(partitionId).getVersion());
        int tabletNum = 0;
        Map<Long, TabletLoadInfo> tabletLoadInfos = job.getIdToTabletLoadInfo();
        for (MaterializedIndex olapTable : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : olapTable.getTablets()) {
                ++tabletNum;
                Assert.assertTrue(tabletLoadInfos.containsKey(tablet.getId()));
            }
        }
        Assert.assertEquals(tabletNum, tabletLoadInfos.size());
    }
    
}
