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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.load.DppConfig;
import org.apache.doris.load.DppScheduler;
import org.apache.doris.load.EtlSubmitResult;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.load.PartitionLoadInfo;
import org.apache.doris.load.Source;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HadoopLoadPendingTask.class, Catalog.class })
@PowerMockIgnore("javax.management.*")
public class LoadPendingTaskTest {
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long backendId;

    private String label;
    
    private Catalog catalog;
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
    public void testRunPendingTask() throws Exception {
        // mock catalog
        db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, 1L, 0L);
        catalog = EasyMock.createNiceMock(Catalog.class);
        EasyMock.expect(catalog.getDb(dbId)).andReturn(db).anyTimes();
        EasyMock.expect(catalog.getDb(db.getFullName())).andReturn(db).anyTimes();
        // mock editLog
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        // mock static getInstance
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
        
        // create job
        LoadJob job = new LoadJob(label);
        job.setState(JobState.PENDING);
        job.setDbId(dbId);
        String cluster = Config.dpp_default_cluster;
        job.setClusterInfo(cluster, Load.clusterToDppConfig.get(cluster));
        // set partition load infos
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        Source source = new Source(new ArrayList<String>());
        List<Source> sources = new ArrayList<Source>();
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
        load = EasyMock.createMock(Load.class);
        EasyMock.expect(load.updateLoadJobState(job, JobState.ETL)).andReturn(true).times(1);
        EasyMock.expect(load.getLoadErrorHubInfo()).andReturn(null).times(1);
        EasyMock.replay(load);
        EasyMock.expect(catalog.getLoadInstance()).andReturn(load).times(1);
        EasyMock.replay(catalog);
        
        // mock dppscheduler
        DppScheduler dppScheduler = EasyMock.createMock(DppScheduler.class);
        EasyMock.expect(dppScheduler.submitEtlJob(EasyMock.anyLong(), EasyMock.anyString(), EasyMock.anyString(),
                                                  EasyMock.anyString(), EasyMock.isA(Map.class), EasyMock.anyInt()))
                .andReturn(new EtlSubmitResult(new TStatus(TStatusCode.OK), "job_123456")).times(1);
        EasyMock.replay(dppScheduler);
        PowerMock.expectNew(DppScheduler.class, EasyMock.anyObject(DppConfig.class)).andReturn(dppScheduler).times(1);
        PowerMock.replay(DppScheduler.class);
    }
}
