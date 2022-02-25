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

import org.apache.doris.transaction.GlobalTransactionMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadPendingTaskTest {
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
    @Mocked
    private DppScheduler dppScheduler;

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
        db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, 1L);

        GlobalTransactionMgr globalTransactionMgr = new GlobalTransactionMgr(catalog);
        globalTransactionMgr.setEditLog(editLog);
        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());

        // mock catalog
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

                Catalog.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = globalTransactionMgr;
            }
        };
        
        // create job
        LoadJob job = new LoadJob(label);
        job.setState(JobState.PENDING);
        job.setDbId(dbId);
        String cluster = Config.dpp_default_cluster;
        job.setClusterInfo(cluster, Load.clusterToDppConfig.get(cluster));
        // set partition load infos
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
        table.setBaseIndexId(0L);
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
        job.setTimeoutSecond(10);

        // mock
        new Expectations() {
            {
                load.updateLoadJobState(job, JobState.ETL);
                times = 1;
                result = true;

                load.getLoadErrorHubInfo();
                times = 1;
                result = null;

                catalog.getLoadInstance();
                times = 1;
                result = load;

                dppScheduler.submitEtlJob(anyLong, anyString, anyString, anyString, (Map) any, anyInt);
                times = 1;
                result = new EtlSubmitResult(new TStatus(TStatusCode.OK), "job_123456");

                editLog.logSaveTransactionId(anyLong);
                minTimes = 0;
            }
        };

        HadoopLoadPendingTask loadPendingTask = new HadoopLoadPendingTask(job);
        loadPendingTask.exec();

        Assert.assertEquals(job.getId(), loadPendingTask.getSignature());
        Assert.assertEquals(JobState.PENDING, job.getState());
    }
}
