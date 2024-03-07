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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.Load;
import org.apache.doris.load.Source;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BrokerLoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testFromLoadStmt(@Injectable LoadStmt loadStmt, @Injectable LabelName labelName,
                                 @Injectable DataDescription dataDescription, @Mocked Env env, @Mocked InternalCatalog catalog,
                                 @Injectable Database database) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);

        String tableName = "table";
        String databaseName = "database";
        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                database.getTableNullable(tableName);
                minTimes = 0;
                result = null;
            }
        };

        try {
            BulkLoadJob.fromLoadStmt(loadStmt);
            Assert.fail();
        } catch (DdlException e) {
            System.out.println("could not find table named " + tableName);
        }

    }

    @Test
    public void testFromLoadStmt2(@Injectable LoadStmt loadStmt, @Injectable DataDescription dataDescription,
                                  @Injectable LabelName labelName, @Injectable Database database, @Injectable OlapTable olapTable,
                                  @Mocked Env env, @Mocked InternalCatalog catalog) {

        String label = "label";
        long dbId = 1;
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        Map<String, String> properties = new HashMap<>();
        properties.put(LoadStmt.PRIORITY, "HIGH");

        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                labelName.getLabelName();
                minTimes = 0;
                result = label;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                database.getTableNullable(tableName);
                minTimes = 0;
                result = olapTable;
                dataDescription.getPartitionNames();
                minTimes = 0;
                result = null;
                database.getId();
                minTimes = 0;
                result = dbId;
                loadStmt.getBrokerDesc();
                minTimes = 0;
                result = brokerDesc;
                loadStmt.getEtlJobType();
                minTimes = 0;
                result = EtlJobType.BROKER;
                loadStmt.getProperties();
                minTimes = 0;
                result = properties;
            }
        };

        new MockUp<Load>() {
            @Mock
            public void checkAndCreateSource(Database db, DataDescription dataDescription,
                                             Map<Long, Map<Long, List<Source>>> tableToPartitionSources, EtlJobType jobType) {

            }
        };

        try {
            BrokerLoadJob brokerLoadJob = (BrokerLoadJob) BulkLoadJob.fromLoadStmt(loadStmt);
            Assert.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(brokerLoadJob, "dbId"));
            Assert.assertEquals(label, Deencapsulation.getField(brokerLoadJob, "label"));
            Assert.assertEquals(JobState.PENDING, Deencapsulation.getField(brokerLoadJob, "state"));
            Assert.assertEquals(EtlJobType.BROKER, Deencapsulation.getField(brokerLoadJob, "jobType"));
            Assert.assertEquals(brokerLoadJob.getPriority(), LoadTask.Priority.HIGH);
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testGetTableNames(@Injectable BrokerFileGroupAggInfo fileGroupAggInfo,
                                  @Injectable BrokerFileGroup brokerFileGroup, @Mocked Env env, @Mocked InternalCatalog catalog,
                                  @Injectable Database database, @Injectable Table table) throws MetaNotFoundException {
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        String tableName = "table";
        new Expectations() {
            {
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                fileGroupAggInfo.getAllTableIds();
                minTimes = 0;
                result = Sets.newHashSet(1L);
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDb(anyLong);
                minTimes = 0;
                result = Optional.of(database);
                database.getTable(1L);
                minTimes = 0;
                result = Optional.of(table);
                table.getName();
                minTimes = 0;
                result = tableName;
            }
        };

        Assert.assertEquals(1, brokerLoadJob.getTableNamesForShow().size());
        Assert.assertTrue(brokerLoadJob.getTableNamesForShow().contains(tableName));
    }

    @Test
    public void testExecuteJob(@Mocked MasterTaskExecutor masterTaskExecutor) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();

        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(1, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithJobCancelled(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);
        brokerLoadJob.onTaskFinished(attachment);

        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(0, finishedTaskIds.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithDuplicated(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Set<Long> finishedTaskIds = Sets.newHashSet();
        long taskId = 1L;
        finishedTaskIds.add(taskId);
        Deencapsulation.setField(brokerLoadJob, "finishedTaskIds", finishedTaskIds);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinished(@Injectable BrokerPendingTaskAttachment attachment, @Mocked Env env,
                                          @Mocked InternalCatalog catalog, @Injectable Database database,
                                          @Injectable BrokerFileGroupAggInfo fileGroupAggInfo, @Injectable BrokerFileGroup brokerFileGroup1,
                                          @Injectable BrokerFileGroup brokerFileGroup2, @Injectable BrokerFileGroup brokerFileGroup3,
                                          @Mocked MasterTaskExecutor masterTaskExecutor, @Injectable OlapTable olapTable,
                                          @Mocked LoadingTaskPlanner loadingTaskPlanner) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        long taskId = 1L;
        long tableId1 = 1L;
        long tableId2 = 2L;
        long partitionId1 = 3L;
        long partitionId2 = 4;

        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> fileGroups1 = Lists.newArrayList();
        fileGroups1.add(brokerFileGroup1);
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId1, null), fileGroups1);

        List<BrokerFileGroup> fileGroups2 = Lists.newArrayList();
        fileGroups2.add(brokerFileGroup2);
        fileGroups2.add(brokerFileGroup3);
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId1)), fileGroups2);
        // add another file groups with different partition id
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId2)), fileGroups2);

        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = olapTable;
                env.getNextId();
                minTimes = 0;
                result = 1L;
                result = 2L;
                result = 3L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        Assert.assertEquals(true, finishedTaskIds.contains(taskId));
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(3, idToTasks.size());
    }

    @Test
    public void testLoadingTaskOnFinishedWithUnfinishedTask(@Injectable BrokerLoadingTaskAttachment attachment,
                                                            @Injectable LoadTask loadTask1,
                                                            @Injectable LoadTask loadTask2) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment.getTaskId();
                minTimes = 0;
                result = 1L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("1", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(50, progress);
    }

    @Test
    public void testLoadingTaskOnFinishedWithErrorNum(@Injectable BrokerLoadingTaskAttachment attachment1,
                                                      @Injectable BrokerLoadingTaskAttachment attachment2,
                                                      @Injectable LoadTask loadTask1,
                                                      @Injectable LoadTask loadTask2,
                                                      @Mocked Env env) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment2.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 20;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment2.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 2;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                attachment2.getTaskId();
                minTimes = 0;
                result = 2L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        brokerLoadJob.onTaskFinished(attachment2);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(2, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("30", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("3", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(99, progress);
        Assert.assertEquals(JobState.CANCELLED, Deencapsulation.getField(brokerLoadJob, "state"));
    }

    @Test
    public void testLoadingTaskOnFinished(@Injectable BrokerLoadingTaskAttachment attachment1,
                                          @Injectable LoadTask loadTask1, @Mocked Env env, @Mocked InternalCatalog catalog,
                                          @Injectable Database database) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 0;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("0", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(99, progress);
    }

    @Test
    public void testExecuteReplayOnAborted(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.CANCELLED;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
    }


    @Test
    public void testExecuteReplayOnVisible(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.LOADING;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.LOADING, brokerLoadJob.getState());
    }
}
