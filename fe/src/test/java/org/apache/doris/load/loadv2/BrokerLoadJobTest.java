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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.Load;
import org.apache.doris.load.PullLoadSourceInfo;
import org.apache.doris.load.Source;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class BrokerLoadJobTest {

    @Test
    public void testFromLoadStmt(@Injectable LoadStmt loadStmt,
                                 @Injectable LabelName labelName,
                                 @Injectable DataDescription dataDescription,
                                 @Mocked Catalog catalog,
                                 @Injectable Database database,
                                 @Injectable BrokerDesc brokerDesc) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);

        String label = "label";
        long dbId = 1;
        String tableName = "table";
        String databaseName = "database";
        new Expectations() {
            {
                loadStmt.getLabel();
                result = labelName;
                labelName.getDbName();
                result = databaseName;
                catalog.getDb(databaseName);
                result = database;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                dataDescription.getTableName();
                result = tableName;
                database.getTable(tableName);
                result = null;
            }
        };

        try {
            BrokerLoadJob brokerLoadJob = BrokerLoadJob.fromLoadStmt(loadStmt);
            Assert.fail();
        } catch (DdlException e) {
            System.out.println("could not find table named " + tableName);
        }

    }

    @Test
    public void testFromLoadStmt(@Injectable LoadStmt loadStmt,
                                 @Injectable DataDescription dataDescription,
                                 @Injectable LabelName labelName,
                                 @Injectable Database database,
                                 @Injectable OlapTable olapTable,
                                 @Mocked Catalog catalog) {

        String label = "label";
        long dbId = 1;
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);


        new Expectations() {
            {
                loadStmt.getLabel();
                result = labelName;
                labelName.getDbName();
                result = databaseName;
                labelName.getLabelName();
                result = label;
                catalog.getDb(databaseName);
                result = database;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                dataDescription.getTableName();
                result = tableName;
                database.getTable(tableName);
                result = olapTable;
                dataDescription.getPartitionNames();
                result = null;
                dataDescription.getColumnNames();
                result = null;
                database.getId();
                result = dbId;
            }
        };

        new MockUp<Load>() {
            @Mock
            public void checkAndCreateSource(Database db, DataDescription dataDescription,
                                             Map<Long, Map<Long, List<Source>>> tableToPartitionSources,
                                             boolean deleteFlag) {
            }
        };

        try {
            BrokerLoadJob brokerLoadJob = BrokerLoadJob.fromLoadStmt(loadStmt);
            Assert.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(brokerLoadJob, "dbId"));
            Assert.assertEquals(label, Deencapsulation.getField(brokerLoadJob, "label"));
            Assert.assertEquals(JobState.PENDING, Deencapsulation.getField(brokerLoadJob, "state"));
            Assert.assertEquals(EtlJobType.BROKER, Deencapsulation.getField(brokerLoadJob, "jobType"));
            Assert.assertEquals(dataDescriptionList, Deencapsulation.getField(brokerLoadJob, "dataDescriptions"));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testGetTableNames(@Injectable PullLoadSourceInfo dataSourceInfo,
                                  @Injectable BrokerFileGroup brokerFileGroup,
                                  @Mocked Catalog catalog,
                                  @Injectable Database database,
                                  @Injectable Table table) throws MetaNotFoundException {
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        Map<Long, List<BrokerFileGroup>> idToFileGroups = Maps.newHashMap();
        idToFileGroups.put(1L, brokerFileGroups);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "dataSourceInfo", dataSourceInfo);
        String tableName = "table";
        new Expectations() {
            {
                dataSourceInfo.getIdToFileGroups();
                result = idToFileGroups;
                catalog.getDb(anyLong);
                result = database;
                database.getTable(1L);
                result = table;
                table.getName();
                result = tableName;
            }
        };

        Assert.assertEquals(1, brokerLoadJob.getTableNamesForShow().size());
        Assert.assertEquals(true, brokerLoadJob.getTableNamesForShow().contains(tableName));
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
                result = taskId;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinished(@Injectable BrokerPendingTaskAttachment attachment,
                                          @Mocked Catalog catalog,
                                          @Injectable Database database,
                                          @Injectable PullLoadSourceInfo dataSourceInfo,
                                          @Injectable BrokerFileGroup brokerFileGroup1,
                                          @Injectable BrokerFileGroup brokerFileGroup2,
                                          @Injectable BrokerFileGroup brokerFileGroup3,
                                          @Mocked MasterTaskExecutor masterTaskExecutor,
                                          @Injectable OlapTable olapTable,
                                          @Mocked LoadingTaskPlanner loadingTaskPlanner) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        long taskId = 1L;
        long tableId1 = 1L;
        long tableId2 = 2L;
        Map<Long, List<BrokerFileGroup>> idToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> fileGroups1 = Lists.newArrayList();
        fileGroups1.add(brokerFileGroup1);
        idToFileGroups.put(tableId1, fileGroups1);
        List<BrokerFileGroup> fileGroups2 = Lists.newArrayList();
        fileGroups2.add(brokerFileGroup2);
        fileGroups2.add(brokerFileGroup3);
        idToFileGroups.put(tableId2, fileGroups2);
        Deencapsulation.setField(brokerLoadJob, "dataSourceInfo", dataSourceInfo);
        new Expectations() {
            {
                attachment.getTaskId();
                result = taskId;
                catalog.getDb(anyLong);
                result = database;
                dataSourceInfo.getIdToFileGroups();
                result = idToFileGroups;
                database.getTable(anyLong);
                result = olapTable;
                catalog.getNextId();
                result = 1L;
                result = 2L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        Assert.assertEquals(true, finishedTaskIds.contains(taskId));
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(2, idToTasks.size());
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
                result = 10;
                attachment.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                result = 1;
                attachment.getTaskId();
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
                                                      @Mocked Catalog catalog) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                result = 10;
                attachment2.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                result = 20;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                result = 1;
                attachment2.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                result = 2;
                attachment1.getTaskId();
                result = 1L;
                attachment2.getTaskId();
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
                                          @Injectable LoadTask loadTask1,
                                          @Mocked Catalog catalog,
                                          @Injectable Database database) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                result = 10;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                result = 0;
                attachment1.getTaskId();
                result = 1L;
                catalog.getDb(anyLong);
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
                result = attachment;
                attachment.getLoadingStatus();
                result = etlStatus;
                attachment.getProgress();
                result = 99;
                attachment.getFinishTimestamp();
                result = 1;
                attachment.getJobState();
                result = JobState.CANCELLED;
            }
        };
        brokerLoadJob.executeReplayOnAborted(txnState);
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
                result = attachment;
                attachment.getLoadingStatus();
                result = etlStatus;
                attachment.getProgress();
                result = 99;
                attachment.getFinishTimestamp();
                result = 1;
                attachment.getJobState();
                result = JobState.LOADING;
            }
        };
        brokerLoadJob.executeReplayOnAborted(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.LOADING, brokerLoadJob.getState());
    }
}
