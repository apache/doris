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
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.SparkLoadJob.SparkLoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class SparkLoadJobTest {
    private long dbId;
    private String dbName;
    private String tableName;
    private String label;
    private String resourceName;
    private String broker;
    private long transactionId;
    private long pendingTaskId;
    private SparkLoadAppHandle sparkLoadAppHandle;
    private String appId;
    private String etlOutputPath;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long backendId;
    private int schemaHash;

    @Before
    public void setUp() {
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        label = "label0";
        resourceName = "spark0";
        broker = "broker0";
        transactionId = 2L;
        pendingTaskId = 3L;
        sparkLoadAppHandle = new SparkLoadAppHandle();
        appId = "application_15888888888_0088";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/doris/100/label/101";
        tableId = 10L;
        partitionId = 11L;
        indexId = 12L;
        tabletId = 13L;
        replicaId = 14L;
        backendId = 15L;
        schemaHash = 146886;
    }

    @Test
    public void testCreateFromLoadStmt(@Mocked Catalog catalog, @Injectable LoadStmt loadStmt,
                                       @Injectable DataDescription dataDescription, @Injectable LabelName labelName,
                                       @Injectable Database db, @Injectable OlapTable olapTable,
                                       @Injectable ResourceMgr resourceMgr) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("spark.executor.memory", "1g");
        resourceProperties.put("broker", broker);
        resourceProperties.put("broker.username", "user0");
        resourceProperties.put("broker.password", "password0");
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, resourceProperties);
        Map<String, String> jobProperties = Maps.newHashMap();
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                catalog.getDb(dbName);
                result = db;
                catalog.getResourceMgr();
                result = resourceMgr;
                db.getTable(tableName);
                result = olapTable;
                db.getId();
                result = dbId;
                loadStmt.getLabel();
                result = labelName;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                loadStmt.getResourceDesc();
                result = resourceDesc;
                loadStmt.getProperties();
                result = jobProperties;
                loadStmt.getEtlJobType();
                result = EtlJobType.SPARK;
                labelName.getDbName();
                result = dbName;
                labelName.getLabelName();
                result = label;
                dataDescription.getTableName();
                result = tableName;
                dataDescription.getPartitionNames();
                result = null;
                resourceMgr.getResource(resourceName);
                result = resource;
            }
        };

        try {
            Assert.assertTrue(resource.getSparkConfigs().isEmpty());
            resourceDesc.analyze();
            BulkLoadJob bulkLoadJob = BulkLoadJob.fromLoadStmt(loadStmt);
            SparkLoadJob sparkLoadJob = (SparkLoadJob) bulkLoadJob;
            // check member
            Assert.assertEquals(dbId, bulkLoadJob.dbId);
            Assert.assertEquals(label, bulkLoadJob.label);
            Assert.assertEquals(JobState.PENDING, bulkLoadJob.getState());
            Assert.assertEquals(EtlJobType.SPARK, bulkLoadJob.getJobType());
            Assert.assertEquals(resourceName, sparkLoadJob.getResourceName());
            Assert.assertEquals(-1L, sparkLoadJob.getEtlStartTimestamp());

            // check update spark resource properties
            Assert.assertEquals(broker, bulkLoadJob.brokerDesc.getName());
            Assert.assertEquals("user0", bulkLoadJob.brokerDesc.getProperties().get("username"));
            Assert.assertEquals("password0", bulkLoadJob.brokerDesc.getProperties().get("password"));
            SparkResource sparkResource = Deencapsulation.getField(sparkLoadJob, "sparkResource");
            Assert.assertTrue(sparkResource.getSparkConfigs().containsKey("spark.executor.memory"));
            Assert.assertEquals("1g", sparkResource.getSparkConfigs().get("spark.executor.memory"));
        } catch (DdlException | AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Mocked Catalog catalog, @Mocked SparkLoadPendingTask pendingTask,
                            @Injectable String originStmt, @Injectable GlobalTransactionMgr transactionMgr,
                            @Injectable MasterTaskExecutor executor) throws Exception {
        new Expectations() {
            {
                Catalog.getCurrentGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.beginTransaction(dbId, Lists.newArrayList(), label, null,
                                                (TransactionState.TxnCoordinator) any, LoadJobSourceType.FRONTEND,
                                                anyLong, anyLong);
                result = transactionId;
                pendingTask.init();
                pendingTask.getSignature();
                result = pendingTaskId;
                catalog.getPendingLoadTaskScheduler();
                result = executor;
                executor.submit((SparkLoadPendingTask) any);
                result = true;
            }
        };

        ResourceDesc resourceDesc = new ResourceDesc(resourceName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, resourceDesc, new OriginStatement(originStmt, 0), new UserIdentity("root", "0.0.0.0"));
        job.execute();

        // check transaction id and id to tasks
        Assert.assertEquals(transactionId, job.getTransactionId());
        Assert.assertTrue(job.idToTasks.containsKey(pendingTaskId));
    }

    @Test
    public void testOnPendingTaskFinished(@Mocked Catalog catalog, @Injectable String originStmt) throws MetaNotFoundException {
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, resourceDesc, new OriginStatement(originStmt, 0), new UserIdentity("root", "0.0.0.0"));
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        attachment.setAppId(appId);
        attachment.setOutputPath(etlOutputPath);
        job.onTaskFinished(attachment);

        // check pending task finish
        Assert.assertTrue(job.finishedTaskIds.contains(pendingTaskId));
        Assert.assertEquals(appId, Deencapsulation.getField(job, "appId"));
        Assert.assertEquals(etlOutputPath, Deencapsulation.getField(job, "etlOutputPath"));
        Assert.assertEquals(JobState.ETL, job.getState());
    }

    private SparkLoadJob getEtlStateJob(String originStmt) throws MetaNotFoundException {
        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        SparkLoadJob job = new SparkLoadJob(dbId, label, null, new OriginStatement(originStmt, 0), new UserIdentity("root", "0.0.0.0"));
        job.state = JobState.ETL;
        job.setMaxFilterRatio(0.15);
        job.transactionId = transactionId;
        Deencapsulation.setField(job, "appId", appId);
        Deencapsulation.setField(job, "etlOutputPath", etlOutputPath);
        Deencapsulation.setField(job, "sparkResource", resource);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        job.brokerDesc = brokerDesc;
        return job;
    }

    @Test
    public void testUpdateEtlStatusRunning(@Mocked Catalog catalog, @Injectable String originStmt,
                                           @Mocked SparkEtlJobHandler handler) throws Exception {
        String trackingUrl = "http://127.0.0.1:8080/proxy/application_1586619723848_0088/";
        int progress = 66;
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.RUNNING);
        status.setTrackingUrl(trackingUrl);
        status.setProgress(progress);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkResource) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl running
        Assert.assertEquals(JobState.ETL, job.getState());
        Assert.assertEquals(progress, job.progress);
        Assert.assertEquals(trackingUrl, job.loadingStatus.getTrackingUrl());
    }

    @Test(expected = LoadException.class)
    public void testUpdateEtlStatusCancelled(@Mocked Catalog catalog, @Injectable String originStmt,
                                             @Mocked SparkEtlJobHandler handler) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.CANCELLED);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkResource) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();
    }

    @Test(expected = DataQualityException.class)
    public void testUpdateEtlStatusFinishedQualityFailed(@Mocked Catalog catalog, @Injectable String originStmt,
                                                         @Mocked SparkEtlJobHandler handler) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "8");
        status.getCounters().put("dpp.abnorm.ALL", "2");

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkResource) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();
    }

    @Test
    public void testUpdateEtlStatusFinishedAndCommitTransaction(
            @Mocked Catalog catalog, @Injectable String originStmt,
            @Mocked SparkEtlJobHandler handler, @Mocked AgentTaskExecutor executor,
            @Injectable Database db, @Injectable OlapTable table, @Injectable Partition partition,
            @Injectable MaterializedIndex index, @Injectable Tablet tablet, @Injectable Replica replica,
            @Injectable GlobalTransactionMgr transactionMgr) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "9");
        status.getCounters().put("dpp.abnorm.ALL", "1");
        Map<String, Long> filePathToSize = Maps.newHashMap();
        String filePath = String.format("hdfs://127.0.0.1:10000/doris/jobs/1/label6/9/V1.label6.%d.%d.%d.0.%d.parquet",
                                        tableId, partitionId, indexId, schemaHash);
        long fileSize = 6L;
        filePathToSize.put(filePath, fileSize);
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.addPartition(partitionId, null, new ReplicaAllocation((short) 1), false);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkResource) any, (BrokerDesc) any);
                result = status;
                handler.getEtlFilePaths(etlOutputPath, (BrokerDesc) any);
                result = filePathToSize;
                catalog.getDb(dbId);
                result = db;
                db.getTablesOnIdOrderOrThrowException((List<Long>) any);
                result = Lists.newArrayList(table);
                table.getId();
                result = tableId;
                table.getPartition(partitionId);
                result = partition;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getSchemaByIndexId(Long.valueOf(12));
                result = Lists.newArrayList(new Column("k1", PrimitiveType.VARCHAR));
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                index.getId();
                result = indexId;
                index.getTablets();
                result = Lists.newArrayList(tablet);
                tablet.getId();
                result = tabletId;
                tablet.getReplicas();
                result = Lists.newArrayList(replica);
                replica.getId();
                result = replicaId;
                replica.getBackendId();
                result = backendId;
                replica.getLastFailedVersion();
                result = -1;
                AgentTaskExecutor.submit((AgentBatchTask) any);
                Catalog.getCurrentGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.commitTransaction(dbId, (List<Table>) any, transactionId, (List<TabletCommitInfo>) any,
                                                 (LoadJobFinalOperation) any);
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl finished
        Assert.assertEquals(JobState.LOADING, job.getState());
        Assert.assertEquals(0, job.progress);
        Map<String, Pair<String, Long>> tabletMetaToFileInfo = Deencapsulation.getField(job, "tabletMetaToFileInfo");
        Assert.assertEquals(1, tabletMetaToFileInfo.size());
        String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
        Assert.assertTrue(tabletMetaToFileInfo.containsKey(tabletMetaStr));
        Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
        Assert.assertEquals(filePath, fileInfo.first);
        Assert.assertEquals(fileSize, (long) fileInfo.second);
        Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask
                = Deencapsulation.getField(job, "tabletToSentReplicaPushTask");
        Assert.assertTrue(tabletToSentReplicaPushTask.containsKey(tabletId));
        Assert.assertTrue(tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId));
        Map<Long, Set<Long>> tableToLoadPartitions = Deencapsulation.getField(job, "tableToLoadPartitions");
        Assert.assertTrue(tableToLoadPartitions.containsKey(tableId));
        Assert.assertTrue(tableToLoadPartitions.get(tableId).contains(partitionId));
        Map<Long, Integer> indexToSchemaHash = Deencapsulation.getField(job, "indexToSchemaHash");
        Assert.assertTrue(indexToSchemaHash.containsKey(indexId));
        Assert.assertEquals(schemaHash, (long) indexToSchemaHash.get(indexId));

        // finish push task
        job.addFinishedReplica(replicaId, tabletId, backendId);
        job.updateLoadingStatus();
        Assert.assertEquals(99, job.progress);
        Set<Long> fullTablets = Deencapsulation.getField(job, "fullTablets");
        Assert.assertTrue(fullTablets.contains(tabletId));
    }

    @Test
    public void testSubmitTasksWhenStateFinished(@Mocked Catalog catalog, @Injectable String originStmt,
                                                 @Injectable Database db) throws Exception {
        new Expectations() {
            {
                catalog.getDb(dbId);
                result = db;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.state = JobState.FINISHED;
        Set<Long> totalTablets = Deencapsulation.invoke(job, "submitPushTasks");
        Assert.assertTrue(totalTablets.isEmpty());
    }

    @Test
    public void testStateUpdateInfoPersist() throws IOException {
        String fileName = "./testStateUpdateInfoPersistFile";
        File file = new File(fileName);

        // etl state
        long id = 1L;
        JobState state = JobState.ETL;
        long etlStartTimestamp = 1592366666L;
        long loadStartTimestamp = -1;
        Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        SparkLoadJobStateUpdateInfo info = new SparkLoadJobStateUpdateInfo(
                id, state, transactionId, sparkLoadAppHandle, etlStartTimestamp, appId, etlOutputPath,
                loadStartTimestamp, tabletMetaToFileInfo);
        info.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        SparkLoadJobStateUpdateInfo replayedInfo = (SparkLoadJobStateUpdateInfo) LoadJobStateUpdateInfo.read(in);
        Assert.assertEquals(id, replayedInfo.getJobId());
        Assert.assertEquals(state, replayedInfo.getState());
        Assert.assertEquals(transactionId, replayedInfo.getTransactionId());
        Assert.assertEquals(loadStartTimestamp, replayedInfo.getLoadStartTimestamp());
        Assert.assertEquals(etlStartTimestamp, replayedInfo.getEtlStartTimestamp());
        Assert.assertEquals(appId, replayedInfo.getAppId());
        Assert.assertEquals(etlOutputPath, replayedInfo.getEtlOutputPath());
        Assert.assertTrue(replayedInfo.getTabletMetaToFileInfo().isEmpty());
        in.close();

        // loading state
        state = JobState.LOADING;
        loadStartTimestamp = 1592388888L;
        String tabletMeta = String.format("%d.%d.%d.0.%d", tableId, partitionId, indexId, schemaHash);
        String filePath = String.format("hdfs://127.0.0.1:10000/doris/jobs/1/label6/9/V1.label6.%d.%d.%d.0.%d.parquet",
                                        tableId, partitionId, indexId, schemaHash);
        long fileSize = 6L;
        tabletMetaToFileInfo.put(tabletMeta, Pair.create(filePath, fileSize));

        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        out = new DataOutputStream(new FileOutputStream(file));
        info = new SparkLoadJobStateUpdateInfo(id, state, transactionId, sparkLoadAppHandle, etlStartTimestamp,
                appId, etlOutputPath, loadStartTimestamp, tabletMetaToFileInfo);
        info.write(out);
        out.flush();
        out.close();

        in = new DataInputStream(new FileInputStream(file));
        replayedInfo = (SparkLoadJobStateUpdateInfo) LoadJobStateUpdateInfo.read(in);
        Assert.assertEquals(state, replayedInfo.getState());
        Assert.assertEquals(loadStartTimestamp, replayedInfo.getLoadStartTimestamp());
        Map<String, Pair<String, Long>> replayedTabletMetaToFileInfo = replayedInfo.getTabletMetaToFileInfo();
        Assert.assertEquals(1, replayedTabletMetaToFileInfo.size());
        Assert.assertTrue(replayedTabletMetaToFileInfo.containsKey(tabletMeta));
        Pair<String, Long> replayedFileInfo = replayedTabletMetaToFileInfo.get(tabletMeta);
        Assert.assertEquals(filePath, replayedFileInfo.first);
        Assert.assertEquals(fileSize, (long) replayedFileInfo.second);
        in.close();

        // delete file
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testSparkLoadJobPersist(@Mocked Catalog catalog, @Mocked Database db,
                                        @Mocked Table table,
                                        @Mocked ResourceMgr resourceMgr) throws Exception {
        long dbId = 1000L;
        SparkResource sparkResource = new SparkResource("my_spark", Maps.newHashMap(), "/path/to/", "bos",
                Maps.newHashMap());
        new Expectations() {
            {
                catalog.getDb(dbId);
                result = db;
                catalog.getResourceMgr();
                result = resourceMgr;
                //db.getTable(anyLong);
                //result = table;
                //table.getName();
                //result = "table1";
                resourceMgr.getResource(anyString);
                result = sparkResource;
                Catalog.getCurrentCatalogJournalVersion();
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        String label = "label1";
        ResourceDesc resourceDesc = new ResourceDesc("my_spark", Maps.newHashMap());
        String oriStmt = "LOAD LABEL db1.label1\n" +
                "(\n" +
                "DATA INFILE(\"hdfs://127.0.0.1:8000/user/palo/data/input/file\")\n" +
                "INTO TABLE `my_table`\n" +
                "WHERE k1 > 10\n" +
                ")\n" +
                "WITH RESOURCE 'my_spark';";
        OriginStatement originStmt = new OriginStatement(oriStmt, 0);
        UserIdentity userInfo = UserIdentity.ADMIN;
        SparkLoadJob sparkLoadJob = new SparkLoadJob(dbId, label, resourceDesc, originStmt, userInfo);
        sparkLoadJob.setJobProperties(Maps.newHashMap());

        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./testSparkLoadJobPersist");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        sparkLoadJob.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        SparkLoadJob sparkLoadJob2 = (SparkLoadJob) SparkLoadJob.read(dis);
        Assert.assertEquals("my_spark", sparkLoadJob2.getResourceName());
        Assert.assertEquals(label, sparkLoadJob2.getLabel());
        Assert.assertEquals(dbId, sparkLoadJob2.getDbId());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
