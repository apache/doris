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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.task.LoadTaskInfo.ImportColumnDescs;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RoutineLoadJobTest {
    @Test
    public void testAfterAbortedReasonOffsetOutOfRange() throws UserException {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        TransactionState transactionState = Mockito.mock(TransactionState.class);
        RoutineLoadTaskInfo routineLoadTaskInfo = Mockito.mock(RoutineLoadTaskInfo.class);

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        Mockito.when(transactionState.getTransactionId()).thenReturn(txnId);
        Mockito.when(routineLoadTaskInfo.getTxnId()).thenReturn(txnId);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            String txnStatusChangeReasonString = TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString();
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
            routineLoadJob.writeLock();
            routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        }
    }

    @Test
    public void testAfterAborted() throws UserException {
        TransactionState transactionState = Mockito.mock(TransactionState.class);
        KafkaTaskInfo routineLoadTaskInfo = Mockito.mock(KafkaTaskInfo.class);

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        tKafkaRLTaskProgress.partitionCmtOffset = Maps.newHashMap();
        KafkaProgress kafkaProgress = new KafkaProgress(tKafkaRLTaskProgress);
        Deencapsulation.setField(attachment, "progress", kafkaProgress);

        KafkaProgress currentProgress = new KafkaProgress(tKafkaRLTaskProgress);

        Mockito.when(transactionState.getTransactionId()).thenReturn(txnId);
        Mockito.when(routineLoadTaskInfo.getTxnId()).thenReturn(txnId);
        Mockito.doReturn(attachment).when(transactionState).getTxnCommitAttachment();
        Mockito.when(routineLoadTaskInfo.getPartitions()).thenReturn(Lists.newArrayList());

        String txnStatusChangeReasonString = "no data";
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", currentProgress);
        routineLoadJob.writeLock();
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);
        RoutineLoadStatistic jobStatistic = Deencapsulation.getField(routineLoadJob, "jobStatistic");

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertEquals(new Long(1), Deencapsulation.getField(jobStatistic, "abortedTaskNum"));
    }

    @Test
    public void testAfterCommittedWhileTaskAborted() throws UserException {
        Mockito.mock(Env.class);
        TransactionState transactionState = Mockito.mock(TransactionState.class);
        KafkaProgress progress = Mockito.mock(KafkaProgress.class);

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        long txnId = 1L;

        Mockito.when(transactionState.getTransactionId()).thenReturn(txnId);

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", progress);
        try {
            routineLoadJob.writeLock();
            routineLoadJob.afterCommitted(transactionState, true);
        } catch (TransactionException e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetShowInfo() {
        KafkaProgress kafkaProgress = Mockito.mock(KafkaProgress.class);
        UserIdentity userIdentity = Mockito.mock(UserIdentity.class);

        Mockito.when(userIdentity.getQualifiedUser()).thenReturn("root");

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        ErrorReason errorReason = new ErrorReason(InternalErrorCode.INTERNAL_ERR,
                TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString());
        Deencapsulation.setField(routineLoadJob, "pauseReason", errorReason);
        Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);
        Deencapsulation.setField(routineLoadJob, "userIdentity", userIdentity);

        List<String> showInfo = routineLoadJob.getShowInfo();
        Assert.assertEquals(true, showInfo.stream().filter(entity -> !Strings.isNullOrEmpty(entity))
                .anyMatch(entity -> entity.equals(errorReason.toString())));
    }

    @Test
    public void testUpdateWhileDbDeleted() throws UserException {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);
            Mockito.doReturn(null).when(catalog).getDbNullable(Mockito.anyLong());

            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            routineLoadJob.update();

            Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
        }
    }

    @Test
    public void testUpdateWhileTableDeleted() throws UserException {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);
            Mockito.doReturn(database).when(catalog).getDbNullable(Mockito.anyLong());
            Mockito.doReturn(null).when(database).getTableNullable(Mockito.anyLong());

            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            routineLoadJob.update();

            Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
        }
    }

    @Test
    public void testUpdateWhilePartitionChanged() throws UserException {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        KafkaProgress kafkaProgress = Mockito.mock(KafkaProgress.class);

        List<PartitionInfo> partitionInfoList = Lists.newArrayList();
        partitionInfoList.add(partitionInfo);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.doReturn(database).when(catalog).getDbNullable(Mockito.anyLong());
            Mockito.doReturn(table).when(database).getTableNullable(Mockito.anyLong());

            kafkaUtilStatic.when(() -> KafkaUtil.getAllKafkaPartitions(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
                    .thenReturn(Lists.newArrayList(1, 2, 3));

            kafkaUtilStatic.when(() -> KafkaUtil.getRealOffsets(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyMap(), Mockito.anyList()))
                    .thenAnswer(inv -> {
                        List<Pair<Integer, Long>> pairList = new ArrayList<>();
                        pairList.add(Pair.of(1, 0L));
                        return pairList;
                    });

            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);
            routineLoadJob.update();

            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
        }
    }

    @Test
    public void testUpdateNumOfDataErrorRowMoreThanMax() {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "maxErrorNum", 0);
            Deencapsulation.setField(routineLoadJob, "maxBatchRows", 0);
            Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 1L, 1L, 0L, 1L, 1L, false);

            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, Deencapsulation.getField(routineLoadJob, "state"));
        }
    }

    @Test
    public void testUpdateTotalMoreThanBatch() {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 10);
        RoutineLoadStatistic jobStatistic = Deencapsulation.getField(routineLoadJob, "jobStatistic");
        Deencapsulation.setField(jobStatistic, "currentErrorRows", 1);
        Deencapsulation.setField(jobStatistic, "currentTotalRows", 99);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 2L, 0L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, Deencapsulation.getField(routineLoadJob, "state"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(jobStatistic, "currentErrorRows"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(jobStatistic, "currentTotalRows"));

    }

    @Test
    public void testGetBeIdToConcurrentTaskNum() {
        RoutineLoadTaskInfo routineLoadTaskInfo = Mockito.mock(RoutineLoadTaskInfo.class);
        RoutineLoadTaskInfo routineLoadTaskInfo1 = Mockito.mock(RoutineLoadTaskInfo.class);

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        routineLoadTaskInfoList.add(routineLoadTaskInfo1);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

        Mockito.when(routineLoadTaskInfo.getBeId()).thenReturn(1L);
        Mockito.when(routineLoadTaskInfo1.getBeId()).thenReturn(1L);

        Map<Long, Integer> beIdConcurrentTasksNum = routineLoadJob.getBeCurrentTasksNumMap();
        Assert.assertEquals(2, (int) beIdConcurrentTasksNum.get(1L));
    }

    @Test
    public void testGetShowCreateInfo() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(111L, "test_load", 1,
                11, "localhost:9092", "test_topic", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 10);
        String showCreateInfo = routineLoadJob.getShowCreateInfo();
        String expect = "CREATE ROUTINE LOAD test_load ON 11\n"
                + "WITH APPEND\n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"desired_concurrent_number\" = \"0\",\n"
                + "\"max_error_number\" = \"10\",\n"
                + "\"max_filter_ratio\" = \"1.0\",\n"
                + "\"max_batch_interval\" = \"60\",\n"
                + "\"max_batch_rows\" = \"10\",\n"
                + "\"max_batch_size\" = \"1073741824\",\n"
                + "\"format\" = \"csv\",\n"
                + "\"strip_outer_array\" = \"false\",\n"
                + "\"num_as_string\" = \"false\",\n"
                + "\"fuzzy_parse\" = \"false\",\n"
                + "\"strict_mode\" = \"false\",\n"
                + "\"timezone\" = \"Asia/Shanghai\",\n"
                + "\"exec_mem_limit\" = \"2147483648\"\n"
                + ")\n"
                + "FROM KAFKA\n"
                + "(\n"
                + "\"kafka_broker_list\" = \"localhost:9092\",\n"
                + "\"kafka_topic\" = \"test_topic\"\n"
                + ");";
        System.out.println(showCreateInfo);
        Assert.assertEquals(expect, showCreateInfo);
    }

    @Test
    public void testParseUniqueKeyUpdateMode() {
        // Test valid mode strings
        Assert.assertEquals(TUniqueKeyUpdateMode.UPSERT,
                CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("UPSERT"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPSERT,
                CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("upsert"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS,
                CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("UPDATE_FIXED_COLUMNS"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS,
                CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("update_fixed_columns"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS,
                CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("UPDATE_FLEXIBLE_COLUMNS"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS,
                CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("Update_Flexible_Columns"));

        // Test invalid mode strings
        Assert.assertNull(CreateRoutineLoadInfo.parseUniqueKeyUpdateMode(null));
        Assert.assertNull(CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("INVALID"));
        Assert.assertNull(CreateRoutineLoadInfo.parseUniqueKeyUpdateMode(""));
        Assert.assertNull(CreateRoutineLoadInfo.parseUniqueKeyUpdateMode("PARTIAL_UPDATE"));
    }

    @Test
    public void testParseAndValidateUniqueKeyUpdateMode() throws Exception {
        // Test valid mode strings
        Assert.assertEquals(TUniqueKeyUpdateMode.UPSERT,
                CreateRoutineLoadInfo.parseAndValidateUniqueKeyUpdateMode("UPSERT"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS,
                CreateRoutineLoadInfo.parseAndValidateUniqueKeyUpdateMode("UPDATE_FIXED_COLUMNS"));
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS,
                CreateRoutineLoadInfo.parseAndValidateUniqueKeyUpdateMode("UPDATE_FLEXIBLE_COLUMNS"));

        // Test invalid mode string throws exception
        try {
            CreateRoutineLoadInfo.parseAndValidateUniqueKeyUpdateMode("INVALID_MODE");
            Assert.fail("Expected AnalysisException");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("unique_key_update_mode"));
            Assert.assertTrue(e.getMessage().contains("INVALID_MODE"));
        }
    }

    @Test
    public void testUniqueKeyUpdateModeInJobProperties() {
        // Test that uniqueKeyUpdateMode is properly stored in jobProperties
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
        Deencapsulation.setField(job, "jobProperties", jobProperties);
        Deencapsulation.setField(job, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS);

        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS, job.getUniqueKeyUpdateMode());
    }

    @Test
    public void testBackwardCompatibilityPartialColumnsToUniqueKeyUpdateMode() throws Exception {
        // Test backward compatibility: partial_columns=true should map to UPDATE_FIXED_COLUMNS
        // This tests the logic in gsonPostProcess without calling the full method
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.PARTIAL_COLUMNS, "true");
        // Note: UNIQUE_KEY_UPDATE_MODE is NOT set - testing backward compatibility
        Deencapsulation.setField(job, "jobProperties", jobProperties);
        Deencapsulation.setField(job, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPSERT);

        // Simulate the backward compatibility logic from gsonPostProcess
        TUniqueKeyUpdateMode uniqueKeyUpdateMode = Deencapsulation.getField(job, "uniqueKeyUpdateMode");
        boolean isPartialUpdate = false;

        // Process PARTIAL_COLUMNS when UNIQUE_KEY_UPDATE_MODE is not set
        if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPSERT) {
            String partialColumnsValue = jobProperties.get(CreateRoutineLoadInfo.PARTIAL_COLUMNS);
            isPartialUpdate = Boolean.parseBoolean(partialColumnsValue);
            if (isPartialUpdate) {
                uniqueKeyUpdateMode = TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS;
            }
        }

        // Verify the backward compatibility logic
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS, uniqueKeyUpdateMode);
        Assert.assertTrue(isPartialUpdate);
    }

    @Test
    public void testUniqueKeyUpdateModeTakesPrecedenceOverPartialColumns() throws Exception {
        // Test that unique_key_update_mode takes precedence over partial_columns
        // This tests the logic in gsonPostProcess without calling the full method
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
        jobProperties.put(CreateRoutineLoadInfo.PARTIAL_COLUMNS, "true");
        Deencapsulation.setField(job, "jobProperties", jobProperties);

        // Simulate the precedence logic from gsonPostProcess
        TUniqueKeyUpdateMode uniqueKeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
        boolean isPartialUpdate = false;

        // Process UNIQUE_KEY_UPDATE_MODE first (takes precedence)
        if (jobProperties.containsKey(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE)) {
            String modeValue = jobProperties.get(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE);
            TUniqueKeyUpdateMode mode = CreateRoutineLoadInfo.parseUniqueKeyUpdateMode(modeValue);
            if (mode != null) {
                uniqueKeyUpdateMode = mode;
                isPartialUpdate = (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS);
            }
        }

        // Process PARTIAL_COLUMNS only if UNIQUE_KEY_UPDATE_MODE results in UPSERT
        if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPSERT) {
            String partialColumnsValue = jobProperties.get(CreateRoutineLoadInfo.PARTIAL_COLUMNS);
            isPartialUpdate = Boolean.parseBoolean(partialColumnsValue);
            if (isPartialUpdate) {
                uniqueKeyUpdateMode = TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS;
            }
        }

        // unique_key_update_mode should take precedence
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS, uniqueKeyUpdateMode);
        // isPartialUpdate should be false for UPDATE_FLEXIBLE_COLUMNS
        Assert.assertFalse(isPartialUpdate);
    }

    @Test
    public void testValidateFlexiblePartialUpdateForAlterUsesAlteredProperties() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPSERT);

        Map<String, String> currentJobProperties = Maps.newHashMap();
        currentJobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        Deencapsulation.setField(job, "jobProperties", currentJobProperties);

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(db);
        Mockito.when(db.getTableNullable(2L)).thenReturn(table);
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate();
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate(Mockito.anyBoolean());
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate();
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate(
                Mockito.anyBoolean());
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(true);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        Mockito.when(table.getBaseSchema()).thenReturn(Lists.newArrayList(new Column("k", PrimitiveType.INT)));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            Map<String, String> modeAndJsonPathsProperties = Maps.newHashMap();
            modeAndJsonPathsProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
            modeAndJsonPathsProperties.put(JsonFileFormatProperties.PROP_JSON_PATHS, "[\"$.id\"]");
            UserException exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(modeAndJsonPathsProperties, null));
            Assert.assertTrue(exception.getMessage().contains("jsonpaths"));

            Deencapsulation.setField(job, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS);

            Map<String, String> fuzzyParseProperties = Maps.newHashMap();
            fuzzyParseProperties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE, "true");
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(fuzzyParseProperties, null));
            Assert.assertTrue(exception.getMessage().contains("fuzzy_parse"));

            RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(null, null,
                    Lists.newArrayList(new ImportColumnDesc("id", null)), null, null, null, null,
                    LoadTask.MergeType.APPEND, null);
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), routineLoadDesc));
            Assert.assertTrue(exception.getMessage().contains("COLUMNS specification"));

            Deencapsulation.setField(job, "mergeType", LoadTask.MergeType.MERGE);
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), null));
            Assert.assertTrue(exception.getMessage().contains("merge_type"));
            Deencapsulation.setField(job, "mergeType", LoadTask.MergeType.APPEND);

            Deencapsulation.setField(job, "mergeTypeSpecified", true);
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), null));
            Assert.assertTrue(exception.getMessage().contains("merge_type"));
            Deencapsulation.setField(job, "mergeTypeSpecified", false);

            RoutineLoadDesc explicitAppendDesc = new RoutineLoadDesc(null, null, null, null, null, null, null,
                    LoadTask.MergeType.APPEND, true, null);
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), explicitAppendDesc));
            Assert.assertTrue(exception.getMessage().contains("merge_type"));

            Deencapsulation.setField(job, "whereExpr",
                    new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "id"), new IntLiteral(1)));
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), null));
            Assert.assertTrue(exception.getMessage().contains("where"));
            Deencapsulation.setField(job, "whereExpr", null);

            RoutineLoadDesc whereDesc = new RoutineLoadDesc(null, null, null, null,
                    new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "id"), new IntLiteral(1)),
                    null, null, LoadTask.MergeType.APPEND, null);
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), whereDesc));
            Assert.assertTrue(exception.getMessage().contains("where"));

            RoutineLoadDesc deleteDesc = new RoutineLoadDesc(null, null, null, null, null, null,
                    new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(null, "is_delete"),
                            new IntLiteral(1)),
                    LoadTask.MergeType.APPEND, null);
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), deleteDesc));
            Assert.assertTrue(exception.getMessage().contains("delete"));

            Deencapsulation.setField(job, "sequenceCol", "seq");
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), null));
            Assert.assertTrue(exception.getMessage().contains("function_column.sequence_col"));
            Deencapsulation.setField(job, "sequenceCol", null);

            RoutineLoadDesc sequenceDesc = new RoutineLoadDesc(null, null, null, null, null, null, null,
                    LoadTask.MergeType.APPEND, "seq");
            exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(Maps.newHashMap(), sequenceDesc));
            Assert.assertTrue(exception.getMessage().contains("function_column.sequence_col"));
        }
    }

    @Test
    public void testRoutineLoadDescIsInstalledBeforeFlexibleAlterValidation() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(job, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPSERT);

        Map<String, String> currentJobProperties = Maps.newHashMap();
        currentJobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        Deencapsulation.setField(job, "jobProperties", currentJobProperties);

        RoutineLoadDesc columnsDesc = new RoutineLoadDesc(null, null,
                Lists.newArrayList(new ImportColumnDesc("id", null)), null, null, null, null,
                LoadTask.MergeType.APPEND, null);
        AlterRoutineLoadCommand columnsCommand = new AlterRoutineLoadCommand(
                new LabelNameInfo("db", "job"), Maps.newHashMap(), Maps.newHashMap());
        Deencapsulation.setField(columnsCommand, "routineLoadDesc", columnsDesc);

        Map<String, String> flexibleProperties = Maps.newHashMap();
        flexibleProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
        AlterRoutineLoadCommand flexibleCommand = new AlterRoutineLoadCommand(
                new LabelNameInfo("db", "job"), Maps.newHashMap(), Maps.newHashMap());
        Deencapsulation.setField(flexibleCommand, "analyzedJobProperties", flexibleProperties);

        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(db);
        Mockito.when(db.getTableNullable(2L)).thenReturn(table);
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate();
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate(Mockito.anyBoolean());
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate();
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate(
                Mockito.anyBoolean());
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(true);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        Mockito.when(table.getBaseSchema()).thenReturn(Lists.newArrayList(new Column("k", PrimitiveType.INT)));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            job.modifyProperties(columnsCommand);
            Assert.assertEquals(1, job.getColumnExprDescs().descs.size());

            UserException exception = Assert.assertThrows(
                    UserException.class, () -> job.modifyProperties(flexibleCommand));
            Assert.assertTrue(exception.getMessage().contains("COLUMNS specification"));
        }
    }

    @Test
    public void testReplayModifyPropertiesRestoresRoutineLoadDescForFlexibleValidation() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPSERT);

        Map<String, String> currentJobProperties = Maps.newHashMap();
        currentJobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        Deencapsulation.setField(job, "jobProperties", currentJobProperties);

        RoutineLoadDesc columnsDesc = new RoutineLoadDesc(new Separator("|", "|"), null,
                Lists.newArrayList(new ImportColumnDesc("id", null)), null, null, null, null,
                LoadTask.MergeType.APPEND, "seq");
        Map<String, String> replayProperties = Maps.newHashMap();
        replayProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(
                1L, replayProperties, null, columnsDesc);
        job.replayModifyProperties(log);
        Assert.assertEquals(1, job.getColumnExprDescs().descs.size());
        Assert.assertEquals("|", job.getColumnSeparator().getSeparator());
        Assert.assertEquals("seq", job.getSequenceCol());
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS, job.getUniqueKeyUpdateMode());
        Assert.assertEquals("UPDATE_FLEXIBLE_COLUMNS",
                log.getJobProperties().get(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE));

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(db);
        Mockito.when(db.getTableNullable(2L)).thenReturn(table);
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate();
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate(Mockito.anyBoolean());
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate();
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate(
                Mockito.anyBoolean());
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(true);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        Mockito.when(table.getBaseSchema()).thenReturn(Lists.newArrayList(new Column("k", PrimitiveType.INT)));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            Map<String, String> flexibleProperties = Maps.newHashMap();
            flexibleProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
            UserException exception = Assert.assertThrows(UserException.class,
                    () -> job.validateFlexiblePartialUpdateForAlter(flexibleProperties, null));
            Assert.assertTrue(exception.getMessage().contains("COLUMNS specification"));
        }
    }

    @Test
    public void testColumnDescsSerializedInRoutineLoadJobSnapshot() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        ImportColumnDescs columnDescs = new ImportColumnDescs();
        columnDescs.descs.add(new ImportColumnDesc("id", null));
        Deencapsulation.setField(job, "columnDescs", columnDescs);
        Deencapsulation.setField(job, "origStmt", new OriginStatement("INVALID", 0));

        String json = GsonUtils.GSON.toJson(job, RoutineLoadJob.class);
        JsonObject jsonObject = GsonUtils.GSON.fromJson(json, JsonObject.class);

        Assert.assertTrue(jsonObject.has("columnDescs"));
        Assert.assertEquals("id",
                jsonObject.getAsJsonObject("columnDescs").getAsJsonArray("des").get(0).getAsJsonObject().get("cn")
                        .getAsString());

        RoutineLoadJob legacyKeyJob = GsonUtils.GSON.fromJson(
                json.replace("\"columnDescs\"", "\"cd\""), RoutineLoadJob.class);
        Assert.assertEquals("id", legacyKeyJob.getColumnExprDescs().descs.get(0).getColumnName());
    }

    @Test
    public void testColumnDescsSnapshotOverridesOrigStmtAfterRead() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "origStmt", new OriginStatement(
                "CREATE ROUTINE LOAD job ON tbl "
                        + "PROPERTIES (\"format\" = \"json\") "
                        + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                        + "\"kafka_topic\" = \"topic\")",
                0));

        ImportColumnDescs columnDescs = new ImportColumnDescs();
        columnDescs.descs.add(new ImportColumnDesc("score", null));
        Deencapsulation.setField(job, "columnDescs", columnDescs);
        Deencapsulation.setField(job, "columnSeparator", new Separator("|", "|"));
        Deencapsulation.setField(job, "lineDelimiter", new Separator("\n", "\\n"));
        Deencapsulation.setField(job, "partitionNamesInfo",
                new PartitionNamesInfo(false, Lists.newArrayList("p2")));
        Deencapsulation.setField(job, "whereExpr",
                new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "score"), new IntLiteral(10)));
        Deencapsulation.setField(job, "deleteCondition",
                new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(null, "deleted"), new IntLiteral(1)));
        Deencapsulation.setField(job, "mergeType", LoadTask.MergeType.MERGE);
        Deencapsulation.setField(job, "mergeTypeSpecified", true);
        Deencapsulation.setField(job, "sequenceCol", "seq2");

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDb("db")).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDb(1L)).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDbOrAnalysisException("db")).thenReturn(db);
        Mockito.when(db.getName()).thenReturn("db");
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getTableOrAnalysisException("tbl")).thenReturn(table);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            String json = GsonUtils.GSON.toJson(job, RoutineLoadJob.class);
            RoutineLoadJob restoredJob = GsonUtils.GSON.fromJson(json, RoutineLoadJob.class);

            Assert.assertNotEquals(RoutineLoadJob.JobState.CANCELLED, restoredJob.getState());
            Assert.assertEquals(1, restoredJob.getColumnExprDescs().descs.size());
            Assert.assertEquals("score", restoredJob.getColumnExprDescs().descs.get(0).getColumnName());
            Assert.assertEquals("|", restoredJob.getColumnSeparator().getSeparator());
            Assert.assertEquals("\n", restoredJob.getLineDelimiter().getSeparator());
            Assert.assertEquals(Lists.newArrayList("p2"),
                    restoredJob.getPartitionNamesInfo().getPartitionNames());
            Assert.assertNotNull(restoredJob.getWhereExpr());
            Assert.assertNotNull(restoredJob.getDeleteCondition());
            Assert.assertEquals(LoadTask.MergeType.MERGE, restoredJob.getMergeType());
            Assert.assertTrue(Deencapsulation.getField(restoredJob, "mergeTypeSpecified"));
            Assert.assertEquals("seq2", restoredJob.getSequenceCol());
        }
    }

    @Test
    public void testLegacyRoutineLoadDescSnapshotOverridesOrigStmtAfterRead() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "origStmt", new OriginStatement(
                "CREATE ROUTINE LOAD job ON tbl "
                        + "WITH MERGE "
                        + "DELETE ON stale_deleted = 1 "
                        + "PROPERTIES (\"format\" = \"json\") "
                        + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                        + "\"kafka_topic\" = \"topic\")",
                0));

        ImportColumnDescs columnDescs = new ImportColumnDescs();
        columnDescs.descs.add(new ImportColumnDesc("score", null));
        Deencapsulation.setField(job, "columnDescs", columnDescs);
        Deencapsulation.setField(job, "columnSeparator", new Separator("|", "|"));
        Deencapsulation.setField(job, "lineDelimiter", new Separator("\n", "\\n"));
        Deencapsulation.setField(job, "partitionNamesInfo",
                new PartitionNamesInfo(false, Lists.newArrayList("p2")));
        Deencapsulation.setField(job, "precedingFilter",
                new BinaryPredicate(BinaryPredicate.Operator.GE, new SlotRef(null, "id"), new IntLiteral(1)));
        Deencapsulation.setField(job, "whereExpr",
                new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "score"), new IntLiteral(10)));
        Deencapsulation.setField(job, "deleteCondition",
                new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(null, "deleted"), new IntLiteral(1)));
        Deencapsulation.setField(job, "mergeType", LoadTask.MergeType.MERGE);
        Deencapsulation.setField(job, "sequenceCol", "seq2");

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDb("db")).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDb(1L)).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDbOrAnalysisException("db")).thenReturn(db);
        Mockito.when(db.getName()).thenReturn("db");
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getTableOrAnalysisException("tbl")).thenReturn(table);
        Mockito.when(db.getTable(2L)).thenReturn(Optional.of(table));
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.hasDeleteSign()).thenReturn(true);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            JsonObject legacyImage = GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(job, RoutineLoadJob.class), JsonObject.class);
            legacyImage.add("cd", legacyImage.remove("columnDescs"));
            legacyImage.add("partitionNamesInfo", legacyImage.remove("pni"));
            legacyImage.add("precedingFilter", legacyImage.remove("pf"));
            legacyImage.add("whereExpr", legacyImage.remove("filter"));
            legacyImage.add("deleteCondition", legacyImage.remove("dc"));
            legacyImage.add("sequenceCol", legacyImage.remove("scn"));
            legacyImage.remove("cs");
            legacyImage.remove("ocs");
            legacyImage.remove("ld");
            legacyImage.remove("old");
            legacyImage.remove("mt");
            JsonObject legacyColumnSeparator = new JsonObject();
            legacyColumnSeparator.addProperty("separator", "|");
            legacyColumnSeparator.addProperty("oriSeparator", "|");
            legacyImage.add("columnSeparator", legacyColumnSeparator);
            JsonObject legacyLineDelimiter = new JsonObject();
            legacyLineDelimiter.addProperty("separator", "\n");
            legacyLineDelimiter.addProperty("oriSeparator", "\\n");
            legacyImage.add("lineDelimiter", legacyLineDelimiter);

            RoutineLoadJob restoredJob = GsonUtils.GSON.fromJson(legacyImage, RoutineLoadJob.class);

            Assert.assertNotEquals(RoutineLoadJob.JobState.CANCELLED, restoredJob.getState());
            Assert.assertEquals(1, restoredJob.getColumnExprDescs().descs.size());
            Assert.assertEquals("score", restoredJob.getColumnExprDescs().descs.get(0).getColumnName());
            Assert.assertEquals("|", restoredJob.getColumnSeparator().getSeparator());
            Assert.assertEquals("\n", restoredJob.getLineDelimiter().getSeparator());
            Assert.assertEquals(Lists.newArrayList("p2"),
                    restoredJob.getPartitionNamesInfo().getPartitionNames());
            Assert.assertNotNull(restoredJob.getPrecedingFilter());
            Assert.assertNotNull(restoredJob.getWhereExpr());
            Assert.assertNotNull(restoredJob.getDeleteCondition());
            Assert.assertEquals(LoadTask.MergeType.MERGE, restoredJob.getMergeType());
            Assert.assertEquals("seq2", restoredJob.getSequenceCol());
        }
    }

    @Test
    public void testLegacyRoutineLoadImageKeepsMergeTypeFromOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "origStmt", new OriginStatement(
                "CREATE ROUTINE LOAD job ON tbl "
                        + "WITH MERGE "
                        + "DELETE ON is_delete = 1 "
                        + "PROPERTIES (\"format\" = \"json\") "
                        + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                        + "\"kafka_topic\" = \"topic\")",
                0));

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDb("db")).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDb(1L)).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDbOrAnalysisException("db")).thenReturn(db);
        Mockito.when(db.getName()).thenReturn("db");
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getTableOrAnalysisException("tbl")).thenReturn(table);
        Mockito.when(db.getTable(2L)).thenReturn(Optional.of(table));
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.hasDeleteSign()).thenReturn(true);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            String json = GsonUtils.GSON.toJson(job, RoutineLoadJob.class);
            JsonObject legacyImage = GsonUtils.GSON.fromJson(json, JsonObject.class);
            legacyImage.remove("mt");
            RoutineLoadJob restoredJob = GsonUtils.GSON.fromJson(legacyImage, RoutineLoadJob.class);

            Assert.assertEquals(LoadTask.MergeType.MERGE, restoredJob.getMergeType());
            Assert.assertNotNull(restoredJob.getDeleteCondition());
        }
    }

    @Test
    public void testFlexibleRoutineLoadImageRestoreSkipsBackendCapabilityCheck() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "tableId", 2L);
        Deencapsulation.setField(job, "isMultiTable", false);
        Deencapsulation.setField(job, "origStmt", new OriginStatement(
                "CREATE ROUTINE LOAD job ON tbl "
                        + "PROPERTIES (\"format\" = \"json\", "
                        + "\"unique_key_update_mode\" = \"UPDATE_FLEXIBLE_COLUMNS\") "
                        + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                        + "\"kafka_topic\" = \"topic\")",
                0));
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        jobProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS");
        Deencapsulation.setField(job, "jobProperties", jobProperties);

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDb("db")).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDb(1L)).thenReturn(Optional.of(db));
        Mockito.when(catalog.getDbOrAnalysisException("db")).thenReturn(db);
        Mockito.when(db.getName()).thenReturn("db");
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getTableOrAnalysisException("tbl")).thenReturn(table);
        Mockito.when(db.getTable(2L)).thenReturn(Optional.of(table));
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.hasDeleteSign()).thenReturn(true);
        Mockito.doCallRealMethod().when(table).validateForFlexiblePartialUpdate(Mockito.anyBoolean());
        Mockito.doCallRealMethod().when(table).validateVariantColumnsForFlexiblePartialUpdate(
                Mockito.anyBoolean());
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(true);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        Mockito.when(table.getBaseSchema()).thenReturn(Lists.newArrayList(
                new Column("k", PrimitiveType.INT), new Column("v", Type.VARIANT)));
        Mockito.when(table.variantEnableFlattenNested()).thenReturn(false);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            RoutineLoadJob restoredJob = GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(job, RoutineLoadJob.class), RoutineLoadJob.class);

            Assert.assertNotEquals(RoutineLoadJob.JobState.CANCELLED, restoredJob.getState());
            Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS,
                    restoredJob.getUniqueKeyUpdateMode());
        }
    }

}
