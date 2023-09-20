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

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.KafkaUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java_cup.runtime.Symbol;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RoutineLoadJobTest {

    @Mocked
    EditLog editLog;
    @Mocked
    SqlParser sqlParser;
    @Mocked
    CreateRoutineLoadStmt createRoutineLoadStmt;
    @Mocked
    Symbol symbol;

    @Test
    public void testAfterAbortedReasonOffsetOutOfRange(@Mocked Env env,
                                                       @Injectable TransactionState transactionState,
                                                       @Injectable RoutineLoadTaskInfo routineLoadTaskInfo)
            throws UserException {

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        String txnStatusChangeReasonString = TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAfterAborted(@Injectable TransactionState transactionState,
                                 @Injectable KafkaTaskInfo routineLoadTaskInfo) throws UserException {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        tKafkaRLTaskProgress.partitionCmtOffset = Maps.newHashMap();
        KafkaProgress kafkaProgress = new KafkaProgress(tKafkaRLTaskProgress);
        Deencapsulation.setField(attachment, "progress", kafkaProgress);

        KafkaProgress currentProgress = new KafkaProgress(tKafkaRLTaskProgress);

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
                transactionState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                routineLoadTaskInfo.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList();
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        String txnStatusChangeReasonString = "no data";
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", currentProgress);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);
        RoutineLoadStatistic jobStatistic = Deencapsulation.getField(routineLoadJob, "jobStatistic");

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertEquals(new Long(1), Deencapsulation.getField(jobStatistic, "abortedTaskNum"));
    }

    @Test
    public void testAfterCommittedWhileTaskAborted(@Mocked Env env,
                                                   @Injectable TransactionState transactionState,
                                                   @Injectable KafkaProgress progress) throws UserException {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", progress);
        try {
            routineLoadJob.afterCommitted(transactionState, true);
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        } catch (TransactionException e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetShowInfo(@Mocked KafkaProgress kafkaProgress, @Injectable UserIdentity userIdentity) {
        new Expectations() {
            {
                userIdentity.getQualifiedUser();
                minTimes = 0;
                result = "root";
            }
        };
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
    public void testUpdateWhileDbDeleted(@Mocked Env env, @Mocked InternalCatalog catalog) throws UserException {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
            }
        };

        new Expectations() {
            {
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhileTableDeleted(@Mocked Env env, @Mocked InternalCatalog catalog,
            @Injectable Database database) throws UserException {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = null;
            }
        };
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhilePartitionChanged(@Mocked Env env, @Mocked InternalCatalog catalog,
            @Injectable Database database, @Injectable Table table, @Injectable PartitionInfo partitionInfo,
            @Injectable KafkaProgress kafkaProgress) throws UserException {
        List<PartitionInfo> partitionInfoList = Lists.newArrayList();
        partitionInfoList.add(partitionInfo);

        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = table;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                    Map<String, String> convertedCustomProperties) throws UserException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
    }

    @Test
    public void testUpdateNumOfDataErrorRowMoreThanMax(@Mocked Env env) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 0);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 0);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 1L, 1L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, Deencapsulation.getField(routineLoadJob, "state"));

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
    public void testGetBeIdToConcurrentTaskNum(@Injectable RoutineLoadTaskInfo routineLoadTaskInfo,
                                               @Injectable RoutineLoadTaskInfo routineLoadTaskInfo1) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        routineLoadTaskInfoList.add(routineLoadTaskInfo1);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

        new Expectations() {
            {
                routineLoadTaskInfo.getBeId();
                minTimes = 0;
                result = 1L;
                routineLoadTaskInfo1.getBeId();
                minTimes = 0;
                result = 1L;
            }
        };

        Map<Long, Integer> beIdConcurrentTasksNum = routineLoadJob.getBeCurrentTasksNumMap();
        Assert.assertEquals(2, (int) beIdConcurrentTasksNum.get(1L));
    }

    @Test
    public void testGetShowCreateInfo() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(111L, "test_load", "test", 1,
                11, "localhost:9092", "test_topic", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 10);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 10);
        String showCreateInfo = routineLoadJob.getShowCreateInfo();
        String expect = "CREATE ROUTINE LOAD test_load ON 11\n"
                + "WITH APPEND\n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"desired_concurrent_number\" = \"0\",\n"
                + "\"max_error_number\" = \"10\",\n"
                + "\"max_filter_ratio\" = \"1.0\",\n"
                + "\"max_batch_interval\" = \"10\",\n"
                + "\"max_batch_rows\" = \"10\",\n"
                + "\"max_batch_size\" = \"104857600\",\n"
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

}
