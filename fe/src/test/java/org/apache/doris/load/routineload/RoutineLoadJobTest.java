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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.KafkaUtil;
import org.apache.doris.persist.EditLog;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java_cup.runtime.Symbol;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class RoutineLoadJobTest {

    @Mocked
    Catalog catalog;
    @Mocked
    EditLog editLog;
    @Mocked
    SqlParser sqlParser;
    @Mocked
    CreateRoutineLoadStmt createRoutineLoadStmt;
    @Mocked
    Symbol symbol;

    @Test
    public void testAfterAbortedReasonOffsetOutOfRange(@Injectable TransactionState transactionState,
                                                       @Injectable RoutineLoadTaskInfo routineLoadTaskInfo,
                                                       @Injectable ReentrantReadWriteLock lock)
            throws UserException {

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                result = txnId;
            }
        };

        String txnStatusChangeReasonString = TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "lock", lock);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAfterAborted(@Injectable TransactionState transactionState,
                                 @Injectable KafkaTaskInfo routineLoadTaskInfo,
                                 @Injectable KafkaProgress progress,
                                 @Injectable ReentrantReadWriteLock lock) throws UserException {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                result = txnId;
                transactionState.getTxnCommitAttachment();
                result = new RLTaskTxnCommitAttachment();
                routineLoadTaskInfo.getPartitions();
                result = Lists.newArrayList();
            }
        };

        String txnStatusChangeReasonString = "no data";
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", progress);
        Deencapsulation.setField(routineLoadJob, "lock", lock);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertEquals(new Long(1), Deencapsulation.getField(routineLoadJob, "abortedTaskNum"));
    }

    @Test
    public void testAfterCommittedWhileTaskAborted(@Injectable TransactionState transactionState,
                                                   @Injectable KafkaProgress progress,
                                                   @Injectable ReentrantReadWriteLock lock) throws UserException {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                result = txnId;
            }
        };

        String txnStatusChangeReasonString = "no data";
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", progress);
        Deencapsulation.setField(routineLoadJob, "lock", lock);
        try {
            routineLoadJob.afterCommitted(transactionState, true);
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        } catch (TransactionException e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetShowInfo(@Mocked KafkaProgress kafkaProgress) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "pauseReason",
                                 TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString());
        Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);

        List<String> showInfo = routineLoadJob.getShowInfo();
        Assert.assertEquals(true, showInfo.stream().filter(entity -> !Strings.isNullOrEmpty(entity))
                .anyMatch(entity -> entity.equals(TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString())));
    }

    @Test
    public void testUpdateWhileDbDeleted() throws UserException {
        new Expectations() {
            {
                catalog.getDb(anyLong);
                result = null;
            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhileTableDeleted(@Injectable Database database) throws UserException {
        new Expectations() {
            {
                catalog.getDb(anyLong);
                result = database;
                database.getTable(anyLong);
                result = null;
            }
        };
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhilePartitionChanged(@Injectable Database database,
                                                @Injectable Table table,
                                                @Injectable PartitionInfo partitionInfo,
                                                @Injectable KafkaProgress kafkaProgress) throws UserException {
        List<PartitionInfo> partitionInfoList = Lists.newArrayList();
        partitionInfoList.add(partitionInfo);

        new Expectations() {
            {
                catalog.getDb(anyLong);
                result = database;
                database.getTable(anyLong);
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
    public void testUpdateNumOfDataErrorRowMoreThanMax() {
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
        Deencapsulation.setField(routineLoadJob, "currentErrorRows", 1);
        Deencapsulation.setField(routineLoadJob, "currentTotalRows", 99);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 2L, 0L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, Deencapsulation.getField(routineLoadJob, "state"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));

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
                result = 1L;
                routineLoadTaskInfo1.getBeId();
                result = 1L;
            }
        };

        Map<Long, Integer> beIdConcurrentTasksNum = routineLoadJob.getBeIdToConcurrentTaskNum();
        Assert.assertEquals(2, (int) beIdConcurrentTasksNum.get(1L));
    }

}
