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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

public class RoutineLoadJobPersistenceTest {
    private MockedStatic<Env> envMock;
    private ConnectContext previousContext;
    private EditLog editLog;
    private RoutineLoadManager manager;
    private TxnStateCallbackFactory callbackFactory;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext.remove();
        envMock = Mockito.mockStatic(Env.class);
        Env env = Mockito.mock(Env.class);
        GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        editLog = Mockito.mock(EditLog.class);
        manager = new RoutineLoadManager();
        callbackFactory = new TxnStateCallbackFactory();
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getRoutineLoadManager()).thenReturn(manager);
        Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(Mockito.mock(RoutineLoadTaskScheduler.class));
        Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testCreateJournalBeforePublishingJob(LoadDataSourceType dataSourceType) throws Exception {
        RoutineLoadJob job = createJob(dataSourceType);
        Mockito.doAnswer(invocation -> {
            // Exercise scheduling at the old race window, before the create record is serialized.
            for (RoutineLoadJob visibleJob : manager.getRoutineLoadJobByState(EnumSet.of(JobState.NEED_SCHEDULE))) {
                setSourceProgress(visibleJob, dataSourceType);
                visibleJob.divideRoutineLoadJob(1);
            }
            byte[] record = serialize(invocation.getArgument(0));
            Assertions.assertEquals(JobState.NEED_SCHEDULE.name(), serializedState(record));
            Assertions.assertNull(manager.getJob(job.getId()));
            Assertions.assertNull(callbackFactory.getCallback(job.getId()));
            Assertions.assertEquals(0, job.getSizeOfRoutineLoadTaskInfoList());
            return null;
        }).when(editLog).logCreateRoutineLoadJob(job);

        manager.addRoutineLoadJob(job, "db", "tbl");

        Mockito.verify(editLog).logCreateRoutineLoadJob(job);
        Assertions.assertSame(job, manager.getJob(job.getId()));
        Assertions.assertSame(job, callbackFactory.getCallback(job.getId()));
        setSourceProgress(job, dataSourceType);
        job.divideRoutineLoadJob(1);
        Assertions.assertEquals(JobState.RUNNING, job.getState());
        Assertions.assertEquals(1, job.getSizeOfRoutineLoadTaskInfoList());
    }

    private RoutineLoadJob createJob(LoadDataSourceType dataSourceType) {
        if (dataSourceType == LoadDataSourceType.KINESIS) {
            return new KinesisRoutineLoadJob(1L, "job", 1L, 1L, "us-east-1", "stream", UserIdentity.ADMIN);
        }
        return new KafkaRoutineLoadJob(1L, "job", 1L, 1L, "127.0.0.1:9092", "topic", UserIdentity.ADMIN);
    }

    private void setSourceProgress(RoutineLoadJob job, LoadDataSourceType dataSourceType) {
        if (dataSourceType == LoadDataSourceType.KINESIS) {
            job.progress = new KinesisProgress(Map.of("shard-0", "100"));
            Deencapsulation.setField(job, "openKinesisShards", Lists.newArrayList("shard-0"));
        } else {
            job.progress = new KafkaProgress(Map.of(0, 100L));
            Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(0));
        }
    }

    private byte[] serialize(Writable value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            value.write(out);
        }
        return bytes.toByteArray();
    }

    private String serializedState(byte[] record) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(record))) {
            return JsonParser.parseString(Text.readString(in)).getAsJsonObject().get("st").getAsString();
        }
    }
}
