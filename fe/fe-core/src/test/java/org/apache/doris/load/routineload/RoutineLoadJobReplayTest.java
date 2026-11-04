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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * A follower replaying the edit log, a restarted FE and a new master rebuild a routine load job from its metadata:
 * a GSON round trip and gsonPostProcess, on a thread without a session. The rebuilt job has to run as the job did
 * before, including what the tasks are built from.
 */
public class RoutineLoadJobReplayTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("rl_replay_db");
        // load_to_single_tablet needs a randomly distributed table
        createTable("CREATE TABLE rl_replay_db.dup_tbl (k1 INT, v1 VARCHAR(32)) DUPLICATE KEY(k1) "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES ('replication_num' = '1')");
    }

    // What RoutineLoadManager#createRoutineLoadJob does, except registering the job.
    private KafkaRoutineLoadJob createJob(String sql) throws Exception {
        CreateRoutineLoadCommand command = (CreateRoutineLoadCommand) new NereidsParser().parseSingle(sql);
        CreateRoutineLoadInfo info = command.getCreateRoutineLoadInfo();
        info.validate(connectContext);
        KafkaRoutineLoadJob job = KafkaRoutineLoadJob.fromCreateInfo(info, connectContext);
        job.setOrigStmt(new OriginStatement(sql, 0));
        return job;
    }

    private KafkaRoutineLoadJob replay(RoutineLoadJob job) {
        String json = GsonUtils.GSON.toJson(job);
        ConnectContext.remove();
        try {
            return (KafkaRoutineLoadJob) GsonUtils.GSON.fromJson(json, RoutineLoadJob.class);
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testReplayKeepsTheDeclaredProperties() throws Exception {
        KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_declared ON dup_tbl\n"
                + "PROPERTIES (\n"
                + "'max_filter_ratio' = '0.5',\n"
                + "'exec_mem_limit' = '4294967296',\n"
                + "'send_batch_parallelism' = '3',\n"
                + "'load_to_single_tablet' = 'true',\n"
                + "'enclose' = '\"',\n"
                + "'escape' = '\\\\'\n"
                + ")\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_declared')");

        KafkaRoutineLoadJob replayed = replay(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, replayed.getState());
        Assertions.assertEquals(0.5, replayed.getMaxFilterRatio());
        Assertions.assertEquals(4294967296L, replayed.getMemLimit());
        Assertions.assertEquals(3, replayed.getSendBatchParallelism());
        Assertions.assertTrue(replayed.isLoadToSingleTablet());
        NereidsRoutineLoadTaskInfo taskInfo = replayed.toNereidsRoutineLoadTaskInfo();
        Assertions.assertEquals('"', taskInfo.getEnclose());
        Assertions.assertEquals('\\', taskInfo.getEscape());
        Assertions.assertEquals(0.5, taskInfo.getMaxFilterRatio());
        Assertions.assertEquals(4294967296L, taskInfo.getMemLimit());
        Assertions.assertEquals(3, taskInfo.getSendBatchParallelism());
        Assertions.assertTrue(taskInfo.isLoadToSingleTablet());
    }

    // What CREATE takes from the creating session when the statement leaves it out.
    @Test
    public void testReplayKeepsWhatTheCreatingSessionGave() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int sendBatchParallelism = sessionVariable.sendBatchParallelism;
        boolean memtableOnSinkNode = sessionVariable.enableMemtableOnSinkNode;
        KafkaRoutineLoadJob withDefaults;
        KafkaRoutineLoadJob withSessionValues;
        try {
            withDefaults = createJob("CREATE ROUTINE LOAD rl_replay_db.job_session_default ON dup_tbl\n"
                    + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_session')");
            sessionVariable.sendBatchParallelism = 4;
            sessionVariable.enableMemtableOnSinkNode = false;
            withSessionValues = createJob("CREATE ROUTINE LOAD rl_replay_db.job_session_set ON dup_tbl\n"
                    + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_session')");
        } finally {
            sessionVariable.sendBatchParallelism = sendBatchParallelism;
            sessionVariable.enableMemtableOnSinkNode = memtableOnSinkNode;
        }

        // enable_memtable_on_sink_node is on by default
        Assertions.assertTrue(replay(withDefaults).isMemtableOnSinkNode());
        KafkaRoutineLoadJob replayed = replay(withSessionValues);
        Assertions.assertEquals(4, replayed.getSendBatchParallelism());
        Assertions.assertFalse(replayed.isMemtableOnSinkNode());
    }

    // new String(new byte[]{b}) decodes every byte from 0x80 up to U+FFFD, so CREATE records the value the statement
    // gave, as ALTER does, and the replay parses it as CREATE did. Without a value it records the zero byte, as it
    // always has.
    @Test
    public void testCreateRecordsTheGivenEncloseAndEscape() throws Exception {
        KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_high_bit ON dup_tbl\n"
                + "PROPERTIES ('enclose' = 'é', 'escape' = 'é')\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_high_bit')");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        Assertions.assertEquals("é", jobProperties.get(CsvFileFormatProperties.PROP_ENCLOSE));
        Assertions.assertEquals("é", jobProperties.get(CsvFileFormatProperties.PROP_ESCAPE));
        // enclose is the low byte of the character, escape the first byte of its encoding in the default charset
        Assertions.assertEquals((byte) 0xE9, job.getEnclose());
        byte escape = job.getEscape();

        NereidsRoutineLoadTaskInfo taskInfo = replay(job).toNereidsRoutineLoadTaskInfo();
        Assertions.assertEquals((byte) 0xE9, taskInfo.getEnclose());
        Assertions.assertEquals(escape, taskInfo.getEscape());

        KafkaRoutineLoadJob withoutValues = createJob("CREATE ROUTINE LOAD rl_replay_db.job_no_enclose ON dup_tbl\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_no_enclose')");
        jobProperties = Deencapsulation.getField(withoutValues, "jobProperties");
        Assertions.assertEquals("\u0000", jobProperties.get(CsvFileFormatProperties.PROP_ENCLOSE));
        Assertions.assertEquals("\u0000", jobProperties.get(CsvFileFormatProperties.PROP_ESCAPE));
        KafkaRoutineLoadJob replayed = replay(withoutValues);
        Assertions.assertEquals(0, replayed.getEnclose());
        Assertions.assertEquals(0, replayed.getEscape());
    }

    // CREATE of an older version recorded the byte, so the metadata of a job created with a byte from 0x80 up holds
    // U+FFFD. The byte is lost: rather than parse to 0xFD and 0xEF, the job keeps no enclose and no escape, which is
    // what it has run with after every restart.
    @Test
    public void testByteAnOlderCreateLostIsIgnored() throws Exception {
        KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_lost_byte ON dup_tbl\n"
                + "PROPERTIES ('enclose' = 'é', 'escape' = 'é')\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_lost_byte')");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        // What an older CREATE recorded for a byte from 0x80 up
        String lost = String.valueOf((char) 0xFFFD);
        jobProperties.put(CsvFileFormatProperties.PROP_ENCLOSE, lost);
        jobProperties.put(CsvFileFormatProperties.PROP_ESCAPE, lost);

        KafkaRoutineLoadJob replayed = replay(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, replayed.getState());
        Assertions.assertEquals(0, replayed.getEnclose());
        Assertions.assertEquals(0, replayed.getEscape());
    }

    // ALTER ROUTINE LOAD installs the analyzed properties through modifyPropertiesInternal, as the replay of its
    // edit log does.
    @Test
    public void testAlteredEncloseAndEscapeReachTheTasks() throws Exception {
        KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_altered ON dup_tbl\n"
                + "PROPERTIES ('enclose' = '\"', 'escape' = '\\\\')\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_altered')");
        Map<String, String> altered = Maps.newHashMap();
        altered.put(CsvFileFormatProperties.PROP_ENCLOSE, "'");
        altered.put(CsvFileFormatProperties.PROP_ESCAPE, "|");
        job.replayModifyProperties(new AlterRoutineLoadJobOperationLog(job.getId(), altered, null));

        NereidsRoutineLoadTaskInfo taskInfo = job.toNereidsRoutineLoadTaskInfo();
        Assertions.assertEquals('\'', taskInfo.getEnclose());
        Assertions.assertEquals('|', taskInfo.getEscape());
        KafkaRoutineLoadJob replayed = replay(job);
        Assertions.assertEquals('\'', replayed.getEnclose());
        Assertions.assertEquals('|', replayed.getEscape());
    }

    // ALTER of an older version stored enclose and escape without checking them. Such a value never reached the
    // job, and must neither now nor fail the replay of the job or of the ALTER.
    @Test
    public void testInvalidValueAnOlderAlterStoredIsIgnored() throws Exception {
        KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_invalid ON dup_tbl\n"
                + "PROPERTIES ('enclose' = '\"', 'escape' = '\\\\')\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_invalid')");
        Map<String, String> altered = Maps.newHashMap();
        altered.put(CsvFileFormatProperties.PROP_ENCLOSE, "ab");
        job.replayModifyProperties(new AlterRoutineLoadJobOperationLog(job.getId(), altered, null));
        Assertions.assertEquals('"', job.getEnclose());

        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        Assertions.assertEquals("ab", jobProperties.get(CsvFileFormatProperties.PROP_ENCLOSE));
        KafkaRoutineLoadJob replayed = replay(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, replayed.getState());
        Assertions.assertEquals(0, replayed.getEnclose());
        Assertions.assertEquals('\\', replayed.getEscape());
    }
}
