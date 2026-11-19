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

    // Without enclose and escape CREATE records the zero byte, and the replay gives the job none.
    @Test
    public void testCreateWithoutEncloseAndEscapeRecordsTheZeroByte() throws Exception {
        KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_no_enclose ON dup_tbl\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_no_enclose')");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        Assertions.assertEquals("\u0000", jobProperties.get(CsvFileFormatProperties.PROP_ENCLOSE));
        Assertions.assertEquals("\u0000", jobProperties.get(CsvFileFormatProperties.PROP_ESCAPE));
        KafkaRoutineLoadJob replayed = replay(job);
        Assertions.assertEquals(0, replayed.getEnclose());
        Assertions.assertEquals(0, replayed.getEscape());
    }

    // CREATE of an older version took any character as enclose or escape, and recorded the byte it took, which reads
    // back as U+FFFD from 0x80 up. The rebuild re-parses the stored statement, which can hold such a character (U+FFFD
    // where the client did not send UTF-8), and must not reject it as CREATE does now, since that would cancel the
    // job. The job keeps no enclose and no escape for the U+FFFD recorded, which is what it has run with after every
    // restart.
    @Test
    public void testOlderCreateStatementWithNonAsciiValueReplays() throws Exception {
        for (String value : new String[] {String.valueOf((char) 0xFFFD), "é"}) {
            KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_older_create ON dup_tbl\n"
                    + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_older_create')");
            // What an older version admitted and recorded
            job.setOrigStmt(new OriginStatement("CREATE ROUTINE LOAD rl_replay_db.job_older_create ON dup_tbl\n"
                    + "PROPERTIES ('enclose' = '" + value + "', 'escape' = '" + value + "')\n"
                    + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_older_create')", 0));
            Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
            jobProperties.put(CsvFileFormatProperties.PROP_ENCLOSE, String.valueOf((char) 0xFFFD));
            jobProperties.put(CsvFileFormatProperties.PROP_ESCAPE, String.valueOf((char) 0xFFFD));

            KafkaRoutineLoadJob replayed = replay(job);
            Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, replayed.getState(), value);
            Assertions.assertEquals(0, replayed.getEnclose(), value);
            Assertions.assertEquals(0, replayed.getEscape(), value);
        }
    }

    // CREATE takes only an ASCII character as enclose or escape: the tasks would get 0xFD or 0xEF for any byte from
    // 0x80 up, which in UTF-8 data is absent or part of a character anyway. U+FFFD is what a statement whose bytes
    // were not UTF-8 decodes to, and the full-width comma was taken as its low byte, 0x0C, as enclose.
    @Test
    public void testCreateRejectsNonAsciiEncloseAndEscape() {
        for (String key : new String[] {CsvFileFormatProperties.PROP_ENCLOSE, CsvFileFormatProperties.PROP_ESCAPE}) {
            for (String value : new String[] {"é", String.valueOf((char) 0xFFFD), "，"}) {
                String sql = "CREATE ROUTINE LOAD rl_replay_db.job_non_ascii ON dup_tbl\n"
                        + "PROPERTIES ('" + key + "' = '" + value + "')\n"
                        + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_non_ascii')";
                Exception e = Assertions.assertThrows(Exception.class, () -> createJob(sql));
                Assertions.assertTrue(e.getMessage().contains(key + " must be an ASCII character"), e.getMessage());
            }
        }
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

    // ALTER of an older version also stored a value that is not ASCII. Neither the replay of that ALTER nor a rebuild
    // of the job, whose metadata then holds the value, applies it: the job keeps the byte it has.
    @Test
    public void testNonAsciiValueAnOlderAlterStoredIsIgnored() throws Exception {
        for (String value : new String[] {"é", String.valueOf((char) 0xFFFD)}) {
            KafkaRoutineLoadJob job = createJob("CREATE ROUTINE LOAD rl_replay_db.job_older_alter ON dup_tbl\n"
                    + "PROPERTIES ('enclose' = '\"', 'escape' = '\\\\')\n"
                    + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_older_alter')");
            Map<String, String> altered = Maps.newHashMap();
            altered.put(CsvFileFormatProperties.PROP_ENCLOSE, value);
            altered.put(CsvFileFormatProperties.PROP_ESCAPE, value);
            job.replayModifyProperties(new AlterRoutineLoadJobOperationLog(job.getId(), altered, null));
            Assertions.assertEquals('"', job.getEnclose(), value);
            Assertions.assertEquals('\\', job.getEscape(), value);

            Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
            Assertions.assertEquals(value, jobProperties.get(CsvFileFormatProperties.PROP_ENCLOSE));
            KafkaRoutineLoadJob replayed = replay(job);
            Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, replayed.getState(), value);
            Assertions.assertEquals(0, replayed.getEnclose(), value);
            Assertions.assertEquals(0, replayed.getEscape(), value);
        }
    }
}
