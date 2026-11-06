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
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SHOW CREATE ROUTINE LOAD prints the statement a job is recreated from, to rename it, to move it, or to start it
 * over after STOP. Each case creates a job the way CREATE ROUTINE LOAD does, recreates it from what SHOW CREATE
 * prints for it, and checks that the recreated job is defined the same way.
 */
public class RoutineLoadShowCreateRoundTripTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("rl_db");
        // load_to_single_tablet needs a randomly distributed table
        createTable("CREATE TABLE rl_db.dup_tbl (k1 INT, v1 VARCHAR(32)) DUPLICATE KEY(k1) "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        // flexible partial updates need the skip bitmap column
        createTable("CREATE TABLE rl_db.mow_tbl (k1 INT, v1 VARCHAR(32)) UNIQUE KEY(k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1', "
                + "'enable_unique_key_merge_on_write' = 'true', "
                + "'enable_unique_key_skip_bitmap_column' = 'true')");
        createTable("CREATE TABLE rl_db.range_tbl (k1 INT, v1 VARCHAR(32)) DUPLICATE KEY(k1) "
                + "PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('100')) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        alterTableSync("ALTER TABLE rl_db.range_tbl ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ('100')");
    }

    // What RoutineLoadManager#createRoutineLoadJob does, except registering the job, so that the statement
    // SHOW CREATE prints can create it again under the same name.
    private KafkaRoutineLoadJob createJob(String sql) throws Exception {
        CreateRoutineLoadCommand command = (CreateRoutineLoadCommand) new NereidsParser().parseSingle(sql);
        CreateRoutineLoadInfo info = command.getCreateRoutineLoadInfo();
        info.validate(connectContext);
        KafkaRoutineLoadJob job = KafkaRoutineLoadJob.fromCreateInfo(info, connectContext);
        job.setOrigStmt(new OriginStatement(sql, 0));
        job.setComment(info.getComment());
        return job;
    }

    // Everything CREATE ROUTINE LOAD sets on a job, as the job uses it.
    private static Map<String, Object> definition(KafkaRoutineLoadJob job) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("desired_concurrent_number", Deencapsulation.getField(job, "desireTaskConcurrentNum"));
        d.put("max_error_number", Deencapsulation.getField(job, "maxErrorNum"));
        d.put("max_filter_ratio", job.getMaxFilterRatio());
        d.put("max_batch_interval", job.getMaxBatchIntervalS());
        d.put("max_batch_rows", job.getMaxBatchRows());
        d.put("max_batch_size", job.getMaxBatchSizeBytes());
        d.put("strict_mode", job.isStrictMode());
        d.put("timezone", job.getTimezone());
        d.put("exec_mem_limit", job.getMemLimit());
        d.put("send_batch_parallelism", job.getSendBatchParallelism());
        d.put("load_to_single_tablet", job.isLoadToSingleTablet());
        d.put("partial_columns", job.isFixedPartialUpdate());
        d.put("unique_key_update_mode", job.getUniqueKeyUpdateMode());
        d.put("partial_update_new_key_behavior", Deencapsulation.getField(job, "partialUpdateNewKeyPolicy"));
        d.put("workload_group", job.getWorkloadGroup());
        d.put("compute_group", job.getDeclaredComputeGroup());
        d.put("format", job.getFormat());
        d.put("jsonpaths", job.getJsonPaths());
        d.put("strip_outer_array", job.isStripOuterArray());
        d.put("num_as_string", job.isNumAsString());
        d.put("fuzzy_parse", job.isFuzzyParse());
        d.put("json_root", job.getJsonRoot());
        d.put("enclose", job.getEnclose());
        d.put("escape", job.getEscape());
        d.put("empty_field_as_null", job.getEmptyFieldAsNull());
        d.put("merge_type", job.getMergeType());
        d.put("column_separator", job.getColumnSeparator() == null ? null : job.getColumnSeparator().getSeparator());
        d.put("columns", job.getColumnExprDescs().descs.toString());
        d.put("where", String.valueOf(job.getWhereExpr()));
        d.put("preceding_filter", String.valueOf(job.getPrecedingFilter()));
        d.put("delete_on", String.valueOf(job.getDeleteCondition()));
        d.put("partitions", job.getPartitionNamesInfo() == null ? null
                : job.getPartitionNamesInfo().isTemp() + " " + job.getPartitionNamesInfo().getPartitionNames());
        d.put("comment", Deencapsulation.getField(job, "comment"));
        d.put("data_source", job.getDataSourceProperties());
        d.put("custom", job.getCustomProperties());
        return d;
    }

    // Recreates the job from what SHOW CREATE prints for it, which prints the same statement for the new job.
    private KafkaRoutineLoadJob assertRoundTrip(String sql) throws Exception {
        KafkaRoutineLoadJob job = createJob(sql);
        String showCreate = job.getShowCreateInfo();
        KafkaRoutineLoadJob recreated = createJob(showCreate);
        Assertions.assertEquals(definition(job), definition(recreated), showCreate);
        Assertions.assertEquals(showCreate, recreated.getShowCreateInfo());
        return job;
    }

    @Test
    public void testCsvJob() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_csv ON dup_tbl\n"
                + "COLUMNS TERMINATED BY ',',\n"
                + "COLUMNS(k1, v1),\n"
                + "WHERE k1 > 0\n"
                + "PROPERTIES (\n"
                + "'desired_concurrent_number' = '2',\n"
                + "'max_error_number' = '5',\n"
                + "'max_filter_ratio' = '0.5',\n"
                + "'max_batch_interval' = '20',\n"
                + "'max_batch_rows' = '300000',\n"
                + "'max_batch_size' = '209715200',\n"
                + "'strict_mode' = 'true',\n"
                + "'timezone' = 'Asia/Tokyo',\n"
                + "'exec_mem_limit' = '4294967296',\n"
                + "'send_batch_parallelism' = '3',\n"
                + "'load_to_single_tablet' = 'true',\n"
                + "'workload_group' = 'normal',\n"
                + "'format' = 'csv',\n"
                + "'enclose' = '\"',\n"
                + "'escape' = '\\\\',\n"
                + "'empty_field_as_null' = 'true'\n"
                + ")\n"
                + "FROM KAFKA (\n"
                + "'kafka_broker_list' = '127.0.0.1:9092',\n"
                + "'kafka_topic' = 'topic_csv',\n"
                + "'property.kafka_default_offsets' = 'OFFSET_BEGINNING'\n"
                + ")\n"
                + "COMMENT 'it''s the \"csv\" job'");
        Assertions.assertEquals(3, job.getSendBatchParallelism());
        Assertions.assertTrue(job.isLoadToSingleTablet());
        Assertions.assertEquals("normal", job.getWorkloadGroup());
        Assertions.assertEquals('"', job.getEnclose());
        Assertions.assertEquals('\\', job.getEscape());
        Assertions.assertTrue(job.getEmptyFieldAsNull());
        Assertions.assertEquals("it's the \"csv\" job", Deencapsulation.getField(job, "comment"));
    }

    // UPDATE_FLEXIBLE_COLUMNS leaves partial_columns false, the only thing the statement used to carry.
    @Test
    public void testFlexiblePartialUpdateJob() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_flexible ON mow_tbl\n"
                + "PROPERTIES (\n"
                + "'format' = 'json',\n"
                + "'unique_key_update_mode' = 'UPDATE_FLEXIBLE_COLUMNS',\n"
                + "'partial_update_new_key_behavior' = 'ERROR',\n"
                + "'strip_outer_array' = 'true',\n"
                + "'num_as_string' = 'true',\n"
                + "'json_root' = '$.data'\n"
                + ")\n"
                + "FROM KAFKA (\n"
                + "'kafka_broker_list' = '127.0.0.1:9092',\n"
                + "'kafka_topic' = 'topic_flexible'\n"
                + ")");
        Assertions.assertEquals(TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS, job.getUniqueKeyUpdateMode());
        Assertions.assertEquals(TPartialUpdateNewRowPolicy.ERROR,
                Deencapsulation.getField(job, "partialUpdateNewKeyPolicy"));
    }

    // The quotes in the json paths used to end the string literal, so the statement did not parse.
    @Test
    public void testFixedPartialUpdateJobWithJsonPaths() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_fixed ON mow_tbl\n"
                + "COLUMNS(k1, v1)\n"
                + "PROPERTIES (\n"
                + "'format' = 'json',\n"
                + "'partial_columns' = 'true',\n"
                + "'partial_update_new_key_behavior' = 'ERROR',\n"
                + "'jsonpaths' = '[\"$.k1\",\"$.v1\"]',\n"
                + "'strip_outer_array' = 'true',\n"
                + "'fuzzy_parse' = 'true'\n"
                + ")\n"
                + "FROM KAFKA (\n"
                + "'kafka_broker_list' = '127.0.0.1:9092',\n"
                + "'kafka_topic' = 'topic_fixed'\n"
                + ")");
        Assertions.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS, job.getUniqueKeyUpdateMode());
        Assertions.assertEquals("[\"$.k1\",\"$.v1\"]", job.getJsonPaths());
    }

    // The default new key policy and an UPSERT job print nothing new.
    @Test
    public void testFixedPartialUpdateJobWithDefaultPolicy() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_fixed_append ON mow_tbl\n"
                + "COLUMNS(k1, v1)\n"
                + "PROPERTIES ('format' = 'json', 'unique_key_update_mode' = 'UPDATE_FIXED_COLUMNS')\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_fixed_append')");
        Assertions.assertFalse(job.getShowCreateInfo().contains(CreateRoutineLoadInfo.PARTIAL_UPDATE_NEW_KEY_POLICY));
        KafkaRoutineLoadJob upsert = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_upsert ON mow_tbl\n"
                + "PROPERTIES ('format' = 'json')\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_upsert')");
        Assertions.assertFalse(upsert.getShowCreateInfo().contains(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE));
    }

    @Test
    public void testMergeJob() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_merge ON mow_tbl\n"
                + "WITH MERGE\n"
                + "COLUMNS TERMINATED BY '\\t',\n"
                + "PRECEDING FILTER k1 > 1,\n"
                + "DELETE ON v1 = 'x'\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_merge')");
        Assertions.assertEquals(LoadTask.MergeType.MERGE, job.getMergeType());
    }

    // TEMPORARY used to be dropped, so the statement named a formal partition that does not exist.
    @Test
    public void testTemporaryPartitions() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_temp_partition ON range_tbl\n"
                + "TEMPORARY PARTITION(tp1)\n"
                + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_temp_partition')");
        Assertions.assertTrue(job.getPartitionNamesInfo().isTemp());
    }

    @Test
    public void testQuotedKafkaProperty() throws Exception {
        KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_client_id ON dup_tbl\n"
                + "FROM KAFKA (\n"
                + "'kafka_broker_list' = '127.0.0.1:9092',\n"
                + "'kafka_topic' = 'topic_client_id',\n"
                + "'property.client.id' = 'a\"b\\\\c'\n"
                + ")");
        Assertions.assertEquals("a\"b\\c", job.getCustomProperties().get("property.client.id"));
    }

    // Under NO_BACKSLASH_ESCAPES a backslash is an ordinary character, both in what the session types and in
    // what SHOW CREATE prints for it.
    @Test
    public void testNoBackslashEscapes() throws Exception {
        long sqlMode = connectContext.getSessionVariable().getSqlMode();
        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        try {
            KafkaRoutineLoadJob job = assertRoundTrip("CREATE ROUTINE LOAD rl_db.job_no_backslash_escapes ON dup_tbl\n"
                    + "PROPERTIES ('enclose' = '\"', 'escape' = '\\')\n"
                    + "FROM KAFKA ('kafka_broker_list' = '127.0.0.1:9092', 'kafka_topic' = 'topic_no_escapes')");
            Assertions.assertEquals('"', job.getEnclose());
            Assertions.assertEquals('\\', job.getEscape());
        } finally {
            connectContext.getSessionVariable().setSqlMode(sqlMode);
        }
    }
}
