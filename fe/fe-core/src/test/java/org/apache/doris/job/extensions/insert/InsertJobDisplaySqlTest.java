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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TCell;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;

public class InsertJobDisplaySqlTest extends TestWithFeService {
    private static final String VALUES_SQL = "insert into t_test_BASE_inSert_job (timestamp, type, user_id)"
            + " values ('2023-03-18','1','12213')";
    private static final String START_TIME = "2052-03-18 00:00:00";
    private static final String SOURCE = "S3('uri'='s3://bucket/data.parquet', 'format'='parquet',"
            + " 's3.access_key'='source-access', 's3.secret_key'='source-secret')";
    private static final String TARGET = "S3('file_path'='s3://bucket/result_', 'format'='csv',"
            + " 's3.access_key'='target-access', 's3.secret_key'='target-secret')";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("job_display");
        connectContext.setDatabase("job_display");
    }

    @Override
    protected void runBeforeEach() {
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void removeContext() {
        ConnectContext.remove();
    }

    @ParameterizedTest
    @EnumSource(value = JobExecuteType.class, names = {"ONE_TIME", "RECURRING"})
    public void freshValuesWithoutContext(JobExecuteType type) throws Exception {
        InsertJob job = newJob(type, VALUES_SQL);
        Assertions.assertNull(job.getEncryptedSql());
        ConnectContext.remove();
        assertDisplay(job, VALUES_SQL, false);
        Assertions.assertNull(ConnectContext.get());
    }

    @ParameterizedTest
    @CsvSource({"ONE_TIME,false", "ONE_TIME,true", "RECURRING,false", "RECURRING,true"})
    public void restoredValuesWithoutContext(JobExecuteType type, boolean showFirst) throws Exception {
        InsertJob job = newJob(type, VALUES_SQL);
        assertDisplay(job, VALUES_SQL, false);
        Assertions.assertEquals(VALUES_SQL, job.getEncryptedSql());
        InsertJob restored = restore(job);
        ConnectContext.remove();
        assertDisplay(restored, VALUES_SQL, showFirst);
        Assertions.assertNull(ConnectContext.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void sourceAndTargetMaskingSurvivesRecovery(boolean withContext) throws Exception {
        String[] statements = {
                "INSERT INTO t SELECT 1",
                "INSERT INTO t SELECT * FROM " + SOURCE,
                "WITH src AS (SELECT * FROM " + SOURCE + ") INSERT INTO t SELECT * FROM src",
                "INSERT INTO t WITH src AS (SELECT * FROM " + SOURCE + ") SELECT * FROM src",
                "INSERT INTO " + TARGET + " SELECT * FROM " + SOURCE,
                "WITH src AS (SELECT * FROM " + SOURCE + ") INSERT INTO " + TARGET + " SELECT * FROM src"
        };
        for (String sql : statements) {
            connectContext.setThreadLocalInfo();
            InsertJob job = newJob(JobExecuteType.ONE_TIME, sql);
            // Populate the cache before persistence, then exercise the real cache miss after recovery.
            String expected = job.getTvfInfo().getColumnValue().get(6).getStringVal();
            assertMasked(sql, expected);
            InsertJob restored = restore(job);
            if (!withContext) {
                ConnectContext.remove();
            }
            assertDisplay(restored, expected, true);
            Assertions.assertEquals("job_display", connectContext.getDatabase());
            Assertions.assertSame(withContext ? connectContext : null, ConnectContext.get());
        }
    }

    @Test
    public void malformedSqlIsRedactedWithoutChangingPersistedSql() throws Exception {
        String sql = "INSERT INTO S3('s3.secret_key'='malformed-secret' SELECT";
        InsertJob restored = restore(newJob(JobExecuteType.ONE_TIME, sql));
        ConnectContext.remove();
        assertDisplay(restored, "<redacted>", false);
        Assertions.assertNull(restored.getEncryptedSql());
        Assertions.assertNull(ConnectContext.get());
    }

    private InsertJob newJob(JobExecuteType type, String sql) throws Exception {
        TimerDefinition timer = new TimerDefinition();
        timer.setStartTimeMs(TimeUtils.timeStringToLong(START_TIME));
        if (type == JobExecuteType.RECURRING) {
            timer.setInterval(1L);
            timer.setIntervalUnit(IntervalUnit.HOUR);
        }
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        config.setExecuteType(type);
        config.setTimerDefinition(timer);
        return new InsertJob(type == JobExecuteType.ONE_TIME ? "JOB_ONETIME" : "JOB_RECURRING",
                JobStatus.RUNNING, "job_display", "display test", UserIdentity.ROOT, config, 0L, sql);
    }

    private InsertJob restore(InsertJob job) {
        String json = GsonUtils.GSON.toJson(job);
        Assertions.assertFalse(json.contains("\"encryptedSql\""));
        InsertJob restored = (InsertJob) GsonUtils.GSON.fromJson(json, AbstractJob.class);
        Assertions.assertNull(restored.getEncryptedSql());
        Assertions.assertEquals(job.getExecuteSql(), restored.getExecuteSql());
        Assertions.assertEquals(job.getCurrentDbName(), restored.getCurrentDbName());
        Assertions.assertEquals(job.getJobId(), restored.getJobId());
        Assertions.assertEquals(json, GsonUtils.GSON.toJson(restored));
        return restored;
    }

    private void assertDisplay(InsertJob job, String expected, boolean showFirst) {
        String persisted = GsonUtils.GSON.toJson(job);
        String executeSql = job.getExecuteSql();
        List<String> show = showFirst ? job.getCommonShowInfo() : null;
        List<String> tvf = job.getTvfInfo().getColumnValue().stream()
                .map(TCell::getStringVal).collect(Collectors.toList());
        if (!showFirst) {
            show = job.getCommonShowInfo();
        }
        String strategy = job.getJobConfig().getExecuteType() == JobExecuteType.ONE_TIME
                ? "AT " + START_TIME : "EVERY 1 HOUR STARTS " + START_TIME;
        for (List<String> row : List.of(tvf, show)) {
            Assertions.assertEquals(job.getJobName(), row.get(1));
            Assertions.assertEquals(job.getJobConfig().getExecuteType().name(), row.get(3));
            Assertions.assertEquals(strategy, row.get(4));
            Assertions.assertEquals(expected, row.get(6), "ExecuteSql must preserve the masked SQL text");
        }
        Assertions.assertEquals(executeSql, job.getExecuteSql());
        Assertions.assertEquals(persisted, GsonUtils.GSON.toJson(job));
    }

    private void assertMasked(String sql, String displayed) {
        Assertions.assertNotEquals("<redacted>", displayed);
        Assertions.assertFalse(displayed.contains("source-secret"));
        Assertions.assertFalse(displayed.contains("source-access"));
        Assertions.assertFalse(displayed.contains("target-secret"));
        Assertions.assertFalse(displayed.contains("target-access"));
        if (sql.contains("S3(")) {
            Assertions.assertTrue(displayed.contains("*XXX"));
            Assertions.assertTrue(displayed.contains("s3://bucket/"));
        } else {
            Assertions.assertEquals(sql, displayed);
        }
    }
}
