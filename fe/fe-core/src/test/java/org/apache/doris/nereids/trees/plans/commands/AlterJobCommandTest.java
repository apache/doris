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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collections;

public class AlterJobCommandTest extends TestWithFeService {
    private static final String SOURCE = "S3('uri'='s3://bucket/data.csv', 'format'='csv', "
            + "'csv_schema'='c1:int', 's3.endpoint'='s3.example.com', 's3.region'='us-east-1', "
            + "'s3.access_key'='test', 's3.secret_key'='test')";
    private static final String ORIGINAL = "INSERT INTO alter_job_priv.target SELECT c1 FROM " + SOURCE;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("alter_job_priv");
        createTable("CREATE TABLE target (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE private_source (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        String createUser = "CREATE USER 'alter_job_editor'";
        ((CreateUserCommand) new NereidsParser().parseSingle(createUser))
                .run(connectContext, new StmtExecutor(connectContext, createUser));
        grantPriv("GRANT LOAD_PRIV ON alter_job_priv.target TO 'alter_job_editor'");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT p.id FROM %s s CROSS JOIN alter_job_priv.private_source p",
            "SELECT (SELECT max(id) FROM alter_job_priv.private_source) FROM %s s",
            "SELECT c1 FROM %s UNION ALL SELECT id FROM alter_job_priv.private_source"
    })
    public void testAlterChecksSourcePrivilegesAsCaller(String query) throws Exception {
        Env env = Env.getCurrentEnv();
        JobManager<?, ?> originalManager = env.getJobManager();
        JobManager<?, ?> manager = Mockito.mock(JobManager.class);
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        config.setExecuteType(JobExecuteType.STREAMING);
        Mockito.when(job.getJobStatus()).thenReturn(JobStatus.PAUSED);
        Mockito.when(job.getJobConfig()).thenReturn(config);
        Mockito.when(job.getExecuteSql()).thenReturn(ORIGINAL);
        Mockito.when(job.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(job.checkPrivilege(connectContext)).thenReturn(true);
        Mockito.doReturn(job).when(manager).getJobByName("alter_priv");
        UserIdentity originalUser = connectContext.getCurrentUserIdentity();
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = true;
            Deencapsulation.setField(env, "jobManager", manager);
            connectContext.setCurrentUserIdentity(
                    UserIdentity.createAnalyzedUserIdentWithIp("alter_job_editor", "%"));
            connectContext.setThreadLocalInfo();
            String sql = "ALTER JOB alter_priv INSERT INTO alter_job_priv.target " + String.format(query, SOURCE);
            AlterJobCommand command = (AlterJobCommand) new NereidsParser().parseSingle(sql);
            Exception denied = Assertions.assertThrows(Exception.class,
                    () -> command.run(connectContext, new StmtExecutor(connectContext, sql)));
            Assertions.assertTrue(denied.getMessage().contains("denied"), denied.getMessage());
            Assertions.assertTrue(denied.getMessage().contains("private_source"), denied.getMessage());
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
            connectContext.setCurrentUserIdentity(originalUser);
            Deencapsulation.setField(env, "jobManager", originalManager);
        }
    }
}
