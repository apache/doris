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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class AlterJobCommandTest {
    private static final String JOB_NAME = "test_s3_express_streaming_job";
    private static final String REJECTION_MESSAGE =
            "S3 Express One Zone is not supported for S3 streaming jobs";

    @Test
    public void testAlterRejectExplicitS3ExpressStreamingSource() throws Exception {
        String originSql = explicitS3ExpressSql("origin_access_key");
        String alteredSql = explicitS3ExpressSql("altered_access_key");

        assertAlterRejected(originSql, alteredSql);
    }

    @Test
    public void testAlterRejectLegacyS3ExpressStreamingSource() throws Exception {
        String originSql = legacyS3ExpressSql("origin_access_key");
        String alteredSql = legacyS3ExpressSql("altered_access_key");

        assertAlterRejected(originSql, alteredSql);
    }

    private void assertAlterRejected(String originSql, String alteredSql) throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        JobManager<?, ?> jobManager = Mockito.mock(JobManager.class);
        StreamingInsertJob streamingJob = Mockito.mock(StreamingInsertJob.class);
        JobExecutionConfiguration jobConfig = new JobExecutionConfiguration();
        jobConfig.setExecuteType(JobExecuteType.STREAMING);

        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog("internal")).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn("internal");
        Mockito.when(env.getJobManager()).thenReturn(jobManager);
        Mockito.doReturn(streamingJob).when(jobManager).getJobByName(JOB_NAME);
        Mockito.when(streamingJob.getJobStatus()).thenReturn(JobStatus.PAUSED);
        Mockito.when(streamingJob.getJobConfig()).thenReturn(jobConfig);
        Mockito.when(streamingJob.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(streamingJob.getExecuteSql()).thenReturn(originSql);
        Mockito.when(streamingJob.getJobId()).thenReturn(1L);

        AlterJobCommand command = new AlterJobCommand(JOB_NAME, Collections.emptyMap(), alteredSql,
                null, null, Collections.emptyMap(), Collections.emptyMap());
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext context = new ConnectContext();
            context.setEnv(env);
            context.setThreadLocalInfo();
            Mockito.when(streamingJob.checkPrivilege(context)).thenReturn(true);
            try {
                JobException exception = Assertions.assertThrows(
                        JobException.class, () -> command.doRun(context, null));
                Assertions.assertTrue(exception.getMessage().contains(REJECTION_MESSAGE));
            } finally {
                ConnectContext.remove();
            }
        }

        Mockito.verify(jobManager, Mockito.never()).alterJob(command);
    }

    private String explicitS3ExpressSql(String accessKey) {
        return "INSERT INTO internal.test_db.test_table SELECT * FROM S3("
                + "\"uri\" = \"s3://bucket--usw2-az1--x-s3/path/*.csv\", "
                + "\"provider\" = \"S3EXPRESS\", "
                + "\"s3.region\" = \"us-west-2\", "
                + "\"s3.access_key\" = \"" + accessKey + "\", "
                + "\"s3.secret_key\" = \"secret_key\")";
    }

    private String legacyS3ExpressSql(String accessKey) {
        return "INSERT INTO internal.test_db.test_table SELECT * FROM S3("
                + "\"uri\" = \"s3://bucket--usw2-az1--x-s3/path/*.csv\", "
                + "\"provider\" = \"S3\", "
                + "\"s3.endpoint\" = \"s3express-usw2-az1.us-west-2.amazonaws.com\", "
                + "\"s3.region\" = \"us-west-2\", "
                + "\"s3.access_key\" = \"" + accessKey + "\", "
                + "\"s3.secret_key\" = \"secret_key\")";
    }
}
