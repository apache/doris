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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.offset.jdbc.JdbcOffset;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;
import org.apache.doris.job.offset.s3.S3EventSourceOffsetProvider;
import org.apache.doris.nereids.trees.plans.commands.AlterJobCommand;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamingInsertJobLagTest {

    @Test
    public void testMessageLagUsesOffsetProvider() {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Assert.assertEquals(-1L, job.getLagMessages());
        Deencapsulation.setField(job, "offsetProvider", new JdbcSourceOffsetProvider());
        Assert.assertEquals(-1L, job.getLagMessages());

        S3EventSourceOffsetProvider provider = new S3EventSourceOffsetProvider("test-queue");
        Deencapsulation.setField(job, "offsetProvider", provider);
        Assert.assertEquals(-1L, job.getLagMessages());
        Deencapsulation.setField(provider, "approximateNumberOfMessages", 12L);
        Assert.assertEquals(12L, job.getLagMessages());
    }

    @Test
    public void testLastSourceEventTimestampUsesOffsetProvider() {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Assert.assertEquals(0L, job.getLastSourceEventTimestampSeconds());

        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        Map<String, String> committedOffset = new HashMap<>();
        committedOffset.put("file", "mysql-bin.000001");
        committedOffset.put("pos", "100");
        committedOffset.put("ts_sec", "1787039821");
        provider.setCurrentOffset(new JdbcOffset(Collections.singletonList(new BinlogSplit(committedOffset))));
        Deencapsulation.setField(job, "offsetProvider", provider);

        Assert.assertEquals(1787039821L, job.getLastSourceEventTimestampSeconds());
        job.setLastTaskSuccessTime(1787039821123L);
        Assert.assertEquals(1787039821L, job.getLastTaskSuccessTimeSeconds());
    }

    @Test
    public void testExplicitOffsetChangeInvalidatesLastObservedLag() throws Exception {
        StreamingInsertJob job = Mockito.spy(new StreamingInsertJob());
        Mockito.doNothing().when(job).logUpdateOperation();
        job.setJobStatus(JobStatus.PAUSED);
        Map<String, String> properties = new HashMap<>();
        properties.put(StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY, "10");
        Deencapsulation.setField(job, "properties", properties);
        Deencapsulation.setField(job, "jobProperties", new StreamingJobProperties(properties));

        JobExecutionConfiguration configuration = new JobExecutionConfiguration();
        configuration.setTimerDefinition(new TimerDefinition());
        Deencapsulation.setField(job, "jobConfig", configuration);

        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setLagBytes(4096);
        Deencapsulation.setField(job, "offsetProvider", provider);

        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(StreamingJobProperties.OFFSET_PROPERTY, "{\"lsn\":\"200\"}");
        job.alterJob(new AlterJobCommand("test_job", alterProperties, null, null, null,
                Collections.emptyMap(), Collections.emptyMap()));

        Assert.assertEquals(-1, provider.getLagBytes());
    }
}
