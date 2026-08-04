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
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class StreamingInsertJobLagTest {

    @Test
    public void testExplicitOffsetChangeKeepsLastObservedLagUntilNextFetch() throws Exception {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
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
        Deencapsulation.invoke(job, "modifyPropertiesInternal", alterProperties);

        Assert.assertEquals(4096, provider.getLagBytes());
    }
}
