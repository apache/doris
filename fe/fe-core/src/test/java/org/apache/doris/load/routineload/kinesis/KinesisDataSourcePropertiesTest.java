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

package org.apache.doris.load.routineload.kinesis;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KinesisDataSourcePropertiesTest {

    @Test
    public void testConvertAndCheckDataSourcePropertiesWithAwsEndpoint() throws Exception {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), "us-east-1");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_STREAM.getName(), "test_stream");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_ENDPOINT.getName(), "http://localhost:4566");

        KinesisDataSourceProperties properties = new KinesisDataSourceProperties(dataSourceProperties);
        properties.convertAndCheckDataSourceProperties();

        Assert.assertEquals("http://localhost:4566", properties.getEndpoint());
    }

    @Test
    public void testConvertAndCheckDataSourcePropertiesWithLegacyEndpoint() throws Exception {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), "us-east-1");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_STREAM.getName(), "test_stream");
        dataSourceProperties.put("kinesis_endpoint", "http://localhost:4566");

        KinesisDataSourceProperties properties = new KinesisDataSourceProperties(dataSourceProperties);
        properties.convertAndCheckDataSourceProperties();

        Assert.assertEquals("http://localhost:4566", properties.getEndpoint());
    }

    @Test
    public void testPositionsShouldRejectDatetimeString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), "us-east-1");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_STREAM.getName(), "test_stream");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_SHARDS.getName(), "shard-000");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_POSITIONS.getName(), "2026-04-08 00:00:00");

        KinesisDataSourceProperties properties = new KinesisDataSourceProperties(dataSourceProperties);
        AnalysisException e = Assert.assertThrows(AnalysisException.class,
                properties::convertAndCheckDataSourceProperties);
        Assert.assertTrue(e.getMessage().contains("must be TRIM_HORIZON, LATEST, or a valid sequence number"));
    }

    @Test
    public void testDefaultPositionShouldRejectDatetimeString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), "us-east-1");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_STREAM.getName(), "test_stream");
        dataSourceProperties.put(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(),
                "2026-04-08 00:00:00");

        KinesisDataSourceProperties properties = new KinesisDataSourceProperties(dataSourceProperties);
        AnalysisException e = Assert.assertThrows(AnalysisException.class,
                properties::convertAndCheckDataSourceProperties);
        Assert.assertTrue(e.getMessage().contains("TRIM_HORIZON, LATEST, or a valid sequence number"));
    }
}
