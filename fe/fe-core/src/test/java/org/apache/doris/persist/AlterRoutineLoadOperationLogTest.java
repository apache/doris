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

package org.apache.doris.persist;

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class AlterRoutineLoadOperationLogTest {
    private static final String A8928245_LEGACY_LOG =
            "/upgrade/routine-load/a8928245/alter-routine-load-log.b64";

    @Test
    public void testSerializeAlterRoutineLoadOperationLog() throws IOException, UserException {
        long jobId = 1000;
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "0, 1");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000");
        dataSourceProperties.put("property.group.id", "mygroup");
        KafkaDataSourceProperties routineLoadDataSourceProperties = new KafkaDataSourceProperties(
                dataSourceProperties);
        routineLoadDataSourceProperties.setAlter(true);
        routineLoadDataSourceProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        routineLoadDataSourceProperties.analyze();

        OriginStatement originStatement = new OriginStatement(
                "ALTER ROUTINE LOAD FOR job WHERE mapped_col > 10", 0);
        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(jobId,
                jobProperties, routineLoadDataSourceProperties, originStatement);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            log.write(out);
        }

        AlterRoutineLoadJobOperationLog log2;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            log2 = AlterRoutineLoadJobOperationLog.read(in);
        }
        Assert.assertEquals(1, log2.getJobProperties().size());
        Assert.assertEquals("5", log2.getJobProperties().get(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) log2.getDataSourceProperties();
        Assert.assertEquals(null, kafkaDataSourceProperties.getBrokerList());
        Assert.assertEquals(null, kafkaDataSourceProperties.getTopic());
        Assert.assertEquals(1, kafkaDataSourceProperties.getCustomKafkaProperties().size());
        Assert.assertEquals("mygroup", kafkaDataSourceProperties.getCustomKafkaProperties().get("group.id"));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(0),
                kafkaDataSourceProperties.getKafkaPartitionOffsets().get(0));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(1),
                kafkaDataSourceProperties.getKafkaPartitionOffsets().get(1));
        Assert.assertEquals(originStatement.originStmt, log2.getOriginStatement().originStmt);
        Assert.assertEquals(originStatement.idx, log2.getOriginStatement().idx);
    }

    @Test
    public void testDeserializeLegacyLogWithoutOriginStatement() throws IOException {
        byte[] bytes = loadBase64Fixture(A8928245_LEGACY_LOG);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            AlterRoutineLoadJobOperationLog log = AlterRoutineLoadJobOperationLog.read(in);
            Assert.assertEquals(7001L, log.getJobId());
            Assert.assertTrue(log.getJobProperties().isEmpty());
            Assert.assertNull(log.getDataSourceProperties());
            Assert.assertNull(log.getOriginStatement());
        }
    }

    private static byte[] loadBase64Fixture(String resource) throws IOException {
        try (InputStream in = AlterRoutineLoadOperationLogTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing fixture " + resource);
            }
            String base64 = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
            return Base64.getDecoder().decode(base64);
        }
    }

}
