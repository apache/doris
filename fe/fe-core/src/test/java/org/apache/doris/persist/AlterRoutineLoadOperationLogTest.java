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

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class AlterRoutineLoadOperationLogTest {
    private static String fileName = "./AlterRoutineLoadOperationLogTest";

    @Test
    public void testSerializeAlterRoutineLoadOperationLog() throws IOException, UserException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        long jobId = 1000;
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "0, 1");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000");
        dataSourceProperties.put("property.group.id", "mygroup");
        KafkaDataSourceProperties routineLoadDataSourceProperties = new KafkaDataSourceProperties(
                dataSourceProperties);
        routineLoadDataSourceProperties.setAlter(true);
        routineLoadDataSourceProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        routineLoadDataSourceProperties.analyze();

        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(jobId,
                jobProperties, routineLoadDataSourceProperties);
        log.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        AlterRoutineLoadJobOperationLog log2 = AlterRoutineLoadJobOperationLog.read(in);
        Assert.assertEquals(1, log2.getJobProperties().size());
        Assert.assertEquals("5", log2.getJobProperties().get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) log2.getDataSourceProperties();
        Assert.assertEquals(null, kafkaDataSourceProperties.getBrokerList());
        Assert.assertEquals(null, kafkaDataSourceProperties.getTopic());
        Assert.assertEquals(1, kafkaDataSourceProperties.getCustomKafkaProperties().size());
        Assert.assertEquals("mygroup", kafkaDataSourceProperties.getCustomKafkaProperties().get("group.id"));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(0),
                kafkaDataSourceProperties.getKafkaPartitionOffsets().get(0));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(1),
                kafkaDataSourceProperties.getKafkaPartitionOffsets().get(1));

        in.close();
    }


}
