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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AlterRoutineLoadOperationLogTest {
    private static String fileName = "./AlterRoutineLoadOperationLogTest";

    @Test
    public void testSerialzeAlterViewInfo() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        long jobId = 1000;
        String type = "KAFKA";
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("group.id", "mygroup");
        List<Pair<Integer, Long>> kafkaPartitioinOffset = Lists.newArrayList();
        kafkaPartitioinOffset.add(Pair.create(0, 10000L));
        kafkaPartitioinOffset.add(Pair.create(1, 20000L));

        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(jobId, type,
                jobProperties, customProperties, kafkaPartitioinOffset);
        log.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        AlterRoutineLoadJobOperationLog log2 = AlterRoutineLoadJobOperationLog.read(in);
        Assert.assertEquals(1, log2.getJobProperties().size());
        Assert.assertEquals("5", log2.getJobProperties().get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        Assert.assertEquals(1, log2.getCustomProperties().size());
        Assert.assertEquals("mygroup", log2.getCustomProperties().get("group.id"));
        Assert.assertEquals(kafkaPartitioinOffset.get(0), log2.getKafkaPartitioinOffset().get(0));
        Assert.assertEquals(kafkaPartitioinOffset.get(1), log2.getKafkaPartitioinOffset().get(1));

        in.close();
    }


}
