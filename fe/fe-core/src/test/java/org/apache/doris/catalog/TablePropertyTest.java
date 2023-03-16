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

package org.apache.doris.catalog;


import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class TablePropertyTest {

    @Test
    public void testNormal() throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        // 1. Write objects to file
        final Path path = Files.createTempFile("TablePropertyTest", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.START, "-3");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");
        properties.put("otherProperty", "unknownProperty");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.setReplicaAlloc(ReplicaAllocation.DEFAULT_ALLOCATION);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));
        TableProperty readTableProperty = TableProperty.read(in);
        DynamicPartitionProperty readDynamicPartitionProperty = readTableProperty.getDynamicPartitionProperty();
        DynamicPartitionProperty dynamicPartitionProperty = new DynamicPartitionProperty(properties);
        Assert.assertEquals(readTableProperty.getProperties(), properties);
        Assert.assertEquals(readDynamicPartitionProperty.getEnable(), dynamicPartitionProperty.getEnable());
        Assert.assertEquals(readDynamicPartitionProperty.getBuckets(), dynamicPartitionProperty.getBuckets());
        Assert.assertEquals(readDynamicPartitionProperty.getPrefix(), dynamicPartitionProperty.getPrefix());
        Assert.assertEquals(readDynamicPartitionProperty.getStart(), dynamicPartitionProperty.getStart());
        Assert.assertEquals(readDynamicPartitionProperty.getEnd(), dynamicPartitionProperty.getEnd());
        Assert.assertEquals(readDynamicPartitionProperty.getTimeUnit(), dynamicPartitionProperty.getTimeUnit());
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, readTableProperty.getReplicaAllocation());

        in.close();
        Files.delete(path);
    }
}
