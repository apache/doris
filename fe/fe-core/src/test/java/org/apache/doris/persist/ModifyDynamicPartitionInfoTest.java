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

import org.apache.doris.catalog.DynamicPartitionProperty;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class ModifyDynamicPartitionInfoTest {
    private String fileName = "./ModifyTablePropertyOperationLogTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.START, "-3");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");
        ModifyTablePropertyOperationLog modifyDynamicPartitionInfo = new ModifyTablePropertyOperationLog(100L, 200L, "test", properties);
        modifyDynamicPartitionInfo.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        ModifyTablePropertyOperationLog readModifyDynamicPartitionInfo = ModifyTablePropertyOperationLog.read(in);
        Assert.assertEquals(readModifyDynamicPartitionInfo.getDbId(), 100L);
        Assert.assertEquals(readModifyDynamicPartitionInfo.getTableId(), 200L);
        Assert.assertEquals(readModifyDynamicPartitionInfo.getProperties(), properties);
        in.close();
    }

    @Test
    public void testToSql() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.START, "-3");
        ModifyTablePropertyOperationLog modifyDynamicPartitionInfo = new ModifyTablePropertyOperationLog(100L, 200L,
                "test", properties);
        Assert.assertTrue(modifyDynamicPartitionInfo.toSql().contains("\"dynamic_partition.enable\" = \"true\""));
        Assert.assertTrue(modifyDynamicPartitionInfo.toSql().contains("\"dynamic_partition.time_unit\" = \"day\""));
        Assert.assertTrue(modifyDynamicPartitionInfo.toSql().contains("\"dynamic_partition.start\" = \"-3\""));
    }
}
