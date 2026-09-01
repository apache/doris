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

package org.apache.doris.alter;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class CloudSchemaChangeJobV2Test {
    @Test
    public void testSchemaChangeJobDoesNotPersistFormatSpecificSchemaVersions() throws Exception {
        CloudSchemaChangeJobV2 job = new CloudSchemaChangeJobV2("", 1L, 2L, 3L, "tbl", 4L);
        job.addIndexSchema(101L, 100L, "__doris_shadow_tbl", 3, 4, (short) 1, null);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            job.write(output);
        }

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            CloudSchemaChangeJobV2 restored = (CloudSchemaChangeJobV2) AlterJobV2.read(input);
            Assert.assertEquals(Long.valueOf(100L), restored.getIndexIdMap().get(101L));
        }
    }
}
