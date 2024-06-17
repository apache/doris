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

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConsistencyCheckInfoTest {
    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("consistencyCheckInfo", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        ConsistencyCheckInfo consistencyCheckInfo1 = new ConsistencyCheckInfo(1L, 2L, 3L, 4L, 5L, 6L, 7L, true);

        consistencyCheckInfo1.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        ConsistencyCheckInfo consistencyCheckInfo2 = ConsistencyCheckInfo.read(in);

        Assert.assertEquals(consistencyCheckInfo1.getDbId(), consistencyCheckInfo2.getDbId());

        // 3. delete files
        in.close();
        Files.delete(path);
    }
}
