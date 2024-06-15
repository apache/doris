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

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class BatchAlterJobPersistInfoTest {
    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("batchAlterJobPersistInfo", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        List<AlterJobV2> alterJobV2s = Lists.newArrayList();
        alterJobV2s.add(new RollupJobV2());
        BatchAlterJobPersistInfo info = new BatchAlterJobPersistInfo(alterJobV2s);

        info.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        BatchAlterJobPersistInfo info2 = BatchAlterJobPersistInfo.read(in);

        Assert.assertEquals(info.getAlterJobV2List().get(0).getType(), info2.getAlterJobV2List().get(0).getType());

        // 3. delete files
        in.close();
        Files.delete(path);
    }
}
