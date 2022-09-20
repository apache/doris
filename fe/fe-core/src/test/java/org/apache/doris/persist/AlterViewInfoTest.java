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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AlterViewInfoTest {
    private static final String fileName = "./AlterViewInfoTest";

    private final long dbId = 10000L;
    private final long tableId = 30000L;
    private final String inlineViewDef = "Select a1, a2 From test_tbl Order By a1";
    private final long sqlMode = 0L;

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(fileName));
    }

    @Test
    public void testSerializeAlterViewInfo() throws IOException {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get(fileName));
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);

        AlterViewInfo alterViewInfo = new AlterViewInfo(dbId, tableId, inlineViewDef,
                Lists.newArrayList(column1, column2), sqlMode);
        alterViewInfo.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        AlterViewInfo readAlterViewInfo = AlterViewInfo.read(in);
        Assert.assertEquals(alterViewInfo, readAlterViewInfo);

        in.close();
    }


}
