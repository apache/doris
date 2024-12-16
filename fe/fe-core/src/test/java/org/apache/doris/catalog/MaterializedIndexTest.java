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

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class MaterializedIndexTest {

    private MaterializedIndex index;
    private long indexId;

    private List<Column> columns;
    @Mocked
    private Env env;

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        indexId = 10000;

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        index = new MaterializedIndex(indexId, IndexState.NORMAL);

        fakeEnv = new FakeEnv();
        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(indexId, index.getId());
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./index"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        Text.writeString(dos, GsonUtils.GSON.toJson(index));

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        MaterializedIndex rIndex = GsonUtils.GSON.fromJson(Text.readString(dis), MaterializedIndex.class);
        Assert.assertEquals(index, rIndex);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
