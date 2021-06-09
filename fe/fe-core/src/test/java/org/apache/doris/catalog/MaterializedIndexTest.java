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

import mockit.Mocked;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.FeConstants;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaterializedIndexTest {

    private MaterializedIndex index;
    private long indexId;

    private List<Column> columns;
    @Mocked
    private Catalog catalog;

    private FakeCatalog fakeCatalog;

    @Before
    public void setUp() {
        indexId = 10000;

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        index = new MaterializedIndex(indexId, IndexState.NORMAL);

        fakeCatalog = new FakeCatalog();
        FakeCatalog.setCatalog(catalog);
        FakeCatalog.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(indexId, index.getId());
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./index");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        index.write(dos);

        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        MaterializedIndex rIndex = MaterializedIndex.read(dis);
        Assert.assertTrue(index.equals(rIndex));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
