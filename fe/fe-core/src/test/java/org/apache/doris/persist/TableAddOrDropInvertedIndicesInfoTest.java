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
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TableAddOrDropInvertedIndicesInfoTest {
    private static String fileName = "./TableAddOrDropInvertedIndicesInfoTest";

    @Test
    public void testSerialization() throws IOException {
        File file = new File(fileName);
        file.createNewFile();

        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        long dbId = 12345678;
        long tableId = 87654321;
        long jobId = 23456781;
        long createTimeMs = 1234L;
        long finishedTimeMs = 5678L;
        LinkedList<Column> fullSchema = new LinkedList<>();
        fullSchema.add(new Column("testCol1", ScalarType.createType(PrimitiveType.INT)));
        fullSchema.add(new Column("testCol2", ScalarType.createType(PrimitiveType.VARCHAR)));

        Map<Long, LinkedList<Column>> indexSchemaMap = new HashMap<>();
        indexSchemaMap.put(tableId, fullSchema);

        List<Index> indexes = Lists.newArrayList(
                new Index(0, "index", Lists.newArrayList("testCol1"), IndexType.INVERTED, null, "xxxxxx"));
        List<Index> alterIndexes = Lists.newArrayList(
                new Index(1, "index_1", Lists.newArrayList("testCol2"), IndexType.INVERTED, null, "yyyyyy"));

        TableAddOrDropInvertedIndicesInfo info1 = new TableAddOrDropInvertedIndicesInfo(
                "", dbId, tableId, indexSchemaMap, indexes, alterIndexes, false, jobId, createTimeMs, finishedTimeMs);

        Text.writeString(out, GsonUtils.GSON.toJson(info1));
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        String readJson = Text.readString(in);
        TableAddOrDropInvertedIndicesInfo info2 = GsonUtils.GSON.fromJson(readJson,
                TableAddOrDropInvertedIndicesInfo.class);

        Assert.assertEquals(info1.getDbId(), info2.getDbId());
        Assert.assertEquals(info1.getTableId(), info2.getTableId());
        Assert.assertEquals(info1.getIndexSchemaMap(), info2.getIndexSchemaMap());
        Assert.assertEquals(info1.getCreateTimeMs(), info2.getCreateTimeMs());
        Assert.assertEquals(info1.getFinishedTimeMs(), info2.getFinishedTimeMs());
    }

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }
}
