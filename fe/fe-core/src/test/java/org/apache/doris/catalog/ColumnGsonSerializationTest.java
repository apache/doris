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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ColumnGsonSerializationTest {

    private static String fileName = "./ColumnGsonSerializationTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    public static class ColumnList implements Writable {
        @SerializedName(value = "columns")
        public List<Column> columns = Lists.newArrayList();

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static ColumnList read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, ColumnList.class);
        }
    }

    @Test
    public void testSerializeColumn() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        Column c1 = new Column("c1", Type.fromPrimitiveType(PrimitiveType.BIGINT), true, null, true, "1", "abc");

        String c1Json = GsonUtils.GSON.toJson(c1);
        Text.writeString(out, c1Json);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        String readJson = Text.readString(in);
        Column readC1 = GsonUtils.GSON.fromJson(readJson, Column.class);

        Assert.assertEquals(c1, readC1);
    }
    
    @Test
    public void testSerializeColumnList() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        Column c1 = new Column("c1", Type.fromPrimitiveType(PrimitiveType.BIGINT), true, null, true, "1", "abc");
        Column c2 = new Column("c2", ScalarType.createType(PrimitiveType.VARCHAR, 32, -1, -1), true, null, true, "cmy", "");
        Column c3 = new Column("c3", ScalarType.createDecimalV2Type(27, 9), false, AggregateType.SUM, false, "1.1", "decimalv2");

        ColumnList columnList = new ColumnList();
        columnList.columns.add(c1);
        columnList.columns.add(c2);
        columnList.columns.add(c3);

        columnList.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        ColumnList readList = ColumnList.read(in);
        List<Column> columns = readList.columns;

        Assert.assertEquals(3, columns.size());
        Assert.assertEquals(c1, columns.get(0));
        Assert.assertEquals(c2, columns.get(1));
        Assert.assertEquals(c3, columns.get(2));
    }

}
